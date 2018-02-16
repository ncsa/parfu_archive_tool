////////////////////////////////////////////////////////////////////////////////
// 
//  University of Illinois/NCSA Open Source License
//  http://otm.illinois.edu/disclose-protect/illinois-open-source-license
//  
//  Parfu is copyright © 2017, The Trustees of the University of Illinois. 
//  All rights reserved.
//  
//  Parfu was developed by:
//  The University of Illinois
//  The National Center For Supercomputing Applications (NCSA)
//  Blue Waters Science and Engineering Applications Support Team (SEAS)
//  Craig P Steffen <csteffen@ncsa.illinois.edu>
//  
//  https://github.com/ncsa/parfu_archive_tool
//  http://www.ncsa.illinois.edu/People/csteffen/parfu/
//  
//  For full licnse text see the LICENSE file provided with the source
//  distribution.
//  
////////////////////////////////////////////////////////////////////////////////

#include "parfu_primary.h"
#include "tarentry.hh"

// this is an MPI function; expected to be run by all ranks.
// we assume that each rank can allocate, fill, and use a buffer
// the size of a whole bucket.

// This function takes the original (unsplit) list as input.  It's assumed
// to be sorted and combined in the right way; the files are ordered as they
// are supposed to be.  
// This function splits the list along the way, then broadcasts the split
// lists to the ranks for transferring.

// blocking_size is the size that all files are blocked up to by reserving
// room.  This will mostly be the same as the blocking size for tar files, 
// which as of this writing we generally assume is 512 bytes.  Since this
// size is tied with interoperability with tar, this will generally be
// fixed at compile-time.

// Bucket size is how much data each rank of parfu operates on.  Files are
// blocked together to fill up that size.  If files are larger than that, 
// they're split across multiple buckets and data is moved by multiple 
// ranks.  

// max_files_per_bucket is a limit to the number of individual files in 
// a bucket.  If files are super-small, the ranks that read the individual
// files from disk and push that data into the archive file may end up 
// incurring unreasonable amount of latency to do so.  This limit can be 
// set to limit this effect; the downside is that more space is 
// wasted in the archive file.

extern "C" 
int parfu_wtar_archive_list_to_singeFP(parfu_file_fragment_entry_list_t *myl,
				       int n_ranks, int my_rank,
				       long int blocking_size,
				       long int bucket_size,
				       int max_files_per_bucket,
				       char *archive_file_name){
  int debug=0;
  
  MPI_File *archive_file_ptr=NULL;
  MPI_Info my_Info=MPI_INFO_NULL;
  long int data_offset;
  
  int number_of_rank_buckets;

  char *padding_filename=NULL;
  char *arch_file_catalog_buffer=NULL;
  long int arch_file_catalog_buffer_length;
  long int arch_file_catalog_buffer_length_w_tar_header;
  char *split_list_catalog_buffer=NULL;
  long int split_list_catalog_buffer_length=0;
  char *shared_split_list_catalog_buffer=NULL;

  char *catalog_tar_header_buffer=NULL;
  
  parfu_file_fragment_entry_list_t *my_split_list=NULL;
  parfu_file_fragment_entry_list_t *shared_split_list=NULL;

  char *shared_archive_file_name=NULL;
  int archive_filename_length;

  int file_result;

  int catalog_takes_n_buckets;
  int catalog_tar_entry_size;
  int return_bucket_size;

  int *rank_call_list=NULL;
  int *shared_rank_call_list=NULL;
  int rank_call_list_length;

  MPI_Status my_MPI_Status;

  //  int i;

  if(debug)
    fprintf(stderr," *** data move 00\n");

  if((padding_filename=(char*)malloc(sizeof(char)*(strlen(PARFU_BLOCKING_FILE_NAME)+1)))
     ==NULL){
    fprintf(stderr,"parfu_wtar_archive_list_to_singeFP:\n");
    fprintf(stderr,"could not allocate space for pad file name.\n");
    return 101;
  }
  sprintf(padding_filename,"%s",PARFU_BLOCKING_FILE_NAME);

  // extract the archive (in file) catalog
  // this will be written to the archive file on disk
  if(my_rank==0){
    if((arch_file_catalog_buffer=
	parfu_fragment_list_to_buffer(myl,&arch_file_catalog_buffer_length,
				      bucket_size,1))
       ==NULL){
      fprintf(stderr,"parfu_wtar_archive_list_to_singeFP:\n");
      fprintf(stderr,"  could not write catalog to buffer\n");
      return 103;
    }
    if(archive_file_name==NULL){
      fprintf(stderr,"parfu_wtar_archive_list_to_singeFP:\n");
      fprintf(stderr," rank 0 archive_file_name == NULL!!\n");
      return 104;
    }
    if((archive_filename_length=strlen(archive_file_name))<1){
      fprintf(stderr,"parfu_wtar_archive_list_to_singeFP:\n");
      fprintf(stderr," archive filename length = %d!!\n",archive_filename_length);
      return 105;
    }
  }
  if(debug)
    fprintf(stderr," *** data move 01\n");
  MPI_Bcast(&archive_filename_length,1,MPI_INT,0,MPI_COMM_WORLD);

  /////////
  //  OOPS
  //  if((shared_archive_file_name=(char*)malloc(archive_filename_length)+1)==NULL){
  //
  /////////

  if((shared_archive_file_name=(char*)malloc(archive_filename_length+1))==NULL){
    fprintf(stderr,"parfu_wtar_archive_list_to_singeFP:\n");
    fprintf(stderr," could not archive shared buffer for archive name!!\n");
    return 106;
  }
  if(my_rank==0){
    sprintf(shared_archive_file_name,"%s",archive_file_name);
    fprintf(stderr," arch filename debug: >%s<  >%s<\n",
	    archive_file_name,shared_archive_file_name); 
  }
  MPI_Bcast(shared_archive_file_name,archive_filename_length+1,MPI_CHAR,0,MPI_COMM_WORLD);
  // open the archive file on all ranks with a shared pointer
  if(debug)
    fprintf(stderr," *** data move 02\n");
  if((archive_file_ptr=(MPI_File*)malloc(sizeof(MPI_File)))==NULL){
    fprintf(stderr,"parfu_wtar_archive_list_to_singeFP:\n");
    fprintf(stderr,"rank %d could not allocate space for archive file pointer!\n",my_rank);
    MPI_Finalize();
    return 75;
  }
  if(my_rank==0)
    fprintf(stderr,"about to open archive file on all ranks.\n");
  if(debug)
    fprintf(stderr," *** data move 03\n");
  file_result=MPI_File_open(MPI_COMM_WORLD, shared_archive_file_name, 
			    MPI_MODE_WRONLY | MPI_MODE_CREATE , 
			    my_Info, archive_file_ptr);
  if(file_result != MPI_SUCCESS){
    fprintf(stderr,"parfu_wtar_archive_list_to_singeFP:\n");
    fprintf(stderr,"MPI_File_open for archive buffer:  returned error!  error=%d  Rank %d file >%s<\n",
	    file_result,
	    my_rank,
	    archive_file_name);
    return 3; 
  }
  if(debug)
    fprintf(stderr," *** data move 04, file open on all ranks.\n");
  // the shared archive file is open, rank 0 writes the catalog to it
  if(my_rank==0){
    catalog_tar_entry_size = 
      tarentry::compute_hdr_size(PARFU_CATALOG_FILE_NAME,"",arch_file_catalog_buffer_length);
    if((catalog_tar_header_buffer=(char*)malloc(catalog_tar_entry_size))==NULL){
      fprintf(stderr,"parfu_wtar_archive_list_to_singeFP:\n");
      fprintf(stderr,"  couldn not allocate catalog_tar_header_buffer!\n");
      return 9;
    }
    parfu_construct_tar_header_for_catalog(catalog_tar_header_buffer,
					   arch_file_catalog_buffer_length);
    file_result=MPI_File_write_at(*archive_file_ptr,
				  0, // tar header for catalog goes at beginning of file
				  catalog_tar_header_buffer,
				  catalog_tar_entry_size,
				  MPI_CHAR,&my_MPI_Status);
    if(file_result != MPI_SUCCESS){
      fprintf(stderr,"parfu_wtar_archive_list_to_singeFP:\n");      
      fprintf(stderr,"rank 0 got %d from MPI_File_write_at_all\n",file_result);
      fprintf(stderr," trying to write catalog tar header at location zero\n");
      MPI_Finalize();
      return 169;
    }
    
					   

    fprintf(stderr,"rank 0 about to write data catalog to archive file.\n");
    file_result=MPI_File_write_at(*archive_file_ptr,
				  catalog_tar_entry_size, // write catalog just AFTER its tar header
				  arch_file_catalog_buffer,
				  arch_file_catalog_buffer_length,
				  MPI_CHAR,&my_MPI_Status);
    if(file_result != MPI_SUCCESS){
      fprintf(stderr,"parfu_wtar_archive_list_to_singeFP:\n");      
      fprintf(stderr,"rank 0 got %d from MPI_File_write_at_all\n",file_result);
      fprintf(stderr," trying to write catalog of size %ld at location %d\n",
	      arch_file_catalog_buffer_length,catalog_tar_entry_size);
      MPI_Finalize();
      return 160;
    }
    // create tar header for parfu catalog
  }  
  
  // calculate and distribute data offset
  if(my_rank==0){
    int loc_of_end_of_catalog;
    int space_between_catalog_and_data;
    int loc_of_tar_header_for_spacing;
    int spacer_file_size;
    arch_file_catalog_buffer_length_w_tar_header = 
      arch_file_catalog_buffer_length + catalog_tar_entry_size;
    catalog_takes_n_buckets = arch_file_catalog_buffer_length_w_tar_header / bucket_size;
    if( arch_file_catalog_buffer_length_w_tar_header % bucket_size )
      catalog_takes_n_buckets++;
    data_offset = catalog_takes_n_buckets * bucket_size;
    
    loc_of_end_of_catalog 
      = catalog_tar_entry_size + arch_file_catalog_buffer_length;
    space_between_catalog_and_data = data_offset - loc_of_end_of_catalog;
    if(space_between_catalog_and_data > blocking_size){
      // then we need to put in a tar header to fill in the gap between
      // the end of the catalog and the beginning of the main data block
      loc_of_tar_header_for_spacing = loc_of_end_of_catalog;
      if(loc_of_end_of_catalog % blocking_size){
	// if the end of the catalog isn't on a block boundary, push it
	// exactly to the next one
	loc_of_tar_header_for_spacing += 
	  (blocking_size - (loc_of_end_of_catalog % blocking_size));
      }
      spacer_file_size = 
	(data_offset - loc_of_tar_header_for_spacing) 
	- catalog_tar_entry_size;
      
      parfu_construct_tar_header_for_space(catalog_tar_header_buffer,
					   spacer_file_size);
      file_result=MPI_File_write_at(*archive_file_ptr,
				    loc_of_tar_header_for_spacing,
				    catalog_tar_header_buffer,
				    catalog_tar_entry_size,
				    MPI_CHAR,&my_MPI_Status);
      if(file_result != MPI_SUCCESS){
	fprintf(stderr,"parfu_wtar_archive_list_to_singeFP:\n");      
	fprintf(stderr,"rank 0 got %d from MPI_File_write_at_all\n",file_result);
	fprintf(stderr," trying to write tar_header for post-catalog file.\n");
	MPI_Finalize();
	return 169;
      }
    } // if(space_between_catalog_and_data...
  } // if(my_rank==0....
  MPI_Bcast(&data_offset,1,MPI_LONG_INT,0,MPI_COMM_WORLD);
  if(debug)
    fprintf(stderr," *** data move 05\n");
  
  
  // split file list and distribute results to all ranks
  if(my_rank==0){
    if((my_split_list=parfu_set_offsets_and_split_ffel(myl,blocking_size,
						       bucket_size,
						       padding_filename,
						       &number_of_rank_buckets,
						       -1 // indicating no max files per bucket
						       ))
       ==NULL){
      fprintf(stderr,"parfu_wtar_archive_list_to_singeFP:\n");
      fprintf(stderr," error from parfu_set_offsets_and_split_ffel() !!!\n");
      return 102;
    }
    if((split_list_catalog_buffer=
	parfu_fragment_list_to_buffer(my_split_list,&split_list_catalog_buffer_length,
				      bucket_size,0))
       ==NULL){
      fprintf(stderr,"parfu_wtar_archive_list_to_singeFP:\n");
      fprintf(stderr,"  could not write catalog to buffer\n");
      return 103;
    }
    if((rank_call_list=
	parfu_rank_call_list_from_ffel(my_split_list,&rank_call_list_length))
       ==NULL){
      fprintf(stderr,"parfu_wtar_archive_list_to_singeFP:\n");
      fprintf(stderr,"  error from parfu_rank_call_list_from_ffel!!\n");
      return 104;
    }
  }
  MPI_Bcast(&split_list_catalog_buffer_length,1,MPI_LONG_INT,0,MPI_COMM_WORLD);
  if(debug)
    fprintf(stderr," *** data move 06\n");
  
  if((shared_split_list_catalog_buffer=(char*)malloc(split_list_catalog_buffer_length))==NULL){
    fprintf(stderr,"parfu_wtar_archive_list_to_singeFP:\n");
    fprintf(stderr," could not allocate split_list buffer!  rank %d\n",my_rank);
    return 76;
  }
  if(my_rank==0)
    memcpy(shared_split_list_catalog_buffer,split_list_catalog_buffer,split_list_catalog_buffer_length);
  MPI_Bcast(shared_split_list_catalog_buffer,split_list_catalog_buffer_length,MPI_CHAR,0,MPI_COMM_WORLD);
  if(debug)
    fprintf(stderr," *** data move 07\n");
  if((shared_split_list=
      parfu_buffer_to_file_fragment_list(shared_split_list_catalog_buffer,
					 &return_bucket_size,0))==NULL){
    fprintf(stderr,"parfu_wtar_archive_list_to_singeFP:\n");
    fprintf(stderr,"rank %d could not create split list!!\n",my_rank);
    return 78;
  }
  if(debug)
    fprintf(stderr," *** data move 08\n");
  
  
  MPI_Bcast(&rank_call_list_length,1,MPI_INT,0,MPI_COMM_WORLD);
  if((shared_rank_call_list=(int*)malloc(sizeof(int)*rank_call_list_length))
     ==NULL){
    fprintf(stderr,"parfu_wtar_archive_list_to_singeFP:\n");
    fprintf(stderr,"  rank %d can't allocate shared_rank_call_list!!\n",my_rank);
    return 79;
  }
  if(my_rank==0)
    memcpy(shared_rank_call_list,rank_call_list,rank_call_list_length*sizeof(int));
  MPI_Bcast(shared_rank_call_list,rank_call_list_length,MPI_INT,0,MPI_COMM_WORLD);
  //  fprintf(stderr," *** data move 09  rcll=%d rank=%d\n",rank_call_list_length,my_rank);
  // all ranks have their own list, and valid file pointers, and the call list
  
  if(my_rank==0){
    fprintf(stderr,"rank 0 debugging\n");
    fprintf(stderr," original list length: %d\n",myl->n_entries_full);
    fprintf(stderr," split list length: %d\n",shared_split_list->n_entries_full);
    fprintf(stderr,"dumping rank call list: endpoint=%d\n",
	    shared_split_list->n_entries_full);
    //    for(i=0;i<rank_call_list_length;i++){
    //      fprintf(stderr," i=%05d  bucket_beginning_index=%05d\n",
    //	      i,rank_call_list[i]);
    //    }
  }
						      
  if((file_result=parfu_wtar_archive_allbuckets_singFP(shared_split_list,
						       n_ranks,my_rank,
						       shared_rank_call_list,
						       rank_call_list_length,
						       blocking_size,
						       bucket_size,
						       data_offset,
						       archive_file_ptr))!=0){
    fprintf(stderr,"parfu_wtar_archive_list_to_singeFP:\n");
    fprintf(stderr,"  rank %d got error %d from parfu_wtar_archive_allbuckets_singFP()!\n",
	    my_rank,file_result);
    return 88;
  }
  return 0;
}

extern "C" 
int parfu_wtar_archive_allbuckets_singFP(parfu_file_fragment_entry_list_t *myl,
					 int n_ranks, int my_rank,
					 int *rank_call_list,
					 int rank_call_list_length,
					 long int blocking_size,
					 long int bucket_size,
					 long int data_region_start,
					 MPI_File *archive_file_ptr){
  //  MPI_Info my_Info=MPI_INFO_NULL;
  int return_val;

  //  int file_result;
  
  int rank_iter;
  
  void *transfer_buffer=NULL;

  if(archive_file_ptr==NULL){
    fprintf(stderr,"parfu_wtar_archive_allbuckets_singFP:\n");
    fprintf(stderr,"  help!  archive_file_ptr is NULL!!\n");
    return 2;
  }
  
  if((transfer_buffer=malloc(bucket_size))==NULL){
    fprintf(stderr,"parfu_wtar_archive_allbuckets_singFP:\n");
    fprintf(stderr,"  could not allocate transfer buffer!\n");
    return 3;
  }
  memset(transfer_buffer,'\0',bucket_size);

  if(rank_call_list==NULL){
    fprintf(stderr,"parfu_wtar_archive_allbuckets_singFP:\n");
    fprintf(stderr,"  We received a rank_call_list that was NULL!!\n");
    return 7;
  }

  /*
  if((archive_file_ptr=(MPI_File*)malloc(sizeof(MPI_File)))==NULL){
    fprintf(stderr,"parfu_wtar_archive_allbuckets_singFP:\n");
    fprintf(stderr,"rank %d could not allocate space for archive file pointer!\n",my_rank);
    MPI_Finalize();
    return 75;
  }
  file_result=MPI_File_open(MPI_COMM_WORLD, archive_file_name, 
			    MPI_MODE_WRONLY | MPI_MODE_CREATE | MPI_MODE_EXCL, 
			    my_Info, archive_file_ptr);
  if(file_result != MPI_SUCCESS){
    fprintf(stderr,"parfu_wtar_archive_allbuckets_singFP:\n");
    fprintf(stderr,"parfu_archive_1file_singFP:\n");
    fprintf(stderr,"MPI_File_open for archive buffer:  returned error!  Rank %d file >%s<\n",
	    my_rank,
	    archive_file_name);
    return 3; 
  }
  */

  rank_iter = my_rank;

  //  fprintf(stderr,"  @@@ rank %5d about to enter loop of _one_bucket calls\n",
  //	  my_rank);
  while(rank_iter < rank_call_list_length){
    if((return_val=parfu_wtar_archive_one_bucket_singFP(myl,n_ranks,my_rank,
						       rank_call_list,
						       rank_iter,
						       blocking_size,bucket_size,
						       transfer_buffer,
						       data_region_start,
							archive_file_ptr))){
      fprintf(stderr,"parfu_wtar_archive_allbuckets_singFP:\n");
      fprintf(stderr,"rank_iter=%d\n",rank_iter);
      fprintf(stderr," received value %d from parfu_wtar_archive_one_bucket_singFP.  Help!\n",
	      return_val);
      return 12;
    }
    rank_iter += n_ranks;
  } // while(rank_iter < ...
  fprintf(stderr," &&& rank %d exiting all buckets loop at rank iter %d\n",
	  my_rank,rank_iter);
  return 0;
}

extern "C"
void parfu_print_MPI_error(FILE *target,
			   int error_number){
  char MPI_error_msg[MPI_MAX_ERROR_STRING];
  int return_length;
  MPI_Error_string(error_number, MPI_error_msg, &return_length);
  fprintf(target,"MPI error string: >%s<\n",MPI_error_msg);
  return; 
}

extern "C"
int parfu_wtar_archive_one_bucket_singFP(parfu_file_fragment_entry_list_t *myl,
					 int n_ranks, int my_rank,
					 int *rank_call_list,
					 int rank_call_list_index,
					 long int blocking_size,
					 long int bucket_size,
					 void *transfer_buffer,
					 long int data_region_start,
					 MPI_File *archive_file_ptr){
  int current_entry;
  int last_bucket_index;
  
  //  long int source_file_loc;
  long int archive_file_loc;
  long int current_bucket_loc;
  long int current_write_loc_in_bucket;
  long int blocked_data_written;
  //  long int total_blocked_data_written;
  long int data_offset_in_buffer;

  //  long int pad_file_location_in_bucket;
  
  MPI_File *target_file_ptr_ptr=NULL;
  MPI_Info my_Info=MPI_INFO_NULL;
  MPI_Status my_MPI_Status;

  std::vector<char> temp_file_header_C;
  tarentry my_tar_header_C;
  int file_result;
  int local_pad_file_size;
  int pad_space;
  
  //  int remainder;

  int i;
  int terminator_length=1024;


  if(bucket_size < 10000){
    fprintf(stderr,"parfu_wtar_archive_one_bucket_singFP:\n");
    fprintf(stderr,"  Input bucket_size is: %ld !!!\n",bucket_size);
    fprintf(stderr,"  this is very unlikely to be right!\n");
    return 100;
  }
  if(transfer_buffer == NULL){
    fprintf(stderr,"parfu_wtar_archive_one_bucket_singFP:\n");
    fprintf(stderr,"  transfer buffer is NULL!\n");
    return 101;
  }
  if((target_file_ptr_ptr=(MPI_File*)malloc(sizeof(MPI_File)))==NULL){
    fprintf(stderr,"parfu_wtar_archive_one_bucket_singFP:\n");
    fprintf(stderr,"could not allocate target_file_ptr_ptr!!\n");
    return 102;
  }
  current_entry=rank_call_list[rank_call_list_index];
  last_bucket_index = myl->list[current_entry].rank_bucket_index;
  
  // copy data from file(s) into buffer
  // walk through entries until we hit the next bucket

  current_bucket_loc = 
    bucket_size * rank_call_list_index;
  current_write_loc_in_bucket = 0L;
  //  total_blocked_data_written=0L;
  //  fprintf(stderr," ^^^ rank %d beginning single bucket while loop\n",my_rank);
  //  fprintf(stderr,"CCC_1 rank=%5d  index=%5d\n",my_rank,rank_call_list_index);
  while( current_entry < myl->n_entries_full &&
	 myl->list[current_entry].rank_bucket_index == last_bucket_index ){
    
    // open target file
    //    fprintf(stderr," @@@  rank %d opening file > %s <\n",
    //	    my_rank,
    //	    myl->list[current_entry].relative_filename);

    // we only need to open and read from files that are 
    // actual files and have non-zero size
    
    if(  ( myl->list[current_entry].type == PARFU_FILE_TYPE_REGULAR ) && 
	 ( myl->list[current_entry].our_file_size > 0 ) ){
      file_result=MPI_File_open(MPI_COMM_SELF,
				myl->list[current_entry].relative_filename,
				MPI_MODE_RDONLY,
				my_Info,target_file_ptr_ptr);
      if(file_result != MPI_SUCCESS){
	fprintf(stderr,"parfu_wtar_archive_one_bucket_singFP:\n");
	fprintf(stderr,"rank %d got non-zero result when opening target file >%s<!\n",
		my_rank,
		myl->list[current_entry].relative_filename);
	fprintf(stderr,"for reading.  result=%d\n",file_result);
	return 138;
      }
      //      fprintf(stderr,"   ??? rank %5d has opened 
      
      
      // copy data from target file to buffer
      //      file_result=
      //	MPI_File_read_at(target_file_ptr,
      //		 myl->list[current_entry].location_in_orig_file,
      //		 ((void*)(((char*)(transfer_buffer)+current_write_loc_in_bucket))),
      //		 myl->list[current_entry].our_file_size,
      //		 MPI_CHAR,&my_MPI_Status);
      data_offset_in_buffer=
	myl->list[current_entry].location_in_archive_file-
	current_bucket_loc;
      if(data_offset_in_buffer < 0 ||
	 data_offset_in_buffer >= bucket_size){
	fprintf(stderr,"parfu_wtar_archive_one_bucket_singFP:\n");		
	fprintf(stderr," rank %d has data_offset_in_buffer=%ld which is outside buffer!\n",
		my_rank,data_offset_in_buffer);
	fprintf(stderr," rank %d bucketsz=%ld datasz=%ld filename=>%s<\n",my_rank, bucket_size,
		myl->list[current_entry].our_file_size,
		myl->list[current_entry].relative_filename);
	return 149;
      }
      file_result=
	MPI_File_read_at(*target_file_ptr_ptr,
			 myl->list[current_entry].location_in_orig_file,
			 (void*)(((char*)(transfer_buffer)+
				  (myl->list[current_entry].location_in_archive_file-
				   current_bucket_loc))),
			 myl->list[current_entry].our_file_size,
			 MPI_CHAR,&my_MPI_Status);
      if(file_result != MPI_SUCCESS){
	fprintf(stderr,"parfu_wtar_archive_one_bucket_singFP:\n");	
	fprintf(stderr,"rank %d got %d from MPI_File_read_at\n",my_rank,file_result);	
	fprintf(stderr,"reading fragment %d",rank_call_list_index);
	fprintf(stderr,"file %s\n",myl->list[current_entry].relative_filename);
	parfu_print_MPI_error(stderr,file_result);
	fprintf(stderr,"YY:r=%d,OF=%ld,SZ=%ld,FN=>%s<\n",
		my_rank,myl->list[current_entry].location_in_orig_file,myl->list[current_entry].our_file_size,
		myl->list[current_entry].relative_filename);
	return 159;
      }
      
    } // if it's a real file with data
    
      // create the tar header for this file, if it's the first chunk of the file
    if(!(myl->list[current_entry].location_in_orig_file)){
      my_tar_header_C = tarentry(myl->list[current_entry].relative_filename,0);
      temp_file_header_C = my_tar_header_C.make_tar_header();
      std::copy(temp_file_header_C.begin(), temp_file_header_C.end(),
		((((char*)(transfer_buffer))+
		  ((myl->list[current_entry].location_in_archive_file - 
		    current_bucket_loc)
		   - myl->list[current_entry].our_tar_header_size)
		  ))); 
    }
    
    blocked_data_written = 
      (myl->list[current_entry].our_file_size >= 0 ? myl->list[current_entry].our_file_size : 0);
    if(!(myl->list[current_entry].location_in_orig_file)){
      blocked_data_written += myl->list[current_entry].our_tar_header_size;
    }
    if( blocked_data_written % blocking_size ){
      blocked_data_written = 
	( (blocked_data_written / blocking_size) + 1) * blocking_size;
    }
    current_write_loc_in_bucket += blocked_data_written;

    // 
    current_entry++;
    MPI_File_close(target_file_ptr_ptr);
  } // while(myl->list[current_entry].rank_bucket_index == ....
  free(target_file_ptr_ptr);
  target_file_ptr_ptr=NULL;

  // now check to see if we need to add a tar header at the end to pad space 
  // from the end of our data to the beginning of the next bucket

  pad_space = bucket_size-current_write_loc_in_bucket;
  if( (pad_space - blocking_size) >= 0 ){
    //write a tar header to bridge the gap to the next bucket
    local_pad_file_size = pad_space - blocking_size;
    parfu_construct_tar_header_for_space( ((char*)(transfer_buffer))+
					  current_write_loc_in_bucket,
					  local_pad_file_size);
    /*    my_tar_header_C = tarentry(".parfu_spacer",0);
	  temp_file_header_C = my_tar_header_C.make_tar_header();
	  std::copy(temp_file_header_C.begin(), temp_file_header_C.end(),
	  ((((char*)(transfer_buffer))+
	  current_write_loc_in_bucket)));*/
    // we're assuming here the tar header is the blocking size
    // not guaranteed but in practice is always true.
    current_write_loc_in_bucket += blocking_size;
  }
  
  /*
  if( current_entry < myl->n_entries_full ){
    // if we're not at the end of the list

    // we look at first entry of the NEXT rank to see if it needs a pad
    // in our section.  If it has a name, then there's an entry.
    if(myl->list[current_entry].pad_file_archive_filename != NULL){


      //      pad_file_location_in_bucket = 
      //	myl->list[current_entry].pad_file_location_in_archive_file - 
      //	current_bucket_loc;
      //      if(pad_file_location_in_bucket < 0 || 
      //	 pad_file_location_in_bucket >= bucket_size){
      //	fprintf(stderr,"  HELP HELP!!   spaceer tar headre outside buffer!!! rank=%5d ; %ld\n",
      //		my_rank,pad_file_location_in_bucket);
      //      }
      //      parfu_construct_tar_header_for_space( ((char*)(transfer_buffer))+
      //					    pad_file_location_in_bucket,
      //
      //myl->list[current_entry].pad_file_size);     
      if(
      
      parfu_construct_tar_header_for_space( ((char*)(transfer_buffer))+
					    current_write_loc_in_bucket,
					    myl->list[current_entry].pad_file_size);
      

    }
    } */
  
  

  //  fprintf(stderr," ### rank %d copied data to buffer.  Now moving to archive file.\n",my_rank);
  

  //  fprintf(stderr,"CCC_2 rank=%5d  index=%5d\n",my_rank,rank_call_list_index);

  // now copy the whole buffer to the archive file
  archive_file_loc = data_region_start +
    (rank_call_list_index * bucket_size);
  /*  file_result=MPI_File_write_at_all(*archive_file_ptr,
				    archive_file_loc,
				    transfer_buffer,
				    current_write_loc_in_bucket,
				    MPI_CHAR,&my_MPI_Status);*/
  file_result=MPI_File_write_at(*archive_file_ptr,
				archive_file_loc,
				transfer_buffer,
				current_write_loc_in_bucket, // size of transfer
				MPI_CHAR,&my_MPI_Status);
  if(file_result != MPI_SUCCESS){
    
    fprintf(stderr,"rank %d got %d from MPI_File_write_at_all\n",my_rank,file_result);
    fprintf(stderr,"archive_file_loc: %ld\n",archive_file_loc);
    fprintf(stderr,"bytes to move: %ld\n",current_write_loc_in_bucket);
    fprintf(stderr,"writing slice %d\n",rank_call_list_index);
    MPI_Finalize();
    return 160;
  }

  if(current_entry == myl->n_entries_full){
    //    if((remainder=(current_write_loc_in_bucket % blocking_size))){
      //      current_write_loc_in_bucket += 
      //	(blocking_size - remainder);
      for(i=0;i<terminator_length;i++){
	sprintf(  ((char*)(transfer_buffer))+i,"%c",'\0');
      }
      archive_file_loc = data_region_start +
	((rank_call_list_index+1) * bucket_size);      
      file_result=MPI_File_write_at(*archive_file_ptr,
				    archive_file_loc,
				    transfer_buffer,
				    terminator_length,
				    MPI_CHAR,&my_MPI_Status);
      if(file_result != MPI_SUCCESS){
	
	fprintf(stderr,"rank %d got %d from MPI_File_write_at_all\n",my_rank,file_result);
	fprintf(stderr,"  writing tar file terminator block.\n");
	MPI_Finalize();
	return 160;

      }
      
      //    } // if(remainder==(current_write_loc_in_bucket....
  } // if(current_entry == ...
  
  // if we're the very very last transfer in the archive file, pad the file out to
  // the block size to make tar happy.
  //  fprintf(stderr,"CCC_3 rank=%5d  index=%5d\n",my_rank,rank_call_list_index);
  
  return 0;
}
					 

/*
extern "C"
int parfu_archive_1file_singFP(parfu_file_fragment_entry_list_t *raw_list,
			       char *arch_file_name,
			       int n_ranks, int my_rank, 
			       int my_max_block_size_exponent,
			       long int transfer_buffer_size,
			       int my_blocks_per_fragment,
			       int check_if_already_exist){
  // the assumption of this function is that MPI stuff has already been 
  // initialized.  The inputs for n_ranks and my_ranks are valid for all
  // ranks; the others may only be valid for rank 0.

  int filename_length;
  char *archive_filename_buffer=NULL;
  MPI_File *archive_file_MPI=NULL;
  char *catalog_buffer_for_ranks=NULL;
  long int rank0_catalog_buffer_for_ranks_length;
  long int catalog_buffer_for_ranks_length;
  char *catalog_buffer_for_archive=NULL;
  long int catalog_buffer_for_archive_length;
  MPI_File target_file_ptr;
  // split list on rank 0 before broadcasting to all the ranks
  parfu_file_fragment_entry_list_t *split_list=NULL;
  // local-to-rank list from broadcast catalog
  parfu_file_fragment_entry_list_t *rank_list=NULL;
  int data_starting_position;
  int max_block_size;
  int archive_catalog_takes_n_blocks;
  int crosscheck_max_block_size_exponent;

  int current_target_fragment;
  int rank0_current_target_fragment;

  long int bytes_to_move;

  void *transfer_buffer=NULL;

  long int stage_bytes_left_to_move;
  long int stage_target_file_offset;
  long int stage_archive_file_offset;
  long int file_block_size;

  long int times_through_loop;
  long int loop_divisor=100;

  MPI_Info my_Info=MPI_INFO_NULL;
  MPI_Status my_MPI_Status;

  int file_result;
  char *file_catalog_distributed=NULL;
  //  char *file_catalog_buffer_for_ranks=NULL;
  //  int target_fragment_increment;

  time_t *local_start=NULL,*local_end=NULL;
  int local_started=0;


  if((transfer_buffer=(char*)malloc(transfer_buffer_size))==NULL){
    fprintf(stderr,"parfu_archive_1file_singFP:\n");
    fprintf(stderr," could not allocate transfer buffer of size %ld bytes\n",
	    transfer_buffer_size);
    return 99;
  }

  if((archive_file_MPI=(MPI_File*)malloc(sizeof(MPI_File)))==NULL){
    fprintf(stderr,"parfu_archive_1file_singFP:\n");
    fprintf(stderr,"rank %d could not allocate space for archive file pointer!\n",my_rank);
    MPI_Finalize();
    return 75;
  }

  if((local_start=(time_t*)malloc(sizeof(time_t)))==NULL){
    fprintf(stderr,"parfu_archive_1file_singFP:\n");
    fprintf(stderr,"  could not allocate local_start!!!\n");
    MPI_Finalize();
    return 85;
  }

  if((local_end=(time_t*)malloc(sizeof(time_t)))==NULL){
    fprintf(stderr,"parfu_archive_1file_singFP:\n");
    fprintf(stderr,"  could not allocate local_end!!!\n");
    MPI_Finalize();
    return 85;
  }

  //  if(my_rank==0) fprintf(stderr,"  ***** about to broadcast out archive file name.\n");
  // distribute name of archive file to all ranks
  if(my_rank==0){
    filename_length=(strlen(arch_file_name))+1;
    if(check_if_already_exist){
      if(parfu_does_exist_quiet(arch_file_name)){
	fprintf(stderr,"parfu_archive_1file_singFP:\n");
	fprintf(stderr,"  Checking if archive file already exists.  It does!\n");
	fprintf(stderr,"  Returning with an error.\n");
	return 101;
      }
    }
  }
  MPI_Barrier(MPI_COMM_WORLD);

  MPI_Bcast(&filename_length,1,MPI_INT,0,MPI_COMM_WORLD);
  if((archive_filename_buffer=(char*)malloc(filename_length))==NULL){
    fprintf(stderr,"parfu_archive_1file_singFP:\n");
    fprintf(stderr,"rank %d could not allocate archive filename buffer!!!\n",my_rank);
    MPI_Finalize();
    return 74;
  }
  if(my_rank==0){
    sprintf(archive_filename_buffer,"%s",arch_file_name);
  }
  MPI_Bcast(archive_filename_buffer,filename_length,MPI_CHAR,0,MPI_COMM_WORLD);

  // open archive file on all ranks with shared pointer
  file_result=MPI_File_open(MPI_COMM_WORLD, archive_filename_buffer, 
			    MPI_MODE_WRONLY | MPI_MODE_CREATE | MPI_MODE_EXCL, 
			    my_Info, archive_file_MPI);
  if(file_result != MPI_SUCCESS){
    fprintf(stderr,"parfu_archive_1file_singFP:\n");
    fprintf(stderr,"MPI_File_open for archive buffer:  returned error!  Rank %d file >%s<\n",
	    my_rank,
	    archive_filename_buffer);
    return 3; 
  }
  free(archive_filename_buffer);
  archive_filename_buffer=NULL;
  

  // (during constructing parfu) 
  // I don't really know if we need this??

  //  file_result=
  //    MPI_File_set_view( *archive_file_MPI, 0, MPI_CHAR, MPI_CHAR,
  //		       "native", MPI_INFO_NULL ) ; 
  
  // this writes the archive catalog to the output file 
  // from rank 0
  //  if(my_rank==0) fprintf(stderr,"  ***** about to create catalog.\n");
  if(my_rank==0){
    if((catalog_buffer_for_archive=
	parfu_fragment_list_to_buffer(raw_list,
				      &catalog_buffer_for_archive_length,
				      my_max_block_size_exponent,
				      1)) // 1 is an archive buffer
       ==NULL){
      fprintf(stderr,"parfu_archive_1file_singFP:\n");
      fprintf(stderr,"failed to generate catalog for archive!\n");
      return -1;
    }
    //    if(my_rank==0) fprintf(stderr,"  ***** about to write catalog to archive file.\n");
    file_result=MPI_File_write(*archive_file_MPI,catalog_buffer_for_archive,
			       catalog_buffer_for_archive_length,
			       MPI_CHAR,&my_MPI_Status);
    if(file_result != MPI_SUCCESS){
      fprintf(stderr,"parfu_archive_1file_singFP:\n");
      fprintf(stderr,"MPI_File_write for catalog to archive file:  returned error!\n");
      return 3; 
    }
    
    //    if(my_rank==0) fprintf(stderr,"  ***** just wrote catalog.\n");
    max_block_size = int_power_of_2(my_max_block_size_exponent);
    archive_catalog_takes_n_blocks = 
      catalog_buffer_for_archive_length / max_block_size;
    if( catalog_buffer_for_archive_length % max_block_size)
      archive_catalog_takes_n_blocks++;
    data_starting_position = archive_catalog_takes_n_blocks * max_block_size;
  }
  // rank 0 just wrote the catalog to the beginning of the file
  // now we broadcast to all ranks where the data contents section
  // of the archive file begins, so they're all starting with the 
  // same initial offset
  //  if(my_rank==0) fprintf(stderr,"  ***** about bcast data starting position.\n");
  MPI_Bcast(&data_starting_position,1,MPI_INT,0,MPI_COMM_WORLD);
  
  // rank 0 splits the initial (by file) catalog into the by-slice
  // catalog for distribution to the other ranks
  if(my_rank==0){
    //    fprintf(stderr,"  ***** about to split list.\n");
    if((split_list=
	// construct split list
	parfu_split_fragments_in_list(raw_list,
				      PARFU_DEFAULT_MIN_BLOCK_SIZE_EXPONENT,
				      my_max_block_size_exponent,
				      my_blocks_per_fragment))
       ==NULL){
      fprintf(stderr,"parfu_archive_1file_singFP:\n");
      fprintf(stderr,"  failed to split file list!\n");
      return -1;
    }
    // make split list into buffer
    //    fprintf(stderr,"  ***** about to write split list to buffer.\n");
    if((catalog_buffer_for_ranks=parfu_fragment_list_to_buffer(split_list,
						     &rank0_catalog_buffer_for_ranks_length,
						     my_max_block_size_exponent,
						     0))
       ==NULL){
      fprintf(stderr,"help! failed to write stuff to buffer!!!\n");
      return 1;
    }
    //    fprintf(stderr,"  ***** have buffer; about to broadcast its length.\n");
    catalog_buffer_for_ranks_length = rank0_catalog_buffer_for_ranks_length;
    //    fprintf(stderr,"about to bcast catalog buffer size: %ld\n",
    //	    catalog_buffer_for_ranks_length);
  }
  // broadcast the LENGTH of the catalog buffer to all ranks
  MPI_Bcast(&catalog_buffer_for_ranks_length,1,MPI_LONG_INT,0,MPI_COMM_WORLD);
  // all ranks allocate space for the distributed catalog buffer
  //  if(my_rank==0) fprintf(stderr,"  ****  successfully bcast buffer length.\n");
  
  if((file_catalog_distributed=(char*)malloc(catalog_buffer_for_ranks_length))==NULL){
      fprintf(stderr,"parfu_archive_1file_singFP:\n");
      fprintf(stderr,"  rank %d failed to allocate shared buffer list!\n",my_rank);
      fprintf(stderr,"  requested size: %ld\n",catalog_buffer_for_ranks_length);
    return 91;
  }
  //  if(my_rank==0) fprintf(stderr,"  ****  about to memcpy file.\n");
  if(my_rank==0){
    memcpy(file_catalog_distributed,catalog_buffer_for_ranks,
	   catalog_buffer_for_ranks_length);
  }
  //  if(my_rank==0) fprintf(stderr,"  ****  about to broadcast file catalog.\n");
  MPI_Bcast(file_catalog_distributed,catalog_buffer_for_ranks_length,
	    MPI_CHAR,0,MPI_COMM_WORLD);
  // now all ranks build their catalog from the distributed buffer
  if((rank_list=parfu_buffer_to_file_fragment_list(file_catalog_distributed,
						   &crosscheck_max_block_size_exponent,
						   0))
     ==NULL){
    fprintf(stderr,"parfu_archive_1file_singFP:\n");
    fprintf(stderr,"  Rank %d could not recover catalog from broadcast buffer!\n",my_rank);
    return 101;
  }
  free(file_catalog_distributed);
  file_catalog_distributed=NULL;
  if(crosscheck_max_block_size_exponent != my_max_block_size_exponent){
    fprintf(stderr,"parfu_archive_1file_singFP:\n");
    fprintf(stderr,"miss-match between max block sizes. Should never happen!\n");
    return 102;
  }

  // (parfu:) I think this bit has been replaced with the data_starting_position stuff above
  // 
  // if(my_rank==0){
  //    // the catalog is blocked with the MAX block size
  //    how_many_blocks_in_the_catalog = ( catalog_buffer_length / dart_max_file_block_size ) + 1;
  //   }
  //   MPI_Bcast(&how_many_blocks_in_the_catalog,1,MPI_INT,0,MPI_COMM_WORLD);

  // all ranks have the full catalog
  // all ranks have the pointer for the shared archive file
  // now figure out what fragment

  // we space out fragments by ranks
  // this is our starting fragment

  
  //  current_target_fragment = my_rank;
  
  rank0_current_target_fragment = 0;


  //  if(my_rank==0){
  //  fprintf(stdout,"about to dump create catalog. ******\n");
  //  parfu_dump_fragment_entry_list(rank_list,stdout);
  //}

  // size archive to correct size to include correct zero padding at the end
  // as well as block size constraints by tar
  long int file_size = -1;
  if(my_rank==0){
    const parfu_file_fragment_entry_t *last_fragment =
      &raw_list->list[raw_list->n_entries_full-1];
    file_block_size = int_power_of_2(last_fragment->block_size_exponent);
    if(file_block_size < BLOCKSIZE)
      file_block_size = BLOCKSIZE;
    file_size=data_starting_position+
      (last_fragment->first_block+last_fragment->number_of_blocks)*
      file_block_size + 2*BLOCKSIZE;
  }
  MPI_Bcast(&file_size,1,MPI_LONG_INT,0,MPI_COMM_WORLD);
  file_result=MPI_File_set_size(*archive_file_MPI,file_size);

//  if(my_rank==0) fprintf(stderr,"  ****** about to begin big data transfer loop.\n");
  if(my_rank==0) times_through_loop=0;
  
  //  <<<<<<< HEAD:parfu_data_transfer.c
  //  while( current_target_fragment < rank_list->n_entries_full ){
  while( rank0_current_target_fragment < rank_list->n_entries_full ){
    current_target_fragment = rank0_current_target_fragment + my_rank;
    //    =======
    

    // set up for the transfer
//    stage_bytes_left_to_move = 
//    rank_list->list[current_target_fragment].size;
//    stage_target_file_offset = 
//    rank_list->list[current_target_fragment].fragment_offset;
 //   stage_archive_file_offset = 
//    data_starting_position +
 //   ( file_block_size * rank_list->list[current_target_fragment].first_block );

    
    // TODO: check if one can get away with not calling stat() inside of tarentry
    
    //    while(stage_bytes_left_to_move > 0){
    //      >>>>>>> rhaas/new_features_0_5:parfu_data_transfer.cc
    // All ranks must participate in the collective data write to the archive file. 
    // However, in the last pass through the ranks, we'll run out of fragments before
    // ranks.  So some ranks are "lame ducks"; they have no fragment to write but 
    // they must participate in the write call.  This next loop takes care of  
    // those ranks participating in a zero-byte write in the collective call.
    if( (current_target_fragment >= rank_list->n_entries_full ) ){
      // this is a lame-duck transfer transferring zero bytes
      
      // NOTE: since this is NOT a file fragment, we do not write a tar header here
      
      // setting destination of zero starting position; I *think* that's safe?
      stage_archive_file_offset = data_starting_position;
      
      file_result=MPI_File_write_at_all(*archive_file_MPI,stage_archive_file_offset,
					transfer_buffer,0,MPI_CHAR,&my_MPI_Status); 
      if(file_result != MPI_SUCCESS){
	fprintf(stderr,"rank %d got %d from MPI_File_write_at_all\n",my_rank,file_result);
	fprintf(stderr," (lame duck writing blank slice)\n");
	fprintf(stderr,"container offset: %ld\n",stage_archive_file_offset);
	fprintf(stderr,"writing slice %d\n",current_target_fragment);
	MPI_Finalize();
	return 160;
      }
    } // if ( ( current_target_fragment >= ...
    else{
      // this is NOT a lame duck rank, thus current_target_fragment is valid, so it's safe
      // to test against to see if it's zero bytes or something that doesn't require writing. 
      
      if( (rank_list->list[current_target_fragment].type != PARFU_FILE_TYPE_REGULAR) || 
	  (rank_list->list[current_target_fragment].size < 1) ){
	// entry is for a directory or a symlink, or a zero-byte file.  
	// here also we must do a placebo write of zero bytes.
	
	// we do need to write a tar header here
	// so that tar unpacking will get directories, symlinks, and zero-length files.
	// Putting the code in position now; this needs to be sorted out in the 
	// next pass through this code.
	{
	  tarentry entry(rank_list->list[current_target_fragment].relative_filename, 0);
	  std::vector<char> tarheader = entry.make_tar_header();
	    // we write the first fragment and thus also the header
	  if(rank_list->list[current_target_fragment].fragment_offset == 0) {
	    file_result=MPI_File_write_at(*archive_file_MPI,
					  stage_archive_file_offset-tarheader.size(),
					  &tarheader[0],
					  tarheader.size(),MPI_CHAR,
					  &my_MPI_Status);
	    if(file_result != MPI_SUCCESS){
	      fprintf(stderr,"rank %d got %d from MPI_File_write_at\n",my_rank,file_result);
	      fprintf(stderr,"container offset: %ld\n",stage_archive_file_offset);
	      fprintf(stderr,"bytes to move: %zu\n",tarheader.size());
	      fprintf(stderr,"writing slice %d\n",current_target_fragment);
	      MPI_Finalize();
	      return 160;
	    }
	  }
	  // do *not* move file pointer since we insert tar header *before* content
	}
	// end tar header stuff for dirs, symlinks, and zero-byte files
	
	// tar header above, now we sort out whatever we need for collective writes of zero-byte files
	// setting destination of zero starting position; I *think* that's safe?
	stage_archive_file_offset = data_starting_position;
	
	file_result=MPI_File_write_at_all(*archive_file_MPI,stage_archive_file_offset,
					  transfer_buffer,0,MPI_CHAR,&my_MPI_Status); 
	if(file_result != MPI_SUCCESS){
	  fprintf(stderr,"rank %d got %d from MPI_File_write_at_all\n",my_rank,file_result);
	  fprintf(stderr," (non-lame duck, is dir or symlink or 0byte file, skipping writing)\n");
	  fprintf(stderr,"container offset: %ld\n",stage_archive_file_offset);
	  fprintf(stderr,"writing slice %d\n",current_target_fragment);
	  MPI_Finalize();
	  return 160;
	}
      } // if we're writing a real fragment but it has zero bytes
      else{
	// this fragment isn't a lame duck, and it's an actual file containing one
	// or more bytes.  So we MUST actually copy data.
	
	// do tar header stuff for this file fragment if necessary
	{
	  tarentry entry(rank_list->list[current_target_fragment].relative_filename, 0);
	  std::vector<char> tarheader = entry.make_tar_header();
	  // we write the first fragment and thus also the header
	  if(rank_list->list[current_target_fragment].fragment_offset == 0) {
	    file_result=MPI_File_write_at(*archive_file_MPI,
					  stage_archive_file_offset-tarheader.size(),
					  &tarheader[0],
					  tarheader.size(),MPI_CHAR,
					  &my_MPI_Status);
	    if(file_result != MPI_SUCCESS){
	      fprintf(stderr,"rank %d got %d from MPI_File_write_at\n",my_rank,file_result);
	      fprintf(stderr,"container offset: %ld\n",stage_archive_file_offset);
	      fprintf(stderr,"bytes to move: %zu\n",tarheader.size());
	      fprintf(stderr,"writing slice %d\n",current_target_fragment);
	      MPI_Finalize();
	      return 160;
	    }
	  }
	  // do *not* move file pointer since we insert tar header *before* content
	}
	// end tar header stuff for this real file
	
	// now on to actual data movement for this file
	// open the file we're reading from 
	file_result=MPI_File_open(MPI_COMM_SELF,
				  rank_list->list[current_target_fragment].relative_filename,
				  MPI_MODE_RDONLY,
				  my_Info,&target_file_ptr);
	if(file_result != MPI_SUCCESS){
	  fprintf(stderr,"rank %d got non-zero result when opening %s!\n",
		  my_rank,
		  rank_list->list[current_target_fragment].relative_filename);
	  fprintf(stderr,"result=%d\n",file_result);
	  return 138;
	}
	//    fprintf(stderr,"  rank %d  fragment   %d     file >%s< successfully open!\n",
	//	    my_rank,current_target_fragment,rank_list->list[current_target_fragment].relative_filename);
	file_block_size = int_power_of_2(rank_list->list[current_target_fragment].block_size_exponent);
	
	// set up for the transfer
	stage_bytes_left_to_move = 
	  rank_list->list[current_target_fragment].size;
	stage_target_file_offset = 
	  rank_list->list[current_target_fragment].fragment_offset;
	stage_archive_file_offset = 
	  data_starting_position +
	  ( file_block_size * rank_list->list[current_target_fragment].first_block );
	
	while(stage_bytes_left_to_move > 0){
	  
	  //      fprintf(stderr,"rank %4d fragment %4d starting\n",my_rank,current_target_fragment);
	  
	  // make sure transfer is the minimum 
	  bytes_to_move = stage_bytes_left_to_move;
	  if(bytes_to_move > transfer_buffer_size){
	    bytes_to_move = transfer_buffer_size;
	    fprintf(stderr,"parfu_archive_1file_singFP:\n");
	    fprintf(stderr,"bytes_to_move bigger than trans buffer!  \n");
	    fprintf(stderr,"  This is no longer allowed with collective file transfers.\n");
	    fprintf(stderr,"  FATAL error!!!\n");
	    fprintf(stderr,"  Either fragment size is too big or transfer buffer too small.\n");
	    return 102;
	  }
	  
	  //if(debug) 
	  //    fprintf(stderr,"rank %d slice %d moving %ld bytes file_offset %ld container_offset %ld\n",
	  //		     my_rank,current_target_slice,bytes_to_move, staging_file_offset,staging_container_offset);
	  
	  // move bytes to RAM buffer
	  //	  items_read=fread(transfer_buffer,sizeof(char),bytes_to_move,target_file_ptr);
	  file_result=MPI_File_read_at(target_file_ptr,stage_target_file_offset,transfer_buffer,
				       bytes_to_move,MPI_CHAR,&my_MPI_Status);
	  if(file_result != MPI_SUCCESS){
	    fprintf(stderr,"rank %d got %d from MPI_File_read_at\n",my_rank,file_result);
	    fprintf(stderr,"writing fragment %d",current_target_fragment);
	    return 159;
	  }
	  //      fprintf(stderr,"rd tgt: rank %4d ofst:%10ld bytes:%10ld  >%s<\n",my_rank,stage_target_file_offset,bytes_to_move,
	  //	      rank_list->list[current_target_fragment].relative_filename);
	  
	  //      fprintf(stderr,"rank %4d fragment %4d middle\n",my_rank,current_target_fragment);
	  
	  // move bytes from RAM buffer to container file
	  file_result=MPI_File_write_at_all(*archive_file_MPI,stage_archive_file_offset,transfer_buffer,
					    bytes_to_move,MPI_CHAR,&my_MPI_Status);
	  if(file_result != MPI_SUCCESS){
	    fprintf(stderr,"rank %d got %d from MPI_File_write_at_all\n",my_rank,file_result);
	    fprintf(stderr,"container offset: %ld\n",stage_archive_file_offset);
	    fprintf(stderr,"bytes to move: %ld\n",bytes_to_move);
	    fprintf(stderr,"writing slice %d\n",current_target_fragment);
	    MPI_Finalize();
	    return 160;
	  }
	  //      fprintf(stderr,"wt arc: rank %4d ofst:%10ld bytes:%10ld  >%s<\n",my_rank,stage_archive_file_offset,bytes_to_move,
	  //	      rank_list->list[current_target_fragment].relative_filename);
	  
	  
	  // update the quantities left
	  // staging_bytes_remaining -= staging_buffer_size;
	  // staging_container_offset += staging_buffer_size;
	  // staging_file_offset += staging_buffer_size;
	  //      fprintf(stderr,"rank %4d fragment %4d loop_end\n",my_rank,current_target_fragment);
	  
	  stage_bytes_left_to_move -= bytes_to_move;
	  stage_target_file_offset += bytes_to_move;
	  stage_archive_file_offset += bytes_to_move;
	  
	} // while(staging_bytes_remaining
	
	MPI_File_close(&target_file_ptr);
	
      } // else (doing actual transfers
    } // else (not a lame duck}
    if(my_rank==0){
      if(!(times_through_loop % loop_divisor)){
	//	fprintf(stderr,".");
	fprintf(stderr,
		"rank 0 processed slice %d of total %d : ",
		current_target_fragment,
		rank_list->n_entries_full);
	if(local_started){
	  time(local_end);
	  fprintf(stderr,"time: %d seconds",
		  ((int)((*local_end)-(*local_start))));
	  (*local_start)=(*local_end);
	}
	else{
	  local_started=1;
	  time(local_start);
	}
	fprintf(stderr,"\n");
      } // if(!(times_through_loop % loop_divisor))
      times_through_loop++;
    } // if(my_rank==0)
    rank0_current_target_fragment += n_ranks;
  } // while(rank0_current_target_fragment
  
  if(my_rank==0) fprintf(stderr,"\n  ****** rank zero finished big data transfer loop.\n");
  
  //  fprintf(stderr,"rank %4d got out of fragment loop.\n",my_rank);
  
  free(transfer_buffer);
  transfer_buffer=NULL;
  
  MPI_Barrier(MPI_COMM_WORLD);
  if(my_rank==0){
    fprintf(stderr,"  ****** all ranks finished big data transfer loop.\n");
    fprintf(stderr,"      about to close archive file.\n");
  }
  MPI_File_close(archive_file_MPI);  
  free(archive_file_MPI);
  archive_file_MPI=NULL;
  
  return 0;
}
*/  


/*
extern "C"
int parfu_extract_1file_singFP(char *arch_file_name,
			       char *extract_target_dir,
			       int n_ranks, int my_rank, 
			       int *my_max_block_size_exponent,
			       long int transfer_buffer_size,
			       int my_blocks_per_fragment,
			       int check_if_already_exist){
  // the assumption of this function is that MPI stuff has already been 
  // initialized.  The inputs for n_ranks and my_ranks are valid for all
  // ranks; the others may only be valid for rank 0.

  int filename_length;
  char *archive_filename_buffer=NULL;
  MPI_File *archive_file_MPI=NULL;
  char *catalog_buffer_for_ranks=NULL;
  long int catalog_buffer_for_ranks_length;
  //  char *catalog_buffer_for_archive=NULL;
  int catalog_buffer_for_archive_length;
  MPI_File target_file_ptr;
  parfu_file_fragment_entry_list_t *list_fr_archive=NULL;
  // split list on rank 0 before broadcasting to all the ranks
  parfu_file_fragment_entry_list_t *split_list=NULL;
  // local-to-rank list from broadcast catalog
  parfu_file_fragment_entry_list_t *rank_list=NULL;
  int data_starting_position;
  int max_block_size;
  int archive_catalog_takes_n_blocks;
  int crosscheck_max_block_size_exponent;

  int current_target_fragment;

  long int bytes_to_move;

  void *transfer_buffer=NULL;

  long int stage_bytes_left_to_move;
  long int stage_target_file_offset;
  long int stage_archive_file_offset;
  long int file_block_size;

  MPI_Info my_Info=MPI_INFO_NULL;
  MPI_Status my_MPI_Status;

  int file_result;
  char *file_catalog_distributed=NULL;
  //  char *file_catalog_buffer_for_ranks=NULL;
  //  int target_fragment_increment;
  char *directory_to_mkdir=NULL;
  int base_dirname_length;

  int i;
  long int times_through_loop;
  long int loop_divisor=100;

  time_t *local_start=NULL,*local_end=NULL;
  int local_started=0;


  if((transfer_buffer=(char*)malloc(transfer_buffer_size))==NULL){
    fprintf(stderr,"parfu_extract_1file_singFP:\n");
    fprintf(stderr," could not allocate transfer buffer of size %ld bytes\n",
	    transfer_buffer_size);
    return 99;
  }

  if((archive_file_MPI=(MPI_File*)malloc(sizeof(MPI_File)))==NULL){
    fprintf(stderr,"parfu_extract_1file_singFP:\n");
    fprintf(stderr,"rank %d could not allocate space for archive file pointer!\n",my_rank);
    MPI_Finalize();
    return 75;
  }

  if((local_start=(time_t*)malloc(sizeof(time_t)))==NULL){
    fprintf(stderr,"parfu_archive_1file_singFP:\n");
    fprintf(stderr,"  could not allocate local_start!!!\n");
    MPI_Finalize();
    return 85;
  }

  if((local_end=(time_t*)malloc(sizeof(time_t)))==NULL){
    fprintf(stderr,"parfu_archive_1file_singFP:\n");
    fprintf(stderr,"  could not allocate local_end!!!\n");
    MPI_Finalize();
    return 85;
  }

  //  if(my_rank==0) fprintf(stderr,"  ***** about to broadcast out archive file name.\n");
  // distribute name of archive file to all ranks
  if(my_rank==0){
    filename_length=(strlen(arch_file_name))+1;
  }
  MPI_Bcast(&filename_length,1,MPI_INT,0,MPI_COMM_WORLD);
  if((archive_filename_buffer=(char*)malloc(filename_length))==NULL){
    fprintf(stderr,"parfu_extract_1file_singFP:\n");
    fprintf(stderr,"rank %d could not allocate archive filename buffer!!!\n",my_rank);
    MPI_Finalize();
    return 74;
  }
  if(my_rank==0){
    sprintf(archive_filename_buffer,"%s",arch_file_name);
  }
  if(my_rank==0) fprintf(stderr,"about to broadcast filename\n");
  MPI_Bcast(archive_filename_buffer,filename_length,MPI_CHAR,0,MPI_COMM_WORLD);
  
  // (during constructing parfu) 
  // I don't really know if we need this??
  if(my_rank==0){
    fprintf(stderr,"about to to MPI_File_set_view\n");
  }
  //  file_result=
  //    MPI_File_set_view( *archive_file_MPI, 0, MPI_CHAR, MPI_CHAR,
  //		       "native", MPI_INFO_NULL ) ; 
  
  if(my_rank==0) fprintf(stderr,"about to read catalog\n");
    
  // rank 0 reads the catalog buffer
  if(my_rank==0){
    if((list_fr_archive=parfu_ffel_from_file(arch_file_name,
					     my_max_block_size_exponent,
					     &catalog_buffer_for_archive_length))
       ==NULL){
      fprintf(stderr,"parfu_extract_1file_singFP:\n");
      fprintf(stderr,"failed generate file list from file %s!!\n",
	      arch_file_name);
      return 4;
    }
    fprintf(stderr,"back from parfu_ffel_from_file!\n");

    max_block_size = int_power_of_2(*my_max_block_size_exponent);
    archive_catalog_takes_n_blocks = 
      catalog_buffer_for_archive_length / max_block_size;
    if( catalog_buffer_for_archive_length % max_block_size)
      archive_catalog_takes_n_blocks++;
    data_starting_position = archive_catalog_takes_n_blocks * max_block_size;
  }
  if(my_rank==0) fprintf(stderr,"about to broadcast data starting position: %d \n",
			 data_starting_position);

  MPI_Bcast(&data_starting_position,1,MPI_INT,0,MPI_COMM_WORLD);
  
  // open archive file on all ranks with shared pointer
  
  fprintf(stderr,"rank %d about to group open file >%s<\n",my_rank,archive_filename_buffer);
  file_result=MPI_File_open(MPI_COMM_WORLD,archive_filename_buffer, 
			    MPI_MODE_RDONLY, 
			    my_Info, archive_file_MPI);
  if(my_rank==0) fprintf(stderr," successfully opened file!\n");
  if(file_result != MPI_SUCCESS){
    fprintf(stderr,"parfu_extract_1file_singFP:\n");
    fprintf(stderr,"MPI_File_open for archive buffer:  returned error!  Rank %d file >%s<\n",
	    my_rank,
	    archive_filename_buffer);
    return 3; 
  }
  free(archive_filename_buffer);
  archive_filename_buffer=NULL;
  
  
  if(my_rank==0) fprintf(stderr,"about to expland list to slice list\n");

  if(my_rank==0){
    // populate the file list from the file with the local
    // extract pathname
    if(parfu_ffel_fill_rel_filenames(list_fr_archive,extract_target_dir)){
      fprintf(stderr,"parfu_extract_1file_singFP:\n");
      fprintf(stderr,"  parfu_ffel_fill_rel_filenames() failed!\n");
      return -5;
    }
    
    // list_fr_archive now has the pathnames for where extracted files are going
    // to go.  We loop through to see if they already exist, and exit if that's
    // the case.
    
    if(check_if_already_exist){
      for(i=0;i<list_fr_archive->n_entries_full;i++){
	// check for existence of the ith file to be extracted
	if( list_fr_archive->list[i].type == PARFU_FILE_TYPE_REGULAR ||
	    list_fr_archive->list[i].type == PARFU_FILE_TYPE_SYMLINK ){
	  if( access(list_fr_archive->list[i].relative_filename,F_OK) != -1){
	    fprintf(stderr,"parfu_extract_1file_singFP:\n");
	    fprintf(stderr,"  going to extract file %s\n",
		    list_fr_archive->list[i].relative_filename);
	    fprintf(stderr,"  but that file already exists!!!\n");
	    return -8;
	  }
	}
      } // for(i=0;
    } //     if(check_if_already_exist){

    // rank 0 splits the initial (by file) catalog into the by-slice
    // catalog for distribution to the other ranks
    //    fprintf(stderr,"  ***** about to split list.\n");
    if((split_list=
	// construct split list
	parfu_split_fragments_in_list(list_fr_archive,
				      PARFU_DEFAULT_MIN_BLOCK_SIZE_EXPONENT,
				      *my_max_block_size_exponent,
				      my_blocks_per_fragment))
       ==NULL){
      fprintf(stderr,"parfu_extract_1file_singFP:\n");
      fprintf(stderr,"  failed to split file list!\n");
      return -1;
    }
    // make split list into buffer
    //    fprintf(stderr,"  ***** about to write split list to buffer.\n");
    if((catalog_buffer_for_ranks=parfu_fragment_list_to_buffer(split_list,
							       &catalog_buffer_for_ranks_length,
							       *my_max_block_size_exponent,
							       0))
       ==NULL){
      fprintf(stderr,"help! failed to write stuff to buffer!!!\n");
      return 1;
    }
    //    fprintf(stderr,"  ***** have buffer; about to broadcast its length.\n");
  }
  // broadcast the LENGTH of the catalog buffer to all ranks
  MPI_Bcast(&catalog_buffer_for_ranks_length,1,MPI_LONG_INT,0,MPI_COMM_WORLD);
  // all ranks allocate space for the distributed catalog buffer
  //  if(my_rank==0) fprintf(stderr,"  ****  successfully bcast buffer length.\n");
  
  if((file_catalog_distributed=(char*)malloc(catalog_buffer_for_ranks_length))==NULL){
      fprintf(stderr,"parfu_extract_1file_singFP:\n");
      fprintf(stderr,"  rank %d failed to allocate shared buffer list!\n",my_rank);
      fprintf(stderr,"  requested size: %ld\n",catalog_buffer_for_ranks_length);
    return 91;
  }
  //  if(my_rank==0) fprintf(stderr,"  ****  about to memcpy file.\n");
  if(my_rank==0){
    memcpy(file_catalog_distributed,catalog_buffer_for_ranks,
	   catalog_buffer_for_ranks_length);
  }
  //  if(my_rank==0) fprintf(stderr,"  ****  about to broadcast file catalog.\n");
  MPI_Bcast(file_catalog_distributed,catalog_buffer_for_ranks_length,
	    MPI_CHAR,0,MPI_COMM_WORLD);
  // now all ranks build their catalog from the distributed buffer
  if((rank_list=parfu_buffer_to_file_fragment_list(file_catalog_distributed,
						   &crosscheck_max_block_size_exponent,
						   0))
     ==NULL){
    fprintf(stderr,"parfu_extract_1file_singFP:\n");
    fprintf(stderr,"  Rank %d could not recover catalog from broadcast buffer!\n",my_rank);
    return 101;
  }
  free(file_catalog_distributed);
  file_catalog_distributed=NULL;
  if(my_rank==0){
    if(crosscheck_max_block_size_exponent != (*my_max_block_size_exponent)){
      fprintf(stderr,"parfu_extract_1file_singFP:\n");
      fprintf(stderr,"miss-match between max block sizes. Should never happen!\n");
      fprintf(stderr," crossccheck: %d   my_max_block: %d\n",
	      crosscheck_max_block_size_exponent,(*my_max_block_size_exponent) );
      return 102;
    }
  }

  // (parfu:) I think this bit has been replaced with the data_starting_position stuff above
  // 
  // if(my_rank==0){
  //    // the catalog is blocked with the MAX block size
  //    how_many_blocks_in_the_catalog = ( catalog_buffer_length / dart_max_file_block_size ) + 1;
  //   }
  //   MPI_Bcast(&how_many_blocks_in_the_catalog,1,MPI_INT,0,MPI_COMM_WORLD);

  // before extracting anything, we need to make sure all the directories
  // exist

  MPI_Barrier(MPI_COMM_WORLD);
  if(my_rank==0){
    fprintf(stderr,"about to ensure directories\n");
    int dir_count=0;
    current_target_fragment=0;
    while(rank_list->list[current_target_fragment].type == PARFU_FILE_TYPE_DIR){
      fprintf(stderr,"ensuring directory # %d\n",current_target_fragment);
      
  //    //      if(!strcmp("./",rank_list->list[current_target_fragment].relative_filename)){
//	current_target_fragment++;
	//continue;
//	}
      base_dirname_length=strlen(rank_list->list[current_target_fragment].relative_filename);
      if( ((rank_list->list[current_target_fragment].relative_filename)[base_dirname_length-1])
	  == '/' ){
	current_target_fragment++;
	continue;
      }
	
//	  if((directory_to_mkdir=malloc(base_dirname_length+2))==NULL){
//	  fprintf(stderr,"parfu_extract_1file_singFP:\n");
//	  fprintf(stderr," cannot allocate new directory_to_dirname!!\n");
//	  return 110;
//	  }
//	  if( ((rank_list->list[current_target_fragment].relative_filename)[base_dirname_length-1])
//	  == '/' ){
//	  sprintf(directory_to_mkdir,"%s.",
//	  rank_list->list[current_target_fragment].relative_filename);
//	  }
//	  else{
//	  sprintf(directory_to_mkdir,"%s",
//	  rank_list->list[current_target_fragment].relative_filename);
//	  
//	  }

      directory_to_mkdir=
	rank_list->list[current_target_fragment].relative_filename;
      if(mkdir(directory_to_mkdir,0777)){
	fprintf(stderr,"parfu_extract_1file_singFP:\n");
	fprintf(stderr," could not mkdir directory %s\n",directory_to_mkdir);
	return -21;
      }
      free(directory_to_mkdir);
      directory_to_mkdir=NULL;
      dir_count++;
      current_target_fragment++;
    }
    fprintf(stderr,"All %d directories in archive file created.\n",dir_count);

    
    // fprintf(stdout,"about to dump extract catalog. ******\n");
    // parfu_dump_fragment_entry_list(rank_list,stdout);
    
    
  }
  
  // synch before continuing to ensure all directories have been created.
  // before we attempt to extract anything.
  MPI_Barrier(MPI_COMM_WORLD);

  // all ranks have the full catalog
  // all ranks have the pointer for the shared archive file
  // now figure out what fragment

  // we space out fragments by ranks
  // this is our starting fragment

  
  current_target_fragment = my_rank;

  if(my_rank==0) fprintf(stderr,"  ****** about to begin big data transfer loop.\n");
  times_through_loop=0;
  while( current_target_fragment < rank_list->n_entries_full ){
    // rank 0 created all the directories before we started, so we just skip them here
    if(rank_list->list[current_target_fragment].type == PARFU_FILE_TYPE_DIR){
      // if there were something to do to directories, we'd do it here
      
      // cleanup before continuing loop
      current_target_fragment += n_ranks;
      continue;
    }
    // if the file is a symlink, create it
    if(rank_list->list[current_target_fragment].type == PARFU_FILE_TYPE_SYMLINK){
      if(symlink(rank_list->list[current_target_fragment].target,
		 rank_list->list[current_target_fragment].relative_filename)){
	fprintf(stderr,"parfu_extract_1file_singFP:\n");
	fprintf(stderr," rank %d could not create link at >%s< to >%s<!!\n",
		my_rank,
		rank_list->list[current_target_fragment].relative_filename,
		rank_list->list[current_target_fragment].target);
	return -22;
      }
      // cleanup before continuing loop
      current_target_fragment += n_ranks;
      continue;
    }
    if(rank_list->list[current_target_fragment].size == 0){
      // This fragment is a zero-length file.  Need to create it, but no 
      // data transfer will happen, just open it and then close it again.
      file_result=MPI_File_open(MPI_COMM_SELF,
				rank_list->list[current_target_fragment].relative_filename,
				MPI_MODE_WRONLY | MPI_MODE_EXCL | MPI_MODE_CREATE ,
				my_Info,&target_file_ptr);
      if(file_result != MPI_SUCCESS){
	fprintf(stderr,"parfu_extract_1file_singFP:\n");
	fprintf(stderr,"rank %d got non-zero result when opening zero-length target file %s!\n",
		my_rank,
		rank_list->list[current_target_fragment].relative_filename);
	fprintf(stderr,"result=%d\n",file_result);
	return 138;
      }
      MPI_File_close(&target_file_ptr);

      // cleanup before continuing while loop
      current_target_fragment += n_ranks;
      continue;
    }
    if(rank_list->list[current_target_fragment].size < 1){
      fprintf(stderr,"parfu_extract_1file_singFP:\n");
      fprintf(stderr,"  HELP!!  Fragment %d has a length less than 1, but \n",current_target_fragment);
      fprintf(stderr,"  we've already accounted for the types of non-files we know\n");
      fprintf(stderr,"  how to deal with.  Help!  File: >%s<\n",
	      rank_list->list[current_target_fragment].relative_filename);

      // cleanup before continuing while loop
      current_target_fragment += n_ranks;
      continue;
    }
    // open the file we're writing to

    // 2016nov15 removing MPI_MODE_EXCL; that check is done up above
    //     file_result=MPI_File_open(MPI_COMM_SELF,
    //		      rank_list->list[current_target_fragment].relative_filename,
    //		      MPI_MODE_WRONLY | MPI_MODE_EXCL | MPI_MODE_CREATE ,
    //		      my_Info,&target_file_ptr);
    file_result=MPI_File_open(MPI_COMM_SELF,
			      rank_list->list[current_target_fragment].relative_filename,
			      MPI_MODE_WRONLY | MPI_MODE_CREATE ,
			      my_Info,&target_file_ptr);
    if(file_result != MPI_SUCCESS){
      fprintf(stderr,"rank %d got non-zero result when opening target file %s to extract!\n",
	      my_rank,
	      rank_list->list[current_target_fragment].relative_filename);
      fprintf(stderr,"result=%d\n",file_result);
      return 138;
    }
    //    fprintf(stderr,"  rank %d  fragment   %d     file >%s< successfully open!\n",
    //	    my_rank,current_target_fragment,rank_list->list[current_target_fragment].relative_filename);
    file_block_size = int_power_of_2(rank_list->list[current_target_fragment].block_size_exponent);

    // set up for the transfer
    stage_bytes_left_to_move = 
      rank_list->list[current_target_fragment].size;
    stage_target_file_offset = 
      rank_list->list[current_target_fragment].fragment_offset;
    stage_archive_file_offset = 
      data_starting_position +
      ( file_block_size * rank_list->list[current_target_fragment].first_block );
    
    while(stage_bytes_left_to_move > 0){

      //      fprintf(stderr,"rank %4d fragment %4d starting\n",my_rank,current_target_fragment);
      
      // make sure transfer is the minimum 
      bytes_to_move = stage_bytes_left_to_move;
      if(bytes_to_move > transfer_buffer_size) 
	bytes_to_move = transfer_buffer_size;
      
      //if(debug) 
      //    fprintf(stderr,"rank %d slice %d moving %ld bytes file_offset %ld container_offset %ld\n",
      //		     my_rank,current_target_slice,bytes_to_move, staging_file_offset,staging_container_offset);
      
      // move bytes to RAM buffer
      //	  items_read=fread(transfer_buffer,sizeof(char),bytes_to_move,target_file_ptr);

      //      file_result=MPI_File_read_at(target_file_ptr,stage_target_file_offset,transfer_buffer,
      //				   bytes_to_move,MPI_CHAR,&my_MPI_Status);
      file_result=MPI_File_read_at(*archive_file_MPI,stage_archive_file_offset,transfer_buffer,
				   bytes_to_move,MPI_CHAR,&my_MPI_Status);
      
      if(file_result != MPI_SUCCESS){
	fprintf(stderr,"rank %d got %d from MPI_File_read_at\n",my_rank,file_result);
	fprintf(stderr,"writing fragment %d",current_target_fragment);
	return 159;
      }
      //      fprintf(stderr,"rd arc: rank %4d ofst:%10ld bytes:%10ld  >%s<\n",my_rank,stage_archive_file_offset,bytes_to_move,
      //	      rank_list->list[current_target_fragment].relative_filename);

      //      fprintf(stderr,"rank %4d fragment %4d middle\n",my_rank,current_target_fragment);
      
      // move bytes from RAM buffer to extracted file
      //      file_result=MPI_File_write_at(*archive_file_MPI,stage_archive_file_offset,transfer_buffer,
      //				    bytes_to_move,MPI_CHAR,&my_MPI_Status);
      file_result=MPI_File_write_at(target_file_ptr,stage_target_file_offset,transfer_buffer,
				    bytes_to_move,MPI_CHAR,&my_MPI_Status);
      if(file_result != MPI_SUCCESS){
	fprintf(stderr,"rank %d got %d from MPI_File_write_at\n",my_rank,file_result);
	fprintf(stderr,"container offset: %ld\n",stage_archive_file_offset);
	fprintf(stderr,"bytes to move: %ld\n",bytes_to_move);
	fprintf(stderr,"writing slice %d\n",current_target_fragment);
	MPI_Finalize();
	return 160;
      }
      //      fprintf(stderr,"wt tgt: rank %4d ofst:%10ld bytes:%10ld  >%s<\n",my_rank,stage_target_file_offset,bytes_to_move,
      //	      rank_list->list[current_target_fragment].relative_filename);
      // update the quantities left
      // staging_bytes_remaining -= staging_buffer_size;
      // staging_container_offset += staging_buffer_size;
      // staging_file_offset += staging_buffer_size;
      //      fprintf(stderr,"rank %4d fragment %4d loop_end\n",my_rank,current_target_fragment);
      
      stage_bytes_left_to_move -= bytes_to_move;
      stage_target_file_offset += bytes_to_move;
      stage_archive_file_offset += bytes_to_move;
      
    } // while(staging_bytes_remaining
    
    MPI_File_close(&target_file_ptr);
    
    if(my_rank==0){
      if(!(times_through_loop % loop_divisor)){
	//	fprintf(stderr,".");
	fprintf(stderr,
		"rank 0 processed slice %d of total %d : ",
		current_target_fragment,
		rank_list->n_entries_full);
	if(local_started){
	  time(local_end);
	  fprintf(stderr,"time: %d seconds",
		  ((int)((*local_end)-(*local_start))));
	  (*local_start)=(*local_end);
	}
	else{
	  local_started=1;
	  time(local_start);
	}
	fprintf(stderr,"\n");
      } // if(!(times_through_loop % loop_divisor))
      times_through_loop++;
    } // if(my_rank==0)
    current_target_fragment += n_ranks;
  } // while(current_target_fragment
  if(my_rank==0) fprintf(stderr,"  ****** rank 0 finished big data transfer loop.\n");
  
  //  fprintf(stderr,"rank %4d got out of fragment loop.\n",my_rank);
  
  free(transfer_buffer);
  transfer_buffer=NULL;

  MPI_Barrier(MPI_COMM_WORLD);
  if(my_rank==0) fprintf(stderr,"  ****** all ranks finished big data transfer loop.\n");
  MPI_File_close(archive_file_MPI);  
  free(archive_file_MPI);
  archive_file_MPI=NULL;

  return 0;
}


*/
