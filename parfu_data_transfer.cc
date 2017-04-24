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

  
  current_target_fragment = my_rank;

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

  if(my_rank==0) fprintf(stderr,"  ****** about to begin big data transfer loop.\n");
  if(my_rank==0) times_through_loop=0;
  while( current_target_fragment < rank_list->n_entries_full ){
    // we transfer data for regular files that have non-zero size. 
    // skip over non-files and zero-length files
    if(rank_list->list[current_target_fragment].type != PARFU_FILE_TYPE_REGULAR){
      current_target_fragment += n_ranks;
      continue;
    }
    if(rank_list->list[current_target_fragment].size < 1){
      current_target_fragment += n_ranks;
      continue;
    }
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

    // TODO: check if one can get away with not calling stat() inside of tarentry
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
      file_result=MPI_File_write_at(*archive_file_MPI,stage_archive_file_offset,transfer_buffer,
				    bytes_to_move,MPI_CHAR,&my_MPI_Status);
      if(file_result != MPI_SUCCESS){
	fprintf(stderr,"rank %d got %d from MPI_File_write_at\n",my_rank,file_result);
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
      
      /*      if(!strcmp("./",rank_list->list[current_target_fragment].relative_filename)){
	current_target_fragment++;
	continue;
	}*/
      base_dirname_length=strlen(rank_list->list[current_target_fragment].relative_filename);
      if( ((rank_list->list[current_target_fragment].relative_filename)[base_dirname_length-1])
	  == '/' ){
	current_target_fragment++;
	continue;
      }
	/*
	  if((directory_to_mkdir=malloc(base_dirname_length+2))==NULL){
	  fprintf(stderr,"parfu_extract_1file_singFP:\n");
	  fprintf(stderr," cannot allocate new directory_to_dirname!!\n");
	  return 110;
	  }
	  if( ((rank_list->list[current_target_fragment].relative_filename)[base_dirname_length-1])
	  == '/' ){
	  sprintf(directory_to_mkdir,"%s.",
	  rank_list->list[current_target_fragment].relative_filename);
	  }
	  else{
	  sprintf(directory_to_mkdir,"%s",
	  rank_list->list[current_target_fragment].relative_filename);
	  
	  }
	*/
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


