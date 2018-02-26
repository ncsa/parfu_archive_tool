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
//  Roland Haas <roland@illinois.edu>
//  Craig P Steffen <csteffen@ncsa.illinois.edu>
//  
//  https://github.com/ncsa/parfu_archive_tool
//  http://www.ncsa.illinois.edu/People/csteffen/parfu/
//  
//  For full licnse text see the LICENSE file provided with the source
//  distribution.
//  
////////////////////////////////////////////////////////////////////////////////

#include <parfu_primary.h>

int main(int nargs, char *args[]){
  int debug=0;

  int blocking_size = PARFU_DEFAULT_BLOCKING_FACTOR;
  int bucket_size = PARFU_DEFAULT_RANK_BLOCK_SIZE;

  char mode;
  char *arc_filename=NULL;
  char *target_filename=NULL;
  int run_usage=0;

  char *pad_filename=NULL;
  char *bcast_archive_file_name=NULL;
  
  parfu_file_fragment_entry_list_t *raw_list=NULL;
  //  parfu_file_fragment_entry_list_t *split_list=NULL;
  //  parfu_file_fragment_entry_list_t *shared_split_list=NULL;
  int raw_file_entries;

    // MPI stuff
  int n_ranks;
  int my_rank;

  int **bcast_archive_file_name_length=NULL;

  //  int rank_buckets_total;
  time_t timer_before,timer_after;

  //  int *rank_call_list=NULL;
  //  int rank_call_list_length;

  int return_val;

  float elapsed_time_for_list=-1.0;
  float elapsed_time_to_transfer=-1.0;

  //  char *split_list_buffer=NULL;
  //  char *shared_split_list_buffer=NULL;
  //  long int shared_split_list_buffer_length;

  //  int shared_bucket_size;

  if(nargs > 1)
    mode=*args[1];
  if(nargs < 3)
    run_usage=1;
  else
    if(nargs == 3 && mode!='T')
      run_usage=1;
  if(run_usage){
    fprintf(stderr,"\nusage: parfu_0_5_1 C|X|T <archive.pfu> <target>\n");
    fprintf(stderr,"  C to create an archive from the target file/directory\n");
    fprintf(stderr,"  X to extract an archive file to the target directory\n");
    fprintf(stderr,"  T to list the files in an archive file\n\n");
    return -1;
  }

  MPI_Init(NULL,NULL);
  MPI_Comm_size(MPI_COMM_WORLD,&n_ranks);
  MPI_Comm_rank(MPI_COMM_WORLD,&my_rank);
  
  if(my_rank==0){
    arc_filename = args[2];
    switch(mode){
    case 'C':
      target_filename = args[3];
      fprintf(stderr,"Create mode.  Going to create an archive. \n");
      fprintf(stderr,"  archive file: %s\n",arc_filename);
      fprintf(stderr,"  target path: %s\n",target_filename);
      break;
    case 'X':
      target_filename = args[3];
      fprintf(stderr,"eXtract mode.  Extracting\n");
      fprintf(stderr,"  archive file: %s\n",arc_filename);
      fprintf(stderr,"  to target path: %s\n",target_filename);
      break;
    case 'T':
      fprintf(stderr,"Table-of-contents mode.  Going to list the directories and\n");
      fprintf(stderr,"  files contain in archive file: %s\n",arc_filename);
      break;
    default:
      fprintf(stderr,"Mode \"%c\" UNKNOWN!!\n",mode);
      return -2;
    }
    if((pad_filename=
	(char*)malloc(strlen(PARFU_BLOCKING_FILE_NAME)+1))==NULL){
      fprintf(stderr,"Could not allocate pad filename in rank zero!\n");
      return -10;
    }
    sprintf(pad_filename,PARFU_BLOCKING_FILE_NAME);
  }
  
  if((bcast_archive_file_name_length=(int**)malloc(sizeof(int*)))==NULL){
    fprintf(stderr,"rank %d could not allocation bcast_archive_file_name_length!\n",my_rank);
    MPI_Finalize();
    return 237;
  }
      
  if(((*bcast_archive_file_name_length)=(int*)malloc(sizeof(int)))==NULL){
    fprintf(stderr,"rank %d could not allocate *bcast_archive_file_name_length!\n",my_rank);
    MPI_Finalize();
    return 237;
  }
  // dummy assign to make SURE to initialize the value, even though it will be overwritten by the bcast
  **bcast_archive_file_name_length=0;
  

  switch(mode){
  case 'C':
    if(my_rank==0){
      fprintf(stderr,"About to search path: %s\n",target_filename);
      time(&timer_before);
      if((raw_list=parfu_build_file_list_from_directory(target_filename,
							0, // don't follow symlinks
							&raw_file_entries))==NULL){
	fprintf(stderr," got error from parfu_build_file_list_from_directory\n");
	fprintf(stderr,"  when constructing initial file list!\n");
	return 333;
      }
      time(&timer_after);
      fprintf(stdout," ***** Building the directory list took %4.1f seconds.\n",
	      elapsed_time_for_list=difftime(timer_after,timer_before));

      time(&timer_before);
      if((return_val=
	  parfu_ffel_fill_rel_filenames(raw_list,
					target_filename))){
	fprintf(stderr,"got error from parfu_ffel_fill_rel_filenames!\n");
	fprintf(stderr,"  return value: %d\n",return_val);
	return 337;
      }
      time(&timer_after);
      fprintf(stdout," ***** Filling in relative filenames took %4.1f seconds.\n",
	      difftime(timer_after,timer_before));
      
      if(debug){
	fprintf(stderr,"unsorted file list: \n");
	parfu_dump_fragment_entry_list(raw_list,stdout);
	fprintf(stderr,"finishing dumping list.\n");
      }
      
      time(&timer_before);
      parfu_qsort_entry_list(raw_list);
      time(&timer_after);
      fprintf(stderr," sorting directory list took %4.1f seconds.\n",
	      difftime(timer_after,timer_before));
      
      if(debug){
	fprintf(stderr,"sorted file list: \n");
	parfu_dump_fragment_entry_list(raw_list,stdout);
	fprintf(stderr,"finishing dumping list.\n");
      }

      /*
	time(&timer_before);
	if((split_list=
	parfu_set_offsets_and_split_ffel(raw_list,blocking_size,
	bucket_size,
	pad_filename,
	&rank_buckets_total,
	-1
	))==NULL){
	fprintf(stderr," error from parfu_set_offsets_and_split_ffel!\n");
	return 334;
	}					   					   
	time(&timer_after);
	fprintf(stderr," splitting lists took %4.1f seconds.\n",
	difftime(timer_after,timer_before));
	
	time(&timer_before);      
	if((rank_call_list=parfu_rank_call_list_from_ffel(split_list,
	&rank_call_list_length))==NULL){
	fprintf(stderr,"got error from parfu_rank_call_list_from_ffel()!\n");
	return 335;
	}
	time(&timer_after);
	fprintf(stderr," creating rank call list took %4.1f seconds.\n",
	difftime(timer_after,timer_before));
	
	if((split_list_buffer=
	parfu_fragment_list_to_buffer(split_list,
	&shared_split_list_buffer_length,
	bucket_size,
	0 // is not archive catalog
	))==NULL){
	fprintf(stderr,"got error from parfu_fragment_list_to_buffer()!\n");
	return 338;
	}
      */
      
    } // if my_rank==0
    if(my_rank==0)
      (**bcast_archive_file_name_length)=strlen(arc_filename)+1;
    else
      (**bcast_archive_file_name_length)=0;
    MPI_Bcast(*bcast_archive_file_name_length,1,MPI_INT,0,MPI_COMM_WORLD);
    if((bcast_archive_file_name=(char*)malloc(**bcast_archive_file_name_length))==NULL){
      fprintf(stderr,"rank %d cannot allocate bcast archive file name!\n",my_rank);
      MPI_Finalize();
      return 238;
    }
    if(my_rank==0){
      sprintf(bcast_archive_file_name,"%s",arc_filename);
    }
    MPI_Bcast(bcast_archive_file_name,**bcast_archive_file_name_length,MPI_CHAR,0,MPI_COMM_WORLD);

    /*
      MPI_Bcast(&shared_split_list_buffer_length,1,MPI_LONG_INT,0,MPI_COMM_WORLD);
      if((shared_split_list_buffer=
      (char*)malloc(shared_split_list_buffer_length))==NULL){
      fprintf(stderr,"rank %d could not allocate shared split list buffer!\n",
      my_rank);
      MPI_Finalize();
      return 247;
      }
      if(my_rank==0)
      memcpy(shared_split_list_buffer,
      split_list_buffer,
      shared_split_list_buffer_length);
      MPI_Bcast(shared_split_list_buffer,
      shared_split_list_buffer_length,
      MPI_CHAR,
      0,MPI_COMM_WORLD);
      if((shared_split_list=
      parfu_buffer_to_file_fragment_list(shared_split_list_buffer,
      &shared_bucket_size,
      0 // not archive catalog
      ))==NULL){
      fprintf(stderr,"got error from parfu_buffer_t_file_fragment_list()!\n");
      return 242;
      }
    */
    MPI_Barrier(MPI_COMM_WORLD);
    if(my_rank==0){
      fprintf(stderr," [***] about to really move data\n");
      time(&timer_before);
    }
    if((return_val=
	parfu_wtar_archive_list_to_singeFP(raw_list,
					   n_ranks,my_rank,
					   blocking_size,
					   bucket_size,
					   -1, // no max files per bucket
					   bcast_archive_file_name))){
      fprintf(stderr,"rank %d got error (%d) from parfu_wtar_archive_list_to_singeFP!\n",
	      my_rank,return_val);
      return 243;
    }
    MPI_Barrier(MPI_COMM_WORLD);
    if(my_rank==0){
      fprintf(stderr,"All ranks finished transferring data!\n");
      time(&timer_after);
      elapsed_time_to_transfer=difftime(timer_after,timer_before);
      fprintf(stderr," Timer summary:\n");
      fprintf(stderr,"CATALOG_TIME=%.1f\n",elapsed_time_for_list);
      fprintf(stderr,"TRANSFER_TIME=%.1f\n",elapsed_time_to_transfer);
    }
    break; // end of "Create" mode section
  } // switch(mode)

  MPI_Finalize();
  return 0;
}
