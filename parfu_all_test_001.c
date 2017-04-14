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

#include <parfu_primary.h>

int main(int nargs, char *args[]){

  ////////////////////////
  // This version of the code can do sweeps of fragment sizes
  // by default, this capability is not enabled in 0.4.0 alpha release
  
  // to do sweep, set these to, say, start=2, limit=800
  int blocks_per_fragment_start=128;
  int blocks_per_fragment_limit=129;
  int blocks_per_fragment_mult=2;

  // to do more than one rep per fragment size, set this > 1
  int blocks_per_fragment_reps=1;

  // end of user tweakable hard-coded values
  ////////////////////////

  int *total_entries=NULL;

  parfu_file_fragment_entry_list_t *my_list=NULL;
  //  parfu_file_fragment_entry_list_t *second_list=NULL;
  //  parfu_file_fragment_entry_list_t *expanded_list=NULL;

  //  char *expanded_list_buffer;
  //  int expanded_list_buffer_size;

  //  char *mid_buffer=NULL;
  //  long int mid_buffer_length;
  int return_exponent;

  //  MPI_File *archive_file_MPI=NULL;
  //  MPI_Status my_MPI_Status;
  // MPI_Info my_Info;
  //  MPI_File target_file_ptr=NULL;
  int is_cat_test=0;
  char *my_command=NULL;
  int valid_command=0;

  char *command_argument;
  char *directory_argument;
  char *local_archive_file_name=NULL;
  char *bcast_archive_file_name=NULL;

  // int local_archive_file_name_length;
  int *bcast_archive_file_name_length=NULL;

  // MPI stuff
  int n_ranks;
  int my_rank;
  int default_transfer_buffer_size=1000000000; // 1 billion ~ 1 GB

  time_t timer_before,timer_after;

  int bpf;
  char *bpf_filename=NULL;
  int bpf_filename_length;
  int sub;

  if(nargs < 3){
    fprintf(stderr,"\n\n");
    fprintf(stderr,"usage:\n");
    fprintf(stderr,"  parfu_all_test_001 <command> <target_dir> <archive1>.pfu [<archive2>.pfu]\n");
    fprintf(stderr,"  <command> sets up what we're going to do.  Valid commands are:\n");
    fprintf(stderr,"    \"cat\": catalog test.\n");
    fprintf(stderr,"    \"C\":  create an archive from a specified directory.\n");
    fprintf(stderr,"    \"X\":  extract an archive to a target directory.\n");
    fprintf(stderr,"\n\n");
    return 1;
  }

  MPI_Init(NULL,NULL);
  MPI_Comm_size(MPI_COMM_WORLD,&n_ranks);
  MPI_Comm_rank(MPI_COMM_WORLD,&my_rank);

  if((my_command=(char*)malloc(sizeof(char)))==NULL){
    fprintf(stderr,"rank %d could not allocate char for my_command!\n",my_rank);
    MPI_Finalize();
    return 1;
  }
  if((bcast_archive_file_name_length=(int*)malloc(sizeof(int)))==NULL){
    fprintf(stderr,"rank %d could not allocate bcast_archive_file_name_length!\n",my_rank);
    MPI_Finalize();
    return 237;
  }

  if(my_rank==0){
    command_argument=args[1];
    directory_argument=args[2];
    local_archive_file_name=args[3];
    *my_command='Z';
    
    
    if(!strcmp(command_argument,"cat")){
      fprintf(stderr,"Command: catalog test.\n");
      is_cat_test=1;
      valid_command=1;
    }
    if(!strcmp(command_argument,"C")){
      *my_command='C';
      valid_command=1;
    }
    if(!strcmp(command_argument,"X")){
      *my_command='X';
      valid_command=1;
    }
    if(!valid_command){
      fprintf(stderr,"The command >%s< is not valid.  Run this program with no arguments\n",
	      command_argument);
      fprintf(stderr,"  for a list of valid commands.\n");
      return 1;
    }
    
    *bcast_archive_file_name_length=strlen(local_archive_file_name)+1;
    
  } // if(my_rank==0)
  else{
    directory_argument=NULL;
  }

  MPI_Bcast(my_command,1,MPI_CHAR,0,MPI_COMM_WORLD);  
  MPI_Bcast(bcast_archive_file_name_length,1,MPI_INT,0,MPI_COMM_WORLD);
  if((bcast_archive_file_name=(char*)malloc(*bcast_archive_file_name_length))==NULL){
    fprintf(stderr,"rank %d cannot allocate bcast archive file name!\n",my_rank);
    MPI_Finalize();
    return 238;
  }
  if(my_rank==0){
    sprintf(bcast_archive_file_name,"%s",local_archive_file_name);
  }
  MPI_Bcast(bcast_archive_file_name,*bcast_archive_file_name_length,MPI_CHAR,0,MPI_COMM_WORLD);
  

  if( (my_rank==0) && (is_cat_test)){
    fprintf(stderr,"Catalog test is disabled in this version.  Sorry.\n");
    return 666;
  }

  /* 
  // this is the catalog test.  Commented out for version 0.4.0 alpha test release
  if( (my_rank==0) && (is_cat_test)){
  
  fprintf(stderr,"About to build directory from: %s\n",directory_argument);
  fprintf(stderr,"  and data will be archived in file %s\n",bcast_archive_file_name);
  time(&timer_before);
  total_entries=(int*)malloc(sizeof(int));
  if((my_list=parfu_build_file_list_from_directory(directory_argument,0,total_entries))==NULL){
  fprintf(stderr,"There was an error in parfu_build_file_list_from_directory!!!\n");
  return 1;
  }
  time(&timer_after);
  fprintf(stderr,"Successfully built directory listing from >%s< with %d entries.\n",
  directory_argument,*total_entries);
  free(total_entries);
  total_entries=NULL;
  fprintf(stdout," ***** Building the directory list took %4.1f seconds.\n",
  difftime(timer_after,timer_before));

  parfu_dump_fragment_entry_list(my_list,stderr);
  fprintf(stderr,"now sort:\n");
  parfu_qsort_entry_list(my_list);
  parfu_dump_fragment_entry_list(my_list,stderr);
  
  if((mid_buffer=parfu_fragment_list_to_buffer(my_list,
  &mid_buffer_length,
  PARFU_DEFAULT_MAX_BLOCK_SIZE_EXPONENT,
  0))
  ==NULL){
  fprintf(stderr,"help! failed to write stuff to buffer!!!\n");
  return 1;
  }
  
  fprintf(stderr,"\n\n   *********\n\n");
  fprintf(stderr,"%s",mid_buffer);
  fprintf(stderr,"\n\n   *********\n\n");
  
  fprintf(stderr,"about to create second list structure.\n");
  if((second_list=parfu_buffer_to_file_fragment_list(mid_buffer,
  &return_exponent,
  0))
  ==NULL){
  fprintf(stderr,"bummer.  failed to read buffer back into structure.\n");
  return 1;
  }
    
  fprintf(stderr,"now dumping new list that was populated from the buffer:\n");
  
  parfu_dump_fragment_entry_list(second_list,stderr);
  
  fprintf(stderr,"creating expanded list:\n");
  if((expanded_list=parfu_split_fragments_in_list(my_list,
  PARFU_DEFAULT_MIN_BLOCK_SIZE_EXPONENT,
  PARFU_DEFAULT_MAX_BLOCK_SIZE_EXPONENT,
  15))
  ==NULL){
  fprintf(stderr,"function parfu_split_fragments_in_list() returned NULL!\n");
  return 1;
  }
  fprintf(stderr,"Now dump the expanded list:\n");
  fprintf(stderr," list should hvae %d entries.\n",expanded_list->n_entries_full);
  parfu_dump_fragment_entry_list(expanded_list,stderr);
  
  fprintf(stderr,"check offsets:\n");
  parfu_check_offsets(expanded_list,stderr);
  
  fprintf(stderr,"cat test doesn't do MPI stuff.  Program ends.\n");
  MPI_Finalize();
  return 0;
  }
  */
  
  if(*my_command == 'C'){
    if(my_rank==0)fprintf(stderr,"Command: Create archive.\n");
    if(my_rank==0){
      time(&timer_before);
      total_entries=(int*)malloc(sizeof(int));
      if((my_list=parfu_build_file_list_from_directory(directory_argument,0,total_entries))==NULL){
	fprintf(stderr,"There was an error in parfu_build_file_list_from_directory!!!\n");
	return 1;
      }
      time(&timer_after);
      fprintf(stderr,"Successfully built directory listing from >%s< with %d entries.\n",
	      directory_argument,*total_entries);
      free(total_entries);
      total_entries=NULL;
      fprintf(stdout," ***** Building the directory list took %4.1f seconds.\n",
	      difftime(timer_after,timer_before));
      
      time(&timer_before);
      parfu_qsort_entry_list(my_list);
      time(&timer_after);
      fprintf(stderr," sorting directory list took %4.1f seconds.\n",
	      difftime(timer_after,timer_before));

      time(&timer_before);
      if(parfu_set_exp_offsets_in_ffel(my_list,PARFU_DEFAULT_MIN_BLOCK_SIZE_EXPONENT,
				       PARFU_DEFAULT_MAX_BLOCK_SIZE_EXPONENT)){
	fprintf(stderr,"had a problem with parfu_set_exp_offsets_in_ffel!\n");
	fprintf(stderr,"  exiting.\n");
	return 19;
      }
      time(&timer_after);

      fprintf(stdout," ***** populating offset list took %4.1f seconds.\n",
	      difftime(timer_after,timer_before));
      
    } // if(my_rank
    MPI_Barrier(MPI_COMM_WORLD);
    
    bpf_filename_length=strlen(bcast_archive_file_name)+8;
    if((bpf_filename=(char*)malloc(bpf_filename_length))==NULL){
      fprintf(stderr,"could not malloc bpf_filename!!!\n");
      return 17;
    }
    for(bpf=blocks_per_fragment_start;bpf<blocks_per_fragment_limit;
	bpf *= blocks_per_fragment_mult){
      for(sub=0;sub<blocks_per_fragment_reps;sub++){
	// +8: 
	//      +5 for bpf index:  _0000
        //      +3 for sub index:  _00
	if(bpf>blocks_per_fragment_start || sub>0){
	  sprintf(bpf_filename,"%s_%04d_%02d",bcast_archive_file_name,bpf,sub);
	}
	else{
	  sprintf(bpf_filename,"%s",bcast_archive_file_name);
	}
	if(my_rank==0) fprintf(stderr,"BPF=%5d , archive filename: >%s<\n",
			       bpf,bpf_filename);
	MPI_Barrier(MPI_COMM_WORLD);
	if(my_rank==0)       time(&timer_before);
	MPI_Barrier(MPI_COMM_WORLD);
	
	//      if(parfu_archive_1file_singFP(my_list,bcast_archive_file_name,
	//				    n_ranks,my_rank,
	//				    PARFU_DEFAULT_MAX_BLOCK_SIZE_EXPONENT,
	//				    536870912)){
	if(parfu_archive_1file_singFP(my_list,bpf_filename,
				      n_ranks,my_rank,
				      PARFU_DEFAULT_MAX_BLOCK_SIZE_EXPONENT,
				      536870912,
				      bpf,0)){
	  fprintf(stderr,"function parfu_archive_1file_singFP failed!!!\n");
	  fprintf(stderr,"rank %d exiting.\n",my_rank);
	  MPI_Finalize();
	  return 1;
	}
	MPI_Barrier(MPI_COMM_WORLD);
	if(my_rank==0){
	  time(&timer_after);
	  fprintf(stdout," ***** Archiving bpf=%d data took %4.1f seconds.\n",
		  bpf,
		  difftime(timer_after,timer_before));
	}
      } // for(sub=
    } // for(bpf=
    free(bpf_filename);
    bpf_filename=NULL;
  } // if(my_command == 'C'

  if(*my_command == 'X'){
    if(my_rank==0){
      fprintf(stderr,"Comand: Extract archive.\n");
      fprintf(stderr,"  extracting from archive file: >%s<\n",
	      bcast_archive_file_name);
      fprintf(stderr,"  extracting to target directory >%s<\n",
	      directory_argument);
      time(&timer_before);
    }
    MPI_Barrier(MPI_COMM_WORLD);
    if(my_rank==0)       time(&timer_before);
    MPI_Barrier(MPI_COMM_WORLD);    
    if(parfu_extract_1file_singFP(bcast_archive_file_name,
				  directory_argument,
				  n_ranks,my_rank,
				  &return_exponent,
				  default_transfer_buffer_size,
				  PARFU_DEFAULT_BLOCKS_PER_FRAGMENT,0)){
      fprintf(stderr,"parfu_extract_1file_singFP failed!!!\n");
      fprintf(stderr,"rank %d exiting.\n",my_rank);
      return 2;      
    }				    
    MPI_Barrier(MPI_COMM_WORLD);     
    if(my_rank==0){
      time(&timer_after);
      fprintf(stdout," ***** Total extracting the data took %4.1f seconds.\n",
	      difftime(timer_after,timer_before));
    }
  }
  
  MPI_Barrier(MPI_COMM_WORLD);
  MPI_Finalize();

  return 0;
}

