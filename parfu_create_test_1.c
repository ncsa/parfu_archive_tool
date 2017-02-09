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
  int *total_entries=NULL;

  parfu_file_fragment_entry_list_t *my_list=NULL;
  parfu_file_fragment_entry_list_t *second_list=NULL;
  parfu_file_fragment_entry_list_t *expanded_list=NULL;

  //  char *expanded_list_buffer;
  //  int expanded_list_buffer_size;

  char *mid_buffer=NULL;
  long int mid_buffer_length;
  int return_exponent;

  //  MPI_File *archive_file_MPI=NULL;
  //  MPI_Status my_MPI_Status;
  // MPI_Info my_Info;
  //  MPI_File target_file_ptr=NULL;
  int is_cat_test=0;
  char my_command='X';
  int valid_command=0;

  char *command_argument;
  char *directory_argument;
  char *archive_file_name;

  // MPI stuff
  int n_ranks;
  int my_rank;

  if(nargs < 3){
    fprintf(stderr,"\n\n");
    fprintf(stderr,"usage:\n");
    fprintf(stderr,"  parfu_create_test_1 <command> <target_dir> <archive1>.pfu [<archive2>.pfu]\n");
    fprintf(stderr,"  <command> sets up what we're going to do.  Valid commands are:\n");
    fprintf(stderr,"    \"cat\" catalog test.\n");
    fprintf(stderr,"  <target_dir> is the directory to scan.\n");
    fprintf(stderr,"  files from target dir will be stored in the specified *.pfu archive\n");
    fprintf(stderr,"  one .pfu is required for archiving; additional archive files are optional.\n");
    fprintf(stderr,"\n\n");
    return 1;
  }
  
  command_argument=args[1];
  directory_argument=args[2];
  archive_file_name=args[3];

  total_entries=malloc(sizeof(int));

  if(!strcmp(command_argument,"cat")){
    fprintf(stderr,"Command: catalog test.\n");
    is_cat_test=1;
    valid_command=1;
  }
  if(!strcmp(command_argument,"C")){
    my_command='C';
    valid_command=1;
  }
  if(!valid_command){
    fprintf(stderr,"The command %s is not valid.  Run this program with no arguments\n",
	    command_argument);
    fprintf(stderr,"  for a list of valid commands.\n");
    return 1;
  }
  
  if(is_cat_test){
    
    fprintf(stderr,"About to build directory from: %s\n",directory_argument);
    fprintf(stderr,"  and data will be archived in file %s\n",archive_file_name);
    if((my_list=parfu_build_file_list_from_directory(directory_argument,0,total_entries))==NULL){
      fprintf(stderr,"There was an error in parfu_build_file_list_from_directory!!!\n");
      return 1;
    }
    fprintf(stderr,"Successfully built directory listing from >%s< with %d entries.\n",
	    directory_argument,*total_entries);
    
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
    return 0;
  }

  // Any single node tests have happened above and exited cleanly
  // so now we're safe to begin MPI initialization
  MPI_Init(NULL,NULL);
  MPI_Comm_size(MPI_COMM_WORLD,&n_ranks);
  MPI_Comm_rank(MPI_COMM_WORLD,&my_rank);
  
  if(my_command == 'C'){
    if(my_rank==0)fprintf(stderr,"Command: Create archive.\n");
    if(my_rank==0){
      if((my_list=parfu_build_file_list_from_directory(directory_argument,0,total_entries))==NULL){
	fprintf(stderr,"There was an error in parfu_build_file_list_from_directory!!!\n");
	return 1;
      }
      fprintf(stderr,"Successfully built directory listing from >%s< with %d entries.\n",
	      directory_argument,*total_entries);
      parfu_qsort_entry_list(my_list);
      
    }
    if(parfu_archive_1file_singFP(my_list,archive_file_name,
				  n_ranks,my_rank,
				  PARFU_DEFAULT_MAX_BLOCK_SIZE_EXPONENT,
				  536870912,
				  PARFU_DEFAULT_BLOCKS_PER_FRAGMENT)){
      fprintf(stderr,"function parfu_archive_1file_singFP failed!!!\n");
      fprintf(stderr,"exiting.\n");
      return 1;
    }
    MPI_Barrier(MPI_COMM_WORLD);
    if(my_rank==0) fprintf(stderr,"got past parfu_archive_1file_singFP()!\n");
  }  

  MPI_Barrier(MPI_COMM_WORLD);
  MPI_Finalize();

  return 0;
}

