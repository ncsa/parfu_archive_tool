////////////////////////////////////////////////////////////////////////////////
// 
//  University of Illinois/NCSA Open Source License
//  http://otm.illinois.edu/disclose-protect/illinois-open-source-license
//  
//  Parfu is copyright © 2017, The Trustees of the University of Illinois. 
//  All rights reserved.
//  
//  Parfu was developed by:
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

  char *mid_buffer=NULL;
  long int mid_buffer_length;
  int return_exponent;
  
  if(nargs < 2){
    fprintf(stderr,"usage:\n");
    fprintf(stderr,"  parfu_file_util_test <target_dir>\n");
    fprintf(stderr,"  where <target_dir> is the directory to scan.\n");
    return 1;
  }
  
  total_entries=malloc(sizeof(int));
  fprintf(stderr,"About to build directory from: %s\n",args[1]);
  
  if((my_list=parfu_build_file_list_from_directory(args[1],0,total_entries))==NULL){
    fprintf(stderr,"There was an error in parfu_build_file_list_from_directory!!!\n");
    return 1;
  }
  fprintf(stderr,"Successfully built directory listing from >%s<.\n",args[1]);

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
  

  return 0;
}

