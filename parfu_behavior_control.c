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

parfu_behavior_control_t *parfu_init_behavior_control_raw(void){
  parfu_behavior_control_t *out=NULL;
  out=parfu_init_behavior_control_raw(DEFAULT_N_VALUES_IN_BEHAVIOR_CONTROL_ARRAY);
  if(out==NULL){
    fprintf(stderr,"parfu_init_behavior_control_raw: \n");
    fprintf(stderr,"   the _raw() function returned NULL!\n");
    fprintf(stderr,"   we told it to parition arrays for %d values.\n", 
	    DEFAULT_N_VALUES_IN_BEHAVIOR_CONTROL_ARRAY);
  }
  return out;
}

parfu_behavior_control_t *parfu_init_behavior_control_raw(int array_len){
  parfu_behavior_control_t *my_control=NULL;
  int i;

  if((my_control=malloc(sizeof(my_control)))==NULL){
    fprintf(stderr,"parfu_init_behavior_control_raw: \n");
    fprintf(stderr,"  could not malloc behavior control structure!\n");
    return NULL;
  }
  if(array_len<1){
    fprintf(stderr,"parfu_init_behavior_control_raw: \n");
    fprintf(stderr,"  array_len=%d!!  This is an error.\n");
    return NULL;
  }

  my_control->yn_iterations_argument=0;
  my_control->n_iterations=-1;
  my_control->array_len=array_len;

  my_control->n_archive_files=0;
  if(((my_control->archive_files)=
      malloc(sizeof(char*)*array_len))==NULL){
    fprintf(stderr,"parfu_init_behavior_control_raw: \n");
    fprintf(stderr,"  Could not malloc archive file name array!!\n");
    return NULL;
  }
  for(i=0;i<array_len;i++){
    my_control->archive_files[i]=NULL;
  }
 
  my_control->n_min_block_exponent_values=0;
  if(((my_control->min_block_exponent_values)=
      malloc(sizeof(int)*array_len))==NULL){
    fprintf(stderr,"parfu_init_behavior_control_raw: \n");
    fprintf(stderr,"  Could not min_block_exp_values array!!\n");
    return NULL;
  }
    
  my_control->n_blocks_per_fragment_values=0;
  if(((my_control->blocks_per_fragment_values)=
      malloc(sizeof(int)*array_len))==NULL){
    fprintf(stderr,"parfu_init_behavior_control_raw: \n");
    fprintf(stderr,"  Could not blocks_per_fragment_values array!!\n");
    return NULL;
  }
  

  
}
