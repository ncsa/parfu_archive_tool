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

int main(int argc, char *argv[]){
  //  char mode='Z'; // 'Z' is the "unset" value.  Valid modes are 'C' and 'X' 
  //  int i;
  parfu_behavior_control_t *marching_orders=NULL;
  //  int opt_integer;
  int total_runs;

  if((marching_orders=parfu_parse_arguments(argc,argv))==NULL){
    fprintf(stderr,"There was an error parsing the command line.  Exiting!\n");
    exit(2);
  }

  // Sanity check; tell the user what we understood them to tell us to do
  fprintf(stderr,"Finished parsing command line.\n\n");
  switch(marching_orders->mode){
  case 'X':
    fprintf(stderr,"We e(X)tracting an archive.\n");
    break;
  case 'C':
    fprintf(stderr,"We are (C)reating an archive.\n");
    break;
  default:
    fprintf(stderr,"You must specify mode \"-C\" (create) or \"-X\" (extract)!\n");
    fprintf(stderr,"  We don't know what to do; aborting.\n");
    exit(2);
    break;
  }

  total_runs=1;
  if(marching_orders->n_min_block_exponent_values){
    fprintf(stderr,"We have %d specified values of min block size exponent.\n",
	    marching_orders->n_min_block_exponent_values);
    total_runs *= marching_orders->n_min_block_exponent_values;
  }
  else{
    marching_orders->n_min_block_exponent_values=1;
    marching_orders->min_block_exponent_values[0]=
      PARFU_DEFAULT_MIN_BLOCK_SIZE_EXPONENT;
    fprintf(stderr,"No values of min block size exponent specified, so we will\n");
    fprintf(stderr,"  be using the default, %d.\n",
	    marching_orders->min_block_exponent_values[0]);
    
  }
  
  if(marching_orders->yn_iterations_argument){
    fprintf(stderr,"You specified %d iterations per parameter combination.\n",
	    marching_orders->n_iterations);
    total_runs *= marching_orders->n_iterations;
  }
  else{
    fprintf(stderr,"No # of iterations specified, so 1 run\n");
    fprintf(stderr,"  per combination of parameters specified.\n");
    marching_orders->yn_iterations_argument=1;
    marching_orders->n_iterations=1;
  }
  fprintf(stderr,"\n");
  fprintf(stderr,"So we'll being running a total of %d times.\n\n",total_runs);

  if(marching_orders->trial_run){
    fprintf(stderr,"You specified the \"-T\" flag for a trial run.\n");
    fprintf(stderr,"  The above list says what we WOULD have done.\n");
    fprintf(stderr,"  Exiting now.\n");
    exit(0);
  }
  
  // now actually do stuff
  
  exit(0);
}

