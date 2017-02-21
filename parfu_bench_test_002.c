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
void parfu_usage(void);

int main(int argc, char *argv[]){
  //  char mode='Z'; // 'Z' is the "unset" value.  Valid modes are 'C' and 'X' 
  //  int i;
  parfu_behavior_control_t *marching_orders=NULL;
  //  int opt_integer;
  int total_runs;

  int ind_E;
  int ind_B;
  int ind_N;
  int ind_arc;
  int target_for_data;
  FILE *data_file=NULL;

  if(argc<2){
    fprintf(stderr,"You didn't enter any command-line flags or arguments\n");
    fprintf(stderr,"  Printing usage explanation:\n");
    parfu_usage();
    exit(2);
  }

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

  // count the runs and confirm to the user what we're going to do
  total_runs=1;

  // count the number of archive files 
  if( marching_orders->n_archive_files < 1){
    fprintf(stderr,"You must specify at least one archive file!\n");
    fprintf(stderr,"  Exiting.\n");
    exit(3);
  }
  if( marching_orders->mode=='X' && 
      marching_orders->n_archive_files > 1 ){
    fprintf(stderr,"In extract mode, you must ONLY specify one archive file!\n");
    fprintf(stderr,"  You specified %d files.  Aborting.\n",
	    marching_orders->n_archive_files);
    exit(4);
  }
  fprintf(stderr,"You specified %d archive files.\n",
	  marching_orders->n_archive_files);
  total_runs *= marching_orders->n_archive_files;
  
  // count the number of specified min block size exponents
  if(marching_orders->n_min_block_exponent_values){
    fprintf(stderr,"You specified %d values of min block size exponent.\n",
	    marching_orders->n_min_block_exponent_values);
  }
  else{
    marching_orders->n_min_block_exponent_values=1;
    marching_orders->min_block_exponent_values[0]=
      PARFU_DEFAULT_MIN_BLOCK_SIZE_EXPONENT;
    fprintf(stderr,"No values of min block size exponent specified, so we will\n");
    fprintf(stderr,"  be using the default, %d.\n",
	    marching_orders->min_block_exponent_values[0]); 
  }
  total_runs *= marching_orders->n_min_block_exponent_values;
  
  // count the number of specified blocks-per-fragment values
  if(marching_orders->n_blocks_per_fragment_values){
    fprintf(stderr,"You specified %d values of blocks per fragment.\n",
	    marching_orders->n_blocks_per_fragment_values);
  }
  else{
    marching_orders->n_blocks_per_fragment_values=1;
    marching_orders->blocks_per_fragment_values[0]=
      PARFU_DEFAULT_BLOCKS_PER_FRAGMENT;
    fprintf(stderr,"No values of blocks per fragment specified, so we will\n");
    fprintf(stderr,"  be using the default, %d.\n",
	    marching_orders->blocks_per_fragment_values[0]); 
  }
  total_runs *= marching_orders->n_blocks_per_fragment_values;

  
  // finally count the number of iterations per above combination of parameters
  if(marching_orders->yn_iterations_argument){
    fprintf(stderr,"You specified %d iterations per parameter combination.\n",
	    marching_orders->n_iterations);
  }
  else{
    fprintf(stderr,"No # of iterations specified, so 1 run\n");
    fprintf(stderr,"  per combination of parameters specified.\n");
    marching_orders->yn_iterations_argument=1;
    marching_orders->n_iterations=1;
  }
  fprintf(stderr,"\n");
  fprintf(stderr,"So we'll being running a total of %d times.\n\n",total_runs);
  total_runs *= marching_orders->n_iterations;
  
  target_for_data=0;
  if(marching_orders->yn_data_to_stdout){
    fprintf(stderr,"Timing data will be written to STDOUT.\n");
    target_for_data=1;
  }
  if(marching_orders->data_output_file){
    if((data_file=fopen(marching_orders->data_output_file,"a"))==NULL){
      fprintf(stderr,"Could not open file >%s< for appending data!\n",
	      marching_orders->data_output_file);
      exit(3);
    }
    fprintf(stderr,"Timing data will be written to file >%s<\n",
	    marching_orders->data_output_file);
    target_for_data=1;
  }
  if(!target_for_data){
    fprintf(stderr,"WARNING!  You have not specified a destination for\n");
    fprintf(stderr,"timing data.  None will be written.\n");
  }
  
  fprintf(stderr,"\nNow looping through trials.\n");
  for(ind_arc=0;ind_arc<marching_orders->n_archive_files;ind_arc++){
    fprintf(stderr,"  Using archive file: %s\n",
	    marching_orders->archive_files[ind_arc]);
    for(ind_E=0;ind_E<marching_orders->n_min_block_exponent_values;ind_E++){
      for(ind_B=0;ind_B<marching_orders->n_blocks_per_fragment_values;ind_B++){
	for(ind_N=0;ind_N<marching_orders->n_iterations;ind_N++){
	  fprintf(stderr,"    Using E=%2d, B=%4d, trial #=%3d\n",
		  marching_orders->min_block_exponent_values[ind_E],
		  marching_orders->blocks_per_fragment_values[ind_B],
		  ind_N);
	  
	}
      }
    }
  }
  fprintf(stderr,"\n\n");
  
  // now actually do stuff
  
  exit(0);
}

void parfu_usage(void){
  fprintf(stderr,"\nparfu_bench_test_002 usage:\n\n");
  
  fprintf(stderr,"  Flags for this version are Gnu-style:\n");
  fprintf(stderr,"  -A     sets \"A\" to true\n");
  fprintf(stderr,"      OR\n");
  fprintf(stderr,"  -A xxx    gives \"A\" the value \"xxx\"\n");
  fprintf(stderr,"Allowed arguments:\n");
  fprintf(stderr,"  -C    to Create an archive\n");
  fprintf(stderr,"  -X    to eXtract an archive\n");
  fprintf(stderr,"        (only ONE of -C or -X may be specified\n");
  fprintf(stderr,"  -d    (Creating: target directory to create an archive of,\n");
  fprintf(stderr,"         eXtracting: directory to extract into) (only one -d value allowed.)\n\n");
  fprintf(stderr,"  -f my_arc_file.pfu   (File to archive into.  For Create\n");
  fprintf(stderr,"        mode, may specify more than one to test different\n");
  fprintf(stderr,"        target directory characteristics, like stripe counts.\n");
  fprintf(stderr,"        In eXtract mode, only one is allowed.)\n\n");
  fprintf(stderr,"  -e EE  Min Block Size Exponent value \"EE\" to test.\n");
  fprintf(stderr,"         my specify any number of values. If nome specified, uses internal default.\n\n");
  fprintf(stderr,"  -B BB  Blocks Per Fragment value \"BB\" to test.\n");
  fprintf(stderr,"         Specify any number of values. If none specified,\n");
  fprintf(stderr,"         uses one value, the internal default.\n\n");
  fprintf(stderr,"  -N NN  Number of trials per parameter setup to run.\n");
  fprintf(stderr,"         Default is 1.\n\n");
  fprintf(stderr,"  -S     Outputs timing results to STDOUT.\n\n");
  fprintf(stderr,"  -L logfile.out   Outputs timing results to specified file\n\n");
}
