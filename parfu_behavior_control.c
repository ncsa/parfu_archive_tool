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

parfu_behavior_control_t *parfu_init_behavior_control(void){
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

int parfu_behav_extend_array(parfu_behavior_control_t *behav){
  int new_length;
  size_t new_size;
  void *new_ptr;
  int i;

  if(behav==NULL){
    fprintf(stderr,"parfu_behav_extend_array: \n");
    fprintf(stderr,"We were passed a NULL structure!\n");
    return 1;
  }
  
  new_length = (behav->array_len)*2;
  if(new_length < 1 || new_length <= behav->array_len){
    fprintf(stderr,"parfu_behav_extend_array: \n");
    fprintf(stderr,"failed to sensibly compute new array length!\n");
    fprintf(stderr,"old length=%d\n",behav->array_len);
    fprintf(stderr,"new length=%d\n",new_length);
    fprintf(stderr,"  Should never happen!\n");
    return 1;
  }

  // extend archive_files pointer array
  new_size = sizeof(char)*new_length;
  if((new_ptr=realloc(behav->archive_files,new_size))==NULL){
    fprintf(stderr,"parfu_behav_extend_array: \n");
    fprintf(stderr,"  failed to realloc() archive_files array!!\n");
    return 1;
  }
  behav->archive_files=new_ptr;
  // initialize new half of values with NULL
  for(i=behav->array_len;i<new_length;i++){
    behav->archive_files[i]=NULL;
  }
  
  // extend min block exponents array
  new_size = sizeof(int)*new_length;
  if((new_ptr=realloc(behav->min_block_exponent_values,new_size))==NULL){
    fprintf(stderr,"parfu_behav_extend_array: \n");
    fprintf(stderr,"  failed to realloc() min_block_exponent_values array!!\n");
    return 1;
  }
  behav->min_block_exponent_values=new_ptr;

  // extend blocks per fragments array
  new_size = sizeof(int)*new_length;
  if((new_ptr=realloc(behav->blocks_per_fragment_values,new_size))==NULL){
    fprintf(stderr,"parfu_behav_extend_array: \n");
    fprintf(stderr,"  failed to realloc() blocks_per_fragment_values array!!\n");
    return 1;
  }
  behav->blocks_per_fragment_values=new_ptr;
  
  behav->array_len = new_length;

  return 0;
}

parfu_behavior_control_t *parfu_init_behavior_control_raw(int array_len){
  parfu_behavior_control_t *my_control=NULL;
  int i;
  size_t new_size;
  
  //  fprintf(stderr,"INIT: %d\n",array_len);
  if((my_control=malloc(sizeof(parfu_behavior_control_t)))==NULL){
    fprintf(stderr,"parfu_init_behavior_control_raw: \n");
    fprintf(stderr,"  could not malloc behavior control structure!\n");
    return NULL;
  }
  if(array_len<1){
    fprintf(stderr,"parfu_init_behavior_control_raw: \n");
    fprintf(stderr,"  array_len=%d!!  This is an error.\n",array_len);
    return NULL;
  }

  my_control->trial_run=0;
  my_control->mode='\0';
  my_control->yn_iterations_argument=0;
  my_control->n_iterations=-1;
  my_control->overwrite_archive_file=0;
  my_control->array_len=array_len;

  my_control->n_archive_files=0;
  new_size = sizeof(char*) * array_len;
  //  fprintf(stderr,"ABOUT_TO: %d\n",new_size);
  if(((my_control->archive_files)=
      malloc(new_size))==NULL){
    fprintf(stderr,"parfu_init_behavior_control_raw: \n");
    fprintf(stderr,"  Could not malloc archive file name array!!\n");
    return NULL;
  }
  //  fprintf(stderr,"MALLOCED\n");
  for(i=0;i<array_len;i++){
    my_control->archive_files[i]=NULL;
  }
  //  fprintf(stderr,"MIDDLE\n");
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
  //  fprintf(stderr,"RETURN\n");
  return my_control;
}

int parfu_behav_add_min_expon(parfu_behavior_control_t *behav,
			      int in_exp){
  int new_index;

  if(behav==NULL){
    fprintf(stderr,"parfu_behav_add_min_expon:\n");
    fprintf(stderr,"  control structure is NULL!\n");
    return 1;
  }
  if(in_exp < PARFU_ABSOLUTE_MIN_BLOCK_SIZE_EXPONENT || 
     in_exp > PARFU_LARGEST_ALLOWED_MAX_BLOCK_SIZE_EXPONENT ){
    fprintf(stderr,"parfu_behav_add_min_expon:\n");
    fprintf(stderr,"  given exponent (%d) invalid!\n",in_exp);
    fprintf(stderr,"  It must be between %d and %d.\n",
	    PARFU_ABSOLUTE_MIN_BLOCK_SIZE_EXPONENT,
	    PARFU_LARGEST_ALLOWED_MAX_BLOCK_SIZE_EXPONENT);
    return 1;
  }
  new_index=behav->n_min_block_exponent_values;
  if(new_index == behav->array_len){
    if(parfu_behav_extend_array(behav)){
      fprintf(stderr,"parfu_behav_add_arc_file:\n");
      fprintf(stderr,"  could not extend array for new min exp!\n");
      return 1;
    }
  }
  behav->min_block_exponent_values[new_index]=in_exp;
  (behav->n_min_block_exponent_values)++;
  return 0;
}

int parfu_behav_add_bl_per_frag(parfu_behavior_control_t *behav,
				int in_bpf){
  int new_index;

  if(behav==NULL){
    fprintf(stderr,"parfu_behav_add_bl_per_frag:\n");
    fprintf(stderr,"  control structure is NULL!\n");
    return 1;
  }
  if(in_bpf < PARFU_MIN_ALLOWED_BLOCKS_PER_FRAGMENT || 
     in_bpf > PARFU_MAX_ALLOWED_BLOCKS_PER_FRAGMENT ){
    fprintf(stderr,"parfu_behav_add_bl_per_frag:\n");
    fprintf(stderr,"  blocks per fragment (%d) invalid!\n",in_bpf);
    fprintf(stderr,"  It must be between %d and %d.\n",
	    PARFU_MIN_ALLOWED_BLOCKS_PER_FRAGMENT,
	    PARFU_MAX_ALLOWED_BLOCKS_PER_FRAGMENT);
    return 1;
  }
  new_index=behav->n_blocks_per_fragment_values;
  if(new_index == behav->array_len){
    if(parfu_behav_extend_array(behav)){
      fprintf(stderr,"parfu_behav_add_bl_per_frag:\n");
      fprintf(stderr,"  could not extend array for new min exp!\n");
      return 1;
    }
  }
  behav->blocks_per_fragment_values[new_index]=in_bpf;
  (behav->n_blocks_per_fragment_values)++;
  return 0;
}

int parfu_behav_add_arc_file(parfu_behavior_control_t *behav,
			     char *in_filename){
  int new_index;
  int in_strlen;
  if(behav==NULL){
    fprintf(stderr,"parfu_behav_add_arc_file:\n");
    fprintf(stderr,"  control structure is NULL!\n");
    return 1;
  }
  if(in_filename==NULL){
    fprintf(stderr,"parfu_behav_add_arc_file:\n");
    fprintf(stderr,"  in_filename is NULL!\n");
    return 1;
  }
  new_index=behav->n_archive_files;
  if(new_index == behav->array_len){
    if(parfu_behav_extend_array(behav)){
      fprintf(stderr,"parfu_behav_add_arc_file:\n");
      fprintf(stderr,"  could not extend array for new arc file!\n");
      return 1;
    }
  }
  in_strlen=strlen(in_filename);
  if((behav->archive_files[new_index]=malloc(sizeof(char)*in_strlen))==NULL){
    fprintf(stderr,"parfu_behav_add_arc_file:\n");
    fprintf(stderr,"  could not allocate string!\n");
    return 1;
  }
  memcpy(behav->archive_files[new_index],in_filename,in_strlen);
  (behav->n_archive_files)++;
  return 0;
}

parfu_behavior_control_t *parfu_parse_arguments(int my_argc, char *my_argv[]){
  parfu_behavior_control_t *my_orders=NULL;
  int i;
  int next_opt;
  int opt_integer;

  // fprintf(stderr,"begining parfu_parse_arguments.\n");
  if((my_orders=parfu_init_behavior_control())==NULL){
    fprintf(stderr,"parfu_parse_arguments:\n");
    fprintf(stderr,"Could not initiate behavior control module.\n");
    fprintf(stderr,"  Catastrophic failure!!!\n");
    return NULL;
  }
  
  // parse command line option arguments 
  // (options of the form either:
  //       -A 
  // or    -A value
  while((next_opt=getopt(my_argc,my_argv,"CXf:F:d:N:e:B:T")) != -1){
    //    fprintf(stderr,"option: %c\n",(char)next_opt);
    switch(next_opt){
    case 'C':
    case 'X':
      if(my_orders->mode != '\0'){
	fprintf(stderr,"parfu_parse_arguments:\n");
	fprintf(stderr,"You must only specify \"C\" or \"X\" ONCE total!\n");
	fprintf(stderr,"Exiting.\n");
	return NULL;
      }
      my_orders->mode=next_opt;
      break;
    case 'T':
      //      fprintf(stderr,"The \"-T\" option indicates trial run.\n");
      my_orders->trial_run=1;
      break;
    case 'F':
      my_orders->overwrite_archive_file=1;
      fprintf(stderr,"Option \"-F\" indicates overwriting existing archivle files.\n");
    case 'f':
      fprintf(stderr,"Adding archive file: %s\n",optarg);
      if(parfu_behav_add_arc_file(my_orders,optarg)){
	fprintf(stderr,"parfu_parse_arguments:\n");
	fprintf(stderr,"Failed to add archive file >%s< to list!\n",
		optarg);
	return NULL;
      }
      break;
    case 'e':
      opt_integer = atoi(optarg);
      if( opt_integer < PARFU_ABSOLUTE_MIN_BLOCK_SIZE_EXPONENT || 
	  opt_integer > PARFU_LARGEST_ALLOWED_MAX_BLOCK_SIZE_EXPONENT ){
	fprintf(stderr,"parfu_parse_arguments:\n");
	fprintf(stderr,"The min block size exponent you specified, %d,\n",
		opt_integer);
	fprintf(stderr,"  is INVALID!  It must be between %d and %d.  Aborting.\n",
		PARFU_ABSOLUTE_MIN_BLOCK_SIZE_EXPONENT,
		PARFU_LARGEST_ALLOWED_MAX_BLOCK_SIZE_EXPONENT);
	return NULL;
      }
      if(parfu_behav_add_min_expon(my_orders,opt_integer)){
	fprintf(stderr,"parfu_parse_arguments:\n");
	fprintf(stderr,"Failed to add min block size exponent %d to list!\n",
		opt_integer);
	return NULL;
      }
      break;
    case 'B':
      opt_integer = atoi(optarg);
      if( opt_integer < PARFU_MIN_ALLOWED_BLOCKS_PER_FRAGMENT || 
	  opt_integer > PARFU_MAX_ALLOWED_BLOCKS_PER_FRAGMENT ){
	fprintf(stderr,"parfu_parse_arguments:\n");
	fprintf(stderr,"The blocks-per-fragment you specified: %d,\n",
		opt_integer);
	fprintf(stderr,"is INVALID!  It must be between %d and %d.  Aborting.\n",
		PARFU_MIN_ALLOWED_BLOCKS_PER_FRAGMENT,
		PARFU_MAX_ALLOWED_BLOCKS_PER_FRAGMENT);
	return NULL;
      }
      if(parfu_behav_add_bl_per_frag(my_orders,opt_integer)){
	fprintf(stderr,"parfu_parse_arguments:\n");
	fprintf(stderr,"Failed to blocks per fragment %d to list!\n",
		opt_integer);
	return NULL;
      }
      break;	 
    case 'N':
      if(my_orders->yn_iterations_argument){
	fprintf(stderr,"parfu_parse_arguments:\n");
	fprintf(stderr,"You may only specify the -N flag once!\n");
	fprintf(stderr,"  Exiting.\n");
	return NULL;
      }
      opt_integer = atoi(optarg);
      if( opt_integer < 1 || opt_integer > PARFU_MAX_TEST_ITERATIONS ){
	fprintf(stderr,"parfu_parse_arguments:\n");
	fprintf(stderr,"The number of iterations, %d, is invalid!\n",opt_integer);
	fprintf(stderr,"  It must be greater than %d and less than %d.\n",
		1,PARFU_MAX_TEST_ITERATIONS);
	return NULL;
      }
      my_orders->n_iterations=opt_integer;
      my_orders->yn_iterations_argument=1;
      fprintf(stderr,"Selecting %d iterations.\n",my_orders->n_iterations=opt_integer);
      break;
    case '?':
      switch(optopt){
	// list options that need arguments here:
      case 'N':
	fprintf(stderr,"parfu_parse_arguments:\n");
	fprintf(stderr,"Option \"-%c\" needs an argument!\n",optopt);
	fprintf(stderr,"  aborting.\n");
	return NULL;
	break;
	// anything not caught above will fall through to here and abort:
      default:
	fprintf(stderr,"parfu_parse_arguments:\n");	
	if(isprint(optopt)){
	  fprintf(stderr,"Unknown option \"-%c\".  Aborting.\n",optopt);
	}
	else{
	  fprintf(stderr,"Invalid (and unprintable) option: \"\\x%x\".\n",optopt);
	}
	return NULL;
      }
      break;
    } // switch(next_opt)
  } // while((next_opt=getopt...
  // parse non-option arguments

  for(i=optind; i < my_argc ; i++){
    fprintf(stderr,"non-option argument: >%s<\n",my_argv[i]);
    if(!strcmp(my_argv[i],"X")){
      fprintf(stderr,"WARNING!  In the new benchmarking test version of parful, the \"X\" option\n");
      fprintf(stderr,"  is now a Gnu-style option; it must have a \"-\" before it!\n");
    }
    if(!strcmp(my_argv[i],"C")){
      fprintf(stderr,"WARNING!  In the new benchmarking test version of parful, the \"C\" option\n");
      fprintf(stderr,"  is now a Gnu-style option; it must have a \"-\" before it!\n");
    }      
  }
  return my_orders;
}


/*
int parfu_are_we_in_MPI(void){
  int major_vers,minor_vers;
  MPI_GET_VERSION(&major_vers,&minor_vers);
  fprintf(stderr,"DEBUG: MPI major version: %d\n",major_vers);
  if(major_vers)
    return 1;
  return 0;  
}
*/
