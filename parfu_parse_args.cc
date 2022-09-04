////////////////////////////////////////////////////////////////////////////////
// 
//  University of Illinois/NCSA Open Source License
//  http://otm.illinois.edu/disclose-protect/illinois-open-source-license
//  
//  Parfu is copyright (c) 2017-2022, 
//  by The Trustees of the University of Illinois. 
//  All rights reserved.
//  
//  Parfu was developed by:
//  The University of Illinois
//  The National Center For Supercomputing Applications (NCSA)
//  Blue Waters Science and Engineering Applications Support Team (SEAS)
//  Craig P Steffen <csteffen@ncsa.illinois.edu>
//  Roland Haas <rhaas@illinois.edu>
//  
//  https://github.com/ncsa/parfu_archive_tool
//  http://www.ncsa.illinois.edu/People/csteffen/parfu/
//  
//  For full licnse text see the LICENSE file provided with the source
//  distribution.
//  
////////////////////////////////////////////////////////////////////////////////

#include "parfu_main.hh"

vector <string> *parfu_parse_args(unsigned nargs,
				  char *args[],
				  unsigned long *bucket_size,
				  unsigned *max_orders_per_bucket,
				  string *archive_file_name,
				  int *archive_file_multiplier){
  vector <string> *target_list;
  bool valid_flag;
  string flag_string;
  string value_string;
  unsigned equals_position;
  
  target_list = new vector <string>;

  // starts index is 1 because 0th argument is our name
  for(unsigned i=1;i<nargs;i++){
    string this_argument;
    this_argument=string(args[i]);
    if( (equals_position=this_argument.find('=',0)) > this_argument.size() ){
      // there are no equals signs; it's a target
      // (presumably directory, though possibly a single file
      // so add it to the vector of output strings.
      target_list->push_back(this_argument);
    }
    else{
      //      cerr << "parsing: equals position: " << equals_position << "\n";
      // There IS an equals sign.
      // we assume the text before it is a flag.  We will
      // check its value.  If it's one we know, we'll assign the remainder
      // of the string to the appropriate thing.  If not, we'll
      // throw an error.
      valid_flag=false;
      flag_string = this_argument.substr(0,equals_position);
      value_string = this_argument.substr(equals_position+1);
      if( flag_string == string("maxorders") ){
	valid_flag=true;
	*max_orders_per_bucket = stoi(value_string);
	cerr << "setting \"max orders per bucket\" to: "
	     << *max_orders_per_bucket << "\n";
      }
      if( flag_string == string("bucketsize") ){
	valid_flag=true;
	*bucket_size = stoi(value_string);
	*bucket_size = parfu_next_block_boundary(*bucket_size);
	cerr << "setting bucket size to: "
	     << *bucket_size << "\n";
	cerr << "(this has been adjusted, if necessary, to the next even\n";
	cerr << "multiple of the tar BLOCKSIZE.\n";
      }
      if( flag_string == string("archivefile") ){
	valid_flag=true;
	*archive_file_name=value_string;
	cerr << "archive file set to: " << *archive_file_name
	     << "\n";
      }
      if(!valid_flag){
	cerr << "invalid flag:" << flag_string << "!\n";
	cerr << "Aborting.\n";
	parfu_usage();
	return nullptr;
      }
    }
    
  }
  
  return target_list;
}

void parfu_usage(void){
  cerr << "\n\nHow to invoke parfu:\n";
  cerr << "parfu [bucketsize=<bucket size in bytes>]\n";
  cerr << "      [maxorders=<max orders per bucket>]\n";
  cerr << "      archivefile=<path to archive to write>\n";
  cerr << "      <target_dir>\n\n";
}
