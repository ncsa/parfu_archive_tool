////////////////////////////////////////////////////////////////////////////////
// 
//  University of Illinois/NCSA Open Source License
//  http://otm.illinois.edu/disclose-protect/illinois-open-source-license
//  
//  Parfu is copyright © 2017-2021, The Trustees of the University of Illinois. 
//  All rights reserved.
//  
//  Parfu was developed by:
//  The University of Illinois
//  The National Center For Supercomputing Applications (NCSA)
//  Blue Waters Science and Engineering Applications Support Team (SEAS)
//  Roland Haas <rhaas@illinois.edu>
//  Craig P Steffen <csteffen@ncsa.illinois.edu>
//  
//  https://github.com/ncsa/parfu_archive_tool
//  
//  For full licnse text see the LICENSE file provided with the source
//  distribution.
//  
////////////////////////////////////////////////////////////////////////////////

#include "parfu_main.hh"

///////////////////////////////////
//
// This file contains some of the utility functions from the original C version
// of the code that we're using in the C++ class functions.  

unsigned int parfu_what_is_path(string pathname,
				       string &target_text,
				       long int *size,
				       bool follow_symlinks){
  struct stat filestruct;
  int returnval;
  int buffer_length;
  char *target_name_buffer;
  ifstream filecheck;
  
  // check for existence first
  
  if(follow_symlinks){ // treat symlinks like what they point to
    if((returnval=stat(pathname.c_str(),&filestruct))){
      filecheck.open(pathname.c_str());
      if(filecheck){
	fprintf(stderr,"parfu_what_is_path:\n");
	fprintf(stderr,"  stat returned %d for existing file!!!\n",returnval);
	return PARFU_WHAT_IS_PATH_ERROR;
      }
      else{
	cerr << "parfu_what_is_path:\n";
	cerr << "ERROR: file >" << pathname << "< does not exist!\n";
	return PARFU_WHAT_IS_PATH_DOES_NOT_EXIST;

      }
    }
  }
  else{ // treat symlinks like symlinks
    if((returnval=lstat(pathname.c_str(),&filestruct))){
      fprintf(stderr,"parfu_what_is_path:\n");
      fprintf(stderr,"  lstat returned %d with path >%s<!!!\n",returnval,pathname.c_str());
      return PARFU_WHAT_IS_PATH_ERROR;
    }
    if(S_ISLNK(filestruct.st_mode)){
      // harvest the symlink target so that we can pass it back
      buffer_length=(filestruct.st_size)+1;
      target_name_buffer = new char[buffer_length];
      //      if(((*target_text)=(char*)malloc(buffer_length))==NULL){
      //	fprintf(stderr,"parfu_what_is_path:\n");
      //	fprintf(stderr,"  failed to allocate target text buffer!!\n");
      //	return PARFU_WHAT_IS_PATH_ERROR;
      //      }
      returnval=readlink(pathname.c_str(),target_name_buffer,buffer_length);
      if(returnval != (buffer_length-1) ){
	fprintf(stderr,"parfu_what_is_path:\n");
	fprintf(stderr,"  error in length of target return!!\n");
	return PARFU_WHAT_IS_PATH_ERROR;
      }
            //      (*target_text)[returnval]='\0'; // add NULL terminator
      target_text = string(target_name_buffer);
      // now (*target_text) is the link target, so we can return
      return PARFU_WHAT_IS_PATH_SYMLINK;
    } // if(S_ISLNK...
  }
  // if it's a directory
  if(S_ISDIR(filestruct.st_mode)){
    return PARFU_WHAT_IS_PATH_DIR;
  }
  // if it's a regular file
  if(S_ISREG(filestruct.st_mode)){
    if(size != NULL){
      *size = filestruct.st_size;
    }
    return PARFU_WHAT_IS_PATH_REGFILE;
  }
  return PARFU_WHAT_IS_PATH_IGNORED_TYPE;
}
