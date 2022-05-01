////////////////////////////////////////////////////////////////////////////////
// 
//  University of Illinois/NCSA Open Source License
//  http://otm.illinois.edu/disclose-protect/illinois-open-source-license
//  
//  Parfu is copyright © 2017-2020, 
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


//#include "parfu_main.hh"
//#include "parfu_2021_legacy.hh"
#include "parfu_file_system_classes.hh"

long int Parfu_directory::spider_directory(void){
  // This is a big fuction, used when creating an 
  // archive.  
  long int total_entries_found=0;
  // OS-level directory structure
  DIR *my_dir=NULL;
  struct dirent *next_entry;  
  // internal parfu variable telling us what the entry is
  // regular file, symlink, directory, etc. 
  unsigned int path_type_result;
  
  string link_target;
  // File size if > 0.  Otherwise, this will be some slightly
  // negative number useful for classification
  long int file_size=(-1);
  // TODO: make this sensitive to command-line input
  int follow_symlinks=0;
  
  if(spidered){
    cerr << "This directory already spidered!  >>" << directory_path << "\n";
    return -1L;
  }
  //    for (const auto & next_entry : std::filesystem::directory_iterator(directory_path)){
  if((my_dir=opendir(directory_path.c_str()))==NULL){
    cerr << "Could not open directory >>" << directory_path << "<<for scanning!\n";
    return -2L;
  }
  // using the C library for traversing this directory
  next_entry=readdir(my_dir);
  while(next_entry!=NULL){
    // traverse once per entry
    // skip over "." and ".."
    if(!strncmp(next_entry->d_name,".",1)){
      next_entry=readdir(my_dir);
      continue;
    }
    if(!strncmp(next_entry->d_name,"..",2)){
      next_entry=readdir(my_dir);
      continue;
    }
    // it's a name so we need to check the type of the entry
    string entry_bare_name = string(next_entry->d_name);
    string entry_relative_name = directory_path;
    entry_relative_name.append("/");
    entry_relative_name.append(entry_bare_name);
    // entry_relative_name is the next entry.  
    // we now need to check it to find out what it is 
    // (directory,regular file, symlink) and how big
    // it is if it's a regular file.
    path_type_result=
      parfu_what_is_path(entry_relative_name.c_str(),link_target,&file_size,follow_symlinks);
    cout << "relative name: >>" << entry_relative_name << "<< file size: " << file_size << "\n";
    switch(path_type_result){
    case PARFU_WHAT_IS_PATH_DOES_NOT_EXIST:
      cerr << "Parfu_directory const; does not exist: >>" << entry_relative_name << "<<\n";
      break;
    case PARFU_WHAT_IS_PATH_IGNORED_TYPE:
      cerr << "Parfu_directory const; ignored type??: >>" << entry_relative_name << "<<\n";
      break;
    case PARFU_WHAT_IS_PATH_REGFILE:
      // it's a regular file that we need to store
    case PARFU_WHAT_IS_PATH_DIR:
      // it's a directory that we need to note and it will need to be spidered in the future
    case PARFU_WHAT_IS_PATH_SYMLINK:
      // simlink that we'll need to store for now
    case PARFU_WHAT_IS_PATH_ERROR:
      // not sure what would cause an error in this function, but catch it here
      cerr << "Parfu_directory const; ERROR from what_is_path: >>" << entry_relative_name << "<<\n";
      break;
    default:
      // don't know if it's possible to fall through to here??
      cerr << "Parfu_directory const; reached default branch??: >>" << entry_relative_name << "<<\n";
      break;
    }
    next_entry=readdir(my_dir);
  } // while(next_entry...)
  
  spidered=true;
  return total_entries_found;
} // long int spider_directory()
