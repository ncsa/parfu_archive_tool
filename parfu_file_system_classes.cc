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

// these functions transform a target file to a line in a catalog, or take a
// line from a catalog (as a string) and constructs the corresponding
// target file class
// For now taking definitions and format for these lines
// from parfu_buffer_utils.c

string Parfu_target_file::generate_archive_catalog_line(void){
  // dump contents as a string
  // format is the *archive* catalog line
  // (the shorter one)
  string out_string;

  // path + filename within the archive
  out_string.append(relative_full_path);
  out_string.append("\t"); // \t

  // type
  switch(file_type_value){
  case PARFU_FILE_TYPE_REGULAR:
    out_string += PARFU_FILE_TYPE_REGULAR_CHAR;
    break;
  case PARFU_FILE_TYPE_DIRECTORY:
    out_string += PARFU_FILE_TYPE_DIRECTORY_CHAR;
    break;
  case PARFU_FILE_TYPE_SYMLINK:
    out_string += PARFU_FILE_TYPE_SYMLINK_CHAR;
    break;
  }
  out_string.append("\t"); // \t

  // symlink target
  out_string.append(symlink_target);
  out_string.append("\t"); // \t

  // size
  out_string.append(to_string(file_size));
  out_string.append("\t");// \t

  // size of tar header
  out_string.append("\t");  // \t

  // location in archive file
  
  out_string.append("\n");// \n
  return out_string;  
}

// OOPS this needs to be per slice?  (called "fragment" in
// .c version?

string Parfu_target_file::generate_full_catalog_line(void){
  // dump contents as a string
  // format is the *full* catalog line
  // (the longer one)
  // this line will be suitable for sending over MPI
  // to other ranks.
  string out_string;

  // path + filename, realtive to parfu process CWD
  out_string.append(relative_full_path);
  out_string.append("\t"); // \t

  // path + filename within the archive
  // \t
  // type
  switch(file_type_value){
  case PARFU_FILE_TYPE_REGULAR:
    out_string += PARFU_FILE_TYPE_REGULAR_CHAR;
    break;
  case PARFU_FILE_TYPE_DIRECTORY:
    out_string += PARFU_FILE_TYPE_DIRECTORY_CHAR;
    break;
  case PARFU_FILE_TYPE_SYMLINK:
    out_string += PARFU_FILE_TYPE_SYMLINK_CHAR;
    break;
  }
  out_string.append("\t"); // \t

  // symlink target
  out_string.append(symlink_target);
  out_string.append("\t"); // \t

  // size
  out_string.append(to_string(file_size));
  out_string.append("\t");// \t

  // size of tar header
  // \t
  // location in archive file
  // \t
  // location of fragment in original file
  // \t
  // location of previous pad file in archive file
  // \t
  // filename of preceeding pad file
  // \t
  // tar header size of preceeding pad file
  // \t
  // size of preceeding pad file
  // \t
  // total slices this file is divided into
  // \t
  // file pointer index (internal use only)
  // \t
  // rank bucket index this slice goes in

  return out_string;

}
Parfu_target_file::Parfu_target_file(string catalog_line){
  // parse the line to extract the target file
  
}





Parfu_target_file::Parfu_target_file(string in_base_path, string in_relative_path,
				     int in_file_type){
  relative_full_path=in_relative_path;
  base_path=in_base_path;
  file_type_value=in_file_type;
}

Parfu_target_file::Parfu_target_file(string in_base_path, string in_relative_path,
				     int in_file_type, string in_symlink_target){
  relative_full_path=in_relative_path;
  base_path=in_base_path;
  file_type_value=in_file_type;
  symlink_target = in_symlink_target;
}



long int Parfu_directory::spider_directory(void){
  // This is a big fuction, used when creating an 
  // archive.  
  long int total_entries_found=0;
  // OS-level directory structure
  DIR *my_dir;
  struct dirent * next_entry;
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
  if((my_dir=opendir(directory_path.c_str()))==nullptr){
    cerr << "Could not open directory >>" << directory_path << "<<for scanning!\n";
    return -2L;
  }
  // This loop is the *initial* traverse of the directory that
  // this instance points to.  That is, this next loop
  // traverses the top level of the directory.  This will
  // mark down symlinks and regular files to use later
  // during the data storage/movement phase.  We will
  // also note down subdirectories that will later
  // need to be traversed.  
  
  // using the C library for traversing this directory
  //  next_entry=readdir(my_dir);
  //  if(next_entry == nullptr){
  //    cerr << "\nFound nullptr entry in directory!!!\n\n";
  //  }
  while( (next_entry=readdir(my_dir)) != nullptr ){


    //    if(next_entry == nullptr){
    //      cerr << "\nFound nullptr entry in directory!!!\n\n";
    //    }
    // traverse once per entry
    // skip over "." and ".."
    if(!strncmp(next_entry->d_name,".",1)){
      continue;
    }
    if(!strncmp(next_entry->d_name,"..",2)){
      continue;
    }
    // We know now that it's an actual thing with a name,
    // so we need to check *what* it is
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
    cout << "file type: \n" << path_type_result << "\n";
    switch(path_type_result){
    case PARFU_WHAT_IS_PATH_DOES_NOT_EXIST:
      cerr << "Parfu_directory const; does not exist: >>" << entry_relative_name << "<<\n";
      // this should generally never happen 
      break;
    case PARFU_WHAT_IS_PATH_IGNORED_TYPE:
      cerr << "Parfu_directory spider_dir function: ignored type, will skip file: >>" << entry_relative_name << "<<\n";
      // I presume for now we'll just jump over this entry without acknowledging it or storing
      // anywhere.  This would be an entry that's not a file, not a symlink, and not a
      // subdirectory.  So....a dev file?  Something else?  Probably safe to not save it
      // to the archive.  Possibly leaving the warning?  
      break;
    case PARFU_WHAT_IS_PATH_REGFILE:
      // it's a regular file that we need to store.  This is the
      // core of what parfu needs to tackle.
      subfiles.push_back(Parfu_target_file(directory_path,entry_relative_name,path_type_result));
      break;
    case PARFU_WHAT_IS_PATH_DIR:
      // it's a directory that we need to note and it will need to be spidered in the future
      Parfu_directory *new_subdir_ptr;
      new_subdir_ptr =
      	new Parfu_directory(entry_relative_name);
      subdirectories.push_back(new_subdir_ptr);
      break;
    case PARFU_WHAT_IS_PATH_SYMLINK:
      // simlink that we'll need to store for now
      Parfu_target_file *my_tempfile;
      my_tempfile =
	new Parfu_target_file(directory_path,entry_relative_name,path_type_result);
      my_tempfile->set_symlink_target(link_target);
      subfiles.push_back(*my_tempfile);
      break;
    case PARFU_WHAT_IS_PATH_ERROR:
      // not sure what would cause an error in this function, but catch it here
      cerr << "Parfu_directory const; ERROR from what_is_path: >>" << entry_relative_name << "<<\n";
      break;
    default:
      // don't know if it's possible to fall through to here??
      cerr << "Parfu_directory const; reached default branch??: >>" << entry_relative_name << "<<\n";
      break;
    }
  } // while(next_entry...)
  
  // Now this directory has been read.  Now we need to
  // go through all the subdirectories and read their
  // entries too.
  //
  // For now (May 19 2022) we'll attempt to do this
  // as a recursive function call.
  // If I've done this right, I think this should
  // basically end up as a callstack as deep as the
  // deepest layer of the file directory.  I don't
  // think we'll get a thread explosion; I think
  // the upper calling threads will just be waiting
  // for the lower directory threads each to finish. 

  // Possibly in the
  // future, here we'll make calls to other ranks
  // to do this, although we'll have to be very
  // careful not to make a giant mess.
  
  for(std::size_t subdir_index=0;subdir_index < subdirectories.size();subdir_index++){
    // fire off the spider function of each subdirectory in turn
    Parfu_directory *local_subdir;
    local_subdir=subdirectories[subdir_index];
    local_subdir->spider_directory();
  }
  
  spidered=true;
  return total_entries_found;
} // long int spider_directory()
