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


#include "parfu_main.hh"
#include "parfu_2021_legacy.hh"
//#include "parfu_primary.h"

#ifndef PARFU_FILE_SYSTEM_CLASSES_HH_
#define PARFU_FILE_SYSTEM_CLASSES_HH_

using namespace std;
//using namespace filesystem;
//namespace fs = std::filesystem;

////////////////////
//
// In thinking about files from parfu's point of view, we
// need to create a very important distinction.  The general
// operation of parfu is like unix tar, in that you're taking 
// a bunch of files (and directories and symlinks and stuff) 
// from a directory on disk and then putting them into an
// archive file.  (In the case of unix tar, this is generally
// the archive file or the "tarfile".)
// 
// So we need to keep track of two kinds of "file"s.  First
// are the "target" files, which are all the files that we need
// to read from disk and store.  Target files are considered
// completely opaque.  They are strings of bytes that parfu is
// responsible for correctly representing but not modifying.  
// 
// The "container" file (parfu's name for it) or the "archive"
// file (tar's name for it) is the file (or files) in which 
// all the data is stored.  Parfu spends a lot of its time
// writing data into the container file or reading data out
// of the container file.  When a "create mode" parfu is run, 
// the target directory on disk containing the target files 
// are the input, and the final container file is the product.  
// When an "extract mode" parfu is run, an existing 
// container file is the input, and the 
// product is a populated directory where the target files
// have been extracted into.  
//
////////////////////

////////////
// 
// Classes for target file information
// 
// a Parfu_target_file generally refers to an actual target file on disk
// that either needs to be extracted (extract mode) or brought into the 
// container (create mode).  
//
// a Parfu_file_slice refers to a region of a target file (possibly all of it).  
//   Each Parfu_target_file will have one or more slices.  The slices are
//   sized for the convenience of storing or extraction.  Slices
//   have no meaning for a stored parfu container.  Files within
//   the parfu container are contiguous, just as on disk and just
//   as within a tar file.  However, if a file consists of more
//   than one slice, the slices will be typically read or written 
//   by different ranks.  
// 

class Parfu_file_slice;
class Parfu_container_file;

////////////////
// 
// A "target file" will be anything that's not a directory that
// needs to be stored in a target file.  A target file could
// be a regular file (a sequence of bytes), a symlink (which
// consists only of a name and the name of its target)
class Parfu_target_file
{ 
public:  
  // constructor
  Parfu_target_file(string in_base_path, string in_relative_path,
		    int in_file_type);
  // copy constructor
  Parfu_target_file(const Parfu_target_file &in_file){
    relative_full_path = in_file.relative_full_path;
    base_path = in_file.base_path;
    slices = in_file.slices;
    parent_container = in_file.parent_container; 
    file_size = in_file.file_size;
    file_type_value = in_file.file_type_value;
  }
  // assignemnt operator
  Parfu_target_file& operator=(const Parfu_target_file &in_file){
    relative_full_path = in_file.relative_full_path;
    base_path = in_file.base_path;
    slices = in_file.slices;
    parent_container = in_file.parent_container; 
    file_size = in_file.file_size;
    slices = in_file.slices;    
    return *this;
  }
  // destructor
  ~Parfu_target_file(void){
  }
  string absolute_path(){
    return base_path+"/"+relative_full_path;
  }
  int file_type(){
    return file_type_value;
  }
  bool are_slices_populated(void){
    return true;
  }
  void set_symlink_target(string target_string){
    symlink_target=target_string;
  }
private:
  // base_path here will typically be the location that parfu was pointed to 
  // to archive.  So the absolute path of this file will typically be:
  // <base_path> / <relative_full_path>
  // base_path should *ALWAYS* begin with the leading "/"; otherwise something
  // is horribly wrong.  base_path should NOT end in a "/".  
  // relative_full_path should NOT begin with a "/".  
  // It may contain zero or more "/" characters to delineate its directory location
  // relative to base_path.  It ends in the actual file name.  We can strip out just
  // the file's filename itself by outputting relative_full_path after the last "/" or 
  // if there aren't any, the entirety of relative_full_path.  
  string relative_full_path;
  string base_path;
  string symlink_target;
  
  // every file is made up of one or more subfiles
  // here's a list of them
  list <Parfu_file_slice> slices;
  // A link to the container file class that this file
  // will/does reside in.  This is null if it hasn't
  // been assigned to a container file.  
  Parfu_container_file *parent_container=NULL;
  
  // Size of the file in bytes
  long int file_size;
  // File type.  Regular file, symlink, etc.  
  int file_type_value=PARFU_FILE_TYPE_INVALID;
};

/////////////////////////
//
class Parfu_file_slice 
{
public: 
  // constructor
  Parfu_file_slice(void){
  }
  Parfu_file_slice(long int in_size){
    slice_size=in_size;
  }
  Parfu_file_slice(long int in_size,
		   long int in_offset_in_file,
		   long int in_offset_in_container){
    slice_size = in_size;
    slice_offset_in_file = in_offset_in_file;
    slice_offset_in_container = in_offset_in_container;
  }
  // copy constructor
  Parfu_file_slice(const Parfu_file_slice &in_slice){
    slice_size = in_slice.slice_size;
    slice_offset_in_file = in_slice.slice_offset_in_file;
    slice_offset_in_container = in_slice.slice_offset_in_container;
  }
  // assignment operator
  Parfu_file_slice& operator=(const Parfu_file_slice &in_slice){
    slice_size = in_slice.slice_size;
    slice_offset_in_file = in_slice.slice_offset_in_file;
    slice_offset_in_container = in_slice.slice_offset_in_container;    
    return *this;
  }
  // destructor
  ~Parfu_file_slice(void){
    
  }
  Parfu_target_file *parent_file;
private:
  // how large the slice is in bytes
  long int slice_size = PARFU_FILE_SIZE_INVALID;
  // Location of the beginning of the slice within the target file
  // itself.  If this is either the first slice of many in the 
  // file, or this is the only slice in the file, then this
  // will be zero.  
  long int slice_offset_in_file=PARFU_OFFSET_INVALID;
  // Location of the beginning of this slice in the DATA AREA 
  // of the container file.  Typically the data area begins after 
  // the end of the catalog.  The code the actually copies the 
  // data into the file will know this and compensate.  
  long int slice_offset_in_container=PARFU_OFFSET_INVALID;
};

///////////////
//
class Parfu_slice_collection
{
public:
  //constructor
  Parfu_slice_collection(void){
  }
  // copy constructor
  Parfu_slice_collection(const Parfu_slice_collection &in_collec){
    for( unsigned int i=0; i<in_collec.slices.size() ; i++ ){
      slices[i] = in_collec.slices[i];
    }
  }
  // assignment operator
  Parfu_slice_collection& operator=(const Parfu_slice_collection &in_collec){
    for( unsigned int i=0; i<in_collec.slices.size() ; i++ ){
      slices[i] = in_collec.slices[i];
    }
    return *this;
  }
  // destructor
  ~Parfu_slice_collection(void){
  }
  void add_slice(Parfu_file_slice* new_slice_ptr){
    slices.push_back(new_slice_ptr);
  }
private:
  vector <Parfu_file_slice*> slices;
};

/////////////////////////////////
// 
// Classes pertaining to information about directories
// 

///////////////
//
class Parfu_directory
{
public:
  // main constructor
  Parfu_directory(void){
  }
  Parfu_directory(string my_dirpath){
    directory_path = my_dirpath;
  }
  bool is_directory_spidered(void){
    return spidered;
  }
  long int spider_directory(void);
  // copy constructor
  Parfu_directory(const Parfu_directory &in_dir){
    directory_path = in_dir.directory_path;
    spidered = in_dir.spidered;
    for( unsigned int i=0 ; i < in_dir.subdirectories.size() ; i++ ){
      subdirectories[i] = in_dir.subdirectories[i];
    }
    for( unsigned int i=0 ; i < in_dir.subfiles.size() ; i++ ){
      subfiles[i] = in_dir.subfiles[i];
    }
  }
  // assignment operator
  Parfu_directory& operator=(const Parfu_directory &in_dir){
    directory_path = in_dir.directory_path;
    spidered = in_dir.spidered;
    for( unsigned int i=0 ; i < in_dir.subdirectories.size() ; i++ ){
      subdirectories[i] = in_dir.subdirectories[i];
    }
    for( unsigned int i=0 ; i < in_dir.subfiles.size() ; i++ ){
      subfiles[i] = in_dir.subfiles[i];
    }    
    return *this;
  }
  // destructor
  ~Parfu_directory(void){
  }  
private:
  // directory path should not end with "/" unless it is the root directory
  string directory_path="";
  // "spider" here is a verb to search down a directory tree from a starting
  // point to list all of the subdirectories and contents.  
  // spidered is false if directory_path has a value but a list has not been 
  // created.  If the directory listing has been completed, 
  // then spidered is true.  
  bool spidered=false;
  // Time stamp when the directory was last searched for contents (spidered)
  time_t last_time_spidered=PARFU_DEFAULT_LAST_TIME_SPIDERED;
  vector <Parfu_directory*> subdirectories;
  vector <Parfu_target_file> subfiles;
};

//////////////////////////////////
//
// Classes pertaining to container files
// 
// Parfu_container_file contains the file pointers and metadata 
// info for one container file.  

//////////////////////////
//
class Parfu_container_file
{
public:
  // everyday-use constructor
  Parfu_container_file(string in_filename){
    this->constructor_stuff();
    full_path = in_filename;
    my_communicator = 0;
  }
  // copy constructor
  Parfu_container_file(const Parfu_container_file &in_container){
    this->constructor_stuff();
    full_path = in_container.full_path;
    file_is_open = in_container.file_is_open;
    *container_file_ptr = *(in_container.container_file_ptr);
    my_communicator = in_container.my_communicator;
  }
  // assignment operator
  Parfu_container_file& operator=(const Parfu_container_file &in_c_file){
    this->constructor_stuff();
    full_path = in_c_file.full_path;
    file_is_open = in_c_file.file_is_open;
    *container_file_ptr = *(in_c_file.container_file_ptr);
    my_communicator = in_c_file.my_communicator;
    return *this;
  }
  // destructor
  ~Parfu_container_file(void){
    if((*container_file_ptr)!=NULL){
      cout << "Parful_container_file destructor called with open file!!!\n";
    }
    *container_file_ptr=NULL;
    delete container_file_ptr;
  }
  int MPI_File_open_write_new(MPI_Comm my_comm,MPI_Info my_info){
    int return_val;
    return_val=MPI_File_open(my_comm,full_path.c_str(),
			     MPI_MODE_WRONLY | MPI_MODE_CREATE,
			     my_info,(*(container_file_ptr)) );
    file_is_open=true;
    return return_val;
  }
private:
  string full_path;
  bool file_is_open=false;
  // needs to be a double pointer so that we can *new*
  // the pointer itself so it doesn't go out of scope
  // when we pass it to MPI_File_open().
  MPI_File **container_file_ptr=NULL;
  MPI_Comm my_communicator;
  void constructor_stuff(void){
    // allocate and set up stuff
    if(container_file_ptr==NULL){
      container_file_ptr = new MPI_File*;
      *container_file_ptr = NULL;
    }
  }
};

// Parfu_container_file_collection is basically a vector
// of container files if we implement the feature where
// a "create" operation can write to more than one 
// container file for performance reasons.  Each of
// the container files in the vector will keep track
// of one of the constituent container files.  

/////////////////////////////////////
//
class Parfu_container_file_collection
{
public:
  // constructor
  Parfu_container_file_collection(void){
  }
  // copy constructor
  Parfu_container_file_collection(const Parfu_container_file_collection &in_collec){
    for( unsigned int i=0; i < in_collec.containers.size() ; i++ ){
      containers[i] = in_collec.containers[i];
    }
  }
  // assignment operator
  Parfu_container_file_collection& operator=(const Parfu_container_file_collection &in_collec){
    for( unsigned int i=0; i < in_collec.containers.size() ; i++ ){
      containers[i] = in_collec.containers[i];
    }    
    return *this;
  }
  // destructor
  ~Parfu_container_file_collection(void){
  }
  void add_file(string filename){
    Parfu_container_file *new_file;
    new_file = new Parfu_container_file(filename);
    containers.push_back(new_file);
  }
private:
  vector <Parfu_container_file*> containers;
};

#endif // #ifndef PARFU_FILE_SYSTEM_CLASSES_HH_
