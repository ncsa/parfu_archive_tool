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

#ifndef PARFU_FILE_SYSTEM_CLASSES_HH_
#define PARFU_FILE_SYSTEM_CLASSES_HH_

using namespace std;

////////////
// 
// Classes that store file information.  
// 
// a Parfu_target_file generally refers to an actual file on disk
//
// a Parfu_file_slice refers to a region of a file (possibly all of it).  
//   Each Parfu_target_file will have one or more slices.  The slices are
//   sized for the convenience of storing or extraction.  Slices
//   have no meaning for a stored parfu container.  Files within
//   the parfu container are contiguous, just as on disk and just
//   as within a tar file.  However, if a file consists of more
//   than one slice, the slices will be typically read or written 
//   by different ranks.  

class Parfu_file_slice;
class Parfu_container_file;

class Parfu_target_file
{ 
public:  
  // constructor
  Parfu_target_file(string in_base_path, string in_relative_path,
		   int in_file_type){
    relative_full_path=in_relative_path;
    base_path=in_base_path;
    file_type_value=in_file_type;
  }
  // copy constructor
  Parfu_target_file(const Parfu_target_file &in_file){
    file_size = in_file.file_size;
    relative_full_path = in_file.relative_full_path;
    base_path = in_file.base_path;
    slices = in_file.slices;
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
  
  // every file is made up of one or more subfiles
  // here's a list of them
  list <Parfu_file_slice> slices;
  Parfu_container_file *parent_container=NULL;

  long int file_size;
  int file_type_value=PARFU_INVALID_FILE_TYPE;
};

class Parfu_file_slice 
{
public: 
  Parfu_target_file *parent_file;
private:
  long int slice_size;
  long int slice_offset_in_file=PARFU_INVALID_OFFSET;
  long int slice_offset_in_container=PARFU_INVALID_OFFSET;
};


/////////////////////////////////
// 
// Classes pertaining to information about directories
// 
class Parfu_directory
{
public:
private:
  // directory path should not end with "/" unless it is the root directory
  string directory_path;
  // "spider" here is a verb to search down a directory tree from a starting
  // point to list all of the subdirectories and contents.  
  // spidered is false if directory_path has a value but a list has not been 
  // created.  If the directory listing has been completed, 
  // then spidered is true.  
  bool spidered=false;
  // Time stamp when the directory was last searched for contents (spi
  time_t last_time_spidered;
  vector <Parfu_directory*> subdirectories;
  vector <Parfu_target_file*> subfiles;
};

//////////////////////////////////
//
// Classes pertaining to container files
// 
// Parfu_container_file contains the file pointers and metadata 
// info for one container file.  

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
    *container_file_ptr = *(in_container.container_file_ptr);
  }
  // destructor
  ~Parfu_container_file(void){
    if((*container_file_ptr)!=NULL){
      cout << "Parful_container_file destructor called with open file!!!\n";
    }
    *container_file_ptr=NULL;
    delete container_file_ptr;
  }
  // assignment operator
  Parfu_container_file& operator=(const Parfu_container_file &in_c_file){
    //    new Parfu_container_file new_CF(in_c_file.full_path);
    full_path = in_c_file.full_path;
    *container_file_ptr = *(in_c_file.container_file_ptr);
    return *this;
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

/////////////////////////////////////////////////////////
//
// Parfu_container_file_collection is basically a vector
// of container files if we implement the feature where
// a "create" operation can write to more than one 
// container file for performance reasons.  Each of
// the container files in the vector will keep track
// of one of the constituent container files.  

class Parfu_container_file_collection
{
public:
  void add_file(string filename){
    Parfu_container_file *new_file;
    new_file = new Parfu_container_file(filename);
    containers.push_back(new_file);
  }
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
  // assignment operator
private:
  vector <Parfu_container_file*> containers;
};

#endif // #ifndef PARFU_FILE_SYSTEM_CLASSES_HH_
