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
class Parfu_directory;
class Parfu_target_file;

////////////////
//
// Parent class for the below derived classes
// (directory, regular file, symlink) that appear as entries
// in a file listing.  This is to keep all of the location
// and size functions together.
class Parfu_storage_entry
{
public:
  string absolute_path(){
    return base_path+"/"+relative_path;
  }
  bool are_locations_set(void){
    return are_locations_filled_out;
  }
  string generate_archive_catalog_line(void);
  string generate_full_catalog_line(void);
  int header_size(void);
  int entry_type(){
    return entry_type_value;
  }
  long int next_available_after_me(long int start_of_available);
  int get_entry_type(void){
    return entry_type_value;
  }
  virtual Parfu_directory* nth_subdir(int my_index){
    return nullptr;
  }
  virtual unsigned int N_subdirs(void){
    return 0;
  }
  virtual Parfu_target_file* nth_subfile(int my_index){
    return nullptr;
  }
  virtual unsigned int N_subfiles(void){
    return 0;
  }
  virtual bool is_symlink(void){
    return false;
  }
  char type_char(void);
  
private:
  // allow derived classes to initialize variables
  // not sure it's the right way to do this, but it'll work
  // for now
  friend class Parfu_target_file;
  friend class Parfu_target_symlink;
  friend class Parfu_directory;
  friend class Parfu_target_collection;
  
  // The absolute path of this file will typically be:
  // <base_path> / <relative_full_path>
  
  // For *create* mode, base_path is the location where parfu
  // was initially pointed.
  // For *extract* mode, base_path is the location where the
  // extraction is being pointed to.
  // In either case, base path is the path that the relative
  // path is relative *to*
  
  // base_path should *ALWAYS* begin with the leading "/"; otherwise something
  // is horribly wrong.  base_path should NOT end in a "/".  
  
  string base_path;
  
  // relative_path is the path in the archive, that is the
  // file's location relative to the above base path.  
  
  // relative_path should NOT begin with a "/".  
  // It may contain zero or more "/" characters to delineate its directory location
  // relative to base_path.  It ends in the actual file name.  We can strip out just
  // the file's filename itself by outputting relative_full_path after the last "/" or 
  // if there aren't any, the entirety of relative_full_path.  
  
  string relative_path;

  int tar_header_size=-1;
  string symlink_target = string("");

  // Size of the file in bytes
  long int file_size=0L;

  // Entry type.  Regular file, symlink, directory, etc.  
  int entry_type_value=PARFU_FILE_TYPE_INVALID;

  
  // indicates if the locations for a given storage entry have been filled
  // out for its container file
  bool are_locations_filled_out=false;
  //  virtual vector <Parfu_directory*> subdirectories;

};

////////////////
// 
// A "target file" will be anything that's not a directory that
// needs to be stored in a target file.  A target file could
// be a regular file (a sequence of bytes), a symlink (which
// consists only of a name and the name of its target)

class Parfu_target_file : public Parfu_storage_entry
{ 
public:
  // constructor
  Parfu_target_file(string in_base_path, string in_relative_path,
		    int in_file_type);
  // constructor (with symlink target)
  Parfu_target_file(string in_base_path, string in_relative_path,
		    int in_file_type, string in_symlink_target);
  Parfu_target_file(string in_base_path, string in_relative_path,
		    int in_file_type, long int in_file_size,
		    string in_symlink_target);
  Parfu_target_file(string in_base_path, string in_relative_path,
		    int in_file_type, long int in_file_size);

  // constructor from a transmitted catalog line as a string
  Parfu_target_file(string catalog_line);
  // copy constructor
  Parfu_target_file(const Parfu_target_file &in_file){
    relative_path = in_file.relative_path;
    base_path = in_file.base_path;
    //    slices = in_file.slices;
    parent_container = in_file.parent_container; 
    file_size = in_file.file_size;
    tar_header_size = in_file.tar_header_size;
    entry_type_value = in_file.entry_type_value;
    symlink_target = in_file.symlink_target;
  }
  // assignment operator
  Parfu_target_file& operator=(const Parfu_target_file &in_file){
    relative_path = in_file.relative_path;
    base_path = in_file.base_path;
    //    slices = in_file.slices;
    parent_container = in_file.parent_container; 
    file_size = in_file.file_size;
    tar_header_size = in_file.tar_header_size;
    entry_type_value = in_file.entry_type_value;
    symlink_target = in_file.symlink_target;    
    return *this;
  }
  // destructor
  ~Parfu_target_file(void){
    
  }
  bool are_slices_populated(void){
    return true;
  }
  void set_symlink_target(string target_string){
    symlink_target=target_string;
  }
  //  int fill_out_locations(long int start_offset,
  //			 long int slice_size);
  //  long int offset_in_container(void);
  virtual bool is_symlink(void){
    if(symlink_target.size() > 0)
      return true;
    else
      return false;
  }
  
  
private:
  
  // every file is made up of one or more subfiles
  // here's a list of them
  // A link to the container file class that this file
  // will/does reside in.  This is null if it hasn't
  // been assigned to a container file.  
  Parfu_container_file *parent_container=NULL;
  
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
  Parfu_file_slice(int in_header_size,
		   long int in_size,
		   long int in_offset_in_file,
		   long int in_offset_in_container){
    header_size_this_slice = in_header_size;
    slice_size = in_size;
    slice_offset_in_file = in_offset_in_file;
    slice_offset_in_container = in_offset_in_container;
  }
  Parfu_file_slice(long int in_size,
		   long int in_offset_in_file){
    slice_size = in_size;
    slice_offset_in_file = in_offset_in_file;
    slice_offset_in_container = -1L;
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
  Parfu_storage_entry *parent_file=nullptr;
private:
  friend class Parfu_target_collection;
  int header_size_this_slice=-1;

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
  // The slidce_offset_in_container is the beginning of the
  // file's area, so it will typically be where the header
  // starts, not the file contentents themselves.  

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
class Parfu_directory : Parfu_storage_entry
{
public:
  // main constructor
  Parfu_directory(void){
  }
  Parfu_directory(string my_base_path){
    base_path = my_base_path;
    relative_path = string("");
  }
  Parfu_directory(string my_base_path,
		  string my_relative_path){
    base_path = my_base_path;
    relative_path = my_relative_path;
  }
  bool is_directory_spidered(void){
    return spidered;
  }
  long int spider_directory(void);
  // copy constructor
  Parfu_directory(const Parfu_directory &in_dir){
    base_path = in_dir.base_path;
    relative_path = in_dir.relative_path;
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
    base_path = in_dir.base_path;
    relative_path = in_dir.relative_path;
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
  virtual Parfu_directory* nth_subdir(int my_index){
    return subdirectories.at(my_index);
  }
  virtual unsigned int N_subdirs(void){
    return subdirectories.size();
  }
  virtual Parfu_target_file* nth_subfile(int my_index){
    return subfiles.at(my_index);
  }
  virtual unsigned int N_subfiles(void){
    return subfiles.size();
  }
private:
  friend class Parfu_target_collection;
  // "spider" here is a verb to search down a directory tree from a starting
  // point to list all of the subdirectories and contents.  
  // spidered is false if directory_path has a value but a list has not been 
  // created.  If the directory listing has been completed, 
  // then spidered is true.  
  bool spidered=false;
  // Time stamp when the directory was last searched for contents (spidered)
  time_t last_time_spidered=PARFU_DEFAULT_LAST_TIME_SPIDERED;
  vector <Parfu_directory*> subdirectories;
  vector <Parfu_target_file*> subfiles;
};

// Parfu_storage_reference is a generic object that points to a single
// reference (to a directory or regular file or symlink) along with
// its size.  This is used to order for the storage container.  

typedef struct{
  // order_size is sort of a virtual size.  In the case of real "regular" files
  // on disk, it's the size of the file in bytes.  For other
  // entries (symlinks, directories, other things) it's some virtual
  // size, possibly negative, that's used in sorting entries by group.
  long int order_size;
  Parfu_storage_entry *storage_ptr;
  //  Parfu_directory *dir_ptr;
  //  Parfu_target_file *tgt_file_ptr;
  list <Parfu_file_slice> slices;
}Parfu_storage_reference;

//////////////////////////////////
//
// These "target_collection" classes will collect
// lots of directories and their files together to
// keep track of them as a unit.  Any one instance of
// parfu may need to be keeping track of lots of
// directories and files, and these classes will
// have the tools to transmit, collect them between
// ranks and subdivide them if needed.
//
//
// The idea here is if parfu is archiving into one file, then
// one instance of Parfu_target_collection could contain the
// entire set of what is going to be archived.  That probably
// wouldn't be worth creating an entire class separately, it
// could just be done in the main code.  However, we want
// to be able to have the vehicle to subdivide the whole
// collection for separate storage.  For instance one
// parfu process might be archiving into four separate
// archive files.  The real on-disk file entries would
// be split and distributed among the four
// Parfu_target_collection entities.  Each regular file
// would only exist in one, and likewise each symlink.
// However, in that case, each directory being archived
// would probably be listed in EVERY SINGLE archive file,
// so that there would never be a possibility that one
// of the archive files would be unpacked and there
// wouldn't be a directory to receive one of the files
// in that sub-collection.  So the directories are
// listed separately so that they can be treated
// separately for splitting.
// 
/////////////////////////
class Parfu_target_collection
{
public:
  // everyday-use constructor
  Parfu_target_collection(){
  }
  // bring in an entire directory tree
  Parfu_target_collection(Parfu_directory *in_directory);

  void dump(void);
  
  // copy constructor
  Parfu_target_collection(const Parfu_target_collection &in_collec){
    for( unsigned int i=0 ; i < in_collec.directories.size() ; i++){
      directories[i] = in_collec.directories[i];
    }
    for( unsigned int i=0 ; i < in_collec.files.size() ; i++){
      files[i] = in_collec.files[i];
    }
  }
  // assignment operator
  Parfu_target_collection& operator=(const Parfu_target_collection &in_collec){
    for( unsigned int i=0 ; i < in_collec.directories.size() ; i++){
      directories[i] = in_collec.directories[i];
    }
    for( unsigned int i=0 ; i < in_collec.files.size() ; i++){
      files[i] = in_collec.files[i];
    }
    return *this;
  }
  
  // destructor
  ~Parfu_target_collection(void){
  }
  void order_files(void);
  void set_offsets();
  void dump_offsets(void);
  vector <string> *create_transfer_orders(int archive_file_index,
					  long unsigned int bucket_size);
  string print_marching_order(int file_index,
			      Parfu_storage_reference myref);
  string print_marching_order_raw(int file_index,
				  Parfu_storage_reference myref,
				  unsigned long mysize,
				  unsigned int my_header_size,
				  unsigned long my_container_offset,
				  unsigned long my_file_offset);
    
private:
  vector <Parfu_storage_reference> directories;
  vector <Parfu_storage_reference> files;
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

string print_marching_order(int file_index,
			    Parfu_storage_reference myref);
