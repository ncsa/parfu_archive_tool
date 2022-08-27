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

#include<algorithm>

// these functions transform a target file to a line in a catalog, or take a
// line from a catalog (as a string) and constructs the corresponding
// target file class
// For now taking definitions and format for these lines
// from parfu_buffer_utils.c

//void Parfu_target_file::slices_init(void){
//  // offset of file within itself is zero
//  slices.push_back(Parfu_file_slice(file_size,0));
//}

char Parfu_storage_entry::type_char(void){
  switch(entry_type_value){
  case PARFU_FILE_TYPE_DIRECTORY:
    return PARFU_FILE_TYPE_DIRECTORY_CHAR;
  case PARFU_FILE_TYPE_SYMLINK:
    return PARFU_FILE_TYPE_SYMLINK_CHAR;
  case PARFU_FILE_TYPE_REGULAR:
    return PARFU_FILE_TYPE_REGULAR_CHAR;
  default:
    return PARFU_FILE_TYPE_INVALID_CHAR;
  }
}


long int Parfu_storage_entry::next_available_after_me(long int start_of_available){

  // this function serves as a sort of informal iterator while
  // stringing files together into an archive.  The argument of
  // this function is location of the very first available byte
  // in the archive file.  This function then works out where after that
  // this file's header begins, where this file's actual file data begins,
  // and where it ends.  It then hands the next byte available AFTER
  // this file back as the return value.

  // As we work through this problem, this is our running
  // pointer keeping track of where we are in the overall
  // container
  long int container_pointer;
  long int header_location;
  long int file_contents_location;
  int my_header_size;
  
  
  container_pointer = start_of_available;
  if(container_pointer % BLOCKSIZE){
    // if the next available block starts on a blocksize
    // boundary we start allocating there.  Otherwise we start
    // at the next even block boundary.  This is a core feature
    // that makes parfu tar-compatible
    container_pointer =
      ( ( container_pointer / BLOCKSIZE ) + 1 ) * BLOCKSIZE;
  }
  header_location = container_pointer;
  my_header_size = this->header_size();
  file_contents_location = header_location + my_header_size;
  //  if(slices.size()>0){
  //    throw "next_available_after_me: file already has base slice!!!\n";
  //  }
  //  slices.push_back(Parfu_file_slice(file_size,0,header_location));
  
  return file_contents_location + file_size;
}

//int Parfu_target_file::fill_out_locations(long int start_offset,
//					  long int slice_size){
//  //  int internal_n_slices;
//  //  int internal_file_size;
//
//  // start_offset is the input starting location in the
//  // archive file.  This function then populates the
//  // slice(s) with the corresponding slice(s) location(s)
//
//  //  internal_n_slices = 1 + ( file_size / slice_size );
//  
//  are_locations_filled_out=true;
//  return slices.size();
//}

string Parfu_storage_entry::generate_archive_catalog_line(void){
  // dump contents as a string
  // format is the *archive* catalog line
  // (the shorter one)
  string out_string;

  // path + filename within the archive
  out_string.append(relative_path);
  out_string.append("\t"); // \t

  // type
  switch(this->entry_type()){
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
  out_string.append(to_string(this->header_size()));
  out_string.append("\t");  // \t

  // location in archive file
  //  out_string.append(to_string(
  out_string.append("\n");// \n

  return out_string;  
}

// OOPS this needs to be per slice?  (called "fragment" in
// .c version?

string Parfu_storage_entry::generate_full_catalog_line(void){
  // dump contents as a string
  // format is the *full* catalog line
  // (the longer one)
  // this line will be suitable for sending over MPI
  // to other ranks.
  string out_string;

  // path + filename, realtive to parfu process CWD
  out_string.append(relative_path);
  out_string.append("\t"); // \t

  // path + filename within the archive
  // \t
  // type
  //  switch(file_type_value){
  switch(this->entry_type()){
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

int Parfu_storage_entry::header_size(void){
  if(tar_header_size<0){
    // compute it
    string my_absolute_path=this->absolute_path();
    tar_header_size =
      tarentry::compute_hdr_size(my_absolute_path.c_str(),symlink_target.c_str(),file_size);
  }
  return tar_header_size;
}

Parfu_target_file::Parfu_target_file(string catalog_line){
  // take a catalog line as a string as input
  // and build a target file class  
}

//Parfu_target_file::Parfu_target_file(string in_base_path, string in_relative_path,
//				     int in_file_type){
//  relative_full_path=in_relative_path;
//  base_path=in_base_path;
//  file_type_value=in_file_type;
  
//}

//Parfu_target_file::Parfu_target_file(string in_base_path, string in_relative_path,
//				     int in_file_type, string in_symlink_target){
//  relative_full_path=in_relative_path;
//  base_path=in_base_path;
//  file_type_value=in_file_type;
//  symlink_target = in_symlink_target;
//}

Parfu_target_file::Parfu_target_file(string in_base_path, string in_relative_path,
				     int in_file_type, long int in_file_size){
  relative_path=in_relative_path;
  base_path=in_base_path;
  entry_type_value=in_file_type;
  file_size = in_file_size;

  //  this->slices_init();
}

Parfu_target_file::Parfu_target_file(string in_base_path, string in_relative_path,
				     int in_file_type, long int in_file_size,
				     string in_symlink_target){

  relative_path=in_relative_path;
  base_path=in_base_path;
  entry_type_value=in_file_type;
  file_size = in_file_size;
  symlink_target = in_symlink_target;
  //  this->slices_init();
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

  string my_directory_path;

  string link_target;
  // File size if > 0.  Otherwise, this will be some slightly
  // negative number useful for classification
  long int file_size=(-1);
  // TODO: make this sensitive to command-line input
  int follow_symlinks=0;
  
  if(spidered){
    cerr << "This directory already spidered!  >>" << base_path << "\n";
    return -1L;
  }
  cerr << "spider dir: base=>" << base_path ;
  cerr << "< relative=>" << relative_path << "<\n";

  
  my_directory_path = base_path;

  if(relative_path.length() > 0){
    my_directory_path.append("/");
    my_directory_path.append(relative_path);
  }
    
  //    for (const auto & next_entry : std::filesystem::directory_iterator(directory_path)){
  if((my_dir=opendir(my_directory_path.c_str()))==nullptr){
    cerr << "Could not open directory >>" << my_directory_path << "<<for scanning!\n";
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

    //    cerr << "Raw filename: >" << string(next_entry->d_name) << "<\n";
    //    if(next_entry == nullptr){
    //      cerr << "\nFound nullptr entry in directory!!!\n\n";
    //    }
    // traverse once per entry
    // skip over "." and ".."
    if(!strncmp(next_entry->d_name,".",1) &&
       (strlen(next_entry->d_name)==1) ){
      continue;
    }
    if(!strncmp(next_entry->d_name,"..",2) &&
       (strlen(next_entry->d_name)==2) ){
      continue;
    }
    // We know now that it's an actual thing with a name,
    // so we need to check *what* it is
    string entry_bare_name = string(next_entry->d_name);
    //    string entry_relative_name = directory_path;
    string entry_relative_name = string("");
    string entry_full_name;
    if(relative_path.length() == 0){
      entry_relative_name = entry_bare_name;
    }
    else{
      entry_relative_name = relative_path;
      entry_relative_name.append("/");
      entry_relative_name.append(entry_bare_name);
    }
    // entry_relative_name is the next entry.  
    // we now need to check it to find out what it is 
    // (directory,regular file, symlink) and how big
    // it is if it's a regular file.
    //
    // for file system purposes we need to combine it with the
    // base path so that we can find it.
    if(base_path.length()){
      entry_full_name = base_path;
      entry_full_name.append("/");
    }
    entry_full_name.append(entry_relative_name);
    path_type_result=
      parfu_what_is_path(entry_full_name.c_str(),link_target,&file_size,follow_symlinks);
    //    cout << "relative name: >>" << entry_full_name << "<< file size: " << file_size;
    //    cout << " file type:" << path_type_result << "\n";
    switch(path_type_result){
    case PARFU_WHAT_IS_PATH_DOES_NOT_EXIST:
      cerr << "Parfu_directory const; does not exist: >>" << entry_full_name << "<<\n";
      // this should generally never happen 
      break;
    case PARFU_WHAT_IS_PATH_IGNORED_TYPE:
      cerr << "Parfu_directory spider_dir function: ignored type, will skip file: >>" << entry_full_name << "<<\n";
      // I presume for now we'll just jump over this entry without acknowledging it or storing
      // anywhere.  This would be an entry that's not a file, not a symlink, and not a
      // subdirectory.  So....a dev file?  Something else?  Probably safe to not save it
      // to the archive.  Possibly leaving the warning?  
      break;
    case PARFU_WHAT_IS_PATH_REGFILE:
      // it's a regular file that we need to store.  This is the
      // core of what parfu needs to tackle.
      Parfu_target_file *new_target_file_ptr;
      new_target_file_ptr = new 
	Parfu_target_file(base_path,entry_relative_name,PARFU_FILE_TYPE_REGULAR,file_size);
      subfiles.push_back(new_target_file_ptr);
      break;
    case PARFU_WHAT_IS_PATH_DIR:
      // it's a directory that we need to note and it will need to be spidered in the future
      Parfu_directory *new_subdir_ptr;
      new_subdir_ptr =
      	new Parfu_directory(base_path,entry_relative_name);
      new_subdir_ptr->file_size = 0L;
      subdirectories.push_back(new_subdir_ptr);
      break;
    case PARFU_WHAT_IS_PATH_SYMLINK:
      // simlink that we'll need to store for now
      Parfu_target_file *my_tempfile;
      my_tempfile = 
	new Parfu_target_file(base_path,entry_relative_name,PARFU_FILE_TYPE_SYMLINK,0,link_target);
      //      my_tempfile->set_symlink_target(link_target);
      subfiles.push_back(my_tempfile);
      break;
    case PARFU_WHAT_IS_PATH_ERROR:
      // not sure what would cause an error in this function, but catch it here
      cerr << "Parfu_directory const; ERROR from what_is_path: >>" << entry_full_name << "<<\n";
      break;
    default:
      // don't know if it's possible to fall through to here??
      cerr << "Parfu_directory const; reached default branch??: >>" << entry_full_name << "<<\n";
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

Parfu_target_collection::Parfu_target_collection(Parfu_directory *in_directory){
  // take a directory tree, presumably a root of a collection of files,
  // and create a target collection with all the contents
  // filed
  // This function will merely bring in the contents and set the sizes
  // properly, this will not set up the container offsets.  That's another
  // function.

  Parfu_storage_reference my_ref;
  Parfu_file_slice my_slice;

  std::list<Parfu_storage_reference> myiter;
  
  //  cerr << "starting constructor function\n";
  // prepare to process directories
  // the collection could be pointed at a file, in which case there will
  // be zero, but most likely we have at least one
  my_ref.order_size = PARFU_FILE_SIZE_DIR;
  //  cerr << "finished first assignment\n";

  // We're creating a boilerplate (unpopulated) slice
  // here just as a placeholder.  The following statements will
  // populate it with actual data. 
  my_ref.slices.push_back(my_slice); // has one and only once slice here
  my_slice = my_ref.slices.front(); // my_slice is within my_ref
  //  cerr << "pulled my_slice back out\n";
  my_slice.slice_offset_in_file = 0L; // ony one; always at the beginning of the file
  my_slice.slice_size = 0L; // directories are zero payload size
  // the offset_in_container's will be set sequentially, probably after we create
  // this collection.  So setting them to an invalid value (-1) for now.
  (my_ref.slices.front()).slice_offset_in_container = -1L; 

  // We expect we have been given the root of a directory tree.  If that's the
  // case, we would expect the relative_path to be an empty string and the
  // base path to be the path to the directory.
  // TODO: it would probably be smart to check that here specifically, and then
  // do the 0th iteration outside the loop, then start the loop at index=1
  // to do the rest of the (sub) directories in that root directory.

  // this is all assuming that the target
  // (argument to this function) is a directory.  We could perform
  // this same function if the target was a file; we'd have to modify it
  // a bit but it would work the same.  
  
  // We need to attach the root spidered directory here.  While its existence
  // could be assumed, it contains files and symlinks, so it has to be in the 
  // list even though it doesn't contain any actual separate information.
  //  my_ref.storage_ptr = *(  (in_directory->subdirectories.data()) + 0 );    
  //  my_ref.storage_ptr = in_directory->nth_subdir(0);

  // first, add the root directory we were given to our directory list.  

  my_ref.storage_ptr = in_directory;
  my_slice.header_size_this_slice =
    my_ref.storage_ptr->header_size();
  directories.push_back(my_ref);
  
  // now walk through the directory list, and for each directory, grab each of its
  // subdirectories and in turn add them to the end of the list.  As we go
  // through the directory list, it will subsequently grow (this is why we
  // can't use an iterator for this).  Once we finish the frequently-growing
  // list, this directories class contains pointers to the directory
  // we were passed and ALL of its subdirectories.  

  for(unsigned int myiter = 0 ; myiter < directories.size(); myiter++ ){

    Parfu_storage_entry *loop_dir_ptr = directories.at(myiter).storage_ptr;
    if(loop_dir_ptr == nullptr){
      cerr << "Warning!!!!!  loop_dir_ptr is NULL!\n";
    }

    //    cerr << "subdirs=" << loop_dir_ptr->N_subdirs() << "\n";
    for(std::size_t dir_ndx=0; dir_ndx < loop_dir_ptr->N_subdirs();dir_ndx++){
      my_ref.storage_ptr =
	loop_dir_ptr->nth_subdir(dir_ndx);
      my_slice.header_size_this_slice =
	my_ref.storage_ptr->header_size();      
      directories.push_back(my_ref);
    } // for(std::size_t dir_ndx=0;
  } // for(auto myiter = directories.begin();

  // Now walk through the directories again, and this time, pull in all the
  // files and sylinks in each directory and add them to our subfiles list.

  for(unsigned int myiter = 0 ; myiter < directories.size(); myiter++ ){
    Parfu_storage_entry *loop_dir_ptr = directories.at(myiter).storage_ptr;
    if(loop_dir_ptr == nullptr){
      cerr << "Warning!!!!!  loop_dir_ptr is NULL!\n";
    }
    
    //    cerr << "subfiles=" << loop_dir_ptr->N_subfiles() << "\n";
    for(std::size_t file_ndx=0; file_ndx < loop_dir_ptr->N_subfiles(); file_ndx++){
      //      cerr << "one file\n";
      my_ref.storage_ptr =
	loop_dir_ptr->nth_subfile(file_ndx);
      my_slice.header_size_this_slice =
	my_ref.storage_ptr->header_size();
      if(my_ref.storage_ptr->is_symlink()){
	my_ref.order_size=PARFU_FILE_SIZE_SYMLINK;
      }
      else{
	my_ref.order_size=my_ref.storage_ptr->file_size;
      }
      files.push_back(my_ref);
    } // for(std::size_t file_ndx=0; 
  } // for(unsigned int myiter = 0 ;
    
}

void Parfu_target_collection::dump(void){
  Parfu_storage_entry *this_entry;
  //  this_entry=directories.start();
  cerr << "Parfu_target_collection::dump() running\n";
  cerr << "dump: first directories\n";
  for(std::size_t ndx=0; ndx < directories.size(); ndx++){
    this_entry = (directories.data() + ndx)->storage_ptr;
    cerr << "index=" << ndx << " base_path=>" << this_entry->base_path << "< ";
    cerr << "relative_path=>" << this_entry->relative_path << "< OS=";
    cerr << (directories.data() + ndx)->order_size << "\n";
  }
  cerr << "dump: then files\n";
  for(std::size_t ndx=0; ndx < files.size(); ndx++){
    this_entry = (files.data() + ndx)->storage_ptr;
    cerr << "index=" << ndx << " base_path=>" << this_entry->base_path << "< ";
    cerr << "relative_path=>" << this_entry->relative_path << "< OS=";
    cerr << (files.data() + ndx)->order_size << "\n";
  }
}

void Parfu_target_collection::dump_offsets(void){
  Parfu_storage_reference my_ref;
  Parfu_storage_entry *this_entry;
  //  cerr << "dumping offsets\n";
  for(std::size_t ndx=0; ndx < directories.size(); ndx++){
    my_ref = directories.at(ndx);
    this_entry = (directories.data() + ndx)->storage_ptr;
       cerr << "rp=" << this_entry->relative_path;
        fprintf(stderr,"       %06lu",my_ref.slices.begin()->slice_offset_in_container);
        cerr << "\n";
  }
  //  cerr << "directory offsets finished; now files:\n";
  for(std::size_t ndx=0; ndx < files.size(); ndx++){
    my_ref = files.at(ndx);
    this_entry = (files.data() + ndx)->storage_ptr;
        cerr << "rp=" << this_entry->relative_path;
        fprintf(stderr,"       %06lu",my_ref.slices.begin()->slice_offset_in_container);

    //    cerr << "  header_size=" << this_entry->header_size();
        cerr << "\n";
  }
}

bool sort_by_size(Parfu_storage_reference item1, Parfu_storage_reference item2){
  return (item1.order_size < item2.order_size);
}

void Parfu_target_collection::order_files(void){
  // sort file entries in order of increasing size_order.
  std::sort(files.begin(),files.end(),sort_by_size);
}

long unsigned int parfu_next_block_boundary(long unsigned int first_available){
  long unsigned int working_location = first_available;
  if(working_location % BLOCKSIZE){
    // if the next available block starts on a blocksize
    // boundary we start allocating there.  Otherwise we start
    // at the next even block boundary.  This is a core feature
    // that makes parfu tar-compatible
    working_location =
      ( ( working_location / BLOCKSIZE ) + 1 ) * BLOCKSIZE;
  }
  return working_location;
}



void Parfu_target_collection::set_offsets(){
  // After this fuction this collection will have valid
  // offsets all the way through it.  The files aren't sub-divded, though. 
  long unsigned int working_offset = 0L;
  long int my_file_size;
  long unsigned int total_extent; // length of header plus file payload
  long unsigned int next_offset;
  //  int return_val;
  int my_header_size;

  // flush the existing directory slices
  for(std::size_t ndx=0; ndx < directories.size(); ndx++){
    directories.at(ndx).slices.erase(directories.at(ndx).slices.begin(),directories.at(ndx).slices.end());
    if(directories.at(ndx).slices.size() > 0){
      cerr << "Warning!  directories index " << ndx << "has length > 0 after erasing!\n";
    }
  }
  // flush the existing file slices
  for(std::size_t ndx=0; ndx < files.size(); ndx++){
    files.at(ndx).slices.erase(files.at(ndx).slices.begin(),files.at(ndx).slices.end());
  }

  // every entry has an empty vector of slices; all offsets are gone
  // now we walk the collection from the beginning, starting with
  // offset zero

  working_offset = 0L;
  // first directories
  for(std::size_t ndx=0; ndx < directories.size(); ndx++){
    my_header_size = directories.at(ndx).storage_ptr->header_size();
    my_file_size = directories.at(ndx).storage_ptr->file_size;
    if(my_file_size < 0){
      cerr << "DANGER!  File size < 0!!! dir:" << directories.at(ndx).storage_ptr->relative_path << "\n";
    }
    total_extent = my_header_size + my_file_size;
    next_offset = working_offset + total_extent;
    directories.at(ndx).slices.push_back(Parfu_file_slice(my_header_size,my_file_size,0L,working_offset));
    //    cerr << "SPECIAL: " << my_header_size << "  " << directories.at(ndx).slices.back().header_size_this_slice;
    //    cerr << working_offset << "\n";
    //    cerr << "\n";
    // now we do book keeping to set up for the next item
    working_offset = next_offset;
    working_offset = parfu_next_block_boundary(working_offset);
  }
  // then files
  //  cerr << "set_offsets function done.\n";
  for(std::size_t ndx=0; ndx < files.size(); ndx++){
    //    fprintf(stderr,"setting_offset->%06lu\n",working_offset);
    my_header_size = files.at(ndx).storage_ptr->header_size();
    my_file_size = files.at(ndx).storage_ptr->file_size;
    if(my_file_size < 0){
      //      cerr << "DANGER!  File size < 0!!!\n";
      cerr << "DANGER!  File size < 0!!! file:" << files.at(ndx).storage_ptr->relative_path << "\n";
    }
    total_extent = my_header_size + my_file_size;
    next_offset = working_offset + total_extent;
    files.at(ndx).slices.push_back(Parfu_file_slice(my_header_size,my_file_size,0L,working_offset));

    // now we do book keeping to set up for the next item
    working_offset = next_offset;
    working_offset = parfu_next_block_boundary(working_offset);
  }  
}

vector <string> *Parfu_target_collection::create_transfer_orders(int archive_file_index,
								 long unsigned int bucket_size){
  // vector of output buffers containing transfer instructions
  // (essentially just a series of buffers that will be sent
  // via MPI but managed by string classes)
  vector <string> *trans_orders = new vector <string>;
  // endpoint is one byte past the last byte the archive file should occupy
  long unsigned int endpoint;
  long unsigned int position_in_archive;
  // bucket here is the chunk of archive that will be handled by a single rank
  // mover node.  
  long unsigned int position_in_bucket;
  long unsigned int next_position_in_bucket;
  long unsigned int total_extent;
  long unsigned int position_jump;
  Parfu_file_slice last_slice;
  
  if(bucket_size < 100000){
    cerr << "create_transfer_orders called w/ bucket_size=" << bucket_size << "\n";
    cerr << "This is extremely unlikely to work.\n";
    return nullptr;
  }
  
  if(files.size() > 0){
    last_slice = files.back().slices.back();
  }
  else{
    if(directories.size() > 0){
      last_slice = directories.back().slices.back();
    }
    else{
      // somehow we got a collection that contains zero directories and
      // zero files.  I don't know that that's possible; except maybe
      // if the initial directory didn't exist at all?
      cerr << "WARNING!  create_transfer_orders received empty set!!!\n";
      return nullptr;
    }
  }
  // at this point we know we have at least one directory or
  // at least one file, so we have stuff to save to an archive file
  // and last_slice indicates the last thing in that archive  

  endpoint = last_slice.slice_offset_in_container;
  endpoint += last_slice.header_size_this_slice;
  endpoint += last_slice.slice_size;
  // endpoint is now one byte past where we should end
  // we use it as a signpost to tell when we're done

  // we start by loading the tranfer orders vector with an empty buffer
  trans_orders->push_back(string(""));
  // Our virtual position in the archive file starts at the
  // beginning of the data area
  position_in_archive = 0UL;
  // We start our position in the virtual buck likewise
  // at its beginning
  position_in_bucket = 0UL;
  
  // iterate through directories
  
  for(unsigned int ndx=0;ndx<directories.size();ndx++){
    Parfu_storage_entry *mydir = directories.at(ndx).storage_ptr;
    //    Parfu_file_slice *myslice = &(directories.at(ndx).slices.back());
    //    unsigned long int position_in_file=0UL;
    total_extent = mydir->header_size();
    position_jump = parfu_next_block_boundary(total_extent);
    next_position_in_bucket = position_in_bucket + position_jump;
    
    if(total_extent <= bucket_size){
      // this file can live in one bucket
      if(next_position_in_bucket > bucket_size){
	// due to previous files in this bucket,
	// it spills off the end, so we jump to the
	// next bucket.  We load a new empty buffer,
	// leaving the other one complete
	trans_orders->push_back(string(""));
	// back to the beginning of the bucket
	position_in_bucket = 0UL;
	// this now *now* where our file will end
	next_position_in_bucket = position_in_bucket + position_jump;
	// if this ((total_extent <= bucket_size) comparison above
	// worked correctly, the following should NEVER be true.
	if(next_position_in_bucket > bucket_size){
	  cerr << "WARNING!!! bucket size math error!\n";
	}
      }
      // whatever bucket we're in, this file will fit in it
      trans_orders->back().append(print_marching_order(archive_file_index,
						       directories.at(ndx)));
      position_in_bucket = next_position_in_bucket;
      position_in_archive += position_jump;
      
    } // if(total_extent <= bucket_size)

    // there would be an "else" here, except that we know that
    // the largest a directory can be in the archive is the size of
    // its tar header, which presumably is smaller than the
    // bucket size.  
    
  } // for( ndx over directories
  
  // now we loop over the files
  for(unsigned int ndx=0 ; ndx < files.size() ; ndx++){
    Parfu_storage_entry *myfile = files.at(ndx).storage_ptr;
    //    Parfu_file_slice *myslice = &(directories.at(ndx).slices.back());
    total_extent = myfile->header_size() + myfile->file_size;
    // position_jump is the next position in the archive file
    // we will write, given the block structure of tarfiles
    position_jump = parfu_next_block_boundary(total_extent);
    next_position_in_bucket = position_in_bucket + position_jump;
    
    if(total_extent <= bucket_size){
      // this file can live in one bucket
      if(next_position_in_bucket > bucket_size){
	// due to others in bucket, must jump to next
	trans_orders->push_back(string(""));
	// back to the beginning of the bucket
	position_in_bucket = 0UL;
	// this now *now* where our file will end
	next_position_in_bucket = position_in_bucket + position_jump;
	// if this ((total_extent <= bucket_size) comparison above
	// worked correctly, the following should NEVER be true.
	if(next_position_in_bucket > bucket_size){
	  cerr << "WARNING!!! bucket size math error!\n";
	}
      }
      // whatever bucket we're in, this file will fit in it
      //      cerr << "check before call: " << files.at(ndx).slices.front().header_size_this_slice << "\n";
      trans_orders->back().append(print_marching_order(archive_file_index,
						       files.at(ndx)));
      if (files.at(ndx).slices.front().slice_offset_in_container !=
	  position_in_archive){
	cerr << "WARNING!  Offset mismatch! "
	     << to_string(files.at(ndx).slices.front().slice_offset_in_container)
	     << to_string(position_in_archive) << "\n";
      }

      // updated/cleanup for next entry
      position_in_bucket = next_position_in_bucket;
      position_in_archive += position_jump;
      
    } // if(total_extent <= bucket_size)
    else{
      // this file will definitely not fit in a bucket, so we'll
      // start by jumping to the next bucket no matter what
      unsigned long int position_in_file=0UL;
      unsigned long int extent_remaining;
      if(trans_orders->back().size()>0)
	trans_orders->push_back(string(""));
      extent_remaining = total_extent;
      
      // loop over the file, cutting into bucket-sized pieces until
      // we put the remainder in the last one

      // iterate by lowering extent_remaining as we set up transfer orders
      // position_in_bucket is not relevant for this section because all
      // transfers start at an integer multiple of buckes from the
      // beginning of the file

      // first we do one iteration that includes the header
      // so that the header will get written, but only once.
      // note that it contains the correct, non-zero header size
      // to indicate to the receiving rank that this entry is to
      // have the header placed before it.
      trans_orders->back().append(print_marching_order_raw(archive_file_index,
							   files.at(ndx),
							   bucket_size,
							   files.at(ndx).storage_ptr->header_size(),
							   position_in_archive,
							   position_in_file));
      
      position_in_file += (bucket_size - files.at(ndx).storage_ptr->header_size());
      extent_remaining -= bucket_size;
      position_in_archive += bucket_size;
      
      while(extent_remaining > bucket_size){
	// we go through this while loop setting up transfers until
	// exactly one bucket or less is left to transfer

	// print the orders for this full bucket
	trans_orders->push_back(string(""));
	trans_orders->back().append(print_marching_order_raw(archive_file_index,
							     files.at(ndx),
							     bucket_size, // full bucket
							     0,   // header zero because header would have
							          // already been entered by first bucket
							          // of the file
							     position_in_archive,
							     position_in_file));
	
	position_in_file += bucket_size;
	extent_remaining -= bucket_size;
	position_in_archive += bucket_size;
      }
      // and now take care of the last file fragment that's smaller than
      // a bucket (possibly zero if the file extent (file itself plus its
      // header) is exactly a multiple of bucket size)
      if(extent_remaining > 0){
	trans_orders->push_back(string(""));
	trans_orders->back().append(print_marching_order_raw(archive_file_index,
							     files.at(ndx),
							     extent_remaining, // just the remainder
							     0,   // header zero because header would have
							          // already been entered by first bucket
							          // of the file
							     position_in_archive,
							     position_in_file));
	// the per-file counters don't need cleaning up, but we do need to roll
	// the main archive position to the next tar-compatible block position
	position_in_archive += extent_remaining;
	position_in_archive = parfu_next_block_boundary(position_in_archive);
      } // if(extent_remaining > 0)
      
    } // else (if the file extent is bigger than a bucket

  } // for( ndx over files
  
  return trans_orders;
}

string Parfu_target_collection::print_marching_order(int file_index,
						     Parfu_storage_reference myref){
  //  cerr << "debug: " << myref.slices.front().header_size_this_slice << "\n";
  return print_marching_order_raw(file_index,
				  myref,
				  myref.slices.front().slice_size,
				  myref.slices.front().header_size_this_slice,
				  myref.slices.front().slice_offset_in_container,
				  myref.slices.front().slice_offset_in_file);
}

string Parfu_target_collection::print_marching_order_raw(int file_index,
							 Parfu_storage_reference myref,
							 unsigned long mysize,
							 unsigned int my_header_size,
							 unsigned long my_container_offset,
							 unsigned long my_file_offset){
  string out_string;
  string type_string;
  
  out_string.append(to_string(file_index));
  out_string.append("\t");
  out_string.append(myref.storage_ptr->relative_path);
  out_string.append("\t");
  type_string = myref.storage_ptr->type_char();
  out_string.append(type_string);
  out_string.append("\t");
  out_string.append(myref.storage_ptr->symlink_target);     
  out_string.append("\t");
  out_string.append(to_string(mysize));
  out_string.append("\t");
  out_string.append(to_string(my_header_size));
  out_string.append("\t");
  out_string.append(to_string(my_container_offset));
  out_string.append("\t");
  out_string.append(to_string(my_file_offset));
  out_string.append("\n");

  return out_string;
}

