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

#include "parfu_primary.h"
#include "tarentry.hh"

extern "C"
parfu_file_fragment_entry_list_t 
*create_parfu_file_fragment_entry_list(int in_n_entries){
  parfu_file_fragment_entry_list_t *my_list=NULL;
  int i;

  if(in_n_entries<1){
    fprintf(stderr,"create_parfu_file_fragment_entry_list:\n");
    fprintf(stderr," number of entries must be 1 or greater.\n");
    fprintf(stderr," you requested %d!!!\n",in_n_entries);
    return NULL;
  }
  if((my_list=(parfu_file_fragment_entry_list_t*)malloc(sizeof(parfu_file_fragment_entry_list_t)+
		     ((sizeof(parfu_file_fragment_entry_t))*(in_n_entries-1))))==NULL){
    fprintf(stderr,"create_parfu_file_fragment_entry_list:\n");
    fprintf(stderr,"Could not allocate list of size %d!\n",in_n_entries);
    return NULL;
  }
  my_list->n_entries_total=in_n_entries;
  my_list->n_entries_full=0;
  for(i=0;i<in_n_entries;i++){
    my_list->list[i].relative_filename=NULL;
    my_list->list[i].archive_filename=NULL;
    my_list->list[i].type=(parfu_file_t)PARFU_FILE_TYPE_INVALID;
    my_list->list[i].target=NULL;

    my_list->list[i].our_file_size=PARFU_SPECIAL_SIZE_INVALID_REGFILE;
    my_list->list[i].our_tar_header_size=-1;
    my_list->list[i].location_in_archive_file=-1;
    my_list->list[i].location_in_orig_file=0;

    my_list->list[i].pad_file_location_in_archive_file=-1;
    my_list->list[i].pad_file_archive_filename=NULL;
    my_list->list[i].pad_file_tar_header_size=-1;

    my_list->list[i].file_contains_n_fragments=-1;
    my_list->list[i].file_ptr_index=-1;

    my_list->list[i].rank_bucket_index=-1;

    //    my_list->list[i].block_size_exponent=-1;
    //    my_list->list[i].num_blocks_in_fragment=-1;
        //    my_list->list[i].fragment_offset=-1;
    //    my_list->list[i].size=PARFU_SPECIAL_SIZE_INVALID_REGFILE;
    // my_list->list[i].first_block=-1;
    //    my_list->list[i].number_of_blocks=-1;
  }
  
  return my_list;
}

extern "C"
void parfu_free_ffel(parfu_file_fragment_entry_list_t *my_list){
  int i;
  for(i=0;i<(my_list->n_entries_total);i++){
    if( my_list->list[i].relative_filename != NULL )
      free(my_list->list[i].relative_filename);
    if( my_list->list[i].archive_filename != NULL )
      free(my_list->list[i].archive_filename);
    if( my_list->list[i].target != NULL )
      free(my_list->list[i].target);
    if( my_list->list[i].pad_file_archive_filename != NULL )
      free(my_list->list[i].pad_file_archive_filename);
  }
  free(my_list);
}

// add name to file parfu file fragment entry list
extern "C"
int parfu_add_name_to_ffel(parfu_file_fragment_entry_list_t **my_list,
			   parfu_file_t my_type,
			   char *my_relative_filename,
			   char *my_archive_filename,
			   char *my_target,
			   long int my_size){
  int return_value;
  long int local_tar_header_size;
  if(my_target){
    local_tar_header_size=
      tarentry::compute_hdr_size(my_relative_filename,
				 my_target,my_size);
  }
  else{
    local_tar_header_size=
      tarentry::compute_hdr_size(my_relative_filename,
				 "",my_size);    
  }
  if((return_value=
      parfu_add_entry_to_ffel_raw(my_list,my_relative_filename,my_archive_filename,
				  my_type,my_target,
				  my_size,local_tar_header_size,
				  -1,-1,-1,NULL,-1,-1,-1,-1,-1))){
    fprintf(stderr,"parfu_add_name_to_ffel:\n");
    fprintf(stderr," return from parfu_add_entry_to_ffel_raw was: %d\n",return_value);
    return return_value;
  }
  
  return 0;
}


extern "C"
int parfu_add_entry_to_ffel_mod(parfu_file_fragment_entry_list_t **list,
				parfu_file_fragment_entry_t entry,
				long int my_size,
				long int my_fragment_loc_in_archive_file,
				long int my_fragment_loc_in_orig_file,
				int my_file_contains_n_fragments,
				int my_file_ptr_index,
				long int my_rank_bucket_index){
  int return_value;
  if((return_value=
     parfu_add_entry_to_ffel_raw(list,
				 entry.relative_filename,
				 entry.archive_filename,
				 entry.type,
				 entry.target,
				 my_size,
				 entry.our_tar_header_size,
				 my_fragment_loc_in_archive_file,
				 my_fragment_loc_in_orig_file,
				 entry.pad_file_location_in_archive_file,
				 entry.pad_file_archive_filename,
				 entry.pad_file_tar_header_size,
				 entry.pad_file_size,
				 my_file_contains_n_fragments,
				 my_file_ptr_index,
				 my_rank_bucket_index))){
    fprintf(stderr,"parfu_add_entry_to_ffel_mod:\n");
    fprintf(stderr," return from parfu_add_entry_to_ffel_raw was: %d\n",return_value);
    return return_value;
  }
  return 0;  
}

extern "C"
int parfu_add_entry_to_ffel(parfu_file_fragment_entry_list_t **list,
			    parfu_file_fragment_entry_t entry){
  int return_value;
  if((return_value=
     parfu_add_entry_to_ffel_raw(list,
				 entry.relative_filename,
				 entry.archive_filename,
				 entry.type,
				 entry.target,
				 entry.our_file_size,
				 entry.our_tar_header_size,
				 entry.location_in_archive_file,
				 entry.location_in_orig_file,
				 entry.pad_file_location_in_archive_file,
				 entry.pad_file_archive_filename,
				 entry.pad_file_tar_header_size,
				 entry.pad_file_size,
				 entry.file_contains_n_fragments,
				 entry.file_ptr_index,
				 entry.rank_bucket_index))){
    fprintf(stderr,"parfu_add_entry_to_ffel:\n");
    fprintf(stderr," return from parfu_add_entry_to_ffel_raw was: %d\n",return_value);
    return return_value;
  }
  return 0;  
}

extern "C"
int parfu_add_entry_to_ffel_raw(parfu_file_fragment_entry_list_t **list,
				char *my_relative_filename,
				char *my_archive_filename,
				parfu_file_t my_type,
				char *my_target,
				long int my_file_size,
				long int my_tar_header_size,
			        long int my_location_in_archive_file,
				long int my_location_in_orig_file,
				long int my_pad_file_location_in_arch_file,
				char *my_pad_file_archive_filename,
				long int my_pad_file_tar_header_size,
				long int my_pad_file_size,
				int my_file_contains_n_fragments,
				int my_file_ptr_index, 
				long int my_rank_bucket_index){
  int total_entries;
  long int total_size;
  int string_size;
  int ind;
  
  //  fprintf(stderr,"add entry function...");

  // check to make sure things that shouldn't be NULL aren't
  // return with an error if detected.
   if(list==NULL){
    fprintf(stderr,"parfu_add_entry_to_ffel_raw:\n");
    fprintf(stderr,"\"list\" was NULL!  Aborting.\n");
    return -1;
  }
  if((*list)==NULL){
    fprintf(stderr,"parfu_add_entry_to_ffel_raw:\n");
    fprintf(stderr,"\"(*list)\" was NULL!  Aborting.\n");
    return -1;
  }
  if((my_target == NULL ) && 
     (my_type == PARFU_FILE_TYPE_SYMLINK)){
    fprintf(stderr,"parfu_add_name_to_ffel: type=SYMLINK\n");
    fprintf(stderr,"but you have not defined a target.  NOT VALID\n");
    return -1;
  }

  // check to see if the list is full, in which case there aren't
  // any empty lots.  If it is, double the size, then continue.
  if((*list)->n_entries_full >= (*list)->n_entries_total){
    // we have to add to the storage in the list
    total_entries = (*list)->n_entries_total * 2;
    total_size = sizeof(parfu_file_fragment_entry_list_t)+
      ((sizeof(parfu_file_fragment_entry_t))*total_entries);
    if(((*list)=(parfu_file_fragment_entry_list_t*)realloc((*list),total_size))==NULL){
      fprintf(stderr,"parfu_add_entry_to_ffel_raw:\n");
      fprintf(stderr,"could not realloc to extend list to length %ld!!\n",total_size);
      return 1;
    }
    (*list)->n_entries_total = total_entries;
  }
  ind = (*list)->n_entries_full;
  
  // now transfer over all the stuff from the individual items to the 
  // new entry in the list

  if(my_relative_filename!=NULL)
    string_size=strlen(my_relative_filename);
  else
    string_size=0;
  if(((*list)->list[ind].relative_filename = (char*)malloc(string_size+1))==NULL){
    fprintf(stderr,"parfu_add_entry_to_ffel_raw:\n");
    fprintf(stderr,"could not allocate relative_filename string!\n");
    return 1;
  }
  if(my_relative_filename!=NULL)
    memcpy( (*list)->list[ind].relative_filename,my_relative_filename,string_size+1);
  else
    ((*list)->list[ind].relative_filename)[0]='\0';
  
  if(my_archive_filename!=NULL)
    string_size=strlen(my_archive_filename);
  else
    string_size=0;
  if(((*list)->list[ind].archive_filename = (char*)malloc(string_size+1))==NULL){
    fprintf(stderr,"parfu_add_name_to_ffel:\n");
    fprintf(stderr,"could not allocate archive_filename string!\n");
    return 1;
  }
  if(my_archive_filename!=NULL)
    memcpy( (*list)->list[ind].archive_filename ,my_archive_filename,string_size+1);
  else
    ((*list)->list[ind].archive_filename)[0]='\0';
  
  (*list)->list[ind].type = my_type;  
  
  if(my_target!=NULL)
    string_size=strlen(my_target);
  else
    string_size=0;
  if(((*list)->list[ind].target = (char*)malloc(string_size+1))==NULL){
    fprintf(stderr,"parfu_add_name_to_ffel:\n");
    fprintf(stderr,"could not allocate target string!\n");
    return 1;
  }
  if(my_target!=NULL)
    memcpy( (*list)->list[ind].target,my_target,string_size+1);
  else
    ((*list)->list[ind].target)[0]='\0';
  
  (*list)->list[ind].our_file_size = my_file_size;
  (*list)->list[ind].our_tar_header_size = my_tar_header_size;

  (*list)->list[ind].location_in_archive_file=my_location_in_archive_file;
  (*list)->list[ind].location_in_orig_file=my_location_in_orig_file;
  //  (*list)->list[ind].block_size_exponent=my_block_size_exponent;
  //  (*list)->list[ind].num_blocks_in_fragment = my_num_blocks_in_fragment;
  (*list)->list[ind].file_contains_n_fragments = my_file_contains_n_fragments;
  //  (*list)->list[ind].fragment_offset = my_fragment_offset;
  //  (*list)->list[ind].size = my_size;
  //  (*list)->list[ind].first_block = my_first_block;
  //  (*list)->list[ind].number_of_blocks = my_number_of_blocks;
  (*list)->list[ind].file_ptr_index = my_file_ptr_index;

  // derive tar header stuff for *this* file
  (*list)->list[ind].pad_file_location_in_archive_file = -1;
  (*list)->list[ind].pad_file_archive_filename=NULL;
  (*list)->list[ind].pad_file_tar_header_size=-1;
  
  (*list)->list[ind].rank_bucket_index = my_rank_bucket_index;

  // now finally update the number of full items in the list. 
  (*list)->n_entries_full = ind+1;
  
  //  fprintf(stderr,"total entries: %d\n",(*list)->n_entries_full);

  return 0;
}

// removing all original parfu_is_a_* functions
// in 0.5.1 redesign in May 2017.  These original functions 
// all did stat() on their own, so we're deprecating them.
// I'm just commenting them out for the moment just in case
// we need to refer to the code.  --csteffen@ncsa.illinois.edu
// 2017May15

/*
extern "C"
int parfu_is_a_dir(char *pathname){
  struct stat filestruct;
  int returnval;
  
  if((returnval=stat(pathname,&filestruct))){
    fprintf(stderr,"parfu_is_a_dir:\n");
    fprintf(stderr," stat returned %d!\n",returnval);
    return -1;
  }
  if(S_ISDIR(filestruct.st_mode))
    return 1;
  else
    return 0;
}
*/

/*
extern "C"
int parfu_does_not_exist(char *pathname){
  return parfu_does_not_exist_raw(pathname,1);
}

extern "C"
int parfu_does_not_exist_quiet(char *pathname){
  return parfu_does_not_exist_raw(pathname,0);
}

extern "C"
int parfu_does_exist_quiet(char *pathname){
  if(parfu_does_not_exist_quiet(pathname))
    return 0;
  else
    return 1;
}

extern "C"
int parfu_does_not_exist_raw(char *pathname, int be_loud){
  struct stat filestruct;
  int returnval;
  if((returnval=stat(pathname,&filestruct))){
    if(be_loud){
      fprintf(stderr,"parfu_is_a_dir:\n");
      fprintf(stderr," stat returned %d!\n",returnval);
    }
    return 1;
  }
  else{
    return 0;
  }
}
*/

/*
extern "C"
int parfu_is_a_regfile(char *pathname, long int *size){
  struct stat filestruct;
  int returnval;
  
  if((returnval=stat(pathname,&filestruct))){
    fprintf(stderr,"parfu_is_a_dir:\n");
    fprintf(stderr," stat returned %d!\n",returnval);
    return -1;
  }
  if(S_ISREG(filestruct.st_mode)){
    if(size != NULL){
      *size = filestruct.st_size;
    }
    return 1;
  }
  else{
    return 0;
  }
}
*/

// this is the function that we'l use from now on (May 2017)
// This function produced a bit-encoded unsigned int with bits
// telling the calling function what the pathname is. 
// this is so we only have to call stat once.  
extern "C"
unsigned int parfu_what_is_path(const char *pathname,
				char **target_text,
				long int *size,
				int follow_symlinks){
  struct stat filestruct;
  int returnval;
  int buffer_length;
  
  if(target_text==NULL){
    fprintf(stderr,"parfu_what_is_path:\n");
    fprintf(stderr,"  target_text is NULL!!!\n");
    return PARFU_WHAT_IS_PATH_ERROR;
  }
  if(follow_symlinks){ // treat symlinks like what they point to
    if((returnval=stat(pathname,&filestruct))){
      fprintf(stderr,"parfu_what_is_path:\n");
      fprintf(stderr,"  stat returned %d!!!\n",returnval);
      return PARFU_WHAT_IS_PATH_ERROR;
    }
  }
  else{ // treat symlinks like symlinks
    if((returnval=lstat(pathname,&filestruct))){
      fprintf(stderr,"parfu_what_is_path:\n");
      fprintf(stderr,"  lstat returned %d!!!\n",returnval);
      return PARFU_WHAT_IS_PATH_ERROR;
    }
    if(S_ISLNK(filestruct.st_mode)){
      // harvest the symlink target so that we can pass it back
      buffer_length=(filestruct.st_size)+1;
      if(((*target_text)=(char*)malloc(buffer_length))==NULL){
	fprintf(stderr,"parfu_what_is_path:\n");
	fprintf(stderr,"  failed to allocate target text buffer!!\n");
	return PARFU_WHAT_IS_PATH_ERROR;
      }
      returnval=readlink(pathname,(*target_text),buffer_length);
      if(returnval != (buffer_length-1) ){
	fprintf(stderr,"parfu_what_is_path:\n");
	fprintf(stderr,"  error in length of target return!!\n");
	return PARFU_WHAT_IS_PATH_ERROR;
      }
      (*target_text)[returnval]='\0'; // add NULL terminator
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

/*
extern "C"
int parfu_is_a_symlink(const char *pathname,
		       char **target_text){
  struct stat filestruct;
  int returnval;
  int buffer_length;
  

  if(target_text==NULL){
    fprintf(stderr,"parfu_is_a_symlink:\n");
    fprintf(stderr,"  target_text is NULL!!!\n");
    return -1;
  }
  
  if((returnval=lstat(pathname,&filestruct))){
    fprintf(stderr,"parfu_is_a_symlink:\n");
    fprintf(stderr," stat returned %d!\n",returnval);
    return -1;
  }
  if(S_ISLNK(filestruct.st_mode)){
    buffer_length=(filestruct.st_size)+1;
    if(((*target_text)=(char*)malloc(buffer_length))==NULL){
      fprintf(stderr,"parfu_is_a_symlink:\n");
      fprintf(stderr,"  failed to allocate target text buffer!!\n");
      return -1;
    }
    returnval=readlink(pathname,(*target_text),buffer_length);
    if(returnval != (buffer_length-1) ){
      fprintf(stderr,"parfu_is_a_symlink:\n");
      fprintf(stderr,"  error in length of target return!!\n");
      return -1;
    }
    (*target_text)[returnval]='\0'; // add NULL terminator
    // now (*target_text) is the link target, so we can return
    return 1;
  }
  else
    return 0;
}
*/

/*
// return value <0 indicates error
extern "C"
int parfu_what_is_file_exponent(long int file_size, 
		     int min_exponent,
		     int max_exponent){
  long int file_size_max_exp;
  long int file_size_min_exp;
  int test_exp;
  if(max_exponent > PARFU_LARGEST_ALLOWED_MAX_BLOCK_SIZE_EXPONENT){
    fprintf(stderr,"specified max_exponent (%d) greater than max allowed(%d)!\n",
	    max_exponent,PARFU_LARGEST_ALLOWED_MAX_BLOCK_SIZE_EXPONENT);
    return -1;
  }
  if(min_exponent < PARFU_ABSOLUTE_MIN_BLOCK_SIZE_EXPONENT){
    fprintf(stderr,"specified min_exponnent (%d) less than min allowed(%d)!\n",
	    min_exponent,PARFU_ABSOLUTE_MIN_BLOCK_SIZE_EXPONENT);
    return -2;
  }
  if(min_exponent > max_exponent){
    fprintf(stderr,"min exponent (%d) must be less than (or equal to) max(%d)!\n",
	    min_exponent,max_exponent);
    return -3;
  }
  file_size_max_exp = int_power_of_2(max_exponent);
  file_size_min_exp = int_power_of_2(min_exponent);
  if(file_size >= file_size_max_exp){
    return max_exponent;
  }
  if(file_size <= file_size_min_exp){
    return min_exponent;
  }
  test_exp = min_exponent + 1;
  while( file_size > int_power_of_2(test_exp)){
    test_exp++;
  }
  return test_exp;
}
*/

// old version.  deprecated 2017May.  Keeping for reference.
/*
extern "C"
int parfu_set_exp_offsets_in_ffel(parfu_file_fragment_entry_list_t *myl,
				  int min_exp,
				  int max_exp){
  int i;
  long int first_avail_byte;
  int block_size;
  int block_size_exponent;
  int n_blocks;
  long int first_block;
  
  first_avail_byte=0L;
  for(i=0;i<myl->n_entries_full;i++){
    if(myl->list[i].type == PARFU_FILE_TYPE_REGULAR){
      block_size_exponent=
	parfu_what_is_file_exponent(myl->list[i].size,min_exp,max_exp);
      if(block_size_exponent<0){
	fprintf(stderr,"parfu_set_exp_offsets_in_ffel:\n");
	fprintf(stderr,"  what_file_is_exponnent returned %d!!\n",block_size_exponent);
	fprintf(stderr,"  this is indicates an error.  Exiting.\n");
	return -1;
      }
      myl->list[i].block_size_exponent=block_size_exponent;

      block_size = int_power_of_2(block_size_exponent);
      n_blocks = myl->list[i].size / block_size;
      if(myl->list[i].size % block_size)
	n_blocks++;
      myl->list[i].number_of_blocks = n_blocks;
      
      first_block = first_avail_byte / block_size;
      if(first_avail_byte % block_size){
	first_block++;
      }
      myl->list[i].first_block=first_block;
      
      first_avail_byte = (first_block * block_size) + myl->list[i].size;
    } // if type is regular
    else{
      // other types of entries
      // directories, symlinks
      myl->list[i].block_size_exponent=-1;
      myl->list[i].number_of_blocks=0;
      myl->list[i].first_block=0; 
    }
    myl->list[i].num_blocks_in_fragment = -1; // don't know fragment size yet   
    myl->list[i].file_contains_n_fragments = -1; // likewise
    myl->list[i].file_ptr_index=i;
    myl->list[i].fragment_offset=0L;  // always zero in archive file
  } // for(i  [loop over entries in myl]
  return 0;
}
*/

// new modified version as of May 2017
// this function lays out the target files in the archive file.  It allows
// space for the tar header for each file before the file itself.  It
// also keeps in mind the blocking. 
// 
// The order that things happen.  Firs we will form a list of the files.
// Then that list is passed into the following function.  This function lays out
// the position of the files in the archive and splits them among ranks.  However, 
// while it's doing so, it also pushes locations back into the list that was input
// to it.  That list will then be used to form the file catalog that will be written
// to the archive file.  So the modifications to myl in this function have to be 
// correct.
// 
// this function now subsumes the "split" function that used to live
// in a separate function farther down this file.

extern "C"
parfu_file_fragment_entry_list_t 
*parfu_set_offsets_and_split_ffel(parfu_file_fragment_entry_list_t *myl,
				  int per_file_blocking_size, 
				  int per_rank_accumulation_size,
				  char *my_pad_file_filename, 
				  int *n_rank_buckets, 
				  int max_files_per_bucket){
  parfu_file_fragment_entry_list_t *outlist=NULL;

  int i,j;

  long int sum_file_size; // sum of tar header size and file size  
  long int blocked_sum_file_size; // above epanded out to per-file block size

  long int write_loc_whole_archive_file;
  long int new_write_loc_whole_archive_file;
  long int old_write_loc_whole_archive_file;
  long int write_loc_this_rank;
  long int new_write_loc_this_rank;

  long int possible_pad_space;
  //  long int pad_file_size;
  long int pad_file_header_size;
  long int new_pad_file_header_size;

  // index of what bucket we're currently setting offsets in
  long int current_rank_bucket;
  int n_whole_buckets_per_file;
  int n_total_buckets_per_file;
  int is_uneven_multiple;

  long int data_to_write;
  long int blocked_data_to_write;
  
  if((outlist=create_parfu_file_fragment_entry_list(myl->n_entries_full))
     ==NULL){
      fprintf(stderr,"parfu_set_offsets_and_split_ffel:\n");
      fprintf(stderr," could not create target list!\n");
      return NULL;
  }

  // just for clarity, all of these locations are relative to the location
  // of the beginning of the "data" section of the archive file, which
  // starts at the next even multiple of the per_rank_accumulation_size
  // AFTER the catalog is stored in the archive file.  The below analysis
  // ignores that, but the actual code that has to reference the archive
  // file has to know that.

  write_loc_whole_archive_file = 0L;
  write_loc_this_rank = 0L;
  current_rank_bucket = 0L;
  for(i=0;i<myl->n_entries_full;i++){

    // check for errors in our_tar_header_size
    // then set sum_file_size
    if(myl->list[i].our_tar_header_size < 0){
      fprintf(stderr,"parfu_set_offsets_in_ffel:\n");
      fprintf(stderr,"  file %d (REG file) tar_header size = %ld!\n",
	      i,myl->list[i].our_tar_header_size);
      return NULL;
    }

    // our_file_size, the internal value, is the size in the structure
    // for the file.  These values may be negative values which flag
    // for certain types of special files (directories, symlinks, and the like)
    // 
    // sum_file_size is the actual size of the file (zero for special files)
    // PLUS the size of the tar header that will preceed the actual file
    // in the container file
    // 
    // blocked_sum_file_size is the sum_file_size increased to the next 
    // even multiple of the (tar-defined) block size
    if(myl->list[i].our_file_size >= 0L){
      sum_file_size = myl->list[i].our_file_size + 
	myl->list[i].our_tar_header_size;
    }
    else{
      sum_file_size = myl->list[i].our_tar_header_size;
      // because this is a special file with a negative "size" 
      // for sorting purposes, but that means that the actual 
      // data payload is zero
    }
    
    // pad the file out if it's not a multiple of the blocking size
    if( ! ( sum_file_size % per_file_blocking_size ) ){
      // if it's already an exact multiple of the block size
      blocked_sum_file_size = sum_file_size;
    }
    else{
      // otherwise, increase it up to the next even multiple
      blocked_sum_file_size = 
	( ( sum_file_size / per_file_blocking_size ) + 1 ) * per_file_blocking_size;
    }
    
    // now using blocked_sum_file_size as the number of bytes we're going to 
    // allocate in the archive file on behalf of the current file, we figure
    // out where to start writing it, depending on how big it is and where it fits
    
    if(blocked_sum_file_size <= per_rank_accumulation_size){
      // We now know that this file would fit in a completely empty 
      // bucket.  Next check to see if this file and header will fit in the 
      // *current* bucket, given the files that have already been allocated
      // for this bucket.
 
      // after we account for the current file, this is where the following
      // one would begin.
      new_write_loc_this_rank = write_loc_this_rank + blocked_sum_file_size;
      
      if( new_write_loc_this_rank <= per_rank_accumulation_size ){
	// it will fit in the remainder of the current bucket
	// known as option "A"

	myl->list[i].location_in_archive_file = write_loc_whole_archive_file;
	// no update to bucket index; we're still in the same one
	myl->list[i].rank_bucket_index = current_rank_bucket;
	myl->list[i].location_in_orig_file = 0L;
	if(parfu_add_entry_to_ffel_mod(&outlist,myl->list[i],
				       myl->list[i].our_file_size,
				       write_loc_whole_archive_file+myl->list[i].our_tar_header_size,
				       0L, // single fragment file so file offset zero
				       1, // one fragment
				       -1, // file pointer index; assigned elsewhere
				       current_rank_bucket)){
	  fprintf(stderr,"parfu_set_offsets_and_split_ffel:\n");
	  fprintf(stderr,"  in OPTION A\n");
	  fprintf(stderr," could not add entry %d to split list bucket %ld!!!!\n",
		  i,current_rank_bucket);
	  return NULL;	  
	}

	// Here's where we would determine if we need to locate a padding file
	// before this one.  Since we're putting it in the same bucket up close, 
	// we believe we won't need to, and so we don't update anything.

	// now we advance our location pointers to the earliest possible location 
	// of the file following this one.
	write_loc_this_rank = new_write_loc_this_rank;
	write_loc_whole_archive_file += blocked_sum_file_size;
      } // if ( new_write_loc_this_rank ......
      else{
	// it would spill off the current bucket, so we'll set it to 
	// be at the beginning of the next one
	// known as "option B"

	// this is a contingency math check; it should never happen
	if(!(write_loc_whole_archive_file % blocked_sum_file_size)){
	  fprintf(stderr,"parfu_set_offsets_in_ffel:\n");
	  fprintf(stderr,"  file %d weird math error; should NEVER happen!!!\n",
		  i);
	  return NULL;
	}
	else{
	  // slide the write position to the beginning of the next bucket
	  current_rank_bucket++;
	  new_write_loc_whole_archive_file = 
	    ( current_rank_bucket * per_rank_accumulation_size );
	  myl->list[i].location_in_archive_file = new_write_loc_whole_archive_file;
	  // Deal with the possibility we may have to put a pad after the last 
	  // file to fill up the space between it and the file we're placing
	  possible_pad_space = 
	    (new_write_loc_whole_archive_file - write_loc_whole_archive_file);
	  if( possible_pad_space >= per_file_blocking_size ){
	    // the space is as big as the blocking size, so we need to pad it
	    if(myl->list[i].target){
	      pad_file_header_size = 
		tarentry::compute_hdr_size(my_pad_file_filename,
					   myl->list[i].target,
					   possible_pad_space);
	    }
	    else{
	      pad_file_header_size = 
		tarentry::compute_hdr_size(my_pad_file_filename,
					   "",
					   possible_pad_space);	      
	    }
	    if(pad_file_header_size > 
	       possible_pad_space){
	      fprintf(stderr,"parfu_set_offsets_and_split_ffel:\n");
	      fprintf(stderr,"  file index %d option B\n",i);
	      fprintf(stderr,"  file %s\n",myl->list[i].relative_filename);
	      fprintf(stderr,"  We need to pad the file but the header is\n");
	      fprintf(stderr,"  bigger than the available pad space.  Help!\n");
	      fprintf(stderr,"  header size: %ld space to fill: %ld\n",
		      pad_file_header_size,possible_pad_space);
	      return NULL;
	    }	
	    if(myl->list[i].target){
	      new_pad_file_header_size = 
		tarentry::compute_hdr_size(my_pad_file_filename,
					   myl->list[i].target,
					   possible_pad_space-pad_file_header_size);
	    }
	    else{
	      new_pad_file_header_size = 
		tarentry::compute_hdr_size(my_pad_file_filename,
					   "",
					   possible_pad_space-pad_file_header_size);	      
	    }
	    if(new_pad_file_header_size != pad_file_header_size){
	      fprintf(stderr,"parfu_set_offsets_and_split_ffel:\n");
	      fprintf(stderr,"  pad file tar header changed size!!!\n");
	      fprintf(stderr,"  Dont' know how to deal with this!\n");
	      return NULL;
	    }
	    // we need a pad before this file after the previous one
	    // put that information into the INCOMING list so that it
	    // gets propogated to the outgoing list
	    myl->list[i].pad_file_location_in_archive_file = 
	      write_loc_whole_archive_file;
	    if((myl->list[i].pad_file_archive_filename=
		(char*)malloc(strlen(my_pad_file_filename)+1))==NULL){
	      fprintf(stderr,"parfu_set_offsets_and_split_ffel:\n");
	      fprintf(stderr," input file index %d \n",i);
	      fprintf(stderr," could not allocate space for pad file!\n");
	      return NULL;
	    }
	    sprintf(myl->list[i].pad_file_archive_filename,"%s",
		    my_pad_file_filename);
	    myl->list[i].pad_file_tar_header_size=pad_file_header_size;
	    myl->list[i].pad_file_size = 
	      possible_pad_space-pad_file_header_size;
	  } // if(possible_pad_space...
	  
	  if(parfu_add_entry_to_ffel_mod(&outlist,myl->list[i],
					 myl->list[i].our_file_size,
					 write_loc_whole_archive_file+myl->list[i].our_tar_header_size,
					 0L, // single fragment; zero by def
					 1,  // just one fragment
					 -1, // file pointer index; assigned elsewhere
					 current_rank_bucket)){
	    fprintf(stderr,"parfu_set_offsets_and_split_ffel:\n");
	    fprintf(stderr,"  in OPTION B\n");
	    fprintf(stderr," could not add entry %d to split list bucket %ld!!!!\n",
		    i,current_rank_bucket);
	    return NULL;	  
	  } // if(parfu_add_entry...
	  myl->list[i].rank_bucket_index=current_rank_bucket;
	  myl->list[i].location_in_orig_file = 0L;

	  // slide write position to the end of the current file
	  write_loc_this_rank = blocked_sum_file_size;
	  write_loc_whole_archive_file += blocked_sum_file_size;
	  
	} // else 
      } // else (if it would spill out of current bucket)
    } // if(blocked_sum_file_size....
    else{
      // file plus tar header is bigger than a bucket, so it will 
      // occupy more than one.  We now split the file up so that 
      // it gets processed by more than one rank.
      n_whole_buckets_per_file = 
	blocked_sum_file_size / per_rank_accumulation_size;
      n_total_buckets_per_file = n_whole_buckets_per_file;
      if(blocked_sum_file_size % per_rank_accumulation_size){
	is_uneven_multiple=1;
	n_total_buckets_per_file++;
      }
      else{
	is_uneven_multiple=0;
      }

      // WHOLE file size, including tar header, padded out to block size.  Bigger than a bucket, 
      // so will be split across multiple buckets (and thus by multiple ranks)
      data_to_write = myl->list[i].our_file_size;
      blocked_data_to_write = blocked_sum_file_size;
      // we lay out the file across multiple buckets

      // update bucket index to make sure that we're beginning multi-bucket file on a bucket boundary
      old_write_loc_whole_archive_file = write_loc_whole_archive_file;
      if( write_loc_whole_archive_file % per_rank_accumulation_size ){
	current_rank_bucket++;
	  write_loc_whole_archive_file = 
	    ( current_rank_bucket * per_rank_accumulation_size );	
      }

      possible_pad_space = 
	(write_loc_whole_archive_file - old_write_loc_whole_archive_file);
      
      myl->list[i].location_in_archive_file = write_loc_whole_archive_file;

      if( possible_pad_space >= per_file_blocking_size ){
	// the space is as big as the blocking size, so we need to pad it
	if(myl->list[i].target){
	  pad_file_header_size = 
	    tarentry::compute_hdr_size(my_pad_file_filename,
				       myl->list[i].target,
				       possible_pad_space);
	}
	else{
	  pad_file_header_size = 
	    tarentry::compute_hdr_size(my_pad_file_filename,
				       "",
				       possible_pad_space);	      
	}
	if(pad_file_header_size > 
	   possible_pad_space){
	  fprintf(stderr,"parfu_set_offsets_and_split_ffel:\n");
	  fprintf(stderr,"  file index %d option C\n",i);
	  fprintf(stderr,"  file %s\n",myl->list[i].relative_filename);
	  fprintf(stderr,"  We need to pad the file but the header is\n");
	  fprintf(stderr,"  bigger than the available pad space.  Help!\n");
	  fprintf(stderr,"  header size: %ld space to fill: %ld\n",
		  pad_file_header_size,possible_pad_space);
	  return NULL;
	}	
	if(myl->list[i].target){
	  new_pad_file_header_size = 
	    tarentry::compute_hdr_size(my_pad_file_filename,
				       myl->list[i].target,
				       possible_pad_space-pad_file_header_size);
	}
	else{
	  new_pad_file_header_size = 
	    tarentry::compute_hdr_size(my_pad_file_filename,
				       "",
				       possible_pad_space-pad_file_header_size);	      
	}
	if(new_pad_file_header_size != pad_file_header_size){
	  fprintf(stderr,"parfu_set_offsets_and_split_ffel op C:\n");
	  fprintf(stderr,"  pad file tar header changed size!!!\n");
	  fprintf(stderr,"  Dont' know how to deal with this!\n");
	  return NULL;
	}
	myl->list[i].pad_file_location_in_archive_file = 
	  write_loc_whole_archive_file;
	if((myl->list[i].pad_file_archive_filename=
	    (char*)malloc(strlen(my_pad_file_filename)+1))==NULL){
	  fprintf(stderr,"parfu_set_offsets_and_split_ffel op C:\n");
	  fprintf(stderr," input file index %d \n",i);
	  fprintf(stderr," could not allocate space for pad file!\n");
	  return NULL;
	}
	sprintf(myl->list[i].pad_file_archive_filename,"%s",
		my_pad_file_filename);
	myl->list[i].pad_file_tar_header_size=pad_file_header_size;
	myl->list[i].pad_file_size = 
	  possible_pad_space-pad_file_header_size;
      } // if(possible_pad_space...
      
      for(j=0;j<n_whole_buckets_per_file;j++){
	//	myl->list[i].rank_bucket_index = current_rank_bucket;
	//	data_to_write -= per_rank_accumulation_size;
	if(j){
	  // do all the middle bucket stuff
	  //	  myl->list[i].location_in_archive_file = 
	  //	    write_loc_whole_archive_file;
	  //	  myl->list[i].location_in_orig_file = 
	  //	    (per_rank_accumulation_size * j) - myl->list[i].our_tar_header_size;
	  if(parfu_add_entry_to_ffel_mod(&outlist,myl->list[i],
					 per_rank_accumulation_size,
					 write_loc_whole_archive_file,
					 (per_rank_accumulation_size * j) - myl->list[i].our_tar_header_size,
					 n_total_buckets_per_file,
					 -1, // file pointer index; assigned elsewhere
					 current_rank_bucket)){
	    fprintf(stderr,"parfu_set_offsets_and_split_ffel:\n");
	    fprintf(stderr,"  in OPTION C1\n");
	    fprintf(stderr," could not add entry to split list!\n");
	    fprintf(stderr,"  in list %d bucket %ld to split list entry %d!!!!\n",
		    i,current_rank_bucket,j);
	    return NULL;	  
	  } // if(parfu_add_entry...
	  data_to_write -= per_rank_accumulation_size;
	  blocked_data_to_write -= per_rank_accumulation_size;
	} // if(j)
	else{
	  // do the special stuff for the *first* bucket
	  //	  myl->list[i].location_in_archive_file = 
	  //	    write_loc_whole_archive_file + myl->list[i].our_tar_header_size;
	  //	  myl->list[i].location_in_orig_file = 0L;
	  if(parfu_add_entry_to_ffel_mod(&outlist,myl->list[i],
					 per_rank_accumulation_size-myl->list[i].our_tar_header_size,
					 write_loc_whole_archive_file+myl->list[i].our_tar_header_size,
					 0L, // first fragment
					 n_total_buckets_per_file,  // n fragments
					 -1, // file pointer index; assigned elsewhere
					 current_rank_bucket)){
	    fprintf(stderr,"parfu_set_offsets_and_split_ffel:\n");
	    fprintf(stderr,"  in OPTION C1\n");
	    fprintf(stderr," could not add entry to split list!\n");
	    fprintf(stderr,"  in list %d bucket %ld to split list entry %d!!!!\n",
		    i,current_rank_bucket,j);
	    return NULL;	  
	  } // if(parfu_add_entry...
	  data_to_write -=
	    ( per_rank_accumulation_size - 
	      myl->list[i].our_tar_header_size );
	  blocked_data_to_write -=
	    ( per_rank_accumulation_size - 
	      myl->list[i].our_tar_header_size );
	} // else
	current_rank_bucket++;
	write_loc_whole_archive_file = 
	  ( current_rank_bucket * per_rank_accumulation_size );	
	
      } // for(j=0;...
      // data_to_write now contains the sub-bucket amount to be written 
      // in the last partial bucket
      if(is_uneven_multiple){
	j=n_whole_buckets_per_file;


	//	myl->list[i].location_in_archive_file = 
	//	  write_loc_whole_archive_file;
	//	myl->list[i].rank_bucket_index = current_rank_bucket;
	
	if(parfu_add_entry_to_ffel_mod(&outlist,myl->list[i],
				       data_to_write,
				       write_loc_whole_archive_file+myl->list[i].our_tar_header_size,
					 (per_rank_accumulation_size * j) - myl->list[i].our_tar_header_size,				     
				       n_total_buckets_per_file,  // n fragments
				       -1, // file pointer index; assigned elsewhere
				       current_rank_bucket)){
	  fprintf(stderr,"parfu_set_offsets_and_split_ffel:\n");
	  fprintf(stderr,"  in OPTION C3\n");
	  fprintf(stderr," could not add entry to split list!\n");
	  fprintf(stderr,"  in list %d bucket %ld to split list entry %d!!!!\n",
		  i,current_rank_bucket,j);
	  return NULL;	  
	} // if(parfu_add_entry...
	
	write_loc_whole_archive_file += blocked_data_to_write;

	current_rank_bucket++;
	myl->list[i].location_in_orig_file = 
	  per_rank_accumulation_size * j;
	data_to_write -= per_rank_accumulation_size;
	
      }
    } // else{   (this file + tar header is bigger than a bucket)
  } // for(i=0;
  *n_rank_buckets = current_rank_bucket;
  return 0;
}

// This function combines two lists.  The order is preserved.  The final
// list is the contents, in order, of l1 and then l2.  
extern "C"
parfu_file_fragment_entry_list_t
*parfu_combine_file_lists(parfu_file_fragment_entry_list_t *l1,
			  parfu_file_fragment_entry_list_t *l2){
  parfu_file_fragment_entry_list_t *my_list=NULL;
  
  return my_list;
}

extern "C"
parfu_file_fragment_entry_list_t 
*parfu_build_file_list_from_directory(char *top_dirname, 
				      int follow_symlinks,
				      int *total_entries){

  parfu_file_fragment_entry_list_t *full_list=NULL;
  parfu_file_fragment_entry_list_t *my_dir_list=NULL;
  parfu_file_fragment_entry_list_t *my_file_list=NULL;
  char **link_target=NULL;

  int current_dir_index;
  DIR *current_dir=NULL;
  struct dirent *next_entry;  

  int next_relative_name_length;
  char *next_relative_name=NULL;
  int next_archive_name_length;
  char *next_archive_name=NULL;

  long int file_size=(-1);

  unsigned int path_type_result;

  //  fprintf(stderr,"starting parfu_build_file_list_from_directory\n");
  //  fprintf(stderr,"from directory: %s\n",top_dirname);
  fprintf(stderr,"scanning directory: %s\n",top_dirname);

  /*
  if(!parfu_is_a_dir(top_dirname)){
    fprintf(stderr,"parfu_build_file_list_from_directory:\n");
    fprintf(stderr," The directory name we were given: %s\n",top_dirname);
    fprintf(stderr," is NOT a directory!\n");
    return NULL;
  }
  */

  if((full_list=create_parfu_file_fragment_entry_list(100))==NULL){
    fprintf(stderr,"parfu_build_file_list_from_directory:\n");
    fprintf(stderr,"  create_parfu_file_fragment_entry_list #1 returned NULL!!\n");
    return NULL;
  }
  if((my_dir_list=create_parfu_file_fragment_entry_list(100))==NULL){
    fprintf(stderr,"parfu_build_file_list_from_directory:\n");
    fprintf(stderr,"  create_parfu_file_fragment_entry_list #2 returned NULL!!\n");
    return NULL;
  }
  if((my_file_list=create_parfu_file_fragment_entry_list(100))==NULL){
    fprintf(stderr,"parfu_build_file_list_from_directory:\n");
    fprintf(stderr,"  create_parfu_file_fragment_entry_list #3 returned NULL!!\n");
    return NULL;
  }
  if((link_target=(char**)malloc(sizeof(char*)))==NULL){
    fprintf(stderr,"parfu_build_file_list_from_directory:\n");
    fprintf(stderr,"  could not allocate link name buffer pointer!!\n");
    return NULL;
  }
  *link_target=NULL;
  
  path_type_result=parfu_what_is_path(top_dirname,link_target,NULL,follow_symlinks);
  
  //  if(parfu_does_not_exist(top_dirname)){
  if(path_type_result==PARFU_WHAT_IS_PATH_ERROR){
    fprintf(stderr,"parfu_build_file_list_from_directory:\n");
    fprintf(stderr,"  target we were pointed to does not exist!\n");
    fprintf(stderr,"  Exiting.\n");
    return NULL;
  }
  
  if(follow_symlinks){
    if(path_type_result==PARFU_WHAT_IS_PATH_SYMLINK){
      
      //       if(parfu_is_a_symlink(top_dirname,link_target) && (!follow_symlinks)){
      fprintf(stderr,"parfu_build_file_list_from_directory:\n");
      fprintf(stderr,"  The dir we were pointed to is a link, \n");
      fprintf(stderr,"  and we\'re not following links.\n");
      fprintf(stderr,"  this is an error.\n");
      return NULL;
    }
  }
  //  if(parfu_is_a_regfile(top_dirname,NULL)){
  if(path_type_result==PARFU_WHAT_IS_PATH_REGFILE){
    fprintf(stderr,"parfu_build_file_list_from_directory:\n");
    fprintf(stderr,"  The dir we were pointed to is a regular file.\n");
    fprintf(stderr,"  this is an error.\n");
    return NULL;
  }
  //  if(!parfu_is_a_dir(top_dirname)){
  if(path_type_result==PARFU_WHAT_IS_PATH_IGNORED_TYPE){
    fprintf(stderr,"parful_build_file_list_from_directory:\n");
    fprintf(stderr,"  The \"directory\" we were given: >%s<\n",top_dirname);
    fprintf(stderr,"  isn't a directory or a file or a symlink.\n");
    fprintf(stderr,"  We don\'t know what to do; throwing an error.\n");
    return NULL;
  }
  
  // At this point, we know that the directory we were pointed to isn't
  // a symlink (or we were told to follow symlinks, and it links to a 
  // directory), it's not a file, and it's not some other entitye (pipe, 
  // socket, block device) that we don't want to deal with.  

  next_archive_name_length=0;
  if((next_archive_name=(char*)malloc(next_archive_name_length+1))==NULL){
    fprintf(stderr,"parful_build_file_list_from_directory:\n");
    fprintf(stderr,"  could not allocate top dir string!!\n");
    return NULL;
  }
  //  sprintf(next_archive_name,"");
  next_archive_name[0]='\0'; // making it a zero-length empty string

  // we add it to the directory list as the first entry
  if(parfu_add_name_to_ffel(&full_list,PARFU_FILE_TYPE_DIR,
			    top_dirname,next_archive_name,NULL,PARFU_SPECIAL_SIZE_DIR)){
    fprintf(stderr,"parful_build_file_list_from_directory:\n");
    fprintf(stderr,"  failed to add top dirname to full file list!!!\n");
    return NULL;
  }
  // we add it to the full file list that we'll output because we also
  // separately list all the directories, including empty ones.
  if(parfu_add_name_to_ffel(&my_dir_list,PARFU_FILE_TYPE_DIR,
			    top_dirname,next_archive_name,NULL,PARFU_SPECIAL_SIZE_DIR)){
    fprintf(stderr,"parful_build_file_list_from_directory:\n");
    fprintf(stderr,"  failed to add top dirname to directory list!!!\n");
    return NULL;
  }

  // now we walk through the directory list, adding files and directories 
  // to the file list and adding new directories to the directory list
  // the directory list grows as we traverse it.  
  current_dir_index=0;
  *total_entries=1;

  // each run through this while loop looks at ONE directory in our list
  while(current_dir_index < my_dir_list->n_entries_full){
    //    fprintf(stderr,"dir index=%d\n",current_dir_index);
    if((current_dir=opendir(my_dir_list->list[current_dir_index].relative_filename))==NULL){
      fprintf(stderr,"parfu_build_file_list_from_directory:\n");
      fprintf(stderr,"couldn't open directory >%s<!\n",
	      my_dir_list->list[current_dir_index].relative_filename);
      return NULL;
    }
    fprintf(stderr,"scanning directory: %s\n",my_dir_list->list[current_dir_index].relative_filename);
    next_entry=readdir(current_dir);
    // for each directory (current_dir), this next while loop runs once for each
    // entry in that directory 
    while(next_entry!=NULL){
      //fprintf(stderr," *** next entry...\n");
      // next_entry->d_name is the next entry *within* the current directory being searched

      // skip directories "." and ".."
      if(!strncmp(next_entry->d_name,".",1)){
	next_entry=readdir(current_dir);
	continue;
      }
      if(!strncmp(next_entry->d_name,"..",2)){
	next_entry=readdir(current_dir);
	continue;
      }
      
      // construct the total path to the next entry
      next_relative_name_length = strlen(my_dir_list->list[current_dir_index].relative_filename) + 
	strlen(next_entry->d_name) + 
	2; // + 2 = 1 byte for the '/' and 1 for the terminating NULL
      if((next_relative_name=(char*)malloc(next_relative_name_length))==NULL){
	fprintf(stderr,"parfu_build_file_list_from_directory:\n");
	fprintf(stderr,"could not allocate next relative name!!\n");
	return NULL;
      }
      if( (my_dir_list->list[current_dir_index].relative_filename)
	  [(strlen(my_dir_list->list[current_dir_index].relative_filename))-1] == '/'){
	sprintf(next_relative_name,"%s%s",
		my_dir_list->list[current_dir_index].relative_filename,
		next_entry->d_name);
      }
      else{
	sprintf(next_relative_name,"%s/%s",
		my_dir_list->list[current_dir_index].relative_filename,
		next_entry->d_name);
      }

      // construct the path relative to the starting point (which is what goes in the 
      // archive)
      next_archive_name_length = strlen(my_dir_list->list[current_dir_index].archive_filename)+
	strlen(next_entry->d_name) +
	2;  // +2 = 1 byte for the '/' + 1 byte for terminating NULL
      if((next_archive_name=(char*)malloc(next_archive_name_length))==NULL){
	fprintf(stderr,"parfu_build_file_list_from_directory:\n");
	fprintf(stderr,"could not allocate next archive name!!\n");
	return NULL;
      }
      if(strlen(my_dir_list->list[current_dir_index].archive_filename)){
	sprintf(next_archive_name,"%s/%s",
		my_dir_list->list[current_dir_index].archive_filename,
		next_entry->d_name);
      }
      else{
	sprintf(next_archive_name,"%s",next_entry->d_name);
      }
      
      // next_name now contains the next thing to check; the next 
      // entry in the directory current_dir.  
      //      fprintf(stderr,"entry name: %s\n",next_name);

      path_type_result=
	parfu_what_is_path(next_relative_name,link_target,&file_size,follow_symlinks);
      
      //      if(parfu_is_a_symlink(next_relative_name,link_target) && (!follow_symlinks)){
      if(path_type_result==PARFU_WHAT_IS_PATH_SYMLINK && (!follow_symlinks)){
	// record the entry as a symlink
	if(parfu_add_name_to_ffel(&full_list,PARFU_FILE_TYPE_SYMLINK,
				  next_relative_name,next_archive_name,(*link_target),
				  PARFU_SPECIAL_SIZE_SYMLINK)){
	  fprintf(stderr,"parful_build_file_list_from_directory:\n");
	  fprintf(stderr,"  Could not add >%s< as a link!!!\n",next_relative_name);
	  return NULL;
	} // if(parfu_add_name_to_ffel(....
	(*total_entries)++;
      }
      else{
	// not a symlink; check to see if it's a dir or a file and 
	// if so do the appropriate things. If not either of those, ignore it
	//	if(parfu_is_a_dir(next_relative_name)){
	if(path_type_result==PARFU_WHAT_IS_PATH_DIR){
	  if(parfu_add_name_to_ffel(&full_list,PARFU_FILE_TYPE_DIR,
				    next_relative_name,next_archive_name,NULL,
				    PARFU_SPECIAL_SIZE_DIR)){
	    fprintf(stderr,"parful_build_file_list_from_directory:\n");
	    fprintf(stderr,"  Could not add >%s< as dir to full list!!!\n",next_relative_name);
	    return NULL;
	  }
	  if(parfu_add_name_to_ffel(&my_dir_list,PARFU_FILE_TYPE_DIR,
				    next_relative_name,next_archive_name,NULL,
				    PARFU_SPECIAL_SIZE_DIR)){
	    fprintf(stderr,"parful_build_file_list_from_directory:\n");
	    fprintf(stderr,"  Could not add >%s< to dir list!!!\n",next_relative_name);
	    return NULL;
	  }
	  (*total_entries)++;
	} // if(parfu_is_a_dir
	//	if(parfu_is_a_regfile(next_relative_name,&file_size)){
	if(path_type_result==PARFU_WHAT_IS_PATH_REGFILE){
	  if(parfu_add_name_to_ffel(&full_list,PARFU_FILE_TYPE_REGULAR,
				    next_relative_name,next_archive_name,NULL,
				    file_size)){
	    fprintf(stderr,"parful_build_file_list_from_directory:\n");
	    fprintf(stderr,"  Could not add >%s< as file to full list!!!\n",next_relative_name);
	    return NULL;
	  }
	  (*total_entries)++;
	} // if(parfu_is_a_regfile
      }
      // we've done whatever we're going to do with the current entry of the 
      // current directory, so we grab the next entry:
      next_entry=readdir(current_dir);
    } // while(next_entry!=NULL)    
    if(closedir(current_dir)){
      fprintf(stderr,"parful_build_file_list_from_directory:\n");
      fprintf(stderr,"  unable to close directory index: %d !!!!\n",current_dir_index);
      return NULL;
    }
    current_dir_index++;
  } //   while(current_dir_index < my_dir_list->n_entries_full){  
  return full_list;
}

extern "C"
char parfu_return_type_char(parfu_file_t in_type){
  switch(in_type){
  case PARFU_FILE_TYPE_REGULAR:
    return PARFU_FILE_TYPE_REGULAR_CHAR;
  case PARFU_FILE_TYPE_DIR:
    return PARFU_FILE_TYPE_DIR_CHAR;
  case PARFU_FILE_TYPE_SYMLINK:
    return PARFU_FILE_TYPE_SYMLINK_CHAR;
  default:
    return PARFU_FILE_TYPE_INVALID;
  }
}

extern "C"
void parfu_dump_fragment_entry_list(parfu_file_fragment_entry_list_t *my_list,
				    FILE *output){
  int i,j;
  int chars_printed;
  fprintf(output,"\n\n");
  fprintf(output,"beginning file dump. There are %d entries in total.\n",
	  my_list->n_entries_full);
  fprintf(output,"\n\n");
  for(i=0;i<my_list->n_entries_full;i++){
    chars_printed=fprintf(output,"  >%s<  >%s<  ",my_list->list[i].relative_filename,
			  my_list->list[i].archive_filename);
    if((my_list->list[i].target)!=NULL){
      chars_printed += fprintf(output," [%s] ",my_list->list[i].target);
    }
    for(j=chars_printed;j<110;j++){
      fprintf(output," ");
    } 
    fprintf(output,"  %c  %ld\n",
	    parfu_return_type_char(my_list->list[i].type),
	    my_list->list[i].our_file_size);
  }
  
  fprintf(output,"\n\n");
}

extern "C"
void parfu_check_offsets(parfu_file_fragment_entry_list_t *my_list,
			 FILE *output){
  int i,j;
  int chars_printed;
  //  long int block_begins;
  //  int block_size;
  //  long int block_ends;
  fprintf(output,"\n\n");
  fprintf(output,"beginning offset checking. There are %d entries in total.\n",
	  my_list->n_entries_full);
  fprintf(output,"\n\n");
  for(i=0;i<my_list->n_entries_full;i++){
    chars_printed=fprintf(output,"  >%s<  ",
			  my_list->list[i].archive_filename);
    if((my_list->list[i].target)!=NULL){
      chars_printed += fprintf(output," [%s] ",my_list->list[i].target);
    }
    for(j=chars_printed;j<110;j++){
      fprintf(output," ");
    } 

    fprintf(output,"  %c   ",parfu_return_type_char(my_list->list[i].type));
    if(my_list->list[i].type == PARFU_FILE_TYPE_REGULAR)
      fprintf(output,"  %10ld  %10ld   %10ld",
	      my_list->list[i].our_file_size,
	      my_list->list[i].our_tar_header_size,
	      my_list->list[i].location_in_archive_file);
    fprintf(output,"\n");

  }
  
  fprintf(output,"\n\n");
}

// has to return int (not long int) because of qsort library function
extern "C"
int parfu_compare_fragment_entry_by_size(const void *vA, const void *vB){
  parfu_file_fragment_entry_t *A,*B;
  long int sizeA, sizeB;
  A=(parfu_file_fragment_entry_t*)vA;
  B=(parfu_file_fragment_entry_t*)vB;
  sizeA=A->our_file_size;
  sizeB=B->our_file_size;
  
  if(sizeA > sizeB)
    return +1;
  if(sizeB > sizeA)
    return -1;
  //  return 0;
  return strcmp(A->archive_filename,B->archive_filename);
}

extern "C"
void parfu_qsort_entry_list(parfu_file_fragment_entry_list_t *my_list){
  qsort( (void*)(my_list->list),
	 my_list->n_entries_full,
	 sizeof(parfu_file_fragment_entry_t),
	 &parfu_compare_fragment_entry_by_size);
}

/*
extern "C"
int int_power_of(int base, int power){
  int answer=1;
  int i;
  if(base<=0)
    return 0;
  for(i=0;i<power;i++){
    answer *= base;
  }
  return answer;
}

extern "C"
int int_power_of_2(int arg){
  if(arg >= 0){
    return 1 << arg;
  }
  else{
    return 1;
  }
}
*/

// this function deprecated as of July 2017
/* 
extern "C"
parfu_file_fragment_entry_list_t 
*parfu_split_fragments_in_list(parfu_file_fragment_entry_list_t *in_list,
			       int min_block_size_exponent,
			       int max_block_size_exponent, 
			       int blocks_per_fragment){
  int i;
  parfu_file_fragment_entry_list_t  *out_list=NULL;
  int n_distributed_file_entries=0;
  //  int filename_length;
  //  int max_filename_length;
  //  int entry_index;
  long int file_offset_local; 
  long int remaining_file_length;
  int max_block_size;
  int file_block_size;
  long int max_fragment_size;
  int test_exponent;

  long int first_available_byte;
  long int next_block_index;
  long int next_block_boundary;
  long int fragment_size;
  long int temp_num_blocks;
  int fragment_index;

  long int file_size_min_block_exponent;
  long int file_size_max_block_exponent;

  max_block_size=int_power_of_2(max_block_size_exponent);
  //  min_block_size=int_power_of_2(min_block_size_exponent);
  max_fragment_size = max_block_size * blocks_per_fragment;

  //  fprintf(stderr,"blocks_per: %d   fragment size: %ld\n",blocks_per_fragment,max_fragment_size);

  //  max_filename_length=0;

  if(min_block_size_exponent < PARFU_ABSOLUTE_MIN_BLOCK_SIZE_EXPONENT){
    fprintf(stderr,"parfu_split_fragments_in_list:\n");
    fprintf(stderr,"  min_block_size_exponent: %d is too small!\n",min_block_size_exponent);
    fprintf(stderr,"  smallest allowed is: %d\n",PARFU_ABSOLUTE_MIN_BLOCK_SIZE_EXPONENT);
    return NULL;
  }
  if(max_block_size_exponent > PARFU_LARGEST_ALLOWED_MAX_BLOCK_SIZE_EXPONENT){
    fprintf(stderr,"parfu_split_fragments_in_list:\n");
    fprintf(stderr,"  max_block_size_exponent: %d is too large!\n",max_block_size_exponent);
    fprintf(stderr,"  largest allowed is: %d\n",PARFU_LARGEST_ALLOWED_MAX_BLOCK_SIZE_EXPONENT);
    return NULL;
  }
  if(min_block_size_exponent > max_block_size_exponent){
    fprintf(stderr,"parfu_split_fragments_in_list:\n");
    fprintf(stderr,"  min_block_size_exponent: %d !\n",min_block_size_exponent);
    fprintf(stderr,"  max_block_size_exponent: %d !!!",max_block_size_exponent);
    return NULL;
  }
  
  file_size_min_block_exponent = int_power_of_2(min_block_size_exponent);
  file_size_max_block_exponent = int_power_of_2(max_block_size_exponent);
  max_fragment_size = file_size_max_block_exponent * blocks_per_fragment;
    
  // - count total entries required for split list
  // - determine max filename length across all entries
  // - make sure each source list entry has the important 
  //     fields populated.  

  for(i=0;i<in_list->n_entries_full;i++){
    n_distributed_file_entries += 
      (in_list->list[i].size / max_fragment_size) + 1;
    
    //    filename_length=strlen(in_list->list[i].relative_filename);
    //    if(filename_length > max_filename_length) max_filename_length=filename_length;
    //filename_length=strlen(in_list->list[i].archive_filename);
    //if(filename_length > max_filename_length) max_filename_length=filename_length;
    //filename_length=strlen(in_list->list[i].target);
    //if(filename_length > max_filename_length) max_filename_length=filename_length;

    switch(in_list->list[i].type){
    case PARFU_FILE_TYPE_REGULAR:
      if(in_list->list[i].size < 0){
	fprintf(stderr,"parfu_split_fragments_in_list:\n");
	fprintf(stderr,"input item %d is a file but has a negative size!\n",i);
	return NULL;
      }
      if(in_list->list[i].size >= file_size_max_block_exponent){
	in_list->list[i].block_size_exponent = max_block_size_exponent;
      }
      else{
	if(in_list->list[i].size <= file_size_min_block_exponent){
	  in_list->list[i].block_size_exponent = min_block_size_exponent;
	}
	else{
	  test_exponent=min_block_size_exponent+1;
	  while( (in_list->list[i].size) > int_power_of_2(test_exponent) ){
	    test_exponent++;
	  }
	  in_list->list[i].block_size_exponent = test_exponent;
	}
      }
      in_list->list[i].num_blocks_in_fragment = blocks_per_fragment;
      in_list->list[i].file_contains_n_fragments = 
	in_list->list[i].size / max_fragment_size;
      if( in_list->list[i].size % max_fragment_size )
	(in_list->list[i].file_contains_n_fragments)++;
      //      fprintf(stderr,"file %d contains %d fragments\n",i,in_list->list[i].file_contains_n_fragments);
      break;
    case PARFU_FILE_TYPE_SYMLINK:
      if(in_list->list[i].target == NULL){
	fprintf(stderr,"parfu_split_fragments_in_list:\n");
	fprintf(stderr,"input item %d is a symlink but has NULL target!\n",i);
	return NULL;
      }
      if(!strlen(in_list->list[i].target)){
	fprintf(stderr,"parfu_split_fragments_in_list:\n");
	fprintf(stderr,"input item %d is a symlink, but target is 0 length!\n",i);
	return NULL;
      }
    case PARFU_FILE_TYPE_DIR:
      in_list->list[i].file_contains_n_fragments = 0;
      break;
    default:
      fprintf(stderr,"parfu_split_fragments_in_list:\n");
      fprintf(stderr,"input item %d has invalid type!\n",i);
      return NULL;
    }    
  } // for(i=0;
  
  if((out_list=
      create_parfu_file_fragment_entry_list(n_distributed_file_entries))
     ==NULL){
    fprintf(stderr,"parfu_split_fragments_in_list:\n");
    fprintf(stderr,"could not allocate split list!\n");
    return NULL;
  }

  // now populate the distributed file list from the sorted non-distributed one
  // i is the index the input non-split list
  // entry_index is the index into the new split list

  // running variables that keep track of where we are in the virtual 
  // archive file that we're creating.

  // first available byte is at the beginning of the data section
  first_available_byte=0L;

  //  entry_index=0;
  for(i=0;i<in_list->n_entries_full;i++){
    if(in_list->list[i].file_contains_n_fragments <= 1){
      //      fprintf(stderr,"  *** split iter %d, single fragment\n",i);
      // this block applies to directories and symlinks as well.  They have 
      // negative sizes, so they fall in here.  That's fine; they take up zero 
      // space in the data part of the archive file, same as a zero-byte file.

      // only one fragment; the fragment IS the file, so no offset
      in_list->list[i].fragment_offset = 0; 
      if(in_list->list[i].size <= file_size_max_block_exponent){
	if(in_list->list[i].size <= 0){
	  in_list->list[i].number_of_blocks = 0;
	}
	in_list->list[i].number_of_blocks = 1;
      }
      else{
	in_list->list[i].number_of_blocks = 
	  in_list->list[i].size / file_size_max_block_exponent;
	if( in_list->list[i].size % file_size_max_block_exponent )
	  (in_list->list[i].number_of_blocks)++;
      }
      // allocate room for tar header and (possibly) file content
      file_block_size = int_power_of_2(in_list->list[i].block_size_exponent);
      //	first_available_byte = last_byte_written + 1;
      
      next_block_index = first_available_byte / file_block_size;
      if(first_available_byte % file_block_size)
        next_block_index++;

      // we squeeze the tar header just before the actual file data, keeping
      // the file data aligned
      // TODO: do not create a temp tarentry since it calls lstat
      const size_t hdr_size =
        tarentry::compute_hdr_size(in_list->list[i].relative_filename,
                                   in_list->list[i].target,
                                   in_list->list[i].size);
      if(next_block_index - first_available_byte < hdr_size){
        next_block_index += (hdr_size + file_block_size-1) / file_block_size;
      }

      in_list->list[i].first_block = next_block_index;
      next_block_boundary = next_block_index * file_block_size;
      // as long as the minimum blocking is 512 this will ensure proper tar
      // alignment
      first_available_byte = next_block_boundary + hdr_size + in_list->list[i].size;
      
      if(parfu_add_entry_to_ffel(&out_list,in_list->list[i])){
	fprintf(stderr,"parfu_split_fragments_in_list:\n");
	fprintf(stderr," couldn't %d th entry as single slice!\n",i);
	return NULL;
      }
    }
    else{
      // the file is more than one fragment, so it's split up
      //      fprintf(stderr,"  *** split iter %d, multiple fragment\n",i);

      file_block_size = int_power_of_2(in_list->list[i].block_size_exponent);
      if(file_block_size != file_size_max_block_exponent){
	fprintf(stderr,"parfu_split_fragments_in_list:\n");
	fprintf(stderr," file %d has impossible block size!!!\n",i);
	return NULL;
      }
      
      // this sets the next available byte at the next open even block index
      next_block_index = first_available_byte / file_block_size;
      if(first_available_byte % file_block_size)
	next_block_index++;

      // we squeeze the tar header just before the actual file data, keeping
      // the file data aligned
      // TODO: do not create a temp tarentry since it calls lstat
      tarentry entry(in_list->list[i].relative_filename, 0);
      if(next_block_index - first_available_byte < entry.hdr_size()){
        next_block_index += (entry.hdr_size() + file_block_size-1) / file_block_size;
      }

      // next statement was missing; caused me fits until I figured it out
      // fixed 2016nov21   -- craig 
      first_available_byte = ((long int)next_block_index) * 
	((long int)file_block_size);

      file_offset_local = 0;
      
      //      block_offset = in_list->list[i].first_block;
      remaining_file_length=in_list->list[i].size;
      //      fprintf(stderr,"file %4d nblocks=%5d\n",i,blocks_remaining);
      fragment_index=0;
      
      while(remaining_file_length > 0){
	
	if(remaining_file_length > max_fragment_size)
	  fragment_size = max_fragment_size;
	else
	  fragment_size = remaining_file_length;
	temp_num_blocks = fragment_size / file_block_size;
	if( fragment_size % file_block_size )
	  temp_num_blocks++;
	if(parfu_add_entry_to_ffel_mod(&out_list,in_list->list[i],
				       fragment_size,
				       file_offset_local,
				       next_block_index,
				       temp_num_blocks)){
	  fprintf(stderr,"parfu_split_fragments_in_list:\n");
	  fprintf(stderr," error from parfu_add_entry_to_ffel_mod()\n");
	  fprintf(stderr," file %d fragment %d!\n",i,fragment_index);
	  return NULL;
	}
	
	file_offset_local += fragment_size;
	remaining_file_length -= fragment_size;
	next_block_index += blocks_per_fragment;
	first_available_byte += fragment_size;
	fragment_index++;
      } // while(remaining_file_length > 0)
    } // else
  } // for(i=0...
    
  return out_list;
}

*/


extern "C"
int *parfu_rank_call_list_from_ffel(parfu_file_fragment_entry_list_t *myl,
				    int *n_rank_buckets){
  int i;
  int last_bucket_index=(-1);
  int current_bucket_index;
  int *prelim_list=NULL;
  int *final_list=NULL;
  if(myl==NULL){
    fprintf(stderr,"parfu_rank_call_list_from_ffel:\n");
    fprintf(stderr,"  received NULL input list!!\n");
    return NULL;
  }
  if((prelim_list=(int*)malloc(sizeof(int)*
			       myl->n_entries_full))==NULL){
    fprintf(stderr,"parfu_rank_call_list_from_ffel:\n");
    fprintf(stderr,"  could not allocate initial list of %d entries!\n",
	    myl->n_entries_full);
    return NULL;
  }
  current_bucket_index=0;
  
  prelim_list[0]=0;
  if(myl->list[0].rank_bucket_index != 0){
    fprintf(stderr,"parfu_rank_call_list_from_ffel:\n");
    fprintf(stderr,"  Very first rank_bucket_index=%d!\n",
	    myl->list[0].rank_bucket_index);
    fprintf(stderr,"  Why isn't it zero!!?!\n");
    return NULL;
  }
  for( i=0 ; i< myl->n_entries_full ; i++){
    if( myl->list[i].rank_bucket_index != last_bucket_index ){
      prelim_list[current_bucket_index] = 
	myl->list[i].rank_bucket_index;
      last_bucket_index = myl->list[i].rank_bucket_index;
      current_bucket_index++;
    }
  }
  *n_rank_buckets = current_bucket_index;
  if((final_list=(int*)malloc(sizeof(int)*
			      current_bucket_index))==NULL){
    fprintf(stderr,"parfu_rank_call_list_from_ffel:\n");
    fprintf(stderr,"  could not allocate final list of %d entries!\n",
	    current_bucket_index);
    return NULL;
  }
  memcpy(final_list,prelim_list,sizeof(int)*current_bucket_index);
  free(prelim_list);
  return final_list;
}
