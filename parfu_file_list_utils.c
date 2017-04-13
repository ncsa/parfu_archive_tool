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
  if((my_list=malloc(sizeof(parfu_file_fragment_entry_list_t)+
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
    my_list->list[i].type=PARFU_FILE_TYPE_INVALID;
    my_list->list[i].target=NULL;
    my_list->list[i].block_size_exponent=-1;
    my_list->list[i].num_blocks_in_fragment=-1;
    my_list->list[i].file_contains_n_fragments=-1;
    my_list->list[i].fragment_offset=-1;
    my_list->list[i].size=PARFU_SPECIAL_SIZE_INVALID_REGFILE;
    my_list->list[i].first_block=-1;
    my_list->list[i].number_of_blocks=-1;
    my_list->list[i].file_ptr_index=-1;
  }
  
  return my_list;
}

void parfu_free_ffel(parfu_file_fragment_entry_list_t *my_list){
  int i;
  for(i=0;i<(my_list->n_entries_total);i++){
    if( my_list->list[i].relative_filename != NULL )
      free(my_list->list[i].relative_filename);
    if( my_list->list[i].archive_filename != NULL )
      free(my_list->list[i].archive_filename);
    if( my_list->list[i].target != NULL )
      free(my_list->list[i].target);
  }
  free(my_list);
}

// add name to file parfu file fragment entry list
int parfu_add_name_to_ffel(parfu_file_fragment_entry_list_t **my_list,
			   parfu_file_t my_type,
			   char *my_relative_filename,
			   char *my_archive_filename,
			   char *my_target,
			   long int my_size){
  int return_value;
  if((return_value=
     parfu_add_entry_to_ffel_raw(my_list,my_relative_filename,my_archive_filename,
				 my_type,my_target,
				 -1,-1,-1,-1,
				 my_size,
				 -1,-1,-1))){
    fprintf(stderr,"parfu_add_name_to_ffel:\n");
    fprintf(stderr," return from parfu_add_entry_to_ffel_raw was: %d\n",return_value);
    return return_value;
  }
    
  return 0;
}


int parfu_add_entry_to_ffel_mod(parfu_file_fragment_entry_list_t **list,
				parfu_file_fragment_entry_t entry,
				long int my_size,
				long int my_fragment_offset,
				long int my_first_block,
				long int my_number_of_blocks){
  int return_value;
  if((return_value=
     parfu_add_entry_to_ffel_raw(list,
				 entry.relative_filename,
				 entry.archive_filename,
				 entry.type,
				 entry.target,
				 entry.block_size_exponent,
				 entry.num_blocks_in_fragment,
				 entry.file_contains_n_fragments,
				 my_fragment_offset,
				 my_size,
				 my_first_block,
				 my_number_of_blocks,
				 entry.file_ptr_index))){
    fprintf(stderr,"parfu_add_entry_to_ffel_mod:\n");
    fprintf(stderr," return from parfu_add_entry_to_ffel_raw was: %d\n",return_value);
    return return_value;
  }
  return 0;  
}

int parfu_add_entry_to_ffel(parfu_file_fragment_entry_list_t **list,
			    parfu_file_fragment_entry_t entry){
  int return_value;
  if((return_value=
     parfu_add_entry_to_ffel_raw(list,
				 entry.relative_filename,
				 entry.archive_filename,
				 entry.type,
				 entry.target,
				 entry.block_size_exponent,
				 entry.num_blocks_in_fragment,
				 entry.file_contains_n_fragments,
				 entry.fragment_offset,
				 entry.size,
				 entry.first_block,
				 entry.number_of_blocks,
				 entry.file_ptr_index))){
    fprintf(stderr,"parfu_add_entry_to_ffel:\n");
    fprintf(stderr," return from parfu_add_entry_to_ffel_raw was: %d\n",return_value);
    return return_value;
  }
  return 0;  
}

int parfu_add_entry_to_ffel_raw(parfu_file_fragment_entry_list_t **list,
				char *my_relative_filename,
				char *my_archive_filename,
				parfu_file_t my_type,
				char *my_target,
				int my_block_size_exponent,
				int my_num_blocks_in_fragment,
				int my_file_contains_n_fragments,
				long int my_fragment_offset,
				long int my_size,
				long int my_first_block,
				long int my_number_of_blocks,
				int my_file_ptr_index){
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
    if(((*list)=realloc((*list),total_size))==NULL){
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
  if(((*list)->list[ind].relative_filename = malloc(string_size+1))==NULL){
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
  if(((*list)->list[ind].archive_filename = malloc(string_size+1))==NULL){
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
  if(((*list)->list[ind].target = malloc(string_size+1))==NULL){
    fprintf(stderr,"parfu_add_name_to_ffel:\n");
    fprintf(stderr,"could not allocate target string!\n");
    return 1;
  }
  if(my_target!=NULL)
    memcpy( (*list)->list[ind].target,my_target,string_size+1);
  else
    ((*list)->list[ind].target)[0]='\0';
  
  (*list)->list[ind].block_size_exponent=my_block_size_exponent;
  (*list)->list[ind].num_blocks_in_fragment = my_num_blocks_in_fragment;
  (*list)->list[ind].file_contains_n_fragments = my_file_contains_n_fragments;
  (*list)->list[ind].fragment_offset = my_fragment_offset;
  (*list)->list[ind].size = my_size;
  (*list)->list[ind].first_block = my_first_block;
  (*list)->list[ind].number_of_blocks = my_number_of_blocks;
  (*list)->list[ind].file_ptr_index = my_file_ptr_index;

  // now finally update the number of full items in the list. 
  (*list)->n_entries_full = ind+1;
  
  //  fprintf(stderr,"total entries: %d\n",(*list)->n_entries_full);

  return 0;
}

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

int parfu_does_not_exist(char *pathname){
  return parfu_does_not_exist_raw(pathname,1);
}

int parfu_does_not_exist_quiet(char *pathname){
  return parfu_does_not_exist_raw(pathname,0);
}

int parfu_does_exist_quiet(char *pathname){
  if(parfu_does_not_exist_quiet(pathname))
    return 0;
  else
    return 1;
}

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
    if(((*target_text)=malloc(buffer_length))==NULL){
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

// return value <0 indicates error
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
  if((link_target=malloc(sizeof(char*)))==NULL){
    fprintf(stderr,"parfu_build_file_list_from_directory:\n");
    fprintf(stderr,"  could not allocate link name buffer pointer!!\n");
    return NULL;
  }
  *link_target=NULL;
  
  if(parfu_does_not_exist(top_dirname)){
    fprintf(stderr,"parfu_build_file_list_from_directory:\n");
    fprintf(stderr,"  target we were pointed to does not exist!\n");
    fprintf(stderr,"  Exiting.\n");
    return NULL;
  }

     
  if(parfu_is_a_symlink(top_dirname,link_target) && (!follow_symlinks)){
    fprintf(stderr,"parfu_build_file_list_from_directory:\n");
    fprintf(stderr,"  The dir we were pointed to is a link, \n");
    fprintf(stderr,"  and we\'re not following links.\n");
    fprintf(stderr,"  this is an error.\n");
    return NULL;
  }
  if(parfu_is_a_regfile(top_dirname,NULL)){
    fprintf(stderr,"parfu_build_file_list_from_directory:\n");
    fprintf(stderr,"  The dir we were pointed to is a regular file.\n");
    fprintf(stderr,"  this is an error.\n");
    return NULL;
  }
  if(!parfu_is_a_dir(top_dirname)){
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
  if((next_archive_name=malloc(next_archive_name_length+1))==NULL){
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
      if((next_relative_name=malloc(next_relative_name_length))==NULL){
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
      if((next_archive_name=malloc(next_archive_name_length))==NULL){
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
      if(parfu_is_a_symlink(next_relative_name,link_target) && (!follow_symlinks)){
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
	if(parfu_is_a_dir(next_relative_name)){
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
	if(parfu_is_a_regfile(next_relative_name,&file_size)){
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
	    my_list->list[i].size);
  }
  
  fprintf(output,"\n\n");
}

void parfu_check_offsets(parfu_file_fragment_entry_list_t *my_list,
			 FILE *output){
  int i,j;
  int chars_printed;
  long int block_begins;
  int block_size;
  long int block_ends;
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

    block_size=int_power_of_2(my_list->list[i].block_size_exponent);
    block_begins = block_size * my_list->list[i].first_block;
    if(my_list->list[i].size)
      block_ends = (block_begins + my_list->list[i].size) - 1;
    else
      block_ends = block_begins;
    fprintf(output,"  %c   ",parfu_return_type_char(my_list->list[i].type));
    if(my_list->list[i].type == PARFU_FILE_TYPE_REGULAR)
      fprintf(output,"  #bl=%3ld  exp=%d  %10ld  %10ld   %10ld",
	      my_list->list[i].number_of_blocks,
	      my_list->list[i].block_size_exponent,
	      block_begins,block_ends,
	      my_list->list[i].size);
    fprintf(output,"\n");

  }
  
  fprintf(output,"\n\n");
}

// has to return int (not long int) because of qsort library function
int parfu_compare_fragment_entry_by_size(const void *vA, const void *vB){
  parfu_file_fragment_entry_t *A,*B;
  long int sizeA, sizeB;
  A=(parfu_file_fragment_entry_t*)vA;
  B=(parfu_file_fragment_entry_t*)vB;
  sizeA=A->size;
  sizeB=B->size;
  
  if(sizeA > sizeB)
    return +1;
  if(sizeB > sizeA)
    return -1;
  //  return 0;
  return strcmp(A->archive_filename,B->archive_filename);
}

void parfu_qsort_entry_list(parfu_file_fragment_entry_list_t *my_list){
  qsort( (void*)(my_list->list),
	 my_list->n_entries_full,
	 sizeof(parfu_file_fragment_entry_t),
	 &parfu_compare_fragment_entry_by_size);
}

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

int int_power_of_2(int arg){
  switch(arg){
  case 12:
    return 4096;
  case 13:
    return 8192;
  case 14:
    return 16384;
  case 15:
    return 32768;
  case 16:
    return 65536;
  case 17:
    return 131072;
  case 18:
    return 262144;
  case 19:
    return 524288;
  case 20:
    return 1048576;
  case 21:
    return 2097152;
  case 0:
    return 1;
  case 1:
    return 2;
  case 2:
    return 4;
  case 3:
    return 8;
  case 4:
    return 16;
  case 5:
    return 32;
  case 6:
    return 64;
  case 7:
    return 128;
  case 8:
    return 256;
  case 9:
    return 512;
  case 10:
    return 1024;
  case 11:
    return 2048;
  case 22:
    return 4194304;
  case 23:
    return 8388608;
  case 24:
    return 16777216;
  case 25:
    return 33554432;
  case 26:
    return 67108864;
  default:
    return int_power_of(2,arg);
  }    
}

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
      if(in_list->list[i].number_of_blocks){
	// it's an actual file with data, so allocate space in the archive for it
	file_block_size = int_power_of_2(in_list->list[i].block_size_exponent);
	//	first_available_byte = last_byte_written + 1;

	next_block_index = first_available_byte / file_block_size;
	if(first_available_byte % file_block_size)
	  next_block_index++;
	in_list->list[i].first_block = next_block_index;
	next_block_boundary = next_block_index * file_block_size;
	first_available_byte = next_block_boundary + in_list->list[i].size;	
      }
      
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

