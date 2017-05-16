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

#ifndef PARFU_PRIMARY_H
#define PARFU_PRIMARY_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <mpi.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>

#include <time.h>
#include <ctype.h>

//////////////////
// 
// Constants that users might want to tweak
// 

// removing exponents in verson 0.5.1, starting May 5, 2017
// leaving here so I have record of what they WERE, so I can 
// search source files for them.
//#define PARFU_DEFAULT_MIN_BLOCK_SIZE_EXPONENT     12
//#define PARFU_DEFAULT_BLOCKS_PER_FRAGMENT             250

// Blocks of files are padded out to be a multiple of this number of bytes
// typically 512, to match the standard blocking of tar.
#define PARFU_DEFAULT_BLOCKING_FACTOR    (512)
// file fragments are padded up to this size in rank buffers and 
// in chunks of data written to the archive file.  Optimum performance
// probably matches this to network buffer size for I/O.  On 
// Blue Waters (home system of parfu) 4 MB seems good.
#define PARFU_DEFAULT_RANK_BLOCK_SIZE    (4194304)

//
// End of recommended user-tweakable constants
//
//////////////////

extern int debug;

typedef enum{PARFU_FILE_TYPE_REGULAR,PARFU_FILE_TYPE_DIR,PARFU_FILE_TYPE_SYMLINK,PARFU_FILE_TYPE_INVALID} parfu_file_t;

#define PARFU_FILE_TYPE_REGULAR_CHAR 'F'
#define PARFU_FILE_TYPE_DIR_CHAR 'd'
#define PARFU_FILE_TYPE_SYMLINK_CHAR 'L'
#define PARFU_FILE_TYPE_INVALID 'X'

// file catalog specifications
#define PARFU_CATALOG_VALUE_TERMINATOR '\t'
#define PARFU_CATALOG_ENTRY_TERMINATOR '\n'


#define PARFU_SPECIAL_SIZE_DIR                (-3L)
#define PARFU_SPECIAL_SIZE_SYMLINK            (-2L)
#define PARFU_SPECIAL_SIZE_INVALID_REGFILE    (-1L)

#define PARFU_LINE_BUFFER_DEFAULT 1000
#define PARFU_DEFAULT_SIZE_PER_LINE 200
#define PARFU_MAXIMUM_BUFFER_SIZE (10000000)

#define PARFU_FILE_PTR_NONSHARED (-1)

//#define PARFU_ABSOLUTE_MIN_BLOCK_SIZE_EXPONENT    8
//#define PARFU_LARGEST_ALLOWED_MAX_BLOCK_SIZE_EXPONENT  26
//#define PARFU_SMALLEST_ALLOWED_MAX_BLOCK_SIZE_EXPONENT 16
//#define PARFU_DEFAULT_MAX_BLOCK_SIZE_EXPONENT          20

//#define PARFU_MIN_ALLOWED_BLOCKS_PER_FRAGMENT           1
//#define PARFU_MAX_ALLOWED_BLOCKS_PER_FRAGMENT        2000

#define PARFU_WHAT_IS_PATH_REGFILE         0x001
#define PARFU_WHAT_IS_PATH_SYMLINK         0x002
#define PARFU_WHAT_IS_PATH_DIR             0x004
#define PARFU_WHAT_IS_PATH_DOES_NOT_EXIST  0x100
#define PARFU_WHAT_IS_PATH_IGNORED_TYPE    0x200
#define PARFU_WHAT_IS_PATH_ERROR           0x400

// example of file layout in archive file
// void space is depicted by underscores:___ (3 void bytes)
// file1=abc
// file2=mnopqrstuvwx
// block exponent=2, block is 4 bytes (characters)
// archive_file=CATALOG_abc_mnopqrstuv__
// file1 will be stored as one fragment with one void byte to get to next block boundary: abc
// file2 will be stored as three fragments: 
// file2 fragment 1: mnop
// file2 fragment 2: qrst
// file2 fragment 3: uv 

  // Examples for the following structure: 
  // In the case of a relative directory beginning with "../", the "../" will be 
  // included in the relative_filename, but not in the archive_filename. 
  // in the case of an absolute path, the relative_filename will including the 
  // leading "/", but the archive_filename will not.

typedef struct{
  // original file pathnames, type, and link target
  char *relative_filename; // apparent filename from point of view of the pwd of parfu process
  char *archive_filename; // name of file that will go in archive
  parfu_file_t type; // PARFU_FILE_TYPE_[REGULAR|DIR|SYMLINK]
  char *target; // string name of target of symlink; otherwise NULL

  // original file size and location, and tar header info
  long int our_file_size; // size of this fragment (or size of whole file) in bytes
  long int our_tar_header_size; // length of tar header in bytes
  long int location_in_archive_file; // beginning of tar header from beginning of archive file
  long int location_in_orig_file; // fragment location (in bytes) from beginning of original file
  //                              =0 for the first fragment of every file
  
  // information about padding pseudo-file following this file
  long int pad_file_location_in_archive_file;
  char *pad_file_archive_filename;
  long int pad_file_tar_header_size;

  int file_contains_n_fragments; // file will be archived/extracted in this many pieces (derived)
  int file_ptr_index; // which of global file pointer this file uses

  //  int block_size_exponent; // each block is 2^block_size_exponent bytes
  //  int num_blocks_in_fragment; // each fragment is this many blocks (or less)
  // this information is used for the individual file fragments when the file 
  //   gets split up among ranks:
  //  long int fragment_offset; // fragment location (in bytes) from beginning of original file
  //  long int first_block; // fragment location (in blocks) from beginning of data in archive file
  //  long int number_of_blocks; // this fragment/file spans this many blocks


  // data is arranged in the archive file by "rank buckets".  These are chunks of data
  // up to the size that one rank deals with.  Files smaller than that are grouped together. 
  // Files bigger than that take up more than one, but each fragment exists in one 
  // rank bucket.
  long int rank_bucket_index;
}parfu_file_fragment_entry_t;

typedef struct{
  int n_entries_total;
  int n_entries_full;
  parfu_file_fragment_entry_t list[1];
}parfu_file_fragment_entry_list_t;


#ifdef __cplusplus
extern "C" {
#endif
  // parfu_file_list_utils.c
  parfu_file_fragment_entry_list_t 
  *create_parfu_file_fragment_entry_list(int in_n_entries);
  
  void parfu_free_ffel(parfu_file_fragment_entry_list_t *my_list);
  
  // used when initially populating a list
  int parfu_add_name_to_ffel(parfu_file_fragment_entry_list_t **list,
			     parfu_file_t type,
			     char *relative_filename, // must be valid string
			     char *archive_filename, // NULL is allowed for no info
			     char *target,  // NULL is allowed for no info
			     long int size);
  
  int parfu_add_entry_to_ffel(parfu_file_fragment_entry_list_t **list,
			      parfu_file_fragment_entry_t entry);
  
  // used when splitting one list to another. list is new list, 
  // entry is from the old list, the 
  // characteristics are transferred to the new list entry with the modified
  // characteristics of the other arguments
  int parfu_add_entry_to_ffel_mod(parfu_file_fragment_entry_list_t **list,
				  parfu_file_fragment_entry_t entry,
				  long int my_size, // fragment size
				  long int my_fragment_loc_in_archive_file,
				  long int my_fragment_loc_in_orig_file,
				  int my_file_contains_n_fragments,
				  int my_file_ptr_index,
				  long int my_rank_bucket_index);
  
  int parfu_add_entry_to_ffel_raw(parfu_file_fragment_entry_list_t **list,
				  char *my_relative_filename,
				  char *my_archive_filename,
				  parfu_file_t my_type,
				  char *my_target,
				  long int my_size,
				  long int my_location_in_archive_file,
				  long int my_location_in_orig_file,
				  int my_file_contains_n_fragments,
				  int my_file_ptr_index, 
				  long int my_rank_bucket_index);
  
  // these are deprecated May 15 2017
  // we'll remove them entirely once we know 
  // there's nothing hidden that we need.
  /*
  int parfu_is_a_dir(char *pathname);
  int parfu_is_a_symlink(const char *pathname,char **target_text);
  int parfu_is_a_regfile(char *pathname, long int *size);
  int parfu_does_not_exist(char *pathname);
  int parfu_does_not_exist_raw(char *pathname, int be_loud);
  int parfu_does_not_exist_quiet(char *pathname);
  int parfu_does_exist_quiet(char *pathname);
  */
  
  parfu_file_fragment_entry_list_t *parfu_build_file_list_from_directory(char *top_dirname, 
									 int follow_symlinks, 
									 int *total_entries);
  char parfu_return_type_char(parfu_file_t in_type);
  void parfu_dump_fragment_entry_list(parfu_file_fragment_entry_list_t *my_list,
				      FILE *output);
  void parfu_check_offsets(parfu_file_fragment_entry_list_t *my_list,
			   FILE *output);
  
  int parfu_compare_fragment_entry_by_size(const void *vA, const void *vB);
  void parfu_qsort_entry_list(parfu_file_fragment_entry_list_t *my_list);
  //  int int_power_of(int base, int power);
  //  int int_power_of_2(int arg);
  parfu_file_fragment_entry_list_t 
  *parfu_split_fragments_in_list(parfu_file_fragment_entry_list_t *in_list,
				 int min_block_size_exponent,
				 int max_block_size_exponent, 
				 int blocks_per_fragment);
  // removing exponent stuff in May 2017 redesign
  /*
  int parfu_what_is_file_exponent(long int file_size, 
				  int min_exponent,
				  int max_exponent);
  int parfu_set_exp_offsets_in_ffel(parfu_file_fragment_entry_list_t *myl,
				    int min_exp,
				    int max_exp);
  */

  // files will be spaced to begin at the next even multiple of the
  // per-file blocking size.  Files will also be spaced so that files
  // smaller than the per-rank blocking size will always live in
  // only one rank's buffer, and files larger than that will 
  // always start on an even multiple of the per-rank blocking size.
  int parfu_set_offsets_in_ffel(parfu_file_fragment_entry_list_t *myl,
				int per_file_blocking_size,
				int per_rank_blocking_size);
  unsigned int parfu_what_is_path(const char *pathname,
				  char **target_text,
				  long int *size,
				  int follow_symlinks);
  
  // parfu_buffer_utils.c
  char *parfu_fragment_list_to_buffer(parfu_file_fragment_entry_list_t *my_list,
				      long int *buffer_length,
				      int max_block_size_exponent,
				      int is_archive_catalog);
  int snprintf_to_full_buffer(char *my_buf, size_t buf_size, 
			      parfu_file_fragment_entry_list_t *my_list,
			      int indx);
  int snprintf_to_archive_buffer(char *my_buf, size_t buf_size, 
				 parfu_file_fragment_entry_list_t *my_list,
				 int indx);
  parfu_file_fragment_entry_list_t 
  *parfu_buffer_to_file_fragment_list(char *in_buffer,
				      int *max_block_size_exponent,
				      int is_archive_catalog);
  char *parfu_get_next_filename(char *in_buf, 
				char end_character,
				int *increment_pointer);
  
  parfu_file_fragment_entry_list_t 
  *parfu_ffel_from_file(char *archive_filename,
			int *max_block_size_exponent,
			int *catalog_buffer_length);
  
  int parfu_ffel_fill_rel_filenames(parfu_file_fragment_entry_list_t *my_list,
				    char *leading_path);
  
  
  // parfu_data_transfer.c
  int parfu_archive_1file_singFP(parfu_file_fragment_entry_list_t *raw_list,
				 char *arch_file_name,
				 int n_ranks, int my_rank, 
				 int my_max_block_size_exponent,
				 long int transfer_buffer_size,
				 int blocks_per_fragment,
				 int check_if_already_exist);
  int parfu_extract_1file_singFP(char *arch_file_name,
				 char *extract_target_dir,
				 int n_ranks, int my_rank, 
				 int *my_max_block_size_exponent,
				 long int transfer_buffer_size,
				 int my_blocks_per_fragment,
				 int check_if_already_exist);
  
  /////////
  //
  // Data structures for main() source files (command-line flags and stuff)
  //
  
#define DEFAULT_N_VALUES_IN_BEHAVIOR_CONTROL_ARRAY   10
  // Next two variables must relate, so that the indexing in file names works
  // They could be 10,1 or 100,2 or 1000,3 or 10000,4 and so on
  // so that a 0-origin index of the as many as the first value can be expressed
  // in terms of digits of the second value
#define PARFU_MAX_TEST_ITERATIONS                  1000
#define PARFU_N_DIGITS_IN_ITERATION_INDEX             3
  
  typedef struct{
    int trial_run;
    char mode;
    int yn_iterations_argument;
    int n_iterations;
    int overwrite_archive_file;
    char *target_directory;
    char *data_output_file;
    int yn_data_to_stdout;
    int array_len;
    int n_archive_files;
    char **archive_files;
    int n_min_block_exponent_values;
    int *min_block_exponent_values;
    int n_blocks_per_fragment_values;
    int *blocks_per_fragment_values;
  }parfu_behavior_control_t;
  
  // parfu_behavior_control.c
  parfu_behavior_control_t *parfu_init_behavior_control(void);
  parfu_behavior_control_t *parfu_init_behavior_control_raw(int array_length);
  int parfu_behav_add_min_expon(parfu_behavior_control_t *behav,
				int in_exp);
  int parfu_behav_add_bl_per_frag(parfu_behavior_control_t *behav,
				  int in_bpf);
  int parfu_behav_add_arc_file(parfu_behavior_control_t *behav,
			       char *in_filename);
  int parfu_behav_extend_array(parfu_behavior_control_t *behav);
  int parfu_are_we_in_MPI(void);
  parfu_behavior_control_t *parfu_parse_arguments(int argc, char *argv[]);
#ifdef __cplusplus
}
#endif




// PARFU_PRIMARY_H
#endif
