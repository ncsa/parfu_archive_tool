////////////////////////////////////////////////////////////////////////////////
// 
//  University of Illinois/NCSA Open Source License
//  http://otm.illinois.edu/disclose-protect/illinois-open-source-license
//  
//  Parfu is copyright (c) 2017-2022, 
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

#ifndef PARFU_RANK_MOVE_DATA_HH__
#define PARFU_RANK_MOVE_DATA_HH__

//#include "parfu_main.hh"
//#include "parfu_2021_legacy.hh"
//#include "parfu_primary.h"

void parfu_make_tar_header_at(string full_filename,
			      void* current_bucket_buffer,
			      unsigned long location_in_bucket);


// each of this is one move order for either a file or a file slice
typedef struct{
  int file_index;
  string rel_filename;
  char file_type;
  string symlink_target;
  unsigned long file_size;
  unsigned header_size;
  unsigned long position_in_archive;
  unsigned long offset_in_file;
}parfu_move_order_t;

#endif

/////////////////////////////
//
class Parfu_rank_order_set
{
public:
  // construct an order set from a buffer with
  // text order instructions
  Parfu_rank_order_set(string order_buffer);
  int move_data_Create(string base_path,
		       unsigned long bucket_size,
		       MPI_File *my_file_handle);
  int n_orders(void);
  unsigned long total_size(void);
private:
  vector <parfu_move_order_t> orders;
  
};

/* 

  std::vector<char> temp_file_header_C;
  tarentry my_tar_header_C;





    if(!(myl->list[current_entry].location_in_orig_file)){
      my_tar_header_C = tarentry(myl->list[current_entry].relative_filename,0);
      temp_file_header_C = my_tar_header_C.make_tar_header();
      std::copy(temp_file_header_C.begin(), temp_file_header_C.end(),
		((((char*)(transfer_buffer))+
		  ((myl->list[current_entry].location_in_archive_file - 
		    current_bucket_loc)
		   - myl->list[current_entry].our_tar_header_size)
		  ))); 
    }




 */ 
