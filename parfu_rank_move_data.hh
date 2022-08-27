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


#include "parfu_main.hh"
#include "parfu_2021_legacy.hh"
//#include "parfu_primary.h"

#ifndef PARFU_RANK_MOVE_DATA_HH__
#define PARFU_RANK_MOVE_DATA_HH__

typedef struct{
  int file_index;
  string rel_filename;
  int file_type;
  string symlink_target;
  unsigned long file_size;
  unsigned header_size;
  unsigned long position_in_archive;
  unsigned long offset_in_file;
}parfu_move_order_t;

#endif