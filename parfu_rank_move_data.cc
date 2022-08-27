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

#include "parfu_rank_move_data.hh"


Parfu_rank_order_set::Parfu_rank_order_set(string order_buffer){
  int begin;
  int end;
  unsigned line_begin,line_end;
  parfu_move_order_t local_move_order;
  line_begin = 0;
  line_end = 0;
  while(line_end < order_buffer.size()){
    line_end = order_buffer.find('\n',line_begin);

    line_begin = line_end + 1;
  }


  orders.push_back(local_move_order);
}
