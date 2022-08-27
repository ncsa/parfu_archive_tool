//////////////////////////////////////////////////////////////////////////////////
// 
//  University of Illinois/NCSA Open Source License
//  http://otm.illinois.edu/disclose-protect/illinois-open-source-license
//  
//  Parfu is copyright (c) 2017-2022, The Trustees of the University of Illinois. 
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
//////////////////////////////////////////////////////////////////////////////////

#include "parfu_main.hh"

Parfu_rank_order_set::Parfu_rank_order_set(string order_buffer){
  unsigned entry_begin,entry_end;
  unsigned line_begin,line_end;
  parfu_move_order_t local_move_order;
  string type_string;
  line_begin = 0;
  line_end = 0;
  while(line_begin < order_buffer.size()){
    line_end = order_buffer.find(PARFU_LINE_SEPARATOR_CHARACTER,line_begin);
    if(line_end > order_buffer.length()){
      cerr << "Parfu_rank_order_set: bad input buffer!\n";
      throw "Could not find line ending!\n";
    }
    entry_begin = line_begin;
    entry_end = order_buffer.find(PARFU_ENTRY_SEPARATOR_CHARACTER,entry_begin);

    // grab file index
    local_move_order.file_index = stoi(order_buffer.substr(entry_begin,entry_end-entry_begin));
    entry_begin = entry_end + 1;
    entry_end = order_buffer.find(PARFU_ENTRY_SEPARATOR_CHARACTER,entry_begin);

    // grab relative filename
    local_move_order.rel_filename = order_buffer.substr(entry_begin,entry_end-entry_begin);
    entry_begin = entry_end + 1;
    entry_end = order_buffer.find(PARFU_ENTRY_SEPARATOR_CHARACTER,entry_begin);
    //    cerr << "debug order parse:>" << local_move_order.rel_filename << "<\n";
    
    // grab file type
    type_string = order_buffer.substr(entry_begin,entry_end-entry_begin);
    local_move_order.file_type = type_string.at(0);
    entry_begin = entry_end + 1;
    entry_end = order_buffer.find(PARFU_ENTRY_SEPARATOR_CHARACTER,entry_begin);
    
    // grab symlink target
    local_move_order.symlink_target = order_buffer.substr(entry_begin,entry_end-entry_begin);
    entry_begin = entry_end + 1;
    entry_end = order_buffer.find(PARFU_ENTRY_SEPARATOR_CHARACTER,entry_begin);
    
    // grab file_size
    local_move_order.file_size = stoi(order_buffer.substr(entry_begin,entry_end-entry_begin));
    entry_begin = entry_end + 1;
    entry_end = order_buffer.find(PARFU_ENTRY_SEPARATOR_CHARACTER,entry_begin);
    
    // grab header_size
    local_move_order.header_size = stoi(order_buffer.substr(entry_begin,entry_end-entry_begin));
    entry_begin = entry_end + 1;
    entry_end = order_buffer.find(PARFU_ENTRY_SEPARATOR_CHARACTER,entry_begin);
    
    // grab position in archive
    local_move_order.position_in_archive = stoi(order_buffer.substr(entry_begin,entry_end-entry_begin));
    entry_begin = entry_end + 1;
    entry_end = order_buffer.find(PARFU_LINE_SEPARATOR_CHARACTER,entry_begin);
    
    // grab offset in file
    local_move_order.offset_in_file = stoi(order_buffer.substr(entry_begin,entry_end-entry_begin));

    orders.push_back(local_move_order);
    
    // cleanup for next line
    line_begin = entry_end + 1;
    entry_begin = line_begin;
  }


  orders.push_back(local_move_order);
}
