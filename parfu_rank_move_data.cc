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
    cerr << "line: >" << order_buffer.substr(line_begin,line_end) << "<\n"; 
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
    
    cerr << "debug read order: " << local_move_order.header_size << "  " << local_move_order.file_size;
    cerr << "  " << local_move_order.position_in_archive << "\n";
    // grab offset in file
    local_move_order.offset_in_file = stoi(order_buffer.substr(entry_begin,entry_end-entry_begin));

    orders.push_back(local_move_order);
    
    // cleanup for next line
    line_begin = entry_end + 1;
    entry_begin = line_begin;
  }


  orders.push_back(local_move_order);
}

// this is a function that populates a single bucket in an archive
// file on disk.  This is typically run by a single worker node
// on one set of orders that it gets
int Parfu_rank_order_set::move_data_Create(string base_path,
					   unsigned long bucket_size,
					   MPI_File *archive_file_handle){
  // create-mode data movement
  // all data should fit within a bucket, so we'll use that for the
  // buffer size
  void *staging_buffer = nullptr;
  MPI_File *target_file = nullptr;
  unsigned long file_start_in_target;
  unsigned long file_start_in_archive;
  unsigned long file_start_in_bucket;
  tarentry my_tarentry;
  string full_filename;
  string header_contents;
  unsigned long bucket_location_in_archive;
  unsigned long total_bucket_length;
  int return_val;
  MPI_Status my_mpi_status;

  
  if((staging_buffer=(void*)malloc(bucket_size))==nullptr){
    cerr << "move_data_Create: could not allocate staging buffer!\n";
    return -1;
  }
  
  bucket_location_in_archive =
    orders.front().position_in_archive;
  total_bucket_length =
    (orders.back().position_in_archive +
     orders.back().header_size +
     orders.back().file_size) -
    bucket_location_in_archive;
  target_file = new MPI_File;

  // go through the files in the order set
  // set up their headers and copy their data
  // into the buffer.  In other words, we're assembling
  // the contents of a bucket in memory
  for(unsigned ndx=0; ndx<orders.size() ; ndx++){
    full_filename = string("");
    if(base_path.size()){
      full_filename += base_path;
      full_filename += "/";
    }
    full_filename += orders.at(ndx).rel_filename;
    file_start_in_bucket =
      orders.at(ndx).position_in_archive - bucket_location_in_archive;
    // first establish the header
    if(orders.at(ndx).header_size){
      // this does a stat to pull the information
      parfu_make_tar_header_at(full_filename,
			       staging_buffer,
			       file_start_in_bucket);
    } // if(orders.at(ndx).header_size)
    // move the payload data if it has any
    if(orders.at(ndx).file_size){
      file_start_in_bucket += orders.at(ndx).header_size;
      if((return_val=MPI_File_open(MPI_COMM_SELF,full_filename.c_str(),
				   MPI_MODE_RDONLY,MPI_INFO_NULL,
				   target_file))!=MPI_SUCCESS){
	cerr << "move_data_Create:MPI_File_open() returned ";
	cerr << return_val << " when trying to open for reading.\n";
      }
      if((return_val=MPI_File_read_at(*target_file,
				      orders.at(ndx).offset_in_file,
				      ((void*)(((char*)(staging_buffer)+file_start_in_bucket))),
				      orders.at(ndx).file_size,
				      MPI_CHAR,&my_mpi_status))!=MPI_SUCCESS){
	cerr << "move_data_Create:MPI_File_read_at() returned ";
	cerr << return_val << " when trying to pull data from target file.\n";
	
      }
    }
  } // for(unsigned ndx=0; ndx<orders.size() ; ndx++)
  
  // And now we copy the assembled contents of the bucket
  // into the appropriate place in the bucket
  if((return_val=MPI_File_write_at(*archive_file_handle,
				   bucket_location_in_archive,
				   staging_buffer,
				   total_bucket_length,MPI_CHAR,&my_mpi_status))!=MPI_SUCCESS){
    cerr << "move_data_Create:MPI_File_write_at() returned ";
    cerr << return_val << " when trying to write complete bucket to archive file.\n";
  }
				   
  return 0;
} // int Parfu_rank_order_set::move_data_Create

void parfu_make_tar_header_at(string full_filename,
			      void* current_bucket_buffer,
			      unsigned long location_in_bucket){
  std::vector<char> temp_file_header_C;
  tarentry my_tarentry;
  
  //  cerr << "creating header at: " << location_in_bucket << "\n";
  my_tarentry = tarentry(full_filename,0);
  temp_file_header_C = my_tarentry.make_tar_header();
  std::copy(temp_file_header_C.begin(), temp_file_header_C.end(),
	    ((((char*)(current_bucket_buffer))+
	      (location_in_bucket)
	      ))); 
}

