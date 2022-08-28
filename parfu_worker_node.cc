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

// this example was tested successfully
// 
//  cout << "now parse the orders.\n\n";
//  my_orders = new Parfu_rank_order_set(transfer_orders->at(0));
//  cout << "\n\n parsing done.\n";
  



// we've already done MPI_Init() and have our rank
int parfu_worker_node(int my_rank, int total_ranks,
		      unsigned long bucket_size){
  string my_message_buffer;
  string my_base_path;
  int mpi_return_val;
  int *my_length=nullptr;
  char *message_buffer=nullptr;
  string instruction_letter;
  string archive_filename;
  string message_string;
  char receive_mode='B'; // we start in "broadcast" receiving mode
  
  MPI_File *file_handle=nullptr;
  vector <MPI_File*> archive_files;
  MPI_Status *message_status=nullptr;
  
  if(my_rank==0){
    cerr << "parfu_worker_node got zero rank!!!\n";
    return -1;
  }

  my_length = new int;
  message_status = new MPI_Status;
  
  ///////////////////
  //
  // Workers run in an infinite loop.
  // They receive a single int that's the length of the message
  // then they receive that message.  The first letter of the
  // message buffer tells the worker what's in the rest of
  // the buffer and how to handle it.
  //
  // If the first letter is an "A", the rest of the buffer
  // is the filename of an archive file that everyone must open
  // as a shared file pointer.
  //
  // If the first letter is "C", the rest of the buffer is a set
  // of marching orders for file transfers from target files
  // into the archive file in "create" mode.
  //
  // If the first letter of the buffer is "X" then the worker
  // function returns.  

  while(1){
    switch(receive_mode){
    case 'B': // receiving in "broadcast" mode
      
      // receive length of order buffer
      mpi_return_val = MPI_Bcast((void*)(my_length),1,MPI_INT,0,MPI_COMM_WORLD);
      
      // receive the order string itself
      message_buffer = (char*)malloc(*my_length);
      mpi_return_val =
	MPI_Bcast(((void*)(message_buffer)),*my_length,MPI_CHAR,0,MPI_COMM_WORLD);
      message_string=string(message_buffer);
      
      // grab the first letter off the message which is our
      // general order of what to do, and tells us what's in
      // the rest of the buffer.  
      instruction_letter = message_string.substr(0,1);
      // now we do stuff based on what the order letter was
      if(instruction_letter == "A"){
	// the rest of the buffer is the name of the archive file we need to open
	// in a collective open.  
	archive_filename = message_string.substr(1);
	if(file_handle==nullptr)
	  file_handle = new MPI_File;
	mpi_return_val =
	  MPI_File_open(MPI_COMM_WORLD,archive_filename.c_str(),
			MPI_MODE_WRONLY|MPI_MODE_CREATE,
			MPI_INFO_NULL,file_handle);
	archive_files.push_back(file_handle);
      } // if(instruction_letter == "A"){
      if(instruction_letter == "N"){
	// we flip from broadcast mode to "iNdividual" receive mode.  
	receive_mode = 'N';
	// if the "N" message buffer contains other information in the
	// future, this is where we would retrieve it and process it.
      }
      if(instruction_letter == "X"){
	// we're done.  exit gracefully.
	free(message_buffer);
	if(file_handle != nullptr){
	  free(file_handle);
	  file_handle=nullptr;
	}
	return 0;
      }
      // if in the future we create more message types for broadcast
      // mode, they would be checked for here
      free(message_buffer);
      message_buffer=nullptr;
      break;
    case 'N':
      // we're in individual receive mode.  
      mpi_return_val = MPI_Recv((void*)(my_length),1,MPI_INT,
				0,MPI_ANY_TAG,MPI_COMM_WORLD,message_status);
      message_buffer = (char*)malloc(*my_length);
      mpi_return_val = MPI_Recv((void*)(message_buffer),*my_length,MPI_CHAR,
				0,MPI_ANY_TAG,MPI_COMM_WORLD,message_status);
      message_string = string(message_buffer);
      // grab the first letter off the message which is our
      // general order of what to do, and tells us what's in
      // the rest of the buffer.  
      instruction_letter = message_string.substr(0,1);
      // now we do stuff based on what the order letter was
      if(instruction_letter == "C"){
	// the rest of a message is a buffer with transfer orders
	cerr << "debug: rank " << my_rank << " received \"create\" order buffer.\n";
	
      } //  if(instruction_letter == "C"){
      if(instruction_letter == "B"){
	// swap back to broadcast receive mode
	receive_mode = 'B';
      }
      if(instruction_letter == "X"){
	// we're done.  exit gracefully.
	free(message_buffer);
	return 0;
      }	
    } // switch(receive_mode)
    
    
  } // while(1)
    //  cout << "rank:" << my_rank << " inst:" << instruction_letter;
    //  cout << "  arch flnm:" << archive_filename << "\n";
  return 0;
}

