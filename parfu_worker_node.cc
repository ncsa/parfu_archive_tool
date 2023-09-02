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

/////////////////
//
// The worker is an infinite loop that receives MPI messages.
// The worker also has "receive mode" that's part of its internal state
// that determines what kind of messages
// it's listening for.  Messages are used to direct it to do things
// and it change its message-listening mode and other aspects of
// its internal state.
//
// It starts in "B" mode, in which it's listening for broadcast
// messages.
//
// B mode valid incoming messages:
//   "A" instuction: the rest of the message buffer is the name of an archive
//       file that you are to do a collective open on now, and retain that
//       parallel file pointer in your state.
//   "U" instruction: the rest of the message buffer is a number that you are
//       to set your internal bucket size to
//   "N" switch out of "B" (broadcast) listening mode to "N" mode
//       (iNdividual listening mode)
//   "X" close down and exit
//
//   in "N" mode, worker is listening for one-to-one individual MPI messages
//      from rank 0.  
// N mode valid incoming messages:
//   "C" "create" mode (referenced to tar).  The rest of the buffer is a series
//       of file transfer orders.  These will be copied from target files to
//       the archive file.
//   "P" rest of the buffer is new base path to set in your state
//   "B" switch out of "N" (iNdividual) broadcast receive mode to "B"
//       (broadcast) receive mode
//   "X" close down and exit
//   
//


// this example was tested successfully
// 
//  cout << "now parse the orders.\n\n";
//  my_orders = new Parfu_rank_order_set(transfer_orders->at(0));
//  cout << "\n\n parsing done.\n";

// we've already done MPI_Init() and have our rank
int parfu_worker_node(int my_rank, int total_ranks){
  string my_message_buffer;
  string my_base_path;
  int mpi_return_val;
  int *my_length=nullptr;
  char *message_buffer=nullptr;
  string instruction_letter;
  string archive_filename;
  string message_string;
  char receive_mode='B'; // we start in "broadcast" receiving mode
  bool valid_instruction;
  unsigned long rank_bucket_size = 0UL;
  
  MPI_File *file_handle=nullptr;
  vector <MPI_File*> archive_files;
  MPI_Status *message_status=nullptr;
  
  Parfu_rank_order_set *my_rank_order;
  
  if(my_rank==0){
    cerr << "parfu_worker_node got zero rank!!!\n";
    return -1;
  }
  
  my_length = new int;
  message_status = new MPI_Status;
  
  
  // preliminary, out of date, and incomplete explanation of messages
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
      if((mpi_return_val =
	  MPI_Bcast((void*)(my_length),1,MPI_INT,0,MPI_COMM_WORLD))!=MPI_SUCCESS){
	cerr << "parfu_worker: 1 MPI_Bcast returned " << mpi_return_val << "!\n";
      }
      
      // receive the order string itself
      message_buffer = (char*)malloc(*my_length);
      if((mpi_return_val =
	  MPI_Bcast(((void*)(message_buffer)),*my_length,MPI_CHAR,0,MPI_COMM_WORLD))!=MPI_SUCCESS){
	cerr << "parfu_worker: 2 MPI_Bcast returned " << mpi_return_val << "!\n";
      }
      message_string=string(message_buffer);
      
      // grab the first letter off the message which is our
      // general order of what to do, and tells us what's in
      // the rest of the buffer.  
      instruction_letter = message_string.substr(0,1);
      // now we do stuff based on what the order letter was
      valid_instruction=false;
      if(instruction_letter == "A"){
	valid_instruction=true;
	// the rest of the buffer is the name of the archive file we need to open
	// in a collective open.  
	archive_filename = message_string.substr(1);
	if(file_handle==nullptr)
	  file_handle = new MPI_File;
	//	MPI_Barrier(MPI_COMM_WORLD);
	if((mpi_return_val =
	    MPI_File_open(MPI_COMM_WORLD,archive_filename.c_str(),
			  MPI_MODE_WRONLY|MPI_MODE_CREATE|MPI_MODE_EXCL,
			  MPI_INFO_NULL,file_handle))!=MPI_SUCCESS){
	  cerr << "parfu_worker rank:" << my_rank << " : 3 MPI_File_open returned " << mpi_return_val << "!\n";
	}
	archive_files.push_back(file_handle);
      } // if(instruction_letter == "A"){
      if(instruction_letter == "U"){
	valid_instruction=true;
	// set bucket size
	rank_bucket_size = stoi(message_string.substr(1));
      }
      if(instruction_letter == "N"){
	valid_instruction=true;
	// we flip from broadcast mode to "iNdividual" receive mode.  
	receive_mode = 'N';
	// if the "N" message buffer contains other information in the
	// future, this is where we would retrieve it and process it.
	//	cerr << "rank " << my_rank << " switching to iNdividual receive mode.\n";
      }
      if(instruction_letter == "X"){
	valid_instruction=true;
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
      if(!valid_instruction){
	cerr << "WARNING!  rank " << my_rank << " received order:>";
	cerr << instruction_letter << "< in (Bcast mode) which is invalid!\n";
      }
      free(message_buffer);
      message_buffer=nullptr;
      break;
    case 'N':
      // we're in individual receive mode.  
      if((mpi_return_val = MPI_Recv((void*)(my_length),1,MPI_INT,
				    0,MPI_ANY_TAG,MPI_COMM_WORLD,message_status))!=MPI_SUCCESS){
	cerr << "parfu_worker: 4 MPI_Recv returned " << mpi_return_val << "!\n";
      }
      //      cerr << "individual receive; got length=" << *my_length << "\n";
      message_buffer = (char*)malloc(*my_length);
      if((mpi_return_val = MPI_Recv((void*)(message_buffer),*my_length,MPI_CHAR,
				    0,MPI_ANY_TAG,MPI_COMM_WORLD,message_status))!=MPI_SUCCESS){
	cerr << "parfu_worker: 5 MPI_Recv returned " << mpi_return_val << "!\n";
      }
      //      cerr << "individual RX: got buffer.\n";
      message_string = string(message_buffer);
      //      cerr << "individual RX: string: " << message_string << "\n";
      // grab the first letter off the message which is our
      // general order of what to do, and tells us what's in
      // the rest of the buffer.  
      instruction_letter = message_string.substr(0,1);
      // now we do stuff based on what the order letter was
      valid_instruction=false;
      if(instruction_letter == "C"){
	valid_instruction=true;
	if(!rank_bucket_size){
	  cerr << "rank " << my_rank << " never got bucket size!  Exiting.\n";
	  return 7;
	}
	
	// the rest of a message is a buffer with transfer orders
	// this creates the order set for this rank
	my_rank_order = new Parfu_rank_order_set(message_string.substr(1));
	cerr << "r:" << my_rank << " Cmode w/ orders:";
	cerr << my_rank_order->n_orders() << ", totsz:";
	cerr << my_rank_order->total_size();
	//<< "\n";
	cerr << " 1st file:" << my_rank_order->order_n_filename(0) << "\n";
	my_rank_order->move_data_Create(my_base_path,
					rank_bucket_size,
					file_handle);
	// Now return to say that I'm done
	message_string = to_string(my_rank);
	if((mpi_return_val = MPI_Send(message_string.c_str(),message_string.size()+1,MPI_CHAR,
				      0,0,MPI_COMM_WORLD))!=MPI_SUCCESS){
	  cerr << "rank " << my_rank << "sending done didn't work!\n";
	}
	
      } //  if(instruction_letter == "C"){
      if(instruction_letter == "P"){
	valid_instruction=true;
	my_base_path = message_string.substr(1);
      }
      if(instruction_letter == "B"){
	valid_instruction=true;
	// swap back to broadcast receive mode
	receive_mode = 'B';
      }
      if(instruction_letter == "X"){
	valid_instruction=true;
	// we're done.  exit gracefully.
	free(message_buffer);
	cerr << "rank " << my_rank << " got individual shutdown.  returning.\n";
	return 0;
      }	
      if(!valid_instruction){
	cerr << "WARNING!  rank " << my_rank << " received order:>";
	cerr << instruction_letter << "< in (Bcast mode) which is invalid!\n";
      }
      free(message_buffer);
      message_buffer=nullptr;
      break;
    default:
      cerr << "WARNING! worker node function has mode=" << receive_mode;
      cerr << "which is invalid and likely fatal!\n"; 
    } // switch(receive_mode)
    
    
    } // while(1)
    //  cout << "rank:" << my_rank << " inst:" << instruction_letter;
    //  cout << "  arch flnm:" << archive_filename << "\n";
  return 0;
  }


