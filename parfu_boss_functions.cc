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

// rank 0 sending instruction messages to all other ranks
int parfu_broadcast_order(string instruction,
			  string message){
  // this function is specifically assuming that rank 0 is
  // sending the message.  

  // instruction is a string with a single letter
  // message is the bulk of the message.
  int *message_length=nullptr;
  //  char *message_buffer=nullptr;
  int mpi_return_val;
  
  message_length = new int;

  string message_contents = string("");
  message_contents.append(instruction);
  message_contents.append(message);
  // the +1 is because of the null-terminated C string.  We're not currently
  // using C string functions to parse these messages, but we might, and this
  // allows the null to be transmitted and allows this buffer to be safe for
  // those functions (I think) in case we change our mind.  
  *message_length = message_contents.size()+1;
  mpi_return_val = MPI_Bcast(message_length,1,MPI_INT,0,MPI_COMM_WORLD);
  mpi_return_val += MPI_Bcast(((void*)(message_contents.data())),
			      message_contents.size()+1,
			      MPI_CHAR,0,MPI_COMM_WORLD);
  delete message_length;
  return mpi_return_val;
}

// rank 0 sending individual instruction to a single destination node
int parfu_send_order_to_rank(int dest_rank,
			     int tag,
			     string instruction,
			     string message){
  // this function is specifically assuming that rank 0 is
  // sending the message.  

  // instruction is a string with a single letter
  // message is the bulk of the message.
  int *message_length=nullptr;
  int mpi_return_val;
  
  message_length = new int;

  string message_contents = string("");
  message_contents.append(instruction);
  message_contents.append(message);
  // the +1 is because of the null-terminated C string.  We're not currently
  // using C string functions to parse these messages, but we might, and this
  // allows the null to be transmitted and allows this buffer to be safe for
  // those functions (I think) in case we change our mind.  
  *message_length = message_contents.size()+1;
  mpi_return_val = MPI_Send(message_length,1,MPI_INT,dest_rank,tag,MPI_COMM_WORLD);
  mpi_return_val += MPI_Send(((void*)(message_contents.data())),
			     message_contents.size()+1,
			     MPI_CHAR,dest_rank,tag,MPI_COMM_WORLD);
  delete message_length;
  return mpi_return_val;

}

#define INT_STRING_BUFFER_SIZE (20)

int push_out_all_orders(vector <string> *transfer_order_list,
			unsigned int total_ranks){
  unsigned int next_order=0;
  unsigned int next_rank=1;
  unsigned int total_orders = transfer_order_list->size();
  char *return_receive_buffer=nullptr;
  string return_receive_string;
  int mpi_return_val;
  //  MPI_Status message_status;
  int worker_rank_received;
  
  // I assume any rank number printed to a string
  // will fit within INT_STRING_BUFFER_SIZE characters
  return_receive_buffer=(char*)malloc(INT_STRING_BUFFER_SIZE);
  
  // First we distribute initial orders to ranks.
  // We start at order index 0 but at rank 1, because
  // *we* are rank zero.  
  //  cerr << "POAO: A ranks:" << total_ranks << " orders:" << total_orders << "\n";
  //  cerr << "POAO: A next
  while( (next_rank < total_ranks) &&
	 (next_order < total_orders)){
    parfu_send_order_to_rank(next_rank,
			     0,  // MPI_Send tag=0
			     string("C"), // C for "create" mode
			     (transfer_order_list->at(next_order)));
    cerr << "debugL1: sending order " << next_order << "\n";
    // update loop
    next_order++;
    next_rank++;
  }
  // we've distributed order sets to ranks until we ran out of
  // one of them.  We need to now deal with our status depending
  // on whether we've run out of ranks, run out of order sets,
  // or neither.

  // If all the orders have been sent, then we don't need to
  // worry about sending any more, we just need to wait for
  // the returns from each of the order sets, then exit.

  // this should work whether or not the ranks have been exhausted
  // Because we're only doing as many receives as orders, so the
  // ranks that never received any orders won't be sending us
  // a complete message.  
  if(next_order >= total_orders){
    for(unsigned i=0;i<total_orders;i++){
      if((mpi_return_val = MPI_Recv((void*)(return_receive_buffer),
				    INT_STRING_BUFFER_SIZE,MPI_CHAR,
				    MPI_ANY_SOURCE,MPI_ANY_TAG,MPI_COMM_WORLD,
				    MPI_STATUS_IGNORE))!=MPI_SUCCESS){
	cerr << "push_out_all_orders:  MPI_Recv returned " << mpi_return_val << "!\n";
      }
      //      cerr << "individual RX: got buffer.\n";
    } // for(unsigned i=0;i<total_orders;i++){
  } // if(next_order >= total_orders){
  else{
    // in this case, we have orders left, which means that every single
    // worker rank got orders.  So starting here, all worker ranks are busy,
    // and we have leftover order sets to hand out.  

    // As the busy worker ranks finish and send back that they're done, we
    // hand each one that does that a new work item while we still have
    // any 
    while(next_order < total_orders){
      if((mpi_return_val = MPI_Recv((void*)(return_receive_buffer),
				    INT_STRING_BUFFER_SIZE,MPI_CHAR,
				    MPI_ANY_SOURCE,MPI_ANY_TAG,MPI_COMM_WORLD,
				    MPI_STATUS_IGNORE))!=MPI_SUCCESS){
	cerr << "push_out_all_orders:  MPI_Recv returned " << mpi_return_val << "!\n";
      }
      cerr << "debugL1: sending order " << next_order << "\n";
      return_receive_string=string(return_receive_buffer);
      cerr << "debug: indicated rank:" << return_receive_string << "\n";
      worker_rank_received=stoi(return_receive_string);
      parfu_send_order_to_rank(worker_rank_received,
			       0,
			       string("C"), // this has the "create" message baked in
			       // we may want to make this an input parameter
			       (transfer_order_list->at(next_order)));
      
      // loop cleanup
      next_order++;
    } // while
    
    // at this point, all work items have been distributed.  So we just need
    // to wait for them all to report that they're finished
    
    // [TODO perhaps we should move writing the catalog to here?]

    // VERY IMPORTANT!!!  Index "i" MUST start at 1, NOT ZERO here.  Otherwise,
    // you end up waiting for one more receive than you're ever going to get
    // and it deadlocks
    for(unsigned i=1;i<total_ranks;i++){
      if((mpi_return_val = MPI_Recv((void*)(return_receive_buffer),
				    INT_STRING_BUFFER_SIZE,MPI_CHAR,
				    MPI_ANY_SOURCE,MPI_ANY_TAG,MPI_COMM_WORLD,
				    MPI_STATUS_IGNORE))!=MPI_SUCCESS){
	cerr << "push_out_all_orders:  MPI_Recv returned " << mpi_return_val << "!\n";
      } // if((mpi_return_val...
      
    }// for(i=0   
  } // else 
  


  return 0;
}
