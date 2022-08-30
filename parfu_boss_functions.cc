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

int push_out_all_orders(vector <string*> transfer_order_list){


  
  return 0;
}
