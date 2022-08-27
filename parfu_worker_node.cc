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
int parfu_worker_node(int my_rank, int total_ranks){
  string my_base_path;
  int mpi_return_val;
  int *my_length=nullptr;
  char *order_buffer=nullptr;
  string instruction_letter;
  string archive_filename;
  string order_string;
  
  if(my_rank==0){
    cerr << "parfu_worker_node got zero rank!!!\n";
    return -1;
  }

  my_length = new int;

  // receive the length of the order string
  mpi_return_val = MPI_Bcast((void*)(my_length),1,MPI_INT,0,MPI_COMM_WORLD);
  order_buffer = (char*)malloc(*my_length);
  // receive the order string itself
  mpi_return_val =
    MPI_Bcast(((void*)(order_buffer)),*my_length,MPI_CHAR,0,MPI_COMM_WORLD);
  // raw string we've been passed is a C string
  order_string=string(order_buffer);
  instruction_letter = order_string.substr(0,1);
  archive_filename = order_string.substr(1);
  cout << "rank:" << my_rank << " inst:" << instruction_letter;
  cout << "  arch flnm:" << archive_filename << "\n";
  return 0;
}
