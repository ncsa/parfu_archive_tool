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

//#define BUCKET_SIZE         (200000000)
// must be a multiple of 512??
#define BUCKET_SIZE         (499200)

int main(int argc, char *argv[]){
  Parfu_directory *test_dir;
  string base_path;
  Parfu_target_collection *my_target_collec;
  vector <string> *transfer_orders=nullptr;
  vector <string> *target_paths=nullptr;
  //  Parfu_rank_order_set *my_orders=nullptr;
  int my_rank,total_ranks;
  string initial_order;
  int mpi_return_val;
  //  int *length_buffer=nullptr;
  unsigned max_orders_per_bucket;
  long unsigned bucket_size=BUCKET_SIZE;
  string *archive_file_name_from_command_line;
  int *archive_file_multiplier;
  
  string archive_file_name;
  
  MPI_File *file_handle=nullptr;
  //  MPI_Info file_info;
  
  //  char *word_buffer=nullptr;
  //  void *order_buffer=nullptr;
  
  //  length_buffer = new int;
  
  
  // Do argument parsing and "exit with the usage() or help() message stuff
  // above this point, so that if run in a scalar context (outside of
  // an MPI launch infrastructure) all that stuff gets done properly
  // and the code exist cleanly.  Users appreciate that.  
  
  MPI_Init(NULL,NULL);
  MPI_Comm_size(MPI_COMM_WORLD,&total_ranks);
  MPI_Comm_rank(MPI_COMM_WORLD,&my_rank);
  
  cout << "I am rank " << my_rank << " out of a total of " << total_ranks << "\n";

  if(my_rank == 0){

    archive_file_name_from_command_line = new string;
    archive_file_multiplier = new int;
    *archive_file_multiplier = 1;
    
    // parse command line
    //    cerr << "checking: max orders per bucket initialized to: ";
    //    cerr << max_orders_per_bucket << "\n";
    if((target_paths =
	parfu_parse_args(argc,argv,&bucket_size,&max_orders_per_bucket,
			 archive_file_name_from_command_line,
			 archive_file_multiplier))
       == nullptr){
      cerr << "Error from command line parsing!  Exiting.\n";
      parfu_broadcast_order(string("X"),string("abort"));
      exit(1);
    }
    //    cerr << "checking: max orders per bucket after argument parsing: ";
    //    cerr << max_orders_per_bucket << "\n";

    cerr << "We got " << target_paths->size() << " target paths from command line.\n";
    for(unsigned i=0;i<target_paths->size();i++){
      cerr << "path " << i << " :" << target_paths->at(i) << "\n";
    }
    
    //    cout << "parfu test build\n";
    if(argc > 1){
      cout << "We will scan directory:";
      cout << argv[1];
      cout << "\n";
    }
    else{
      cout << "You must input a directory to scan!\n";
      return 1;
    }

    if(argc>2){
      archive_file_name = string(argv[2]);
      cout << "archive file: " << archive_file_name << "\n";
    }
    else{
      cerr << "ERROR!  No archive file specified!  \n";
      exit(1);
    }
    
    base_path = string(argv[1]);
    test_dir = new Parfu_directory(base_path);
    
    //  cout << "Have we spidered directory? " << test_dir->is_directory_spidered() << "\n";
    test_dir->spider_directory();
    //  cout << "Have we spidered directory? " << test_dir->is_directory_spidered() << "\n";
    
    //  cout << "First build the target collection\n";
    my_target_collec = new Parfu_target_collection(test_dir);
    cout << "Target collection built.  ";
    //    cout << "Now dump it, unsorted.\n";
    //    my_target_collec->dump();
    cout << "now sort the files...\n";
    my_target_collec->order_files();
    //    cout << "and dump it again.\n";
    //    my_target_collec->dump();
    cout << "set offsets.\n";
    my_target_collec->set_offsets();
    //    cout << "dump offsets\n";
    //    my_target_collec->dump_offsets();
    cout << "generate rank orders\n";
    transfer_orders = my_target_collec->create_transfer_orders(0,BUCKET_SIZE,max_orders_per_bucket);
    //    cout << "there are " << transfer_orders->size() << " orders.\n";
    
    //  cout << "\n\n\nFirst order:\n\n";
    //  cout << transfer_orders->front();
    //  cout << "\n\n end first order.\n\n";
    
    
    //    cout << "transfer orders:\n\n";
    //    for(unsigned int i=0;i<transfer_orders->size();i++){
    //      cout << "transfer order " << i << "\n";
    //      cout << transfer_orders->at(i);
    //      cout << "\n";
    //}
    
    // send initial broadcast orders


    /*
    initial_order = string("");
    
    initial_order.append("A");
    initial_order.append(archive_file_name);

    *length_buffer = initial_order.size()+1;
    //    mpi_return_val = MPI_Send(length_buffer,1,MPI_INT,MPI_Bcast,0,MPI_COMM_WORLD);
    cout << "about to send initial order length\n";
    mpi_return_val = MPI_Bcast(length_buffer,1,MPI_INT,0,MPI_COMM_WORLD);
    cout << "Sent order length.  Now send order itself.\n";
    mpi_return_val =
      MPI_Bcast(((void *)(initial_order.data())),initial_order.size()+1,MPI_CHAR,0,MPI_COMM_WORLD);
    cout << "set up buffer for collective open\n";

    word_buffer = (char*)malloc(archive_file_name.size()+1);
    memcpy(word_buffer,base_path.c_str(),base_path.size()+1);
    file_handle = (MPI_File*)malloc(sizeof(MPI_File));
    
    */

    
    cout << "Now we try collective file open.\n";

    parfu_broadcast_order(string("A"),
			  archive_file_name);
    
    //    mpi_return_val =
    //      MPI_File_open(MPI_COMM_WORLD,word_buffer,
    //    		    MPI_MODE_WRONLY|MPI_MODE_CREATE,
    //		    MPI_INFO_NULL,file_handle);
    file_handle = new MPI_File;

    //    MPI_Barrier(MPI_COMM_WORLD);
    if((mpi_return_val =
      MPI_File_open(MPI_COMM_WORLD,archive_file_name.c_str(),
    		    MPI_MODE_WRONLY|MPI_MODE_CREATE|MPI_MODE_EXCL,
		    MPI_INFO_NULL,file_handle)) != MPI_SUCCESS){
      cerr << "\n\nmain MPI_File_open returned " << mpi_return_val << "!\n";
      cerr << "WARNING!  Attempted to open file:>" << archive_file_name << "<\n";
      cerr << "but it failed.  This is likely because the file already exists,\n";
      cerr << "or the directory doesn't exist or you don't have permission\n";
      cerr << "to write there.  Exiting program.\n";
      parfu_broadcast_order(string("X"),
			    string("bye"));
      MPI_Finalize();
      exit(1);
    }

    cout << "Successfully opened archive file >" << archive_file_name << "< for writing.\n";
		    
    // Now send out a set of orders.
    cout << "We have " << transfer_orders->size();
    cout << " transfer orders available; return val=" << mpi_return_val <<".\n";

    parfu_broadcast_order(string("N"),
			  string("individual"));
    // we're now in individual mode.
    // all commands must be sent to each rank individually including the shutdown
    // until we've either sent each of them a shutdown, or sent each of them a command

    // to go back to broadcast mode.

    for(int i=1; i<total_ranks; i++){
      parfu_send_order_to_rank(i,0,string("P"),base_path);
    }
    
    /*
    for(int i=1; i<total_ranks; i++){
      parfu_send_order_to_rank(i,0,string("C"),transfer_orders->at(i-1));
    }
    */
    cout << "About to call push_out_all_orders\n";
    push_out_all_orders(transfer_orders,total_ranks);
    cout << "push_out_all_orders has returned.\n";
    
    
    //    cerr << "\ndump order zero: \n\n";
    //    cerr << transfer_orders->at(0);
    //    cerr << "\n\nend order zero\n\n";
    
    for(int i=1; i<total_ranks; i++){
      parfu_send_order_to_rank(i,0,string("X"),string("shutdown"));
    }
    cerr << "sent shutdown orders; now we're done.\n";
    
    // This is the shutdown, but only if we're in broadcast mode.  
    //    parfu_broadcast_order(string("X"),
    //			  string("bye"));
    
    
  } // if(my_rank == 0)
  else{
    parfu_worker_node(my_rank,total_ranks,BUCKET_SIZE);
  }

  cout << "rank " << my_rank << " done, about to call MPI_Finalize()\n";
  MPI_Finalize();
  return 0;
}

