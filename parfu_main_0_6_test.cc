////////////////////////////////////////////////////////////////////////////////
// 
//  University of Illinois/NCSA Open Source License
//  http://otm.illinois.edu/disclose-protect/illinois-open-source-license
//  
//  Parfu is copyright © 2017-2020, 
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

int main(int argc, char *argv[]){
  Parfu_directory *test_dir;
  string *my_string;
  Parfu_target_collection *my_target_collec;
  
  cout << "parfu test build\n";
  if(argc > 1){
    cout << "We will scan directory:";
    cout << argv[1];
    cout << "\n";
  }
  else{
    cout << "You must input a directory to scan!\n";
    return 1;
  }
  
  my_string = new string(argv[1]);
  test_dir = new Parfu_directory(*my_string);
  
  //  cout << "Have we spidered directory? " << test_dir->is_directory_spidered() << "\n";
  test_dir->spider_directory();
  //  cout << "Have we spidered directory? " << test_dir->is_directory_spidered() << "\n";

  //  cout << "First build the target collection\n";
  my_target_collec = new Parfu_target_collection(test_dir);
  cout << "Target collection built.  Now dump it, unsorted.\n";
  my_target_collec->dump();
  cout << "now sort the files...\n";
  my_target_collec->order_files();
  cout << "and dump it again.\n";
  my_target_collec->dump();
  cout << "about to set offsets.\n";
  my_target_collec->set_offsets(0L);
  cout << "about to dump offsets\n";
  my_target_collec->dump_offsets();
  cout << "all done.\n";
  
  return 0;
}
