# parfu_archive_tool
Parfu tar-like (but not compatible) MPI tool for creating or extracting directory tree archives.

This release is an alpha-level testing release.  This is to allow people to see if parfu is a useful tool for them and for benchmarking and testing.  
Be aware that the command-lin interface is very likely to change drastically in future releases.  

The executable that the current release builds is called: parfu_all_test_001 

Use examples: 
  To archive a directory:
    parfu_all_test_001 C target_path/directory_to_archive archive_path/archive_file.pfu
  To extract an archive file
    parfu_all_test_001 X extract_path/extract_directory archive_path/archive_file.pfu
    
To build parfu:
  The Makefile is set up to build on a Cray system with wrapped compilers.  When building on a Cray system, you must load PrgEnv-gnu (to invoke gnu compilers).  
  If you are building on another machine, you must edit the Makefile and set the cc= variable to point to the appropriate compiler, probably mpicc.  
