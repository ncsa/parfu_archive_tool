# parfu_archive_tool
Parfu tar-like (but not compatible) MPI tool for creating or extracting directory tree archives.  In "create" mode it packages one (for now) directory into a single archive file.  In "extract" mode it takes an archive file and unpacks the original file and directory structure into a directory the user specifies.  

This release is an alpha-level testing release.  This is to allow people to see if parfu is a useful tool for them and for benchmarking and testing.  
Be aware that the command-lin interface is very likely to change drastically in future releases.  

The executable that the current release builds is called: parfu_all_test_001.  It has this strange name to underscore the fact that the interface is changing rapidly as we decide what it should be.  If interface changes that executable name will change.  That way there's no possibility of accidentally running a stale script with a new exeutable and get argument errors.  (Once the code gets to a feature-complete beta, the name of the executable will just be "parfu".  

If you run parfu with no arguments, it prints an argument list.  You can safely run parfu with no arguments without MPI infrastructure to check its arguments. 

Each rank of parfu transfers a different target file to the archive file (or piece of smaller file), and all the ranks are copying data simultaneously.  The assumption of the design of parfu is that the archive file is on a parallel file system, although there's no requirement for this. All file I/O is done via standard MPIIO commands.  If you use parfu witih archive files on non-parallel file systems, writing/reading the archive file will likely become your bottleneck.

Use examples: 
  To archive a directory:
    parfu_all_test_001 C target_path/directory_to_archive archive_path/archive_file.pfu
  To extract an archive file
    parfu_all_test_001 X extract_path/extract_directory archive_path/archive_file.pfu
    
To build parfu:
  The Makefile is set up to build on a Cray system with wrapped compilers.  When building on a Cray system, you must load PrgEnv-gnu (to invoke gnu compilers).  
  If you are building on another machine, you must edit the Makefile and set the cc= variable to point to the appropriate compiler, probably mpicc.  

Bugs are to be reported here: 
https://github.com/ncsa/parfu_archive_tool/issues
In this early alpha testing stage, please also submit reviews of using the tool (good or bad), suggestions or requests for features or particularly command-line flags.  

