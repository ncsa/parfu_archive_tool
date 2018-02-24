# parfu\_archive\_tool
Parfu tar-like MPI tool for creating or extracting directory tree archives.  In "create" mode it packages one (for now) directory into a single archive file.  In "extract" mode it takes an archive file and unpacks the original file and directory structure into a directory the user specifies.  The current alpha version of parfu produces archive files that are tar-compatible.  

Note: as of now, late February 2018, the master branch is the current development branch of parfu 0.5.1.  It is currently an alpha-release version.  It produces files that are tar-compatible.  It does not currently have extraction or table-of-contents capabilities.  
This alpha release is for performance testing and to allow people to see if parfu is a useful tool for them
Be aware that the command-lin interface is very likely to change drastically in future releases.  

# To build parfu:
  The Makefile is set up to build on a Cray system with wrapped compilers, and also to build on XSEDE systems.  To build on a Cray system, make sure PrgEnv-gnu is loaded, then run: 
  make ACC="cray"
  or to build on TACC Wrangler, run: 
  make ACC="Wrangler"
  Setting the ACC variable sets the compilers to their proper values.  If neither of these options work, you'll have to edit the Makefile and set the variables MY_CC and MY_CXX to your local MPI C and C++ compilers, respectively.  

The executable that the current release builds is called: parfu\_0\_5\_1.  It has this strange name to underscore the fact that the interface is changing rapidly as we decide what it should be.  If interface changes that executable name will change.  That way there's no possibility of accidentally running a stale script with a new exeutable and get argument errors.  (Once the code gets to a feature-complete beta, the name of the executable will just be "parfu".  

If you run parfu with no arguments, it prints an argument list.  You can safely run parfu with no arguments without MPI infrastructure to check its arguments. 

# Command-line examples: 
  To archive a directory:
  
    parfu\_0\_5\_1 C archive\_path/archive\_file.pfu target_path/directory_to_archive 
    
  (extraction is not availble yet in the 0.5.1 alpha)
    
Bugs are to be reported here: 
https://github.com/ncsa/parfu_archive_tool/issues
In this early alpha testing stage, please also submit reviews of using the tool (good or bad), suggestions or requests for features or particularly command-line flags.  The plan will be to make the command line interface mirror that of unix tar as closely as practical.  

# How Parfu Works

Each rank of parfu transfers a different target file to the archive file (or piece of smaller file), and all the ranks are copying data simultaneously.  The assumption of the design of parfu is that the archive file is on a parallel file system, although there's no requirement for this. All file I/O is done via standard MPIIO commands.  If you use parfu with archive files on non-parallel file systems, writing/reading the archive file will likely become your bottleneck.

Parfu archives and restores all directories in the target directory (including empty ones).  It archives and restores regular files, including zero-byte files.  Symbolic links are preserved and restored as symbolic links (Later versions will have a command-line option to follow links instead).  All pathnames are stored relative to the "create" target directory.  When extracting, all files are extracted relative to the target "extract" directory.  Leading slashes in absolute pathnames are removed.  If you wish to extract to an absolute position just specify "/" as the target extract directory.
