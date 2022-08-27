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

#ifndef PARFU_MAIN_HH_
#define PARFU_MAIN_HH_

#include <cstdio>
#include <sys/types.h>
#include <sys/stat.h>
#include <mpi.h>
#include <dirent.h>
#include <string.h>
#include <unistd.h>

#include <iostream>
#include <string>
#include <vector>
#include <list>
#include <filesystem>

///////////////////////
//
// User-adjustable build-time parameters

// These numerical constants can be adjusted (within reason) at build
// time.  

// WARNING:  Setting this to a character that appears
// in filenames is utterly catastrophic for parfu.  We
// used "tab" or '\t' because no reasonable person would
// put a tab in a filename.  If for some reason tab doesn't
// work in your instance, substituting another non-printable
// ascii character might work, but test carefully and be cautious.  
#define PARFU_ENTRY_SEPARATOR_CHARACTER            '\t'

// WARNING:  Setting this to anything other than \n is very
// dangerous.  If you set it to a character that appears in
// filenames that would be catastrophic.  It's not clear that
// any other character would actually ever work.
#define PARFU_LINE_SEPARATOR_CHARACTER             '\n'

///////////////////////
//
// Users: Do not adjust values in the rest of the file

#define PARFU_OFFSET_INVALID        (-1L)

#define PARFU_DEFAULT_LAST_TIME_SPIDERED          (0)

#define PARFU_FILE_TYPE_INVALID      (-1)
#define PARFU_FILE_TYPE_REGULAR          (0)
#define PARFU_FILE_TYPE_DIRECTORY     (1)
#define PARFU_FILE_TYPE_SYMLINK       (2)
#define PARFU_FILE_TYPE_PAD           (22)


#define PARFU_FILE_SIZE_EMPTY              (0L)
// These legacy values should not be used in new .pfu files.  
// These are to catch values from old-format parfu files. 
#define PARFU_FILE_SIZE_LEGACY_INVALID           (-1L)  
#define PARFU_FILE_SIZE_LEGACY_SYMLINK           (-2L)
#define PARFU_FILE_SIZE_LEGACY_DIR               (-3L)
// These values to be used with new .pfu files
#define PARFU_FILE_SIZE_INVALID                 (-10L)
#define PARFU_FILE_SIZE_SYMLINK                 (-11L)
#define PARFU_FILE_SIZE_DIR                     (-12L)

#define PARFU_SPIDER_DIRECTORY_RETURN_ERROR             (-1L)

#include "tarentry.hh"
#include "parfu_file_system_classes.hh"
#include "parfu_2021_legacy.hh"

#define PARFU_WHAT_IS_PATH_REGFILE         0x001
#define PARFU_WHAT_IS_PATH_SYMLINK         0x002
#define PARFU_WHAT_IS_PATH_DIR             0x004
#define PARFU_WHAT_IS_PATH_DOES_NOT_EXIST  0x100
#define PARFU_WHAT_IS_PATH_IGNORED_TYPE    0x200
#define PARFU_WHAT_IS_PATH_ERROR           0x400

#define PARFU_FILE_TYPE_REGULAR_CHAR 'F'
#define PARFU_FILE_TYPE_DIRECTORY_CHAR 'D'
#define PARFU_FILE_TYPE_SYMLINK_CHAR 'L'
#define PARFU_FILE_TYPE_INVALID_CHAR 'X'
  
using namespace std;

// classes to define for new structure of parfu
// Parfu_file: (contains information about target file file on disk)
//       (also contains a list of one or more file slices)
//       parfu files have a type, a "real" file or a "pad" file.  
// Parfu_file_slice: specifies location of subfile within file, and also within container
// pad_file: subclass of subfile; specifies location of padding file within container
// dir: contains information about a directory
//      whether the directory has been spidered
//      if it has been spidered, a list of things within it
//      contains a list of subdirs
//      contains a list of subfiles
//      may be serializable to be passed across MPI
// container_file: contains all the informationa about a container file
//      file pointers, state, etc.  
//      contains list of objects who will reside in the container file 
//         when written to disk.  So a list of subfiles and/or container slices
//      actually probably contains a list of container slides, maybe instead of the above
// container_slice: nicely-sized chunk of container file. written to container file by 
//     one rank
// node_boss: contains a work order for the boss rank of a node to process
// node_worker: contains list that a thread on a node should process.  

#endif  // #ifndef PARFU_MAIN_HH_
