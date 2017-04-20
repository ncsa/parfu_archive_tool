################################################################################
## 
##  University of Illinois/NCSA Open Source License
##  http://otm.illinois.edu/disclose-protect/illinois-open-source-license
##  
##  Parfu is copyright © 2017, The Trustees of the University of Illinois. 
##  All rights reserved.
##  
##  Parfu was developed by:
##  The University of Illinois
##  The National Center For Supercomputing Applications (NCSA)
##  Blue Waters Science and Engineering Applications Support Team (SEAS)
##  Craig P Steffen <csteffen@ncsa.illinois.edu>
##  
##  https://github.com/ncsa/parfu_archive_tool
##  http://www.ncsa.illinois.edu/People/csteffen/parfu/
##  
##  For full licnse text see the LICENSE file provided with the source
##  distribution.
##  
################################################################################

###### site configuration options
# generally speaking, you may have to tweak stuff in this section to get the 
# code to build on your machine
# we call getgrgid which requires a dynamic executable and SEGFAULTs for a
# statically linked one
export CRAYPE_LINK_TYPE=dynamic
export XTPE_LINK_TYPE=dynamic

# set to MPI compiler
# on Cray, this will be CC=cc
# on other systems, this might be CC=mpicc
CC=cc
CXX=CC

# this is assuming the C compiler is a relatively recent gcc variant
# CFLAGS := -g -I. -Wall -Wmissing-prototypes -Wstrict-prototypes 
# CFLAGS := -g -I. -Wall -Wmissing-prototypes -Wstrict-prototypes -O3
CFLAGS := -g -I. -Wall -O3

# The TARGETS variable sets what gets built. 

# By default, this Makefile builds the basic proof-of-concept test code. 
TARGETS := parfu_all_test_001 parfu_bench_test_002

# Using TARGETS line would also build two utilty test codes, which are
# probably only interesting for historical or internal testing reasons.
# TARGETS := parfu_file_util_test parfu_create_test_1 parfu_all_test_001

#
# end of site configuration options
######

###### 
# If you need to touch anything below here to get your code to build, please report
# it as a bug.  

# header and utility function definitions
PARFU_HEADER_FILES := parfu_primary.h tarentry.hh

PARFU_OBJECT_FILES := parfu_file_list_utils.o parfu_buffer_utils.o parfu_data_transfer.o parfu_behavior_control.o tarentry.o

default: ${TARGETS}

# executables

parfu: ${PARFU_OBJECT_FILES} ${PARFU_HEADER_FILES}
	echo "done!"

parfu_file_util_test: parfu_file_util_test.o ${PARFU_OBJECT_FILES} ${PARFU_HEADER_FILES}
	${CXX} -o $@ ${CFLAGS} parfu_file_util_test.o ${PARFU_OBJECT_FILES}

parfu_create_test_1: parfu_create_test_1.o ${PARFU_OBJECT_FILES} ${PARFU_HEADER_FILES}
	${CXX} -o $@ ${CFLAGS} parfu_create_test_1.o ${PARFU_OBJECT_FILES}

parfu_all_test_001: parfu_all_test_001.o ${PARFU_OBJECT_FILES} ${PARFU_HEADER_FILES}
	${CXX} -o $@ ${CFLAGS} parfu_all_test_001.o ${PARFU_OBJECT_FILES}

parfu_bench_test_002: parfu_bench_test_002.o ${PARFU_OBJECT_FILES} ${PARFU_HEADER_FILES}
	${CXX} -o $@ ${CFLAGS} parfu_bench_test_002.o ${PARFU_OBJECT_FILES}

# utility targets

clean:
	rm -f ${TARGETS} *.o

%.o: %.c ${PARFU_HEADER_FILES}
	${CC} ${CFLAGS} -c $<

%.o: %.cc ${PARFU_HEADER_FILES}
	${CXX} ${CXXFLAGS} -c $<

again: clean default
