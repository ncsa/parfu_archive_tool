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

#ifndef PARFU_WORKER_NODE_HH_
#define PARFU_WORKER_NODE_HH_

int parfu_worker_node(int my_rank, int total_ranks,
		      unsigned long bucket_size);

// these functions assume that it's rank 0 doing the broadcasting,
// or rank 0 doing the individual sending.  These functions are from the
// point of view of rank zero sending out orders.  
int parfu_send_order_to_rank(int rank,
			     int tag,
			     string instruction,
			     string message);
int parfu_broadcast_order(string instruction,
			  string message);



#endif
