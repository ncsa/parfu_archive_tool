////////////////////////////////////////////////////////////////////////////////
// 
//  University of Illinois/NCSA Open Source License
//  http://otm.illinois.edu/disclose-protect/illinois-open-source-license
//  
//  Parfu is copyright © 2017, The Trustees of the University of Illinois. 
//  All rights reserved.
//  
//  Parfu was developed by:
//  The University of Illinois
//  The National Center For Supercomputing Applications (NCSA)
//  Blue Waters Science and Engineering Applications Support Team (SEAS)
//  Craig P Steffen <csteffen@ncsa.illinois.edu>
//  
//  https://github.com/ncsa/parfu_archive_tool
//  http://www.ncsa.illinois.edu/People/csteffen/parfu/
//  
//  For full licnse text see the LICENSE file provided with the source
//  distribution.
//  
////////////////////////////////////////////////////////////////////////////////

#include <parfu_primary.h>
#include <time.h>

// TODO: make tarenty usable for this.
// tar header
struct ustar_hdr {
  char name[100];      /*   0 */
  char mode[8];        /* 100 */
  char uid[8];         /* 108 */
  char gid[8];         /* 116 */
  char size[12];       /* 124 */
  char mtime[12];      /* 136 */
  char chksum[8];      /* 148 */
  char typeflag;       /* 156 */
  char linkname[100];  /* 157 */
  char magic[6];       /* 257 */
  char version[2];     /* 263 */
  char uname[32];      /* 265 */
  char gname[32];      /* 297 */
  char devmajor[8];    /* 329 */
  char devminor[8];    /* 337 */
  char prefix[155];    /* 345 */ /* not used in pax format */
  char pad[12];        /* 500 */
};

  // this is the catalog header; it's very important to leave the formatting alone
  // it must match the later re-writing

  // catalog header:
  // each of the initial header numbers is a 10-digit number delineated 
  // by an '\n' at the end

  // all numbers or quantities in the catalog head or catalog are written 
  // out in ASCII.  This makes it human readable, among other things.
  // \n is a newline (single byte)
  // \t is a tab character (single byte)
  // which characters are actually used as the value delimeters can
  // be set in the parfu header file with #define statements

  // There are two options here.  One is a catalog that's sent between ranks
  // as a buffer.  This is a "full" catalog.  The other is an "archive" 
  // catalog, which leaves out some information that's in the full catalog.

  // catalog header (full):
  // SSSSSSSSSS\n  total size of catalog, in bytes, including whole header
  // parfu_v04 \n  version string
  // full      \n  indicates full catalog
  // BBBBBBBBBB\n  bucket size (a whole number of buckets reserved for catalog
  // FFFFFFFFFF\n  total number of file entries in catalog
  
  // catalog header (archive):
  // SSSSSSSSSS\n  total size of catalog, in bytes, including whole header
  // parfu_v04 \n  version string
  // archive   \n  indicates is the catalog to be put in the archive file
  // BBBBBBBBBB\n  bucket size (a whole number of buckets reserved for catalog
  // FFFFFFFFFF\n  total number of file entries in catalog

  // then, depending on if it's a full catalog or an archive catalog, then FFFFFFFFFF as many lines, each one of this format:
  // catalog body line, full catalog:
  // RRR \t AAA \t T \t TGT \t SZ \t THSZ \t LOC_AR \t LOC_OR \t LOC_PD \t NAM_PD \t THSZ_PD \t SZ_PD \t N_FRG \t FP_IND \t RNK_BKT \n

  // catalog body line, archive catalog
  // AAA \t T \t TGT \t SZ \t THSZ \t LOC_AR \n

  // with the entries defined thusly:

  //   RRR path+filename, relative to CWD of running process
  //   AAA path+filename within the archive
  //   T  type of entry: dir, symlink, or regular file
  //   TGT: if symlink, the target, otherwise empty

  //   SZ is size of file in bytes
  //   THSZ is the size of the tar header in bytes
  //   LOC_AR is the beginning of file or fragment in archive file
  //   LOC_OR is location of fragment in orig file (zero for single file)

  //   LOC_PD pad file location in archive file of preceeding pad file
  //   NAM_PD filename name of preceeding pad file
  //   THSZ_PD tar header size of preceeding pad file
  //   SZ_PD size of preceeding pad file

  //   N_FRG file is divided into this many fragments
  //   FP_IND file pointer index; internal use only

  //   RNK_BKT rank bucket index of the file fragment

// my_list is the list to be written to the output buffer
// buffer_length is the returned total length of the buffer
// max_block_size_exponent is the max block size used for storage
//    the catalog is stored using this block size
// is_archive_catalog is a boolean.  If true, then JUST the information relevant
//   to the catalog is put to the buffer.  If false, then local information
//   only relevant to the running program is also included.
char *parfu_fragment_list_to_buffer(parfu_file_fragment_entry_list_t *my_list,
				    long int *buffer_length,
				    int bucket_size,
				    int is_archive_catalog){
  int i;

  int line_buffer_size=PARFU_LINE_BUFFER_DEFAULT;
  char *line_buffer=NULL;
  char *new_line_buffer=NULL;
  
  long int output_buffer_size;
  char *output_buffer=NULL;
  char *new_output_buffer=NULL;
  
  int printed_bytes_to_line_buffer;
  int printed_bytes_to_output_buffer;
  long int total_bytes_to_output_buffer;
  int tar_header_bytes = 0;
  int remaining_bytes;
  int printed_bytes;

  if(my_list==NULL){
    fprintf(stderr,"parfu_fragment_list_to_buffer:\n");
    fprintf(stderr,"received NULL input!\n");
    return NULL;
  }
  
  if((line_buffer=(char*)malloc(line_buffer_size))==NULL){
    fprintf(stderr,"parfu_fragment_list_to_buffer:\n");
    fprintf(stderr,"could not allocate %d bytes for line buffer!\n",line_buffer_size);
    return NULL;
  }
  output_buffer_size=(is_archive_catalog ? sizeof(struct ustar_hdr) : 0) +
                     PARFU_DEFAULT_SIZE_PER_LINE * my_list->n_entries_full;
  if(output_buffer_size>PARFU_MAXIMUM_BUFFER_SIZE){
    fprintf(stderr,"parfu_fragment_list_to_buffer:\n");
    fprintf(stderr," buffer size: %ld\n",output_buffer_size);
    fprintf(stderr," larger than max buffer size %d!\n",PARFU_MAXIMUM_BUFFER_SIZE);
    return NULL;
  }
  if((output_buffer=(char*)malloc(output_buffer_size))==NULL){
    fprintf(stderr,"parfu_fragment_list_to_buffer:\n");
    fprintf(stderr,"could not allocate %ld bytes for line buffer!\n",output_buffer_size);
    return NULL;
  }
  total_bytes_to_output_buffer=0;

  // write SSSSSSSSSS\n
  // writing 0 as a dummy value to hold the place; we'll write the final 
  // value after we've written all the entries
  printed_bytes_to_output_buffer=
    sprintf(output_buffer+total_bytes_to_output_buffer,"%010d%c",
	    0,PARFU_CATALOG_ENTRY_TERMINATOR);
  total_bytes_to_output_buffer += printed_bytes_to_output_buffer;

  // write version string
  printed_bytes_to_output_buffer=
    sprintf(output_buffer+total_bytes_to_output_buffer,
	    "parfu_v04 %c",
	    PARFU_CATALOG_ENTRY_TERMINATOR);  
  total_bytes_to_output_buffer += printed_bytes_to_output_buffer;
  
  // designate if this is a full catalog or a disk archive catalog
  if(is_archive_catalog){
    printed_bytes_to_output_buffer=
      sprintf(output_buffer+total_bytes_to_output_buffer,
	      "archive   %c",
	      PARFU_CATALOG_ENTRY_TERMINATOR);
  }
  else{
    printed_bytes_to_output_buffer=
      sprintf(output_buffer+total_bytes_to_output_buffer,
	      "full      %c",
	      PARFU_CATALOG_ENTRY_TERMINATOR);
  }
  total_bytes_to_output_buffer += printed_bytes_to_output_buffer;

  // write BBBBBBBBBB\n
  printed_bytes_to_output_buffer=
    sprintf(output_buffer+total_bytes_to_output_buffer,
	    "%010d%c",
	    bucket_size,
	    PARFU_CATALOG_ENTRY_TERMINATOR);
  total_bytes_to_output_buffer += printed_bytes_to_output_buffer;
  //  fprintf(stderr,"  ******* max block size exp = %d\n",max_block_size_exponent);
  
  // write FFFFFFFFFF\n
  printed_bytes_to_output_buffer=
    sprintf(output_buffer+total_bytes_to_output_buffer,"%010d%c",
	    my_list->n_entries_full,
	    PARFU_CATALOG_ENTRY_TERMINATOR);
  total_bytes_to_output_buffer += printed_bytes_to_output_buffer;
  
  //  fprintf(stderr,"debug(beginning): bytes in output buffer: %d\n",
  //	  total_bytes_to_output_buffer);

  // buffer header finished; now loop through and write the entries...
  for(i=0;i<my_list->n_entries_full;i++){
    // test print to see if it fits in the line buffer
    if(is_archive_catalog)
      printed_bytes_to_line_buffer=
	snprintf_to_archive_buffer(line_buffer,line_buffer_size,my_list,i);
    else
      printed_bytes_to_line_buffer=
	snprintf_to_full_buffer(line_buffer,line_buffer_size,my_list,i);

    // the +1 in this comparison is because the snprintf doesn't include the terminating
    // null in its character print count, but it needs to be within the line buffer    
    while( (printed_bytes_to_line_buffer+1) > line_buffer_size ){
      // if the line DIDN'T fit in the line buffer, we keep increasing the size until
      //   it fits.
      line_buffer_size *= 2;
      if((new_line_buffer=(char*)realloc(line_buffer,line_buffer_size))==NULL){
	fprintf(stderr,"dart_write_file_info_list_to_buffer:\n");
	fprintf(stderr," could not expand line buffer to %d bytes!\n",line_buffer_size);
	return NULL;
      }
      line_buffer=new_line_buffer;
      if(is_archive_catalog)
	printed_bytes_to_line_buffer=
	  snprintf_to_archive_buffer(line_buffer,line_buffer_size,my_list,i);
      else
	printed_bytes_to_line_buffer=
	  snprintf_to_full_buffer(line_buffer,line_buffer_size,my_list,i);
      
    } // while( (printed_bytes....
    // current space remaining in output buffer
    remaining_bytes = output_buffer_size - total_bytes_to_output_buffer;
    // does the line buffer fit in the output buffer?  If not, 
    // increase the size of the output buffer
    while( (printed_bytes_to_line_buffer+1) > remaining_bytes ){
      output_buffer_size *= 2;
      if(output_buffer_size > PARFU_MAXIMUM_BUFFER_SIZE){
	fprintf(stderr,"HELP!!! Total size of output buffer for file catalog\n");
	fprintf(stderr,"exceeded maximum possible size!\n");
	fprintf(stderr,"max = %d\n",PARFU_MAXIMUM_BUFFER_SIZE);
	fprintf(stderr,"Exiting.\n");
	return NULL;
      }
      if((new_output_buffer=(char*)realloc(output_buffer,output_buffer_size))==NULL){
	fprintf(stderr,"dart_write_file_info_list_to_buffer:\n");
	fprintf(stderr," could not expand output buffer to %ld bytes!\n",output_buffer_size);
	return NULL;
      }
      output_buffer=new_output_buffer;
      remaining_bytes = output_buffer_size - total_bytes_to_output_buffer;
    }
    printed_bytes_to_output_buffer=
      sprintf(output_buffer+total_bytes_to_output_buffer,
	      "%s",line_buffer);
    total_bytes_to_output_buffer += printed_bytes_to_output_buffer;
    //    fprintf(stderr,"debug: bytes in output buffer: %d\n",total_bytes_to_output_buffer);
  } // for(i=0;...
  
  // finally print OVER the initial value of the length with the real value
  printed_bytes=
    sprintf(output_buffer+tar_header_bytes,"%010ld",
	    total_bytes_to_output_buffer-tar_header_bytes);
  if(printed_bytes != 10){
    fprintf(stderr,"dart_write_file_info_list_to_buffer:\n");
    fprintf(stderr,"failed to write final length the beginning of buffer!\n");
    return NULL;
  }
  output_buffer[tar_header_bytes+10]=PARFU_CATALOG_ENTRY_TERMINATOR;

  // line_buffer is just used locally; free()-ing before we return
  *buffer_length=total_bytes_to_output_buffer;
  free(line_buffer);
  
  // we allocated output_buffer here, but it will be used elsewhere to be written 
  // to a file.  It should be free()-ed when it's done being used by the external 
  // function(s)
  return output_buffer;    
}

// structure tar header of catalog

void parfu_construct_tar_header_for_catalog(void *output_buffer, int total_bytes_to_output_buffer){

  {
    // leave this static so that unfilled bytes are zero and not random
    int tar_header_bytes;
    static struct ustar_hdr hdr = {
      ".parfu_catalog",    // name, duplicates don't matter
      "0000600",           // mode
      "0000000", "0000000",// uid and gid
      "size",              // filled in later
      "mtime",             // filled in later
      "        ",          // compute later, *must* be 8 spaces for proper checksum
      '0',                 // a regular file
      "",                  // linkname
      "ustar\0",           // magic
      "00",                // version
      "root",              // uname
      "root",              // gname
      "0000000", "0000000",// devmajor, devminor
      "",                  // prefix
    };
    snprintf(hdr.mtime, sizeof(hdr.mtime), "%0*lo",(int)(sizeof(hdr.mtime)-1), time(NULL));
    tar_header_bytes = (int)sizeof(hdr);
    memcpy(output_buffer, &hdr, tar_header_bytes);
    //    printed_bytes_to_output_buffer = tar_header_bytes;
    //    total_bytes_to_output_buffer += printed_bytes_to_output_buffer;
  }

  // record size in tar header and compute header checksum now that everything is done
  {
    unsigned long int checksum;
    size_t j;
    struct ustar_hdr *hdr = (struct ustar_hdr*)output_buffer;
    // this *technically* breaks if the header is larger than 8GB. 
    snprintf(hdr->size, sizeof(hdr->size), "%0*lo", (int)(sizeof(hdr->size)-1),
             (long unsigned int)(total_bytes_to_output_buffer));
    // this requires that the checksum field is set to space and all other
    // unused bytes are zero
    checksum = 0;
    for(j = 0 ; j < sizeof(*hdr) ; j++)
      checksum += ((unsigned char*)hdr)[j];
    snprintf(hdr->chksum, sizeof(hdr->chksum), "0%-lo", checksum);
  }
} 

void parfu_construct_tar_header_for_space(void *output_buffer, int total_bytes_to_output_buffer){

  {
    // leave this static so that unfilled bytes are zero and not random
    int tar_header_bytes;
    static struct ustar_hdr hdr = {
      ".parfu_spacer ",    // name, duplicates don't matter
      "0000600",           // mode
      "0000000", "0000000",// uid and gid
      "size",              // filled in later
      "mtime",             // filled in later
      "        ",          // compute later, *must* be 8 spaces for proper checksum
      '0',                 // a regular file
      "",                  // linkname
      "ustar\0",           // magic
      "00",                // version
      "root",              // uname
      "root",              // gname
      "0000000", "0000000",// devmajor, devminor
      "",                  // prefix
    };
    snprintf(hdr.mtime, sizeof(hdr.mtime), "%0*lo",(int)(sizeof(hdr.mtime)-1), time(NULL));
    tar_header_bytes = (int)sizeof(hdr);
    memcpy(output_buffer, &hdr, tar_header_bytes);
    //    printed_bytes_to_output_buffer = tar_header_bytes;
    //    total_bytes_to_output_buffer += printed_bytes_to_output_buffer;
  }

  // record size in tar header and compute header checksum now that everything is done
  {
    unsigned long int checksum;
    size_t j;
    struct ustar_hdr *hdr = (struct ustar_hdr*)output_buffer;
    // this *technically* breaks if the header is larger than 8GB. 
    snprintf(hdr->size, sizeof(hdr->size), "%0*lo", (int)(sizeof(hdr->size)-1),
             (long unsigned int)(total_bytes_to_output_buffer));
    // this requires that the checksum field is set to space and all other
    // unused bytes are zero
    checksum = 0;
    for(j = 0 ; j < sizeof(*hdr) ; j++)
      checksum += ((unsigned char*)hdr)[j];
    snprintf(hdr->chksum, sizeof(hdr->chksum), "0%-lo", checksum);
  }
} 

int snprintf_to_full_buffer(char *my_buf, size_t buf_size, 
			    parfu_file_fragment_entry_list_t *my_list,
			    int indx){
  return 
    snprintf(my_buf,buf_size,
	     "%s%c%s%c%c%c%s%c%ld%c%ld%c%ld%c%ld%c%ld%c%s%c%ld%c%ld%c%d%c%d%c%d%c",
	     my_list->list[indx].relative_filename,PARFU_CATALOG_VALUE_TERMINATOR,
	     my_list->list[indx].archive_filename,PARFU_CATALOG_VALUE_TERMINATOR,
	     parfu_return_type_char(my_list->list[indx].type),PARFU_CATALOG_VALUE_TERMINATOR,
	     my_list->list[indx].target,PARFU_CATALOG_VALUE_TERMINATOR,
	     my_list->list[indx].our_file_size,PARFU_CATALOG_VALUE_TERMINATOR,
	     my_list->list[indx].our_tar_header_size,PARFU_CATALOG_VALUE_TERMINATOR,
	     my_list->list[indx].location_in_archive_file,PARFU_CATALOG_VALUE_TERMINATOR,
	     my_list->list[indx].location_in_orig_file,PARFU_CATALOG_VALUE_TERMINATOR,
	     my_list->list[indx].pad_file_location_in_archive_file,PARFU_CATALOG_VALUE_TERMINATOR,
	     my_list->list[indx].pad_file_archive_filename,PARFU_CATALOG_VALUE_TERMINATOR,
	     my_list->list[indx].pad_file_tar_header_size,PARFU_CATALOG_VALUE_TERMINATOR,
	     my_list->list[indx].pad_file_size,PARFU_CATALOG_VALUE_TERMINATOR,
	     my_list->list[indx].file_contains_n_fragments,PARFU_CATALOG_VALUE_TERMINATOR,
	     my_list->list[indx].file_ptr_index,PARFU_CATALOG_VALUE_TERMINATOR,
	     my_list->list[indx].rank_bucket_index,PARFU_CATALOG_ENTRY_TERMINATOR);
  
}

int snprintf_to_archive_buffer(char *my_buf, size_t buf_size, 
			       parfu_file_fragment_entry_list_t *my_list,
			       int indx){
  return 
    snprintf(my_buf,buf_size,
	     "%s%c%c%c%s%c%ld%c%ld%c%ld%c",
	     my_list->list[indx].archive_filename,PARFU_CATALOG_VALUE_TERMINATOR,
	     parfu_return_type_char(my_list->list[indx].type),PARFU_CATALOG_VALUE_TERMINATOR,
	     my_list->list[indx].target,PARFU_CATALOG_VALUE_TERMINATOR,
	     my_list->list[indx].our_file_size,PARFU_CATALOG_VALUE_TERMINATOR,
	     my_list->list[indx].our_tar_header_size,PARFU_CATALOG_VALUE_TERMINATOR,
	     my_list->list[indx].location_in_archive_file,PARFU_CATALOG_ENTRY_TERMINATOR);
}


/* versions for old file format (July 2017)
int snprintf_to_full_buffer(char *my_buf, size_t buf_size, 
			    parfu_file_fragment_entry_list_t *my_list,
			    int indx){
  return 
    snprintf(my_buf,buf_size,
	     "%s%c%s%c%c%c%s%c%d%c%d%c%d%c%ld%c%ld%c%ld%c%ld%c%d%c",
	     my_list->list[indx].relative_filename,PARFU_CATALOG_VALUE_TERMINATOR,
	     my_list->list[indx].archive_filename,PARFU_CATALOG_VALUE_TERMINATOR,
	     parfu_return_type_char(my_list->list[indx].type),PARFU_CATALOG_VALUE_TERMINATOR,
	     my_list->list[indx].target,PARFU_CATALOG_VALUE_TERMINATOR,
	     my_list->list[indx].block_size_exponent,PARFU_CATALOG_VALUE_TERMINATOR,
	     my_list->list[indx].num_blocks_in_fragment,PARFU_CATALOG_VALUE_TERMINATOR,
	     my_list->list[indx].file_contains_n_fragments,PARFU_CATALOG_VALUE_TERMINATOR,
	     my_list->list[indx].fragment_offset,PARFU_CATALOG_VALUE_TERMINATOR,
	     my_list->list[indx].size,PARFU_CATALOG_VALUE_TERMINATOR,
	     my_list->list[indx].first_block,PARFU_CATALOG_VALUE_TERMINATOR,
	     my_list->list[indx].number_of_blocks,PARFU_CATALOG_VALUE_TERMINATOR,
	     my_list->list[indx].file_ptr_index,PARFU_CATALOG_ENTRY_TERMINATOR);
}

int snprintf_to_archive_buffer(char *my_buf, size_t buf_size, 
			       parfu_file_fragment_entry_list_t *my_list,
			       int indx){
  return 
    snprintf(my_buf,buf_size,
	     "%s%c%c%c%s%c%d%c%ld%c%ld%c%ld%c",
	     my_list->list[indx].archive_filename,PARFU_CATALOG_VALUE_TERMINATOR,
	     parfu_return_type_char(my_list->list[indx].type),PARFU_CATALOG_VALUE_TERMINATOR,
	     my_list->list[indx].target,PARFU_CATALOG_VALUE_TERMINATOR,
	     my_list->list[indx].block_size_exponent,PARFU_CATALOG_VALUE_TERMINATOR,
	     my_list->list[indx].size,PARFU_CATALOG_VALUE_TERMINATOR,
	     my_list->list[indx].first_block,PARFU_CATALOG_VALUE_TERMINATOR,
	     my_list->list[indx].number_of_blocks,PARFU_CATALOG_ENTRY_TERMINATOR);
}
*/

parfu_file_fragment_entry_list_t 
*parfu_buffer_to_file_fragment_list(char *in_buffer,
				    int *bucket_size,
				    int is_archive_catalog){
  //  char *line_buffer=NULL;
  //  char *next_value_terminator;
  char *current_read_position;
  char **end_value_ptr=NULL;
  //  int characters_to_get;
  int number_of_entries;
  parfu_file_fragment_entry_list_t *my_list=NULL;
  int i;
  int *increment=NULL;
  long int total_buffer_size;
  
  if(in_buffer==NULL){
    fprintf(stderr,"parfu_buffer_to_file_fragment_list:\n");
    fprintf(stderr," we were given a NULL input buffer!\n");
    return NULL;
  }    
  //  if((line_buffer=malloc(PARFU_LINE_BUFFER_DEFAULT))==NULL){
  //    fprintf(stderr,"parfu_buffer_to_file_fragment_list:\n");
  //  fprintf(stderr," cannot allocate line buffer!\n");
  // return NULL;
  //}
  if((end_value_ptr=(char**)malloc(sizeof(char*)))==NULL){
    fprintf(stderr,"parfu_buffer_to_file_fragment_list:\n");
    fprintf(stderr," could not allocate end_value_ptr pointer!\n");
    return NULL;
  }
  if((increment=(int*)malloc(sizeof(int)))==NULL){
    fprintf(stderr,"parfu_buffer_to_file_fragment_list:\n");
    fprintf(stderr," could not allocate return_value pointer!\n");
    return NULL;
  }
    
  //  if((next_value_terminator=strchr(in_buffer,PARFU_CATALOG_ENTRY_TERMINATOR))==NULL){
  //    fprintf(stderr,"parfu_buffer_to_file_fragment_list:\n");
  //    fprintf(stderr," no terminator character for buffer length! invalid buffer!\n");
  //    return NULL;
  //  }

  /*
  // skip tar header since we don't need it
  if(is_archive_catalog) {
    in_buffer += sizeof(struct ustar_hdr);
    current_read_position = in_buffer;
  }
  */

  // read SSSSSSSSSS
  total_buffer_size=strtol(in_buffer,end_value_ptr,10);
  if( *end_value_ptr == in_buffer ){
    fprintf(stderr,"parfu_buffer_to_file_fragment_list:\n");
    fprintf(stderr," buffer did not have a valid length header!\n");
    return NULL;
  }
  current_read_position = *end_value_ptr + 1;
  
  // check the version string to see if it matches
  if(strncmp(current_read_position,"parfu_v04 ",10)){
    fprintf(stderr,"parfu_buffer_to_file_fragment_list:\n");
    fprintf(stderr,"version mismatch!  Aborting!\n");
    return NULL;
  }
  // manually position for the next item in the header
  current_read_position += 11;
  
  // check to see of we have the right TYPE of header
  if(is_archive_catalog){
    if(strncmp(current_read_position,"archive   ",10)){
      fprintf(stderr,"parfu_buffer_to_file_fragment_list:\n");
      fprintf(stderr,"buffer is archive, we were told full!  Aborting!\n");
      return NULL;
    }
  }
  else{
    if(strncmp(current_read_position,"full      ",10)){
      fprintf(stderr,"parfu_buffer_to_file_fragment_list:\n");
      fprintf(stderr,"buffer is full, we were told archive!  Aborting!\n");
      return NULL;
    }
  }
  // manually position for the next item in the header
  current_read_position += 11;
  
  // get BBBBBBBBBB\n
  (*bucket_size)=strtol(current_read_position,end_value_ptr,10);
  if( *end_value_ptr == current_read_position ){
    fprintf(stderr,"parfu_buffer_to_file_fragment_list:\n");
    fprintf(stderr," buffer did not have a valid length header!\n");
    return NULL;
  }
  if( (*bucket_size) < PARFU_SMALLEST_ALLOWED_BUCKET_SIZE){
    fprintf(stderr,"parfu_buffer_to_file_fragment_list:\n");
    fprintf(stderr,"  bucket_size smaller than allowed!\n");
    fprintf(stderr,"  given bucket size: %d\n",(*bucket_size));
    fprintf(stderr,"  smallest allowed value: %d\n",
	    PARFU_SMALLEST_ALLOWED_BUCKET_SIZE);
    return NULL;
  }
  if( (*bucket_size) > PARFU_LARGEST_ALLOWED_BUCKET_SIZE){
    fprintf(stderr,"parfu_buffer_to_file_fragment_list:\n");
    fprintf(stderr,"  bucket size larger than allowed!\n");
    fprintf(stderr,"  bucket size from buffer: %d\n",(*bucket_size));
    fprintf(stderr,"  largest allowed value: %d\n",
	    PARFU_LARGEST_ALLOWED_BUCKET_SIZE);
    return NULL;
  }
  current_read_position = *end_value_ptr + 1;

  // read FFFFFFFFFF;
  number_of_entries=strtol(current_read_position,end_value_ptr,10);
  if( *end_value_ptr == current_read_position ){
    fprintf(stderr,"parfu_buffer_to_file_fragment_list:\n");
    fprintf(stderr," could not read number of entries!\n");
    return NULL;
  }
  if(number_of_entries<1){
    fprintf(stderr,"parfu_buffer_to_file_fragment_list:\n");
    fprintf(stderr," number of entries from archive file was less than one!\n");
    return NULL;
  }
  current_read_position = *end_value_ptr + 1;

  // we've retrieved the catalog header, now get the rest of the catalog
  if((my_list=create_parfu_file_fragment_entry_list(number_of_entries))==NULL){
    fprintf(stderr,"parfu_buffer_to_file_fragment_list:\n");
    fprintf(stderr," could not allocate my_list buffer for output!\n");
    return NULL;
  }
  my_list->n_entries_full=number_of_entries;
  
  // pass through every entry in the buffer, and put the information
  // into the corresponding entry in the newly-created list.
  for(i=0;i<number_of_entries;i++){
    // emergency check in case we've run off the end of the buffer
    if( (current_read_position - in_buffer) > total_buffer_size ){
      fprintf(stderr,"parfu_buffer_to_file_fragment_list:\n");
      fprintf(stderr,"  we\'ve run off the end of the buffer!!!!\n");
      return NULL;
    }

    //    fprintf(stderr,"parsing header: iteration %d\n",i);
    
    // get relative filename
    if( ! is_archive_catalog){
      if((my_list->list[i].relative_filename=
	  parfu_get_next_filename(current_read_position,
				  PARFU_CATALOG_VALUE_TERMINATOR,
				  increment))==NULL){
	fprintf(stderr,"parfu_buffer_to_file_fragment_list:\n");
	fprintf(stderr,"  could not get string from buffer!\n");
	return NULL;
      }
      current_read_position += (*increment);
    }
    else{
      my_list->list[i].relative_filename=NULL;
    }
    
    // get archive filename
    if((my_list->list[i].archive_filename=
	parfu_get_next_filename(current_read_position,
				PARFU_CATALOG_VALUE_TERMINATOR,
				increment))==NULL){
      fprintf(stderr,"parfu_buffer_to_file_fragment_list:\n");
      fprintf(stderr,"  could not get string from buffer!\n");
      return NULL;
    }
    current_read_position += (*increment);

    // get type of file
    switch((char)(*current_read_position)){
    case PARFU_FILE_TYPE_REGULAR_CHAR:
      my_list->list[i].type=PARFU_FILE_TYPE_REGULAR;
      break;
    case PARFU_FILE_TYPE_DIR_CHAR:
      my_list->list[i].type=PARFU_FILE_TYPE_DIR;
      break;
    case PARFU_FILE_TYPE_SYMLINK_CHAR:
      my_list->list[i].type=PARFU_FILE_TYPE_SYMLINK;
      break;
    default:
      fprintf(stderr,"parfu_buffer_to_file_fragment_list:\n");
      fprintf(stderr,"file %d type was invalid!!!\n",i);
      return NULL;
    }
    current_read_position += 2;  // + 1 for the type char, +1 for the separating character
    
    // get link target
    if((my_list->list[i].target=
	parfu_get_next_filename(current_read_position,
				PARFU_CATALOG_VALUE_TERMINATOR,
				increment))==NULL){
      fprintf(stderr,"parfu_buffer_to_file_fragment_list:\n");
      fprintf(stderr,"  could not get target string from buffer!\n");
      return NULL;
    }
    current_read_position += (*increment);

    // get numerical values

    my_list->list[i].our_file_size =
      strtol(current_read_position,end_value_ptr,10);
    if( (*end_value_ptr) == current_read_position ){
      fprintf(stderr,"parfu_buffer_to_file_fragment_list:\n");
      fprintf(stderr,"iteration %d could not find file size!\n",i);
      return NULL;
    }
    current_read_position = *end_value_ptr + 1;
    
    my_list->list[i].our_tar_header_size =
      strtol(current_read_position,end_value_ptr,10);
    if( (*end_value_ptr) == current_read_position ){
      fprintf(stderr,"parfu_buffer_to_file_fragment_list:\n");
      fprintf(stderr,"iteration %d could not find tar header size for this file!\n",i);
      return NULL;
    }
    current_read_position = *end_value_ptr + 1;
    
    my_list->list[i].location_in_archive_file =
      strtol(current_read_position,end_value_ptr,10);
    if( (*end_value_ptr) == current_read_position ){
      fprintf(stderr,"parfu_buffer_to_file_fragment_list:\n");
      fprintf(stderr,"iteration %d could not find location in archive file!\n",i);
      return NULL;
    }
    current_read_position = *end_value_ptr + 1;
    
    if(!is_archive_catalog){
      my_list->list[i].location_in_orig_file =
	strtol(current_read_position,end_value_ptr,10);
      if( (*end_value_ptr) == current_read_position ){
	fprintf(stderr,"parfu_buffer_to_file_fragment_list:\n");
	fprintf(stderr,"iteration %d could not find location in original file!\n",i);
	return NULL;
      }
      current_read_position = *end_value_ptr + 1;
      
      my_list->list[i].pad_file_location_in_archive_file =
	strtol(current_read_position,end_value_ptr,10);
      if( (*end_value_ptr) == current_read_position ){
	fprintf(stderr,"parfu_buffer_to_file_fragment_list:\n");
	fprintf(stderr,"iteration %d could not find pad_file_location_in_archive_file!\n",i);
	return NULL;
      }
      current_read_position = *end_value_ptr + 1;

      if((my_list->list[i].pad_file_archive_filename=
	  parfu_get_next_filename(current_read_position,
				  PARFU_CATALOG_VALUE_TERMINATOR,
				  increment))==NULL){
	fprintf(stderr,"parfu_buffer_to_file_fragment_list:\n");
	fprintf(stderr,"  could not get pad_file_name string from buffer!\n");
	return NULL;
      }
      current_read_position += (*increment);
      
      my_list->list[i].pad_file_tar_header_size =
	strtol(current_read_position,end_value_ptr,10);
      if( (*end_value_ptr) == current_read_position ){
	fprintf(stderr,"parfu_buffer_to_file_fragment_list:\n");
	fprintf(stderr,"iteration %d could not find pad_file_tar_header_size!\n",i);
	return NULL;
      }
      current_read_position = *end_value_ptr + 1;
      
      my_list->list[i].pad_file_size =
	strtol(current_read_position,end_value_ptr,10);
      if( (*end_value_ptr) == current_read_position ){
	fprintf(stderr,"parfu_buffer_to_file_fragment_list:\n");
	fprintf(stderr,"iteration %d could not find pad_file_size!\n",i);
	return NULL;
      }
      current_read_position = *end_value_ptr + 1;
      
      my_list->list[i].file_contains_n_fragments =
	strtol(current_read_position,end_value_ptr,10);
      if( (*end_value_ptr) == current_read_position ){
	fprintf(stderr,"parfu_buffer_to_file_fragment_list:\n");
	fprintf(stderr,"iteration %d could not find file_contains_n_fragments!\n",i);
	return NULL;
      }
      current_read_position = *end_value_ptr + 1;
      
      my_list->list[i].file_ptr_index =
	strtol(current_read_position,end_value_ptr,10);
      if( (*end_value_ptr) == current_read_position ){
	fprintf(stderr,"parfu_buffer_to_file_fragment_list:\n");
	fprintf(stderr,"iteration %d could not find file_ptr_index!\n",i);
	return NULL;
      }
      current_read_position = *end_value_ptr + 1;
    
      my_list->list[i].rank_bucket_index =
	strtol(current_read_position,end_value_ptr,10);
      if( (*end_value_ptr) == current_read_position ){
	fprintf(stderr,"parfu_buffer_to_file_fragment_list:\n");
	fprintf(stderr,"iteration %d could not find rank_bucket_index!\n",i);
	return NULL;
      }
      current_read_position = *end_value_ptr + 1;
    } // if(!archive_file)
  } // for(i=0;... 
  
  free(end_value_ptr); 
  end_value_ptr=NULL;
  free(increment);
  increment=NULL;
  
  return my_list;
}

char *parfu_get_next_filename(char *in_buf, 
			      char end_character,
			      int *increment_pointer){
  char *next_value_terminator=NULL;
  int characters_to_get;
  char *output_buf=NULL;

  next_value_terminator=strchr(in_buf,end_character);
  if(next_value_terminator==NULL){
    fprintf(stderr,"parfu_get_next_filename:\n");
    fprintf(stderr,"could not find string end!!\n");
    return NULL;
  }
  characters_to_get = (next_value_terminator - in_buf);
  // the +1 is to leave space for the null terminator
  // my_list->list[i].filename=malloc(characters_to_get+1);
  if((output_buf=(char*)malloc(characters_to_get+1))==NULL){
    fprintf(stderr,"parfu_get_next_filename:\n");
    fprintf(stderr,"coulnd\'t allocate buffer!\n");
    return NULL;
  }
  memcpy(output_buf,in_buf,characters_to_get);
  output_buf[characters_to_get]='\0';
  //  current_read_position = next_value_terminator + 1;
  *increment_pointer = (next_value_terminator - in_buf) + 1;

  return output_buf;
}

void parfu_skip_over_catalog_tar_header(FILE *fp){
  // skip over tar header for catalog
  fseek(fp, sizeof(struct ustar_hdr), SEEK_SET);
}

parfu_file_fragment_entry_list_t 
*parfu_ffel_from_file(char *archive_filename,
		      int *max_block_size_exponent,
		      int *catalog_buffer_length, 
		      int skip_tar_header){
  FILE *infile=NULL;
  int size_of_length_string=10;
  char *input_buffer=NULL;
  char **end_of_read_ptr=NULL;
  int my_catalog_length;
  parfu_file_fragment_entry_list_t *outlist=NULL;
  int items_read;

  if((input_buffer=(char*)malloc(size_of_length_string+1))==NULL){
    fprintf(stderr,"parfu_catalog_buffer_from_file: \n");
    fprintf(stderr,"  cannot allocate length buffer!\n");
    return NULL;
  }
  if((end_of_read_ptr=(char**)malloc(sizeof(char*)))==NULL){
    fprintf(stderr,"parfu_catalog_buffer_from_file: \n");
    fprintf(stderr,"  cannot allocate *end_of_read_ptr pointer!\n");
    return NULL;
  }
  (*end_of_read_ptr)=NULL;
  if((infile=fopen(archive_filename,"r"))==NULL){
    fprintf(stderr,"parfu_catalog_buffer_from_file: \n");
    fprintf(stderr,"  could not open >%s< for reading!\n,archive_filename",
	    archive_filename);
    return NULL;
  }

  if(skip_tar_header)
    parfu_skip_over_catalog_tar_header(infile);

  // the first 11 bytes contains the length of the catalog
  // first read that
  items_read = fread(input_buffer,sizeof(char),size_of_length_string+1,
		     infile);
  fprintf(stderr," extracting catalog: was able to read inital length item.\n");
  if( items_read < (size_of_length_string+1) ){
    fprintf(stderr,"parfu_catalog_buffer_from_file: \n");
    fprintf(stderr,"archive file too short for catalog length entry!!\n");
    return NULL;
  }
  my_catalog_length = strtol(input_buffer,end_of_read_ptr,10)+sizeof(struct ustar_hdr);
  if( (*end_of_read_ptr) == input_buffer ){
    fprintf(stderr,"parfu_catalog_buffer_from_file: \n");
    fprintf(stderr," could not make sense out of catalog size entry!!\n");
    return NULL;
  }
  fprintf(stderr," extracting catalog: catalog length: %d.\n",my_catalog_length);
  // we now have the length of the whole catalog
  // allocate a buffer for that size
  free(input_buffer);
  input_buffer=NULL;
  if((input_buffer=(char*)malloc(my_catalog_length))==NULL){
    fprintf(stderr,"parfu_catalog_buffer_from_file: \n");
    fprintf(stderr,"  could not allocate buffer for whole catalog!!\n");
    fprintf(stderr,"  catalog size: %d\n",my_catalog_length);
    return NULL;
  }
  // back the pointer up to the beginning of the file
  // we need to read in the tar header as well since
  // parfu_buffer_to_file_fragment_list expects it
  rewind(infile);
  // read the whole buffer in one operation
  fprintf(stderr," extracting catalog: about to read catalog.\n");
  items_read=fread(input_buffer,sizeof(char),my_catalog_length,
		   infile);
  if(items_read != my_catalog_length){
    fprintf(stderr,"parfu_catalog_buffer_from_file: \n");
    fprintf(stderr,"  could not read archive catalog from: %s !\n",
	    archive_filename);
    return NULL;
  }
  fprintf(stderr," extracting catalog: got buffer; now extract information.\n");
  if((outlist=parfu_buffer_to_file_fragment_list(input_buffer,
						 max_block_size_exponent,
						 1))
     ==NULL){
    fprintf(stderr,"parfu_catalog_buffer_from_file: \n");
    fprintf(stderr,"  parfu_buffer_to_file_fragment_list returned NULL!\n");
    return NULL;
  }
  free(input_buffer);
  fclose(infile);
  free(end_of_read_ptr);
  end_of_read_ptr=NULL;
  
  (*catalog_buffer_length) = my_catalog_length;
  return outlist;
}

int parfu_ffel_fill_rel_filenames(parfu_file_fragment_entry_list_t *my_list,
				  char *leading_path){
  char *rel_filename_buf=NULL;
  int rel_filename_length;
  int leading_path_length;
  int needs_separating_slash;
  int i;
  int archive_filename_length;

  if(my_list==NULL){
    fprintf(stderr,"parfu_ffel_fill_rel_filenames:\n");
    fprintf(stderr,"  my_list is NULL!\n");
    return -1;
  }
  leading_path_length = strlen(leading_path);
  if( (leading_path[leading_path_length-1]) == '/' ){
    needs_separating_slash=0;
  }
  else{
    needs_separating_slash=1;
  }
  for(i=0;i<my_list->n_entries_full;i++){
    if((my_list->list[i].archive_filename)==NULL){
      fprintf(stderr,"parfu_ffel_fill_rel_filenames:\n");
      fprintf(stderr,"  archive_filename for item %d == NULL!!\n",i);
      return -1;
    }
    archive_filename_length=strlen(my_list->list[i].archive_filename);
    if(needs_separating_slash){
      rel_filename_length = 
	archive_filename_length + leading_path_length + 
	2; // 2 extra bytes: 1 for '/' and the other for trailing '\0'
      if((rel_filename_buf=(char*)malloc(rel_filename_length))==NULL){
	fprintf(stderr,"parfu_ffel_fill_rel_filenames:\n");
	fprintf(stderr," could not allocate %d bytes for rel filname buffer!\n",
		rel_filename_length);
	fprintf(stderr," fragment %d\n",i);
	return -1;
      }
      sprintf(rel_filename_buf,"%s/%s",
	      leading_path,my_list->list[i].archive_filename);
    }
    else{
      rel_filename_length = 
	archive_filename_length + leading_path_length + 
	1; // 1 extra byte: 1 for trailing '\0' ('/' already in leading_path
      if((rel_filename_buf=(char*)malloc(rel_filename_length))==NULL){
	fprintf(stderr,"parfu_ffel_fill_rel_filenames:\n");
	fprintf(stderr," could not allocate %d bytes for rel filname buffer!\n",
		rel_filename_length);
	fprintf(stderr," fragment %d\n",i);
	return -1;
      }
      sprintf(rel_filename_buf,"%s%s",
	      leading_path,my_list->list[i].archive_filename);
    }
    my_list->list[i].relative_filename = rel_filename_buf;
  } // for(i=0;i<
  
  return 0;
}
