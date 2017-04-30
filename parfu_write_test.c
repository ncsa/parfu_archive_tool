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
//  Roland Haas <rhaas@illinois.edu>
//  Craig P Steffen <csteffen@ncsa.illinois.edu>
//  
//  https://github.com/ncsa/parfu_archive_tool
//  http://www.ncsa.illinois.edu/People/csteffen/parfu/
//  
//  For full licnse text see the LICENSE file provided with the source
//  distribution.
//  
////////////////////////////////////////////////////////////////////////////////

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <mpi.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>

#include <time.h>
#include <ctype.h>

int main(int argc, char *argv[]){
  int i; 
  int n_ranks;
  int my_rank;
  int data_bytes;
  int buffer_size;
  int n_iterations;
  char *filename_to_write=NULL;
  int *rank_buffer=NULL;

  MPI_Info my_Info=MPI_INFO_NULL;
  MPI_Status my_MPI_Status;
  MPI_File *archive_file_MPI=NULL;
  int file_result;

  long int total_data_transfer;
  int total_elapsed_time_s;
  float total_transferred_gb;
  float transfer_speed_gb_s;

  long int stage_archive_file_offset;


  time_t time_before,time_after;


  //  fprintf(stderr,"parfu_write_test beginning\n");
  
  if(argc < 5){
    fprintf(stderr,"usage: \n");
    fprintf(stderr," parfu_write_test <dat_bytes> <buf_bytes> <file_to_write> <n_iterations>\n");
    MPI_Finalize();
    return -1;
  }
  data_bytes=atoi(argv[1]);
  buffer_size=atoi(argv[2]);
  filename_to_write=argv[3];
  n_iterations=atoi(argv[4]);
  MPI_Init(NULL,NULL);
  MPI_Comm_size(MPI_COMM_WORLD,&n_ranks);
  MPI_Comm_rank(MPI_COMM_WORLD,&my_rank);
  if(my_rank==0){
    fprintf(stderr," Data payload: %d bytes.\n",data_bytes);
    fprintf(stderr," Buffer size: %d bytes.\n",buffer_size);
    fprintf(stderr," Writing to file: >%s<\n",filename_to_write);
    fprintf(stderr,"  Performing %d iterations\n",n_iterations);
  }
  // all MPI stuff below
  
  // allocate transfer buffer
  if((rank_buffer=(void*)malloc(buffer_size))==NULL){
    fprintf(stderr,"rank %d failed to allocate buffer!\n",my_rank);
    MPI_Finalize();
    return -3;
  }
  
  // fill buffer with numbers
  for(i=0;i<(data_bytes/sizeof(int));i++){
    rank_buffer[i]=(i*22)+7;
  }

  // All the ranks have a buffer ready to go
  // now get the collective file set up for writing.
  if((archive_file_MPI=(MPI_File*)malloc(sizeof(MPI_File)))==NULL){
    fprintf(stderr,"rank %d could not allocate space for archive file pointer!\n",my_rank);
    MPI_Finalize();
    return 75;
  }
  file_result=MPI_File_open(MPI_COMM_WORLD, filename_to_write, 
			    MPI_MODE_WRONLY | MPI_MODE_CREATE , 
			    my_Info, archive_file_MPI);
  if(file_result != MPI_SUCCESS){
    fprintf(stderr,"MPI_File_open for archive buffer:  returned error!  Rank %d file >%s<\n",
	    my_rank,
	    filename_to_write);
    return 3; 
  }

  // file is open on all ranks. 
  // time to do a whole mess of writing to it.
  MPI_Barrier(MPI_COMM_WORLD);
  if(my_rank==0){
    fprintf(stderr,"About to begin data writing loop.\n");
    time(&time_before);
  }

  for(i=0;i<n_iterations;i++){
    stage_archive_file_offset = 
      ((long int)( ((long int)i) * (((long int)n_ranks) * ((long int)buffer_size)) )) + 
      ((long int)(( my_rank * buffer_size)));
    //    file_result=MPI_File_write_at_all(*archive_file_MPI,stage_archive_file_offset,rank_buffer,
    //				      data_bytes,MPI_CHAR,&my_MPI_Status);
    file_result=MPI_File_write_at_all(*archive_file_MPI,stage_archive_file_offset,rank_buffer,
				      data_bytes,MPI_CHAR,&my_MPI_Status);
    if(file_result != MPI_SUCCESS){
      fprintf(stderr,"rank %d got %d from MPI_File_write_at_all\n",my_rank,file_result);
      fprintf(stderr,"failed in i=%d!!\n",i);
      MPI_Finalize();
      return 77;
    }
    if(my_rank==0 && (!(i%100))){
      fprintf(stderr,".");
    }
  } // for(i=0....
  if(my_rank==0) fprintf(stderr,"\n");
  MPI_Barrier(MPI_COMM_WORLD);
  if(my_rank==0){
    time(&time_after);
    total_data_transfer = ((long int)data_bytes) * ((long int)n_ranks) * ((long int)n_iterations);
    total_elapsed_time_s = time_after - time_before;
    total_transferred_gb = ((float)(total_data_transfer))/1.0e9;
    fprintf(stderr,"total_time: %d seconds to transfer %3.4f GB\n",
	    total_elapsed_time_s,total_transferred_gb);
    transfer_speed_gb_s = 
      (  total_transferred_gb / 
	 ((float)total_elapsed_time_s) );
    fprintf(stderr,"transfer speed: %3.4f GB/s\n",transfer_speed_gb_s);
  }

  // all MPI stuff above
  MPI_Finalize();
  
  return 0;
}

