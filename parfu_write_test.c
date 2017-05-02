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

#define MAX_N_ARCHIVE_FILES      (10)

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
  MPI_File **archive_file_MPI=NULL;
  int file_result;

  long int total_data_transfer;
  int total_elapsed_time_s;
  float total_transferred_gb;
  float transfer_speed_gb_s;

  long int stage_archive_file_offset;

  char *archive_filenames[MAX_N_ARCHIVE_FILES];
  int n_archive_files=(1);
  int archive_filename_length;

  time_t time_before,time_after;
  //  int my_target_file;

  int rank_ranges[MAX_N_ARCHIVE_FILES][3];
  MPI_Group world_group;
  MPI_Group sub_group[MAX_N_ARCHIVE_FILES];
  MPI_Comm sub_comm[MAX_N_ARCHIVE_FILES];
  int my_communicator; 

  //  fprintf(stderr,"parfu_write_test beginning\n");
  
  if(argc < 6){
    fprintf(stderr,"usage: \n");
    fprintf(stderr," parfu_write_test <dat_bytes> <buf_bytes> <file_to_write> <n_iterations> <# arch files>\n");
    MPI_Finalize();
    return -1;
  }
  data_bytes=atoi(argv[1]);
  buffer_size=atoi(argv[2]);
  filename_to_write=argv[3];
  n_iterations=atoi(argv[4]);
  n_archive_files=atoi(argv[5]);
  if(n_archive_files<1 || n_archive_files>MAX_N_ARCHIVE_FILES){
    fprintf(stderr," you specified %d archive files!  Must be >0 or <%d\n",
	    n_archive_files,MAX_N_ARCHIVE_FILES);
  }

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
  
  // set up multiple file output
  for(i=0;i<MAX_N_ARCHIVE_FILES;i++){
    archive_filenames[i]=NULL;
  }

  // Creating the sub-group communicators
  file_result=MPI_Comm_group(MPI_COMM_WORLD,&world_group);
  if(file_result != MPI_SUCCESS){
    fprintf(stderr,"rank %d MPI_Comm_group to get master returned %d!\n",my_rank,file_result);
  }
  for(i=0;i<n_archive_files;i++){
    rank_ranges[0][0] = i;
    rank_ranges[0][1] = ((n_ranks/n_archive_files)*n_archive_files + i;
    if(rank_ranges[0][1] >= n_ranks){
      rank_ranges[0][1] -= n_archive_files;
    }
    rank_ranges[0][2] = n_archive_files;  
    if(my_rank == 0){
      fprintf(stderr,"triple [%02d]: %4d  %4d  %4d\n",
	      i,rank_ranges[0][0],rank_ranges[0][1],rank_ranges[0][2]);
    }
    file_result=MPI_Group_range_incl(world_group,1,rank_ranges,sub_group+i);
    if(file_result != MPI_SUCCESS){
      fprintf(stderr,"rank %d MPI_Group_range_incl() returned %d\n",
	      my_rank,file_result);
    }
  }
  // sub groups created; now create the sub-communicators
  for(i=0;i<n_archive_files;i++){
    MPI_Comm_create(MPI_COMM_WORLD,sub_group[i],sub_comm+i);
    if(file_result != MPI_SUCCESS){
      fprintf(stderr,"rank_%d MPI_Comm_create() returned %d\n",
	      my_rank,file_result);
    }
  }
  my_communicator = my_rank % n_archive_files;
  
  archive_filename_length = strlen(filename_to_write) + 10;
  for(i=0;i<n_archive_files;i++){
    if((archive_filenames[i]=
	(char*)malloc(sizeof(char)*archive_filename_length))==NULL){
      fprintf(stderr,"Could not allocate archive_filename member # %d!\n",i);
      MPI_Finalize();
      return -4;
    }
    sprintf(archive_filenames[i],"%s__%02d",filename_to_write,i);
  } // for(i=0;
  
  if(my_rank==0){
    fprintf(stderr,"Writing to %d archive files:\n",n_archive_files);
    for(i=0;i<n_archive_files;i++){
      fprintf(stderr,"   %s\n",archive_filenames[i]);
    }
  }
  
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
  // now get the collective file(s) set up for writing.

  if((archive_file_MPI=(MPI_File**)malloc(sizeof(MPI_File*)*n_archive_files))==NULL){
    fprintf(stderr,"rank %d could not allocate array for archive file pointers!\n",my_rank);
    MPI_Finalize();
    return 75;
  }
  for(i=0;i<n_archive_files;i++){
    if((archive_file_MPI[i]=(MPI_File*)malloc(sizeof(MPI_File)))==NULL){
      fprintf(stderr,"rank %d could not allocate MPI file pointer number %d!!\n",my_rank,i);
      return 76;
    }
  }
  
  /*
  for(i=0;i<n_archive_files;i++){
    file_result=MPI_File_open(MPI_COMM_WORLD, archive_filenames[i], 
			      MPI_MODE_WRONLY | MPI_MODE_CREATE , 
			      my_Info, archive_file_MPI[i]);
    if(file_result != MPI_SUCCESS){
      fprintf(stderr,"MPI_File_open for archive buffer:  returned error!  Rank %d file >%s<\n",
	      my_rank,
	      archive_filenames[i]);
      return 3; 
    }
  }
  */

  // all files open THEIR file in THEIR communicator
  file_result=MPI_File_open(sub_comm[my_communicator], archive_filenames[my_communicator], 
			    MPI_MODE_WRONLY | MPI_MODE_CREATE , 
			    my_Info, archive_file_MPI[my_communicator]);
  if(file_result != MPI_SUCCESS){
    fprintf(stderr,"MPI_File_open for archive buffer:  returned error!  Rank %d file >%s< comm %d\n",
	    my_rank,
	    archive_filenames[my_communicator],my_communicator);

    MPI_Finalize();
    return 3; 
  }
  
  
  // file(s) is(are) open on all ranks. 
  // time to do a whole mess of writing to it(them).
  MPI_Barrier(MPI_COMM_WORLD);
  if(my_rank==0){
    fprintf(stderr,"About to begin data writing loop.\n");
    time(&time_before);
  }

  /*  if(n_archive_files>1){
    my_target_file = my_rank % n_archive_files;
  }
  else{
    my_target_file=0;
  }
  */
      
  for(i=0;i<n_iterations;i++){
    stage_archive_file_offset = 
      ((long int)( ((long int)i) * (((long int)(n_ranks/n_archive_files)) * ((long int)buffer_size)) )) + 
      ((long int)(( (my_rank/n_archive_files) * buffer_size)));
    //    file_result=MPI_File_write_at_all(*archive_file_MPI,stage_archive_file_offset,rank_buffer,
    //				      data_bytes,MPI_CHAR,&my_MPI_Status);
    //    file_result=MPI_File_write_at_all(*archive_file_MPI,stage_archive_file_offset,rank_buffer,
    //				      data_bytes,MPI_CHAR,&my_MPI_Status);
    file_result=MPI_File_write_at_all((*(archive_file_MPI[my_communicator])),stage_archive_file_offset,rank_buffer,
				      data_bytes,MPI_CHAR,&my_MPI_Status);
    if(file_result != MPI_SUCCESS){
      fprintf(stderr,"rank %d i=%d got %d from MPI_File_write_at_all\n",my_rank,i,file_result);
      fprintf(stderr,"failed in i=%d communicator %d!!\n",i,my_communicator);
      MPI_Finalize();
      return 77;
    }
    if(my_rank==0 && (!(i%20))){
      fprintf(stderr,".");
    }
  } // for(i=0....
  if(my_rank==0) fprintf(stderr,"\n");
  //  MPI_File_close((*(ar
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

