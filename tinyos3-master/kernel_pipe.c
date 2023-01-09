
#include "tinyos.h"
#include "kernel_streams.h"
#include "util.h"
#include "kernel_dev.h"
#include "kernel_sched.h"
#include "kernel_cc.h"


/*file operations for the reader*/
static file_ops reader_file_ops = {
  .Open = pipe_open,
  .Read = pipe_read,
  .Write = dummy_write,
  .Close = pipe_reader_close
};


/*file operations for the writer*/
static file_ops writer_file_ops = {
  .Open = pipe_open,
  .Read = dummy_read,
  .Write = pipe_write,
  .Close = pipe_writer_close
};


/**
 * @brief Method for creating and initializing pipe
 * 
 * @param pipe's reader and writer position in FIDT table of the process
 * @return 0 on success, -1 on error
 */
int sys_Pipe(pipe_t* pipe)
{

	//if we cannot store reader and writer then there is no point
	if(pipe == NULL)
		return -1;


	int fids[2];//array to store reader and writer indexes of FIDT
	FCB* file_descriptors[2];///array to store reader and writer FCBs
	

	//if reserve is succesful
	if(FCB_reserve(2,fids,file_descriptors))
	{	
		pipe_cb* pipecb=NULL;


		pipe->read = (Fid_t)fids[0];//get the reader
		pipe->write = (Fid_t)fids[1];//get the writer


		//create the pipe control block
		pipecb = (pipe_cb*)xmalloc(sizeof(pipe_cb));

		//initialize pipe
		initialize_pipe_cb(pipecb,file_descriptors[0],file_descriptors[1]);

		//connect FCB with it's stream object and funtion
		file_descriptors[0]->streamobj = pipecb;
		file_descriptors[1]->streamobj = pipecb;

		file_descriptors[0]->streamfunc = &reader_file_ops;
		file_descriptors[1]->streamfunc = &writer_file_ops;


		//if everything went smoothly
		return 0;
	}

	
	//if reserve failed
	return -1;
}

/**
 * @brief initialize the pipe control block
 * 
 * @param pipecb the pipe to initialize
 * @param reader the FCB to connect the read end
 * @param writer the FCB to connect the write end
 */
void initialize_pipe_cb(pipe_cb* pipecb,FCB* reader , FCB* writer) 
{
	//connect the reader and the writer member variables
	pipecb->reader = reader;
	pipecb->writer = writer;

	/*
		initialize the reader and the writer positions
		for the pipe's buffer
	*/
	pipecb->w_position = 0;
	pipecb->r_position = 0;

	//initialize condition variables
	pipecb->has_space = COND_INIT;
	pipecb->has_data = COND_INIT;

	//no elements in the buffer at start
	pipecb->elements = 0;

}


/**
 * @brief write to pipe's buffer
 * 
 * @param pipecb_t pipe control block
 * @param buf the buffer to copy from
 * @param n total characters to copy
 * @return the characters writen or -1 on failure
 */
int pipe_write(void *pipecb_t,const char* buf,unsigned int n)
{

	//get the pipe control block
	pipe_cb* pipecb = (pipe_cb*)pipecb_t;

	//if we don't have a pipe control block there is no point
	if(pipecb == NULL) return -1;

	//if the read end or the write end is closed then we cannot write
	if(pipecb->reader == NULL || pipecb->writer == NULL) return -1;

	//if there is no buffer to copy from then there is no point
	if(buf == NULL) return -1;

	//there is no point writing nothing
	if(n<1) return -1;


	/*
		if the buffer is full and there is a reader
		available , wait.
	*/
	while(pipecb->elements == PIPE_BUFFER_SIZE  && pipecb->reader != NULL)
	{

		kernel_wait(&pipecb->has_space,SCHED_PIPE);
	}

	/*
		if we woke up because reader end is closed,
		then there is no point in writting,there is
		no one to read
	*/
	if (pipecb->reader == NULL) {
		return -1;
	}

	//counter to know how many characters we wrote
	uint count =  0;


	/*
		while we haven't reached the elements to write
		and the buffer is not full, keep writting
	*/
	while(count < n && pipecb->elements < PIPE_BUFFER_SIZE)
	{
		
		pipecb->buffer[pipecb->w_position] = buf[count++];			  //copy from the user's buffer to pipe's buffer
		pipecb->w_position = (pipecb->w_position+1)%PIPE_BUFFER_SIZE;//update the position of writer 
		pipecb->elements++;											//one more character inside pipe's buffer
	}

	/*
		if we wrote nothing then the buffer is full
	*/
	if(count == 0) return -1;
	

	//notify readers that there are data to read
	kernel_broadcast(&pipecb->has_data);


	//return the characters written for this session
	return count;

}


/**
 * @brief read from the pipe's buffer and store the data to user's buffer
 * 
 * @param pipecb_t the pipe control block
 * @param buf the user's buffer
 * @param n total characters to read
 * @return the number of characters read
 */
int pipe_read(void *pipecb_t,char* buf,unsigned int n)
{
	//get the pipe control block
	pipe_cb* pipecb = (pipe_cb*)pipecb_t;

	//if the pipe control block is null then there is no point to continue
	if(pipecb == NULL) return -1;

	//if the reader is null then we cannot read
	if(pipecb->reader == NULL) return -1;

	//if there is no buffer to copy to then there is no point to proceed
	if(buf == NULL) return -1;

	//no point reading 0 characters
	if(n<1) return -1;

	/*
		wait while there are no elements inside pipe and 
		there is a writer who will wrire inside it
	*/
	while(pipecb->elements == 0 && pipecb->writer !=NULL)
	{
		//sleep until there are data available to read
		kernel_wait(&pipecb->has_data,SCHED_PIPE);
	}

	//total characters read
	uint count =  0;


	/*
		while there are characters available and
		we haven't reached the total characters to read 
		then read
	*/
	while(count < n && pipecb->elements > 0)
	{

		buf[count++] = pipecb->buffer[pipecb->r_position];			  //copy characters from pipe's buffer to user's buffer
		pipecb->r_position = (pipecb->r_position+1)%PIPE_BUFFER_SIZE;//update the position of the reader inside the buffer
		pipecb->elements--;											//reduce the number of elements in the buffer									
	}


	//if we haven't read anything then is EOF, return 0
	if(count == 0) return 0;
	

	//inform writers that there is space available to write
	kernel_broadcast(&pipecb->has_space);



	//return the total number of characters read
	return count;

}


/**
 * @brief close the write end of the pipe
 * 
 * @param _pipecb the pipe to close
 * @return 0 in success , -1 in failure
 */
int pipe_writer_close(void * _pipecb)
{

	//get the pipe
	pipe_cb* pipecb = (pipe_cb*)_pipecb;

	//if we have no pipe there is no point
	if(pipecb == NULL)
		return -1;

	//if the writer is already closed then the prosedure fails
	if(pipecb->writer == NULL)
		return -1;

	//close the write end , that means make the pointer to FCB NULL
	pipecb->writer = NULL;

	//if the read end is close too then free the space
	if(pipecb->reader == NULL)
		free(pipecb);
	else
		kernel_broadcast(&pipecb->has_data);//inform the readers that there are data to read
											//in case the writer was created to write once and then get destroyed

	//if everything went smoothly
	return 0;

}


/**
 * @brief close the read end of the pipe
 * 
 * @param _pipecb the pipe control block
 * @return 0 in success , -1 in failure
 */
int pipe_reader_close(void*_pipecb)
{
	//get the pipe
	pipe_cb* pipecb = (pipe_cb*)_pipecb;

	//if there is no pipe there is no point
	if(pipecb == NULL)
		return -1;

	//if the reader end is already closed then we cannot close it again
	if(pipecb->reader == NULL)
		return -1;

	//close the reader,that means make the FCB that corresponds to reader NULL
	pipecb->reader = NULL;

	//if the both ends are closed then release the control block
	if(pipecb->writer == NULL)
		free(pipecb);
	else
		kernel_broadcast(&pipecb->has_space);//inform possible writers that there is space to write


	//if everything went smoothly
	return 0;

}

/********************************dummy functions********************************/
void* pipe_open(unsigned int n)
{
	return NULL;
}
int dummy_write(void *pipecb_t,const char* buf,unsigned int n)
{
	return -1;
}
int dummy_read(void *pipecb_t,char* buf,unsigned int n)
{
	return -1;
}