
#include "tinyos.h"
#include "kernel_streams.h"
#include "util.h"
#include "kernel_dev.h"
#include "kernel_sched.h"
#include "kernel_cc.h"

void initialize_pipe_cb(pipe_cb*,FCB*,FCB*);
int pipe_write(void *,const char*,unsigned int);
int pipe_read(void *,char*,unsigned int);
int pipe_writer_close(void *);
int pipe_reader_close(void*);


/*dummy functions*/
void* pipe_open(unsigned int);
int dummy_write(void *,const char*,unsigned int);
int dummy_read(void *,char*,unsigned int);


static file_ops reader_file_ops = {
  .Open = pipe_open,
  .Read = pipe_read,
  .Write = dummy_write,
  .Close = pipe_reader_close
};


static file_ops writer_file_ops = {
  .Open = pipe_open,
  .Read = dummy_read,
  .Write = pipe_write,
  .Close = pipe_writer_close
};


int sys_Pipe(pipe_t* pipe)
{

	if(pipe == NULL)
		return -1;

	int fids[2];
	FCB* file_descriptors[2];
	

	if(FCB_reserve(2,fids,file_descriptors))
	{	
		pipe_cb* pipecb=NULL;


		pipe->read = (Fid_t)fids[0];
		pipe->write = (Fid_t)fids[1];


		pipecb = (pipe_cb*)xmalloc(sizeof(pipe_cb));


		initialize_pipe_cb(pipecb,file_descriptors[0],file_descriptors[1]);


		file_descriptors[0]->streamobj = pipecb;
		file_descriptors[1]->streamobj = pipecb;

		file_descriptors[0]->streamfunc = &reader_file_ops;
		file_descriptors[1]->streamfunc = &writer_file_ops;

		return 0;
	}

	

	return -1;
}
void initialize_pipe_cb(pipe_cb* pipecb,FCB* reader , FCB* writer) 
{

	pipecb->reader = reader;
	FCB_incref(pipecb->reader);

	pipecb->writer = writer;
	FCB_incref(pipecb->writer);

	pipecb->w_position = 0;
	pipecb->r_position = 0;

	pipecb->has_space = COND_INIT;
	pipecb->has_data = COND_INIT;

	pipecb->elements = 0;

}
//work in progress
int pipe_write(void *pipecb_t,const char* buf,unsigned int n)
{

	pipe_cb* pipecb = (pipe_cb*)pipecb_t;

	if(pipecb == NULL) return -1;

	if(pipecb->reader == NULL || pipecb->writer == NULL) return -1;

	if(buf == NULL) return -1;

	if(n<1) return -1;


	while(pipecb->elements == PIPE_BUFFER_SIZE  && pipecb->reader != NULL)
	{
		kernel_broadcast(&pipecb->has_data);
		kernel_wait(&pipecb->has_space,SCHED_PIPE);
	}

	//if reader end is closed
	if (pipecb->reader == NULL) {
		return -1;
	}

	preempt_off;

	uint count =  0;


	while(count < n && pipecb->elements != PIPE_BUFFER_SIZE)
	{

		pipecb->buffer[pipecb->w_position] = buf[count++];
		pipecb->w_position = (pipecb->w_position+1)%PIPE_BUFFER_SIZE;
		pipecb->elements++;
	}

	preempt_on;

	if(count == 0) return -1;
	
	kernel_broadcast(&pipecb->has_data);

	return count;

}

//work in progress
int pipe_read(void *pipecb_t,char* buf,unsigned int n)
{

	pipe_cb* pipecb = (pipe_cb*)pipecb_t;

	if(pipecb == NULL) return -1;

	if(pipecb->reader == NULL) return -1;

	if(buf == NULL) return -1;

	if(n<1) return -1;

	while(pipecb->elements == 0 && pipecb->writer !=NULL)
	{
		kernel_broadcast(&pipecb->has_space);
		kernel_wait(&pipecb->has_data,SCHED_PIPE);
	}

	preempt_off;

	uint count =  0;


	while(count < n && pipecb->elements > 0)
	{

		buf[count++] = pipecb->buffer[pipecb->r_position];
		pipecb->r_position = (pipecb->r_position+1)%PIPE_BUFFER_SIZE;
		pipecb->elements--;
	}

	preempt_on;

	if(count == 0) return 0;
	
	kernel_broadcast(&pipecb->has_space);


	

	return count;

}
int pipe_writer_close(void * _pipecb)
{

	pipe_cb* pipecb = (pipe_cb*)_pipecb;

	if(pipecb == NULL)
		return -1;

	if(pipecb->writer == NULL)
		return -1;


	FCB_decref(pipecb->writer);

	pipecb->writer = NULL;

	if(pipecb->reader == NULL)
		free(pipecb);
	else
		kernel_broadcast(&pipecb->has_data);
	

	return 0;

}
int pipe_reader_close(void*_pipecb)
{

	pipe_cb* pipecb = (pipe_cb*)_pipecb;

	if(pipecb == NULL)
		return -1;

	if(pipecb->reader == NULL)
		return -1;


	FCB_decref(pipecb->reader);

	pipecb->reader = NULL;


	if(pipecb->writer == NULL)
		free(pipecb);
	else
		kernel_broadcast(&pipecb->has_space);


	return 0;

}
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