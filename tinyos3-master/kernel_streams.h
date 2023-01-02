#ifndef __KERNEL_STREAMS_H
#define __KERNEL_STREAMS_H

#include "tinyos.h"
#include "kernel_dev.h"

/**
	@file kernel_streams.h
	@brief Support for I/O streams.


	@defgroup streams Streams.
	@ingroup kernel
	@brief Support for I/O streams.

	The stream model of tinyos3 is similar to the Unix model.
	Streams are objects that are shared between processes.
	Streams are accessed by file IDs (similar to file descriptors
	in Unix).

	The streams of each process are held in the file table of the
	PCB of the process. The system calls generally use the API
	of this file to access FCBs: @ref get_fcb, @ref FCB_reserve
	and @ref FCB_unreserve.

	Streams are connected to devices by virtue of a @c file_operations
	object, which provides pointers to device-specific implementations
	for read, write and close.

	@{
*/



/** @brief The file control block.

	A file control block provides a uniform object to the
	system calls, and contains pointers to device-specific
	functions.
 */
typedef struct file_control_block
{
  uint refcount;  			/**< @brief Reference counter. */
  void* streamobj;			/**< @brief The stream object (e.g., a device) */
  file_ops* streamfunc;		/**< @brief The stream implementation methods */
  rlnode freelist_node;		/**< @brief Intrusive list node */
} FCB;



/** 
  @brief Initialization for files and streams.

  This function is called at kernel startup.
 */
void initialize_files();


/**
	@brief Increase the reference count of an fcb 

	@param fcb the fcb whose reference count will be increased
*/
void FCB_incref(FCB* fcb);


/**
	@brief Decrease the reference count of the fcb.

	If the reference count drops to 0, release the FCB, calling the 
	Close method and returning its return value.
	If the reference count is still >0, return 0. 

	@param fcb  the fcb whose reference count is decreased
	@returns if the reference count is still >0, return 0, else return the value returned by the
	     `Close()` operation
*/
int FCB_decref(FCB* fcb);


/** @brief Acquire a number of FCBs and corresponding fids.

   Given an array of fids and an array of pointers to FCBs  of
   size @ num, this function will check is available resources
   in the current process PCB and FCB are available, and if so
   it will fill the two arrays with the appropriate values.
   If not, the state is unchanged (but the array contents
   may have been overwritten).

   If these resources are not needed, the operation can be
   reversed by calling @ref FCB_unreserve.

   @param num the number of resources to reserve.
   @param fid array of size at least `num` of `Fid_t`.
   @param fcb array of size at least `num` of `FCB*`.
   @returns 1 for success and 0 for failure.
*/
int FCB_reserve(size_t num, Fid_t *fid, FCB** fcb);


/** @brief Release a number of FCBs and corresponding fids.

   Given an array of fids of size @ num, this function will 
   return the fids to the free pool of the current process and
   release the corresponding FCBs.

   This is the opposite of operation @ref FCB_reserve. 
   Note that this is very different from closing open fids.
   No I/O operation is performed by this function.

   This function does not check its arguments for correctness.
   Use only with arrays filled by a call to @ref FCB_reserve.

   @param num the number of resources to unreserve.
   @param fid array of size at least `num` of `Fid_t`.
   @param fcb array of size at least `num` of `FCB*`.
*/
void FCB_unreserve(size_t num, Fid_t *fid, FCB** fcb);


/** @brief Translate an fid to an FCB.

	This routine will return NULL if the fid is not legal.

	@param fid the file ID to translate to a pointer to FCB
	@returns a pointer to the corresponding FCB, or NULL.
 */
FCB* get_fcb(Fid_t fid);


/*******************************************
 *
 * Pipes
 *
 *******************************************/

#define PIPE_BUFFER_SIZE 4096

typedef struct pipe_control_block
{
  FCB* reader;
  FCB* writer;

  CondVar has_space;
  CondVar has_data;

  int w_position, r_position;
  int elements;

  char buffer[PIPE_BUFFER_SIZE];
} pipe_cb;

void initialize_pipe_cb(pipe_cb*,FCB*,FCB*);
int pipe_write(void *,const char*,unsigned int);
int pipe_read(void *,char*,unsigned int);
int pipe_writer_close(void *);
int pipe_reader_close(void*);

/*dummy functions*/
void* pipe_open(unsigned int);
int dummy_write(void *,const char*,unsigned int);
int dummy_read(void *,char*,unsigned int);


/*******************************************
 *
 * Sockets
 *
 *******************************************/

/*
 * declaration of socket_cb (necessary here so 
 * peer socket type knows what "socket_cb" is)
 */
typedef struct socket_control_block socket_cb;

/*different types of sockets*/
typedef enum 
{

  SOCKET_LISTENER,
  SOCKET_UNBOUND,
  SOCKET_PEER

}socket_type;


/**
  @brief listener type socket.

  This structure holds all information pertaining to a listener type socket.
 */
typedef struct listener
{

  rlnode queue;           /**< @brief Listener's queue of requests */
  CondVar req_available;  /**< @brief Condition variable for @c Accept. 

                             This condition variable is  broadcast each time a request is added 
                             to listener's queue. It is used in the implementation of
                             @c Accept() */

}listener_socket;


/**
  @brief Unbound type socket.

  This structure holds all information pertaining to an unbound type socket.
 */
typedef struct unbound
{

  rlnode unbound_socket;  /**< @brief node to be added to an rlnode list */

}unbound_socket;


/**
  @brief Peer type socket.

  This structure holds all information pertaining to a peer type socket.
 */
typedef struct peer
{

  socket_cb* peer;      /**< @brief the socket with which this socket is connected */
  pipe_cb* write_pipe;  /**< @brief the pipe this socket can write to */
  pipe_cb* read_pipe;   /**< @brief the pipe this socket can read from  */

}peer_socket;


/**
  @brief Socket Control Block.

  This structure holds all information pertaining to a socket control block.
 */
typedef struct socket_control_block
{

  uint refcount;      /**< @brief stores the number of references to this socket_cb
                          if refcount==0 the socket_cb can be deleted  */
  FCB* fcb;           /**< @brief the fcb associated with this socket  */
  port_t port;        /**< @brief the port to which we will connect if this socket 
                            becomes a listener socket  */
  socket_type type;   /**< @brief the type of the socket  */

 /**< @brief Union to store the addition information needed depending of which type is the socket */
  union{

    listener_socket listener_s;
    unbound_socket unbound_s;
    peer_socket peer_s;

  };

}socket_cb;

/**
  @brief Server request.

  This structure holds all information pertaining to a connection
  made by a client to connect a server.
 */
typedef struct request
{
  int admitted;           /**< @brief flaq to check if the rewuest has been admitted */
  socket_cb* peer;        /**< @brief the socket_cb of the client making the request */
  CondVar connected_cv;   /**< @brief Condition variable for @c Connect. 

                             This condition variable is broadcast each time a request has 
                             been admitted. It is used in the implementation of
                             @c Connect() */

  rlnode queue_node;      /**< @brief the node which will be added to server's queue list */

}connection_request;

/*declaration of other useful methods*/
void initialize_socket(socket_cb* ,FCB* , port_t); 
int socket_write(void *,const char*,unsigned int);
int socket_read(void *,char*,unsigned int);
int socket_close(void *);

/*dummy function*/
void* socket_open(unsigned int);



/** @} */

#endif
