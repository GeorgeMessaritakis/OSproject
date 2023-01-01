
#include "tinyos.h"
#include "kernel_streams.h"
#include "kernel_cc.h"

//talbe of ports where we can make servers
static socket_cb* PORT_MAP[MAX_PORT];

//file  operations for sockets
static file_ops socket_file_ops = {
  .Open = socket_open,
  .Read = socket_read,
  .Write = socket_write,
  .Close = socket_close
};

/**
 * @brief Method for creating and initializing a socket
 * 
 * @param port 	socket's port number (useful for making a listener socket)
 * @return 			position of the socket in the FIDT table of the process on success, 
 * 							NOFILE on error
 */
Fid_t sys_Socket(port_t port)
{

	if(port<0 || port>MAX_PORT)		//Checking if port number is within bounds
		return NOFILE;


	int fid;			// variable to store the fid of the process reserved by "FCB_reserve"
	FCB* fcb;			// variable to store the fcb reserved by "FCB_reserve"

	//if "FCB_reserve" is succesfull
	if(FCB_reserve(1,&fid,&fcb))
	{

		socket_cb* socket = (socket_cb*)xmalloc(sizeof(socket_cb));		//create a new socket_cb


		initialize_socket(socket, fcb , port);	// initialize newly created socket_cb

		// connect fcb with the socket and its functions
		fcb->streamobj = socket;								
		fcb->streamfunc = &socket_file_ops;		

		return fid;		// return the index of the FIDT of the process where the socket is made
	}

	return NOFILE;	// if "FCB_reserve" fails, return error
}


/**
 * @brief Method for turning a socket into a listener socket (server)
 * 
 * @param sock 	socket's index in the FIDT of the process
 * @return 			0 on success, -1 on error
 */
int sys_Listen(Fid_t sock)
{	

	if(sock <  0 || sock >= MAX_FILEID)		// Checking if the index given is within bounds of the FIDT
		return -1;
	
	FCB* fcb = (FCB*)get_fcb(sock);				// if index is valid, get the corresponding FCB

	if(fcb == NULL)												// if the fid doesn't correspond to an FCB,
		return -1;													// return error

	socket_cb* socket = (socket_cb*)fcb->streamobj;	// if the FCB exists, take its stream object (should be a sosket)

	if(socket == NULL)										// if there is no stream object connected to FCB,
		return -1;													// return error

	if(socket->port == NOPORT)						// if socket has not a valid port,
		return -1;													// return error
	
	if(PORT_MAP[socket->port] != NULL)		// if there is already a server in the port we want 
		return -1;													// return error

	if(socket->type == SOCKET_LISTENER)		// if the socket we want to make listener is already a listener,
		return -1;													// return error

	if(socket->type == SOCKET_PEER)				// if the socket we want to make listener is a peer type socket,
		return -1;													// return error
	
	
	//if everything went smoothly...

	PORT_MAP[socket->port] = socket;		// add socket to the port table
	socket->type = SOCKET_LISTENER;			// make socket listener

	
	rlnode_init(&socket->listener_s.queue,NULL);		// intitialize listener's member variables
	socket->listener_s.req_available = COND_INIT;		//

	return 0;		// success!
}


Fid_t sys_Accept(Fid_t lsock)
{
	/* Check if file id is not legal */
	if(lsock <  0 || lsock >= MAX_FILEID)
		return NOFILE;

	
	FCB* fcb = get_fcb(lsock);

	/* Check if fcb is not initialized */
	if(fcb == NULL)
		return NOFILE;

	/* Get the listener socket */
	socket_cb* socket = (socket_cb*)fcb->streamobj;

	/* Check if socket is not existed */
	if(socket == NULL)
		return NOFILE;

	/* Check if socket type is listener */
	if(socket->type != SOCKET_LISTENER)
		return NOFILE;

	/* Increase refcount of the socket that we want to make it listener */
	socket->refcount++;

	while(is_rlist_empty((&socket->listener_s.queue)) && PORT_MAP[socket->port] == socket)
	{
		/* Sleep until listener’s queue is not empty and the port position in PORT_MAP array does not exist to a specific socket */
		kernel_wait(&socket->listener_s.req_available,SCHED_PIPE);
	}

	/* if I waked up cause of the port position in PORT_MAP array does not exist to a specific socket return no file */
	if(PORT_MAP[socket->port] != socket)
		return NOFILE;

	/* Taking the request from listener’s queue */
	connection_request* req = rlist_pop_front(&socket->listener_s.queue)->request;

	/* Creating the socket which will serve the client (Server socket)*/
	int fid = sys_Socket(socket->port);
	
	if(fid == NOFILE)
	{
		/* Can’t make a server socket */
		kernel_broadcast(&(req->connected_cv));
		/* Time to decrease refcount */
		socket->refcount--;
		return NOFILE;
	}


	FCB* fcb2 = get_fcb(fid);

	if(fcb2 == NULL)
	{
		kernel_broadcast(&(req->connected_cv));
		socket->refcount--;
		return NOFILE;
	}

	/* Get the client socket */
	socket_cb* peer_client = req->peer;
	peer_client->type = SOCKET_PEER;
	
	/* Get the server socket */
	socket_cb* peer_server = (socket_cb*)fcb2->streamobj;
	peer_server->type = SOCKET_PEER;

	/* Connect client and server sockets */
	peer_server->peer_s.peer = peer_client;
	peer_client->peer_s.peer = peer_server;

	/* Creating two pipes for sockets connection */
	pipe_cb* cb1 = (pipe_cb*)xmalloc(sizeof(pipe_cb));
	pipe_cb* cb2 = (pipe_cb*)xmalloc(sizeof(pipe_cb));

	/* Initializing the pipes */
	initialize_pipe_cb(cb1,peer_client->fcb,peer_server->fcb);
	initialize_pipe_cb(cb2,peer_server->fcb,peer_client->fcb);

	/* Fixing the write and read pointers between two sockets and two pipes */
	peer_server->peer_s.write_pipe = cb1;
	peer_server->peer_s.read_pipe = cb2;
	peer_client->peer_s.read_pipe = cb1;
	peer_client->peer_s.write_pipe = cb2;

	/* The request has been served */
	req->admitted = 1;
	
	/* Wake up cause of request has been served */
	kernel_broadcast(&req->connected_cv);

	/* Decreasing the refcount of the socket */
	socket->refcount--;

	/* Deleting the request */
	free(req);

	return fid;
}


int sys_Connect(Fid_t sock, port_t port, timeout_t timeout)
{	

	/* Check if file id is not legal */
	if(sock <  0 || sock >= MAX_FILEID)
		return -1;
	
	FCB* fcb = get_fcb(sock);

	/* Check if fcb is not initialized */
	if(fcb == NULL)
		return -1;

	socket_cb* socket = (socket_cb*)fcb->streamobj;
	
	/* Check if socket is not existed */
	if(socket == NULL)
		return -1;

	/* Check if socket type is listener */
	if(socket->type == SOCKET_LISTENER)
		return -1;	

	/* Check if port number is out of bounds */
	if(port<0 || port>=MAX_PORT)
		return -1;

	/* Check if the port position in PORT_MAP array does not contain any socket  */
	if(PORT_MAP[port] == NULL)
		return -1;

	/* Check if the port position in PORT_MAP array does not match to a listener  */
	if(PORT_MAP[port]->type != SOCKET_LISTENER)
		return -1;

	/* Increase refcount of the socket we want to connect */
	socket->refcount++;

	/* Creating the request for the listener */
	connection_request* request = (connection_request*)xmalloc(sizeof(connection_request));
	
	/* Initializing the request */
	request->admitted = 0;
	request->peer = socket;
	request->connected_cv = COND_INIT;
	
	/* Sending the request to listener’s queue list */
	rlnode_init(&request->queue_node,request);
	rlist_push_back(&PORT_MAP[port]->listener_s.queue,&request->queue_node);

	/* Wake up the listener. There is request available to execute */
	kernel_broadcast(&PORT_MAP[port]->listener_s.req_available);

	int no_timeout = 1;
	while(request->admitted == 0 && no_timeout)
	{
		/* Sleep until request finished the execution or timeout */
		no_timeout = kernel_timedwait(&request->connected_cv,SCHED_PIPE,timeout);
	}

	/* Decreasing refcount of the socket we connected previously */
	socket->refcount--;

	/* if I waked up cause of timeout return error */
	if(no_timeout == 0)
		return -1;

	/* if I waked up and the request not executed, return error */
	if(request->admitted == 0)
		return -1;

	/* All gone well, return success */
	return 0;
}

/**
 * @brief Method for shutting down a peer socket
 * 
 * @param sock 	socket's index in the FIDT of the process
 * @param how 	the way we want to shutdown the socket (regarding its read/write pipes)
 * @return 			0 on success, -1 on error
 */
int sys_ShutDown(Fid_t sock, shutdown_mode how)
{	

	if(sock <  0 || sock >= MAX_FILEID)				// Checking if the index given is within bounds of the FIDT
		return -1;
	
	FCB* fcb = get_fcb(sock);									// if index is valid, get the corresponding FCB

	if(fcb == NULL)														// if the fid doesn't correspond to an FCB,
		return -1;															// return error

	socket_cb* socket = (socket_cb*)fcb->streamobj;		// if the FCB exists, take its stream object (should be a sosket)

	if(socket == NULL)												// if there is no stream object connected to FCB,
		return -1;															// return error

	if(socket->type != SOCKET_PEER)						// if the socket we want to shutdown is not a peer socket,
		return -1;															// return error


	switch (how)
	{
	case SHUTDOWN_READ:																	// close the connection with the socket's read pipe
			pipe_reader_close(socket->peer_s.read_pipe);
			socket->peer_s.read_pipe = NULL;
		break;
	case SHUTDOWN_WRITE:																// close the connection with the socket's write pipe
			pipe_writer_close(socket->peer_s.write_pipe);
			socket->peer_s.write_pipe = NULL;

		break;

	case SHUTDOWN_BOTH:																	// close the connection with both pipes of the socket
			pipe_reader_close(socket->peer_s.read_pipe);
			pipe_writer_close(socket->peer_s.write_pipe);
			socket->peer_s.read_pipe = NULL;
			socket->peer_s.write_pipe = NULL;
		break;

	default:																						// if the cause is different
		return -1;																				// return error
		break;
	}

	if(socket->refcount == 0)		// if there is no one who needs this socket
		free(socket);							// delete it


	return 0;	// shutdown was successful!
}

/**
 * @brief write to socket's write buffer
 * 
 * @param socketcb_t 	socket control block (should be peer type)
 * @param buf 				the buffer to copy from
 * @param n 					total characters to copy
 * @return 						the characters writen or -1 on failure
 */
int socket_write(void *socketcb_t,const char* buf,unsigned int n)
{
	

	socket_cb* socketcb = (socket_cb*)socketcb_t;	// get the socket we want to write to


	if(socketcb == NULL)			// if it does not exist
		return -1;							// return error

	int ret=-1;								// variable storing the return value (default error)


	if(socketcb->type == SOCKET_PEER)													// if socket is peer type
	{
		ret = pipe_write(socketcb->peer_s.write_pipe,buf,n);		// write to its write pipe and get the return value
	}
	
	return ret;
}

/**
 * @brief read from the socket's read buffer and store the data to user's buffer
 * 
 * @param socketcb_t 	the pipe control block (should be peer type)
 * @param buf 				the user's buffer
 * @param n 					total characters to read
 * @return 						the number of characters read
 */
int socket_read(void *socketcb_t,char* buf,unsigned int n)
{

	socket_cb* socketcb = (socket_cb*)socketcb_t;			// get the socket we want to read to

	if(socketcb == NULL)			// if it does not exist
		return -1;							// return error

	int ret=-1;								// variable storing the return value (default error)


	if(socketcb->type == SOCKET_PEER)												// if socket is peer type
	{
		ret = pipe_read(socketcb->peer_s.read_pipe,buf,n);		// read from its read pipe and get the return value
	}
	
	return ret;

}

/**
 * @brief close the socket
 * 
 * @param _pipecb the socket to close
 * @return 				0 on success, -1 on failure
 */
int socket_close(void * _socketcb)
{

	socket_cb* socketcb = (socket_cb*)_socketcb;		// get the socket we want to close

	if(socketcb == NULL)			// if it does not exist
		return -1;							// return error

	socketcb->fcb = NULL;			// dettach socket from its fcb

	if(socketcb->type == SOCKET_LISTENER)																		// if socket is listener
	{
		while(!is_rlist_empty(&(socketcb->listener_s.queue))){								// delete all of listener's requests
       rlnode* node = rlist_pop_front(&(socketcb->listener_s.queue));
       free(node->request);
    }
    kernel_broadcast(&(socketcb->listener_s.req_available));							// notify anyone who waits for a request to come
    PORT_MAP[socketcb->port] = NULL;																			// remove the socket from the port table
	}

	if(socketcb->type == SOCKET_PEER)											// if socket is peer type
	{
		pipe_reader_close(socketcb->peer_s.read_pipe);			// close the connection with its read pipe
		socketcb->peer_s.read_pipe = NULL;									//
		pipe_writer_close(socketcb->peer_s.write_pipe);			// close the connection with its write pipe
		socketcb->peer_s.write_pipe = NULL;									//

	}

	if(socketcb->refcount == 0)		// if there is no one who needs this socket
		free(socketcb);							// delete it

	return 0;	// close is successful!
}

/*dummy function for opening a socket*/
void* socket_open(unsigned int n)
{
	return NULL;
}	

/**
 * @brief initialize the socket control block
 * 
 * @param socket 	the socket to initialize
 * @param fcb 		the FCB to which thw socket will be connected
 * @param port 		the port of the socket (for servers)
 */
void initialize_socket(socket_cb* socket,FCB* fcb , port_t port)
{
		socket->refcount = 0;
		socket->fcb = fcb;
		socket->port = port;
		socket->type = SOCKET_UNBOUND;		// socket type is Unbound by default
}