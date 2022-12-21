
#include "tinyos.h"
#include "kernel_streams.h"
#include "kernel_cc.h"


static socket_cb* PORT_MAP[MAX_PORT];


static file_ops socket_file_ops = {
  .Open = socket_open,
  .Read = socket_read,
  .Write = socket_write,
  .Close = socket_close
};


Fid_t sys_Socket(port_t port)
{
	if(port<0 || port>MAX_PORT)
		return NOFILE;


	int fid;
	FCB* fcb;

	if(FCB_reserve(1,&fid,&fcb))
	{

		socket_cb* socket = (socket_cb*)xmalloc(sizeof(socket_cb));


		initialize_socket(socket, fcb , port);

		fcb->streamobj = socket;
		fcb->streamfunc = &socket_file_ops;

		return fid;
	}

	return NOFILE;
}

int sys_Listen(Fid_t sock)
{	
	if(sock <  0 || sock >= MAX_FILEID)
		return -1;
	
	FCB* fcb = (FCB*)get_fcb(sock);

	if(fcb == NULL)
		return -1;

	socket_cb* socket = (socket_cb*)fcb->streamobj;

	if(socket == NULL)
		return -1;

	if(socket->port == NOPORT)
		return -1;
	
	if(PORT_MAP[socket->port] != NULL)
		return -1;

	if(socket->type == SOCKET_LISTENER)
		return -1;

	if(socket->type == SOCKET_PEER)
		return -1;
	

	PORT_MAP[socket->port] = socket;
	socket->type = SOCKET_LISTENER;

	
	rlnode_init(&socket->listener_s.queue,NULL);
	socket->listener_s.req_available = COND_INIT;

	return 0;
}


Fid_t sys_Accept(Fid_t lsock)
{
	if(lsock <  0 || lsock >= MAX_FILEID)
		return NOFILE;

	
	FCB* fcb = get_fcb(lsock);

	if(fcb == NULL)
		return NOFILE;


	socket_cb* socket = (socket_cb*)fcb->streamobj;

	if(socket == NULL)
		return NOFILE;



	if(socket->type != SOCKET_LISTENER)
		return NOFILE;


	socket->refcount++;
	while(is_rlist_empty((&socket->listener_s.queue)) && PORT_MAP[socket->port] == socket)
	{
		kernel_wait(&socket->listener_s.req_available,SCHED_PIPE);
	}


	if(PORT_MAP[socket->port] != socket)
		return NOFILE;

	
	
	connection_request* req = rlist_pop_front(&socket->listener_s.queue)->request;


	int fid = sys_Socket(socket->port);
	
	if(fid == NOPORT)
	{
		kernel_broadcast(&(req->connected_cv));
		socket->refcount--;
		return NOFILE;
	}


	socket_cb* peer_client = req->peer;
	peer_client->type = SOCKET_PEER;

	FCB* fcb2 = get_fcb(fid);

	if(fcb2 == NULL)
	{
		kernel_broadcast(&(req->connected_cv));
		socket->refcount--;
		return NOFILE;
	}
	socket_cb* peer_server = (socket_cb*)fcb2->streamobj;
	peer_server->type = SOCKET_PEER;


	peer_server->peer_s.peer = peer_client;
	peer_client->peer_s.peer = peer_server;


	pipe_cb* cb1 = (pipe_cb*)xmalloc(sizeof(pipe_cb));
	pipe_cb* cb2 = (pipe_cb*)xmalloc(sizeof(pipe_cb));


	initialize_pipe_cb(cb1,peer_client->fcb,peer_server->fcb);
	initialize_pipe_cb(cb2,peer_server->fcb,peer_client->fcb);

	peer_server->peer_s.write_pipe = cb1;
	peer_server->peer_s.read_pipe = cb2;

	peer_client->peer_s.read_pipe = cb1;
	peer_client->peer_s.write_pipe = cb2;

	req->admitted = 1;
	
	kernel_broadcast(&req->connected_cv);


	socket->refcount--;

	free(req);

	return fid;
}


int sys_Connect(Fid_t sock, port_t port, timeout_t timeout)
{	

	if(sock <  0 || sock >= MAX_FILEID)
		return -1;
	
	FCB* fcb = get_fcb(sock);

	if(fcb == NULL)
		return -1;

	socket_cb* socket = (socket_cb*)fcb->streamobj;

	if(socket == NULL)
		return -1;

	if(socket->type == SOCKET_LISTENER)
		return -1;	


	if(port<0 || port>=MAX_PORT)
		return -1;

	if(PORT_MAP[port] == NULL)
		return -1;

	if(PORT_MAP[port]->type != SOCKET_LISTENER)
		return -1;


	socket->refcount++;


	connection_request* request = (connection_request*)xmalloc(sizeof(connection_request));

	request->admitted = 0;
	request->peer = socket;
	request->connected_cv = COND_INIT;

	rlnode_init(&request->queue_node,request);

	rlist_push_back(&PORT_MAP[port]->listener_s.queue,&request->queue_node);

	kernel_broadcast(&PORT_MAP[port]->listener_s.req_available);

	int no_timeout = 1;
	while(request->admitted == 0 && no_timeout)
	{
		no_timeout = kernel_timedwait(&request->connected_cv,SCHED_PIPE,timeout);
	}

	socket->refcount--;


	if(no_timeout == 0)
		return -1;

	if(request->admitted == 0)
		return -1;

	
	return 0;
}


int sys_ShutDown(Fid_t sock, shutdown_mode how)
{	

	if(sock <  0 || sock >= MAX_FILEID)
		return -1;
	
	FCB* fcb = get_fcb(sock);

	if(fcb == NULL)
		return -1;

	socket_cb* socket = (socket_cb*)fcb->streamobj;

	if(socket == NULL)
		return -1;

	if(socket->type != SOCKET_PEER)
		return -1;


	switch (how)
	{
	case SHUTDOWN_READ:
			pipe_reader_close(socket->peer_s.read_pipe);
			socket->peer_s.read_pipe = NULL;
		break;
	case SHUTDOWN_WRITE:
			pipe_writer_close(socket->peer_s.write_pipe);
			socket->peer_s.write_pipe = NULL;

		break;

	case SHUTDOWN_BOTH:
			pipe_reader_close(socket->peer_s.read_pipe);
			pipe_writer_close(socket->peer_s.write_pipe);
			socket->peer_s.read_pipe = NULL;
			socket->peer_s.write_pipe = NULL;
		break;

	default:
		return -1;
		break;
	}

	if(socket->refcount == 0)
		free(socket);


	return 0;
}


int socket_write(void *socketcb_t,const char* buf,unsigned int n)
{
	

	socket_cb* socketcb = (socket_cb*)socketcb_t;

	if(socketcb == NULL)
		return -1;

	int ret=-1;

	if(socketcb->type == SOCKET_PEER)
	{
		ret = pipe_write(socketcb->peer_s.write_pipe,buf,n);
	}
	
	return ret;
}


int socket_read(void *socketcb_t,char* buf,unsigned int n)
{

	socket_cb* socketcb = (socket_cb*)socketcb_t;

	if(socketcb == NULL)
		return -1;

	int ret=-1;

	if(socketcb->type == SOCKET_PEER)
	{
		ret = pipe_read(socketcb->peer_s.read_pipe,buf,n);
	}
	
	return ret;

}

//work in progress
int socket_close(void * _socketcb)
{

	socket_cb* socketcb = (socket_cb*)_socketcb;

	if(socketcb == NULL)
		return -1;

	socketcb->fcb = NULL;

	if(socketcb->type == SOCKET_LISTENER)
	{
		while(!is_rlist_empty(&(socketcb->listener_s.queue))){
                rlnode* node = rlist_pop_front(&(socketcb->listener_s.queue));
                free(node->request);
            }
        kernel_broadcast(&(socketcb->listener_s.req_available));
        PORT_MAP[socketcb->port] = NULL;
	}

	if(socketcb->type == SOCKET_PEER)
	{
		pipe_reader_close(socketcb->peer_s.read_pipe);
		pipe_writer_close(socketcb->peer_s.write_pipe);
		socketcb->peer_s.read_pipe = NULL;
		socketcb->peer_s.write_pipe = NULL;

	}

	if(socketcb->refcount == 0)
		free(socketcb);

	return 0;
}


void* socket_open(unsigned int n)
{
	return NULL;
}	

void initialize_socket(socket_cb* socket,FCB* fcb , port_t port)
{
		socket->refcount = 0;
		socket->fcb = fcb;
		socket->port = port;
		socket->type = SOCKET_UNBOUND;
}