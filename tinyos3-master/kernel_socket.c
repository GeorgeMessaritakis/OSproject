
#include "tinyos.h"
#include "kernel_streams.h"

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

	

	PORT_MAP[socket->port] = socket;
	socket->type = SOCKET_LISTENER;


	return 0;
}


Fid_t sys_Accept(Fid_t lsock)
{
	if(lsock <  0 || lsock >= MAX_FILEID)
		return -1;
	
	FCB* fcb = (FCB*)get_fcb(lsock);

	if(fcb == NULL)
		return -1;

	socket_cb* socket = (socket_cb*)fcb->streamobj;

	if(socket == NULL)
		return -1;


	if(socket->type != SOCKET_LISTENER)
		return -1;
	

	socket->refcount++;
	while(is_rlist_empty((&socket->listener_s.queue)) && PORT_MAP[socket->port] == socket)
	{
		kernel_sleep(socket->listener_s.req_available);
	}


	if(PORT_MAP[socket->port] != socket)
		return -1;
	
	
	connection_request* req = rlist_pop_front(&socket->listener_s.queue)->request;


	int fid = sys_Socket(socket->port);
	
	if(fid == NOPORT)
		return -1;


	req->admitted = 1;

	socket_cb* peer_client = req->peer;
	peer_client->type = SOCKET_PEER;

	socket_cb* peer_server = get_fcb(fid)->streamobj;
	peer_server->type = SOCKET_PEER;


	peer_server->peer_s.peer = peer_client;
	peer_client->peer_s.peer = peer_server;


	pipe_cb* cb1 = (pipe_cb*)xmalloc(sizeof(pipe_cb));
	pipe_cb* cb2 = (pipe_cb*)xmalloc(sizeof(pipe_cb));




	return NOFILE;
}


int sys_Connect(Fid_t sock, port_t port, timeout_t timeout)
{
	return -1;
}


int sys_ShutDown(Fid_t sock, shutdown_mode how)
{
	return -1;
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
		PORT_MAP[socketcb->port]=NULL;


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