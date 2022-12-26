
#include <assert.h>
#include "kernel_cc.h"
#include "kernel_proc.h"
#include "kernel_streams.h"



/* 
 The process table and related system calls:
 - Exec
 - Exit
 - WaitPid
 - GetPid
 - GetPPid
 */

/* 
	The current core's CCB. This must only be used in a 
	non-preemtpive context.
 */
#define CURCORE (cctx[cpu_core_id])

/* 
	The current thread. This is a pointer to the TCB of the thread 
	currently executing on this core.

	This must only be used in non-preemptive context.
*/
#define CURTHREAD (CURCORE.current_thread)


/* The process table */
PCB PT[MAX_PROC];
unsigned int process_count;

PCB* get_pcb(Pid_t pid)
{
  return PT[pid].pstate==FREE ? NULL : &PT[pid];
}

Pid_t get_pid(PCB* pcb)
{
  return pcb==NULL ? NOPROC : pcb-PT;
}

/* Initialize a PCB */
static inline void initialize_PCB(PCB* pcb)
{
  pcb->pstate = FREE;
  pcb->argl = 0;
  pcb->args = NULL;

  for(int i=0;i<MAX_FILEID;i++)
    pcb->FIDT[i] = NULL;

  rlnode_init(& pcb->children_list, NULL);
  rlnode_init(& pcb->exited_list, NULL);
  rlnode_init(& pcb->ptcb_list, NULL);/*initially empty list of ptcbs*/
  rlnode_init(& pcb->children_node, pcb);
  rlnode_init(& pcb->exited_node, pcb);
  pcb->child_exit = COND_INIT;
  pcb->thread_count = 0;/*initially no threads for this process*/
}


static PCB* pcb_freelist;

void initialize_processes()
{
  /* initialize the PCBs */
  for(Pid_t p=0; p<MAX_PROC; p++) {
    initialize_PCB(&PT[p]);
  }

  /* use the parent field to build a free list */
  PCB* pcbiter;
  pcb_freelist = NULL;
  for(pcbiter = PT+MAX_PROC; pcbiter!=PT; ) {
    --pcbiter;
    pcbiter->parent = pcb_freelist;
    pcb_freelist = pcbiter;
  }

  process_count = 0;

  /* Execute a null "idle" process */
  if(Exec(NULL,0,NULL)!=0)
    FATAL("The scheduler process does not have pid==0");
}


/*
  Must be called with kernel_mutex held
*/
PCB* acquire_PCB()
{
  PCB* pcb = NULL;

  if(pcb_freelist != NULL) {
    pcb = pcb_freelist;
    pcb->pstate = ALIVE;
    pcb_freelist = pcb_freelist->parent;
    process_count++;
  }

  return pcb;
}

/*
  Must be called with kernel_mutex held
*/
void release_PCB(PCB* pcb)
{
  pcb->pstate = FREE;
  pcb->parent = pcb_freelist;
  pcb_freelist = pcb;
  process_count--;
}


/*
 *
 * Process creation
 *
 */

/*
	This function is provided as an argument to spawn,
	to execute the main thread of a process.
*/
void start_main_thread()
{
  int exitval;

  Task call =  CURPROC->main_task;
  int argl = CURPROC->argl;
  void* args = CURPROC->args;

  exitval = call(argl,args);
  Exit(exitval);
}


/*
  This function is provided as an argument to spawn,
  to execute one of the child threads of a process
*/
void start_child_thread()
{

  //exit value of the task
  int exitval;

  Task call  = cur_thread()->ptcb->task;  //task for this thread to execute
  int argl = cur_thread()->ptcb->argl;   //argl
  void* args = cur_thread()->ptcb->args;//args

  //run the task and return its exit value
  exitval = call(argl,args);

  //Terminate the thread 
  ThreadExit(exitval);
}


/*
	System call to create a new process.
 */
Pid_t sys_Exec(Task call, int argl, void* args)
{
  PCB *curproc, *newproc;

  PTCB* ptcb;
  
  /* The new process PCB */
  newproc = acquire_PCB();

  if(newproc == NULL) goto finish;  /* We have run out of PIDs! */

  if(get_pid(newproc)<=1) {
    /* Processes with pid<=1 (the scheduler and the init process) 
       are parentless and are treated specially. */
    newproc->parent = NULL;
  }
  else
  {
    /* Inherit parent */
    curproc = CURPROC;

    /* Add new process to the parent's child list */
    newproc->parent = curproc;
    rlist_push_front(& curproc->children_list, & newproc->children_node);

    /* Inherit file streams from parent */
    for(int i=0; i<MAX_FILEID; i++) {
       newproc->FIDT[i] = curproc->FIDT[i];
       if(newproc->FIDT[i])
          FCB_incref(newproc->FIDT[i]);
    }
  }


  /* Set the main thread's function */
  newproc->main_task = call;

  /* Copy the arguments to new storage, owned by the new process */
  newproc->argl = argl;
  if(args!=NULL) {
    newproc->args = malloc(argl);
    memcpy(newproc->args, args, argl);
  }
  else
    newproc->args=NULL;

  /* 
    Create and wake up the thread for the main function. This must be the last thing
    we do, because once we wakeup the new thread it may run! so we need to have finished
    the initialization of the PCB.
   */
  if(call != NULL) {

    //spawn the main thread
    newproc->main_thread = spawn_thread(newproc, start_main_thread);

    //xmalloc to allocate space for the ptcb
    ptcb = allocate_ptcb();

    //initialize ptcb's values
    initialize_ptcb(ptcb,newproc->main_thread,call,argl,args);

    //make this ptcb the main threads ptcb
    newproc->main_thread->ptcb = ptcb;
    
    //add the new ptcb to the ptcb list of the new process
    rlist_push_back(&newproc->ptcb_list,&ptcb->ptcb_list_node);

    
    /*
      the new process has a new thread,the main thread,
      increase the thread count.
    */
    newproc->thread_count++;

    /*
      make the new thread READY and 
      add it to the sceduler's queue
    */
    wakeup(newproc->main_thread);
  }


finish:
  return get_pid(newproc);
}


/* System call */
Pid_t sys_GetPid()
{
  return get_pid(CURPROC);
}


Pid_t sys_GetPPid()
{
  return get_pid(CURPROC->parent);
}


static void cleanup_zombie(PCB* pcb, int* status)
{
  if(status != NULL)
    *status = pcb->exitval;

  rlist_remove(& pcb->children_node);
  rlist_remove(& pcb->exited_node);

  release_PCB(pcb);
}


static Pid_t wait_for_specific_child(Pid_t cpid, int* status)
{

  /* Legality checks */
  if((cpid<0) || (cpid>=MAX_PROC)) {
    cpid = NOPROC;
    goto finish;
  }

  PCB* parent = CURPROC;
  PCB* child = get_pcb(cpid);
  if( child == NULL || child->parent != parent)
  {
    cpid = NOPROC;
    goto finish;
  }

  /* Ok, child is a legal child of mine. Wait for it to exit. */
  while(child->pstate == ALIVE)
    kernel_wait(& parent->child_exit, SCHED_USER);
  
  cleanup_zombie(child, status);
  
finish:
  return cpid;
}


static Pid_t wait_for_any_child(int* status)
{
  Pid_t cpid;

  PCB* parent = CURPROC;

  /* Make sure I have children! */
  int no_children, has_exited;
  while(1) {
    no_children = is_rlist_empty(& parent->children_list);
    if( no_children ) break;

    has_exited = ! is_rlist_empty(& parent->exited_list);
    if( has_exited ) break;

    kernel_wait(& parent->child_exit, SCHED_USER);    
  }

  if(no_children)
    return NOPROC;

  PCB* child = parent->exited_list.next->pcb;
  assert(child->pstate == ZOMBIE);
  cpid = get_pid(child);
  cleanup_zombie(child, status);

  return cpid;
}


Pid_t sys_WaitChild(Pid_t cpid, int* status)
{
  /* Wait for specific child. */
  if(cpid != NOPROC) {
    return wait_for_specific_child(cpid, status);
  }
  /* Wait for any child */
  else {
    return wait_for_any_child(status);
  }

}


void sys_Exit(int exitval)
{

  PCB *curproc = CURPROC;  /* cache for efficiency */

  /* First, store the exit status */
  curproc->exitval = exitval;

  /* 
    Here, we must check that we are not the init task. 
    If we are, we must wait until all child processes exit. 
   */
  if(get_pid(curproc)==1) {

    while(sys_WaitChild(NOPROC,NULL)!=NOPROC);

  } 
  
  //It's time to exit the main thread of this process.
  sys_ThreadExit(exitval);
  
}


//file operations for process info
static file_ops procinfo_ops = {
  .Open = procinfo_open,
  .Read = procinfo_read,
  .Write = procinfo_write,
  .Close = procinfo_close
};


/**
 * @brief Create the connection between process and FCB
 * and connect FCB to process info control block
 * 
 * @return Fid_t FIDT's position that connects to FCB which's stream object
 * is process info
 */
Fid_t sys_OpenInfo()
{

  
  int fid;  //the position inside process's FIDT
	FCB* fcb; //the fcb that connects with the FIDT[fid] of the process


	if(FCB_reserve(1,&fid,&fcb))
	{

    //new process info control block
		procinfo_cb* procinfocb = (procinfo_cb*)xmalloc(sizeof(procinfo_cb));


    //initialize procinfocb
    initialize_procinfo(procinfocb);


		fcb->streamobj = procinfocb;      //the stream object of the process info control block
		fcb->streamfunc = &procinfo_ops;  //functions to be used by this file descriptor

    //return the position of the FIDT that connects to fcb
		return fid;
	}

  //if fcb_reserve fails
	return NOFILE;
}


/**
 * @brief copy n bytes from procinfo control block to buf
 * 
 * @param procinfo_cb_t process info control block
 * @param buf buffer to copy data to
 * @param n number of bytes to copy
 * @return int 
 */
int procinfo_read(void * procinfo_cb_t,char* buf,unsigned int n)
{

  //get the process info control block
  procinfo_cb* procinfocb = (procinfo_cb*)procinfo_cb_t;

  //get the process that cursor points to inside PT
  PCB* cur = procinfocb->cursor;

  //cursor should not become NULL
  if(cur==NULL)
    return -1;

  //get the ID of the process
  int pid = get_pid(cur);

  //if the process's state is FREE
  if(pid == NOPROC)
    return -1;

  //the buffer can't be null,where are we gonna copy to?
  if(buf == NULL)
    return -1;

  //cannot copy negative number of elements
  if(n<0)
    return -1;

  //the process info control block can't be NULL,unless we want a segmentation fault :)
  if(procinfocb == NULL)
    return -1;

  //can't work with a NULL PCB
  if(get_pid(procinfocb->cursor) == NOPROC)
    return -1;
  

  //load the info of the process to procinfocb in order to copy them to buffer
  load_procinfo(procinfocb,cur);


  //memory copy from the procinfocb to the buffer
  memcpy(buf, (char*)&procinfocb->procinfo, sizeof(procinfo));


  //now we have to find the next ALIVE or ZOMBIE process of the PT
  do
  {
    //if this was the last process of the PT
    if(pid+1 == MAX_PROC)
    {
      procinfocb->cursor = NULL;   //make the cursor NULL
      break;                      //get out of the loop and return
    }

    //move forward to the next pid
    pid++;

    //get the next process
    procinfocb->cursor = &PT[pid];

  }while(procinfocb->cursor->pstate == FREE);//do while we find the next not FREE process


  //return the number of the elements copied
  return n;
}


/**
 * @brief Just free the process info control block
 * 
 * @param _procinfocb the process info control block to free
 * @return int return 0 for sucess , -1 for failure
 */
int procinfo_close(void * _procinfocb)
{

  //get the procinfocb
  procinfo_cb* procinfocb = (procinfo_cb*)_procinfocb;

  //check if it is not NULL
  if(procinfocb == NULL)
    return -1;


  //just free the process info control block
  free(procinfocb);


  //if everything went smoothly
  return 0;
}


/**
 * @brief load the data of the new process so them will be printed
 * afterwards
 * 
 * @param procinfocb the process info control block to load data to
 * @param pcb the process's data to load to process info control block
 */
void load_procinfo(procinfo_cb* procinfocb , PCB* pcb)
{

  procinfocb->procinfo.pid = get_pid(pcb);                    //get the process's ID
  procinfocb->procinfo.ppid = get_pid(pcb->parent);           //get the parent's ID
  procinfocb->procinfo.alive = pcb->pstate == ALIVE ? 1 : 0;  //check if the processes is alive or not
  procinfocb->procinfo.thread_count = pcb->thread_count;      //the number of threads this process has
  procinfocb->procinfo.main_task = pcb->main_task;            //the main task of the process
  procinfocb->procinfo.argl = pcb->argl;                      //size of args

  //if we have args
  if(pcb->args != NULL)
  { 
    //if the passed arguments are more than the args can handle
    if(pcb->argl > PROCINFO_MAX_ARGS_SIZE)
      memcpy(procinfocb->procinfo.args, pcb->args, PROCINFO_MAX_ARGS_SIZE); //copy as much as you can
    else
      memcpy(procinfocb->procinfo.args, pcb->args, pcb->argl);              //if the data "fit" inside agrs , then copy all of them
  }
}


/**
 * @brief initialize a procinfo control block,
 * the initial values are going to be replaced
 * afterwards by the process we will print it's
 * information
 * 
 * @param procinfocb the process info control block to initialize
 */
void initialize_procinfo(procinfo_cb* procinfocb)
{

   procinfocb->procinfo.pid = 0;          //we don't know the pid at first , let it be 0
  procinfocb->procinfo.ppid = -1;         //we don't know the father at first , let it be -1
  procinfocb->procinfo.alive = 0;         //not alive
  procinfocb->procinfo.thread_count = 0;  //no threads at first
  procinfocb->procinfo.main_task = NULL;  //don't know the main task
  procinfocb->procinfo.argl = 0;          //no arguments
  procinfocb->cursor = &PT[0];            //cursor at the beginning of the Process Table

}


/**
 * @brief dummy function for open , always return NULL , sysinfo does not support open
 * 
 * @param n 
 * @return void* 
 */
void* procinfo_open(unsigned int n)
{
  return NULL;
}


/**
 * @brief dummy function for write,always return -1,sysinfo does not support write
 * 
 * @param procinfo_cb_t 
 * @param buf 
 * @param n 
 * @return int 
 */
int procinfo_write(void * procinfo_cb_t,const char* buf,unsigned int n)
{
  return -1;
}