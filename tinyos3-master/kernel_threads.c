
#include "tinyos.h"
#include "kernel_sched.h"
#include "kernel_proc.h"
#include "kernel_cc.h"
#include "kernel_streams.h"



/** 
  @brief Create a new thread in the current process.


  @param task the task this thread will execute
  @param argl length of args
  @param args 
  @return The new ptcb

  */
Tid_t sys_CreateThread(Task task, int argl, void* args)
{

  /*
    if there is no task
    the thread cannot be created
  */
  if(task == NULL)
    return NOTHREAD;

  PTCB* ptcb = allocate_ptcb(); //malloc for space allocation

  initialize_ptcb(ptcb,NULL,task,argl, args); //initialize ptcb

  TCB* tcb = spawn_thread(CURPROC,start_child_thread); //create the new child thread
                                                      //run start child thread instead of start main thread

  //connect PTCB to TCB and vice versa
  ptcb->tcb = tcb;
  tcb->ptcb = ptcb;


  //put the new ptcb to pcb's list
  rlist_push_back(&CURPROC->ptcb_list,&ptcb->ptcb_list_node);

  //this process has a new thread,increase the counter
  CURPROC->thread_count++;


  //Checking to make sure
  assert(tcb!=NULL);
  assert(ptcb!=NULL);
  assert(ptcb==tcb->ptcb);
  assert(ptcb->task==task);
  assert(ptcb->argl==argl);
  assert(ptcb->exited==0);
  assert(ptcb->detached==0);
  assert(ptcb->refcount==0);

  //put the new tcb to scheduler's queue
  wakeup(ptcb->tcb);

	return (Tid_t)ptcb;
}

/**
  @brief Return the Tid of the current thread.
 */
Tid_t sys_ThreadSelf()
{
	return (Tid_t) (cur_thread()->ptcb);
}

/**
  @brief Join the given thread.
  */
int sys_ThreadJoin(Tid_t tid, int* exitval)
{
  
  //ptcb of the thread we want to join to
  PTCB* ptcb = (PTCB*)tid;

  //if thread is not a thread of the current process then fail
  if(rlist_find(&CURPROC->ptcb_list,ptcb,NULL) == NULL)
    return -1;

  //if tid given is invalid
  if(ptcb==NULL)
    return -1;

  //we should not be able to join a detached thread
  if(ptcb->detached == 1)
    return -1;

  //we cannot join ourselves
  if(tid == sys_ThreadSelf())
    return -1;


  //the thread we want to join to has one more thread (us) that waits for it
  ptcb->refcount++;

  
  while(!(ptcb->exited || ptcb->detached))
  {
      /*
        Sleep as long as the thread we are waiting for 
        is not exited or detached
      */
      kernel_wait(&ptcb->exit_cv, SCHED_USER);
  }

  //once we are awake we no longer waiting for the thread,decrease the counter
  ptcb->refcount--;

  /*
    if the thread we want to join gets 
    detached while we are asleep, then fail
    the join procedure
  */
  if((ptcb-> detached) == 1)
  {
    return -1;
  }


  /*
    If there is an exit value then store it
  */
  if(exitval != NULL)
  {
    *exitval = ptcb->exitval;
  }

  /*
    If we are the last thread that waits
    then free the space and remove the thread from
    the PCB's ptcb list
  */
  if(ptcb->refcount == 0)
  {
    rlist_remove(&ptcb->ptcb_list_node);
    free(ptcb);
  }

  //if everything went smoothly
	return 0;
}

/**
  @brief Detach the given thread.
  */
int sys_ThreadDetach(Tid_t tid)
{
  PTCB* ptcb = (PTCB*)tid;

  if(ptcb==NULL)
    return -1;


  if(rlist_find(&CURPROC->ptcb_list,ptcb,NULL) == NULL)
    return -1;

  if(ptcb->exited == 1)
      return -1;

  ptcb->detached = 1;

  ptcb->refcount = 0;

  kernel_broadcast(&ptcb->exit_cv);


	return 0;
}

/**
  @brief Terminate the current thread.
  */
void sys_ThreadExit(int exitval)
{
    PCB *curproc = CURPROC; 

    //test
    assert(curproc->thread_count>=1);

    PTCB* ptcb = cur_thread()->ptcb;
    ptcb->exited = 1;
    ptcb->exitval = exitval;

    curproc->thread_count--;

    kernel_broadcast(&ptcb->exit_cv);


    if (curproc->thread_count == 0) {
        /* Reparent any children of the exiting process to the 
        initial task */
      
      if(get_pid(curproc)!=1) {
        PCB* initpcb = get_pcb(1);
        while(!is_rlist_empty(& curproc->children_list)) {
          rlnode* child = rlist_pop_front(& curproc->children_list);
          child->pcb->parent = initpcb;
          rlist_push_front(& initpcb->children_list, child);
        }

        /* Add exited children to the initial task's exited list 
          and signal the initial task */
        if(!is_rlist_empty(& curproc->exited_list)) {
          rlist_append(& initpcb->exited_list, &curproc->exited_list);
          kernel_broadcast(& initpcb->child_exit);
        }

        /* Put me into my parent's exited list */
        rlist_push_front(& curproc->parent->exited_list, &curproc->exited_node);
        kernel_broadcast(& curproc->parent->child_exit);


        assert(is_rlist_empty(& curproc->children_list));
        assert(is_rlist_empty(& curproc->exited_list));


        /* 
          Do all the other cleanup we want here, close files etc. 
        */

        /* Release the args data */
        if(curproc->args) {
          free(curproc->args);
          curproc->args = NULL;
        }

        /* Clean up FIDT */
        for(int i=0;i<MAX_FILEID;i++) {
          if(curproc->FIDT[i] != NULL) {
            FCB_decref(curproc->FIDT[i]);
            curproc->FIDT[i] = NULL;
          }
        }


        /* Disconnect my main_thread */
        curproc->main_thread = NULL;

        /* Now, mark the process as exited. */
        curproc->pstate = ZOMBIE;

        }
    }

    //curproc->thread_count--;

    //kernel_broadcast(&ptcb->exit_cv);
    /* Bye-bye cruel world */
    kernel_sleep(EXITED, SCHED_USER);
}