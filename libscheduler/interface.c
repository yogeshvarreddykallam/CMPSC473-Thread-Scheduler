#include "api.h"
#include "scheduler.h"

// Interface implementation
// Implement APIs here...
extern pthread_mutex_t scheduler_lock;
extern pthread_cond_t state_changed_cond;
extern int global_time;
extern thread_tcb_t* tcb_array;
extern int active_threads;
extern thread_tcb_t* cpu_thread;
extern queue_t* ready_queue;

int cpu_me(float current_time, int tid, int remaining_time) {
    if (remaining_time == 0) {
        return global_time;
    }
    pthread_mutex_lock(&scheduler_lock);

    thread_tcb_t* my_tcb = &tcb_array[tid];
    if (my_tcb->state == NEW) {
        my_tcb->arrival_time = current_time;
    }
    enqueue(ready_queue, my_tcb);
    my_tcb->state = READY;
    

    while ((cpu_thread != NULL && cpu_thread->tid != tid) 
        || is_queue_empty(ready_queue) || queue_peek(ready_queue)->tid != tid) {
        pthread_cond_wait(&state_changed_cond, &scheduler_lock);
    }

    dequeue(ready_queue);
    cpu_thread = my_tcb;
    my_tcb->state = RUNNING;
    
    //Move to next tick
    global_time++;
    int time_of_return = global_time;

    // Wake up all other waiting threads
    pthread_cond_broadcast(&state_changed_cond);
    
    pthread_mutex_unlock(&scheduler_lock);
    return time_of_return;
    // return 0;
}

int io_me(float current_time, int tid, int duration) {

    return 0;
}

int P(float current_time, int tid, int sem_id) {

    return 0;
}

int V(float current_time, int tid, int sem_id) {

    return 0;
}

void end_me(int tid) {
    pthread_mutex_lock(&scheduler_lock);
    tcb_array[tid].state = TERMINATED;
    active_threads--;
    // Wake up other threads
    if (cpu_thread != NULL && cpu_thread->tid == tid) {
        cpu_thread = NULL;
    }
    pthread_cond_broadcast(&state_changed_cond);
    pthread_mutex_unlock(&scheduler_lock);
}
