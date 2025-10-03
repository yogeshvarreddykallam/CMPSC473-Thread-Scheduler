#include "api.h"
#include "scheduler.h"
#include <math.h>

// Interface implementation
// Implement APIs here...
extern pthread_mutex_t scheduler_lock;
extern pthread_cond_t state_changed_cond;
extern int global_time;
extern thread_tcb_t* tcb_array;
extern int active_threads;
extern thread_tcb_t* cpu_thread;
extern queue_t* ready_queue;
extern semaphore_t sem_array[MAX_NUM_SEM];

int cpu_me(float current_time, int tid, int remaining_time) {
    if (remaining_time == 0 || tcb_array[tid].state == TERMINATED) {
        return global_time;
    }
    pthread_mutex_lock(&scheduler_lock);
    thread_tcb_t* my_tcb = &tcb_array[tid];
    if (my_tcb->state == NEW) {
        current_time = ceilf(current_time);
        current_time = (int)current_time;
        while(global_time < current_time) {
            pthread_cond_wait(&state_changed_cond, &scheduler_lock);
        }
        my_tcb->arrival_time = current_time;
        my_tcb->state = READY;
        enqueue(ready_queue, my_tcb);
    }

    while ((cpu_thread != NULL && cpu_thread->tid != tid) 
        || (cpu_thread == NULL && (is_queue_empty(ready_queue) || queue_peek(ready_queue)->tid != tid))) {
        pthread_cond_wait(&state_changed_cond, &scheduler_lock);
    }

    if(cpu_thread == NULL) {
        dequeue(ready_queue);
        cpu_thread = my_tcb;
        my_tcb->state = RUNNING;
    }
    
    //Move to next tick
    global_time++;
    
    int time_of_return = global_time;
    pthread_cond_broadcast(&state_changed_cond);
    pthread_mutex_unlock(&scheduler_lock);
    return time_of_return;
}

int io_me(float current_time, int tid, int duration) {
    return 0;
}

int P(float current_time, int tid, int sem_id) {
    pthread_mutex_lock(&scheduler_lock);

    thread_tcb_t *my_tcb = &tcb_array[tid];
    semaphore_t  *sem = &sem_array[sem_id];

    sem->val--;
    if (sem->val < 0) {
        my_tcb->state = BLOCKED;
        enqueue(&sem->wait_queue, my_tcb);   
        if (cpu_thread && cpu_thread->tid == tid) cpu_thread = NULL;
        pthread_cond_broadcast(&state_changed_cond);

        while (my_tcb->state == BLOCKED) {
            pthread_cond_wait(&state_changed_cond, &scheduler_lock);
        }
        int time_of_return = global_time;
        pthread_mutex_unlock(&scheduler_lock);
        return time_of_return;
    }

    int time_of_return = global_time;
    pthread_cond_broadcast(&state_changed_cond);
    pthread_mutex_unlock(&scheduler_lock);
    return time_of_return;
}

int V(float current_time, int tid, int sem_id) {
    pthread_mutex_lock(&scheduler_lock);

    thread_tcb_t *my_tcb  = &tcb_array[tid];
    semaphore_t  *sem = &sem_array[sem_id];

    sem->val++;
    if (sem->val <= 0 && !is_queue_empty(&sem->wait_queue)) {
        thread_tcb_t *tmp = dequeue(&sem->wait_queue);
        tmp->state = READY;
        tmp->arrival_time = global_time;
        enqueue(ready_queue, tmp);
    }

    my_tcb->state = READY;   // Reenqueue as READY at the same tick
    my_tcb->arrival_time = global_time;
    enqueue(ready_queue, my_tcb);
    cpu_thread = NULL; 

    int time_of_return = global_time;
    pthread_cond_broadcast(&state_changed_cond);
    pthread_mutex_unlock(&scheduler_lock);
    return time_of_return;
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
