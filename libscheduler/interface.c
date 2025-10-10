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
extern int total_threads;
extern int io_busy_until;

int cpu_me(float current_time, int tid, int remaining_time) {
    if (remaining_time == 0 || tcb_array[tid].state == TERMINATED) {
        return global_time;
    }
    pthread_mutex_lock(&scheduler_lock);
    thread_tcb_t* my_tcb = &tcb_array[tid];

    // Pushing any IO completions due now into READY.
    for (int i = 0; i < total_threads; ++i) {
        thread_tcb_t* t = &tcb_array[i];
        if (t->state == BLOCKED && isfinite(t->io_finish_time) && (int)t->io_finish_time <= global_time) {
            t->state = READY;                  
            t->arrival_time = global_time;     
            enqueue(ready_queue, t);
            pthread_cond_signal(&t->cond);     // wake the thread waiting in io_me
        }
    }

    if (my_tcb->state == NEW) {
        current_time = ceilf(current_time);
        current_time = (int)current_time;
        my_tcb->arrival_time = current_time;

        // If no one is running and nothing is ready we will increment time to next one.
        if (global_time < my_tcb->arrival_time && cpu_thread == NULL && is_queue_empty(ready_queue)) {
            float next_time = my_tcb->arrival_time;
            for (int i = 0; i < total_threads; ++i) {
                if (tcb_array[i].state == NEW && isfinite(tcb_array[i].arrival_time) && tcb_array[i].arrival_time > global_time && tcb_array[i].arrival_time < next_time) {
                    next_time = tcb_array[i].arrival_time;
                }
                if (tcb_array[i].state == BLOCKED && isfinite(tcb_array[i].io_finish_time) && tcb_array[i].io_finish_time > global_time && tcb_array[i].io_finish_time < next_time) {
                    next_time = tcb_array[i].io_finish_time;
                }
            }
            if (isfinite(next_time)) {
                global_time = (int)next_time;
                // Converting any IO completions due now into READY so they have advantage when ties over same-time arrivals by TID.
                for (int i = 0; i < total_threads; ++i) {
                    thread_tcb_t* t = &tcb_array[i];
                    if (t->state == BLOCKED && isfinite(t->io_finish_time) && (int)t->io_finish_time <= global_time) {
                        t->state = READY;
                        t->arrival_time = global_time;
                        enqueue(ready_queue, t);
                        pthread_cond_signal(&t->cond);
                    }
                }
                for (int i = 0; i < total_threads; ++i) {
                    if (tcb_array[i].state == NEW && isfinite(tcb_array[i].arrival_time) && (int)tcb_array[i].arrival_time <= global_time) {
                        pthread_cond_signal(&tcb_array[i].cond);
                    }
                    if (tcb_array[i].state == BLOCKED && isfinite(tcb_array[i].io_finish_time) && (int)tcb_array[i].io_finish_time <= global_time) {
                        pthread_cond_signal(&tcb_array[i].cond);
                    }
                }
            }
        }

        while (global_time < my_tcb->arrival_time) {
            pthread_cond_wait(&my_tcb->cond, &scheduler_lock);
        }
        my_tcb->state = READY;
        enqueue(ready_queue, my_tcb);
        if (cpu_thread == NULL && !is_queue_empty(ready_queue) && queue_peek(ready_queue)->tid == tid) { // CPU is idle
            dequeue(ready_queue);
            cpu_thread = my_tcb;
            my_tcb->state = RUNNING;
        }
    }

    while ((cpu_thread != NULL && cpu_thread->tid != tid) 
        || (cpu_thread == NULL && (is_queue_empty(ready_queue) || queue_peek(ready_queue)->tid != tid))) {
        pthread_cond_wait(&my_tcb->cond, &scheduler_lock);
    }

    if (cpu_thread == NULL) {
        dequeue(ready_queue);
        cpu_thread = my_tcb;
        my_tcb->state = RUNNING;
    }

    // Move to next tick
    global_time++;
    for (int i = 0; i < total_threads; ++i) {
        thread_tcb_t* t = &tcb_array[i];
        if (t->state == BLOCKED && isfinite(t->io_finish_time) && (int)t->io_finish_time <= global_time) {
            t->state = READY;
            t->arrival_time = global_time;
            enqueue(ready_queue, t);
            pthread_cond_signal(&t->cond);
        }
    }
    for (int i = 0; i < total_threads; ++i) {
        if (tcb_array[i].state == NEW && isfinite(tcb_array[i].arrival_time) && (int)tcb_array[i].arrival_time <= global_time) {
            pthread_cond_signal(&tcb_array[i].cond);
        }
    }
    for (int i = 0; i < total_threads; ++i) {
        if (tcb_array[i].state == BLOCKED && isfinite(tcb_array[i].io_finish_time) && (int)tcb_array[i].io_finish_time <= global_time) {
            pthread_cond_signal(&tcb_array[i].cond);
        }
    }

    int time_of_return = global_time;
    pthread_mutex_unlock(&scheduler_lock);
    return time_of_return;
}

int io_me(float current_time, int tid, int duration) {
    pthread_mutex_lock(&scheduler_lock);

    thread_tcb_t *my_tcb = &tcb_array[tid];
    int finish_time = -1;

    if (my_tcb->state == NEW) {
        int at = (int)ceilf(current_time);
        my_tcb->arrival_time = at;
        if (global_time < at && cpu_thread == NULL && is_queue_empty(ready_queue)) {
            float next_time = at;
            for (int i = 0; i < total_threads; ++i) {
                if (tcb_array[i].state == NEW && isfinite(tcb_array[i].arrival_time) && tcb_array[i].arrival_time > global_time && tcb_array[i].arrival_time < next_time) {
                    next_time = tcb_array[i].arrival_time;
                }
                if (tcb_array[i].state == BLOCKED && isfinite(tcb_array[i].io_finish_time) && tcb_array[i].io_finish_time > global_time && tcb_array[i].io_finish_time < next_time) {
                    next_time = tcb_array[i].io_finish_time;
                }
            }
            if (isfinite(next_time)) {
                global_time = (int)next_time;
                for (int i = 0; i < total_threads; ++i) {
                    thread_tcb_t* t = &tcb_array[i];
                    if (t->state == BLOCKED && isfinite(t->io_finish_time) && (int)t->io_finish_time <= global_time) {
                        t->state = READY;
                        t->arrival_time = global_time;
                        enqueue(ready_queue, t);
                        pthread_cond_signal(&t->cond);
                    }
                }
                for (int i = 0; i < total_threads; ++i) {
                    if (tcb_array[i].state == NEW && isfinite(tcb_array[i].arrival_time) && (int)tcb_array[i].arrival_time <= global_time) {
                        pthread_cond_signal(&tcb_array[i].cond);
                    }
                    if (tcb_array[i].state == BLOCKED && isfinite(tcb_array[i].io_finish_time) && (int)tcb_array[i].io_finish_time <= global_time) {
                        pthread_cond_signal(&tcb_array[i].cond);
                    }
                }
                if (cpu_thread == NULL && !is_queue_empty(ready_queue)) {
                    while (!is_queue_empty(ready_queue)) {
                        thread_tcb_t* h = queue_peek(ready_queue);
                        if (h && h->state == READY) break;
                        dequeue(ready_queue);
                    }
                    if (!is_queue_empty(ready_queue)) {
                        thread_tcb_t *n = queue_peek(ready_queue);
                        pthread_cond_signal(&n->cond);
                    }
                }
            } else {
                thread_tcb_t *n = queue_peek(ready_queue);
                if (n) pthread_cond_signal(&n->cond);
            }
        } else {
            bool flag = false;
            for (int i = 0; i < total_threads; ++i) {
                if (tcb_array[i].state == NEW && isfinite(tcb_array[i].arrival_time) && (int)tcb_array[i].arrival_time <= global_time) {
                    pthread_cond_signal(&tcb_array[i].cond);
                    flag = true;
                }
            }
            for (int i = 0; i < total_threads; ++i) {
                if (tcb_array[i].state == BLOCKED && isfinite(tcb_array[i].io_finish_time) && (int)tcb_array[i].io_finish_time <= global_time) {
                    pthread_cond_signal(&tcb_array[i].cond);
                    flag = true;
                }
            }
            if (!flag) {
                // Fast-forward time only if all arrivals are known
                bool unknown_new_exists = false;
                for (int i = 0; i < total_threads; ++i) {
                    if (tcb_array[i].state == NEW && !isfinite(tcb_array[i].arrival_time)) {
                        unknown_new_exists = true;
                        break;
                    }
                }
                if (!unknown_new_exists) {
                    float next_time = INFINITY;
                    for (int i = 0; i < total_threads; ++i) {
                        if (tcb_array[i].state == NEW && isfinite(tcb_array[i].arrival_time) && tcb_array[i].arrival_time > global_time && tcb_array[i].arrival_time < next_time) {
                            next_time = tcb_array[i].arrival_time;
                        }
                        if (tcb_array[i].state == BLOCKED && isfinite(tcb_array[i].io_finish_time) && tcb_array[i].io_finish_time > global_time && tcb_array[i].io_finish_time < next_time) {
                            next_time = tcb_array[i].io_finish_time;
                        }
                    }
                    if (isfinite(next_time)) {
                        global_time = (int)next_time;
                        for (int i = 0; i < total_threads; ++i) {
                            thread_tcb_t* t = &tcb_array[i];
                            if (t->state == BLOCKED && isfinite(t->io_finish_time) && (int)t->io_finish_time <= global_time) {
                                t->state = READY;
                                t->arrival_time = global_time;
                                enqueue(ready_queue, t);
                                pthread_cond_signal(&t->cond);
                            }
                        }
                        for (int i = 0; i < total_threads; ++i) {
                            if (tcb_array[i].state == NEW && isfinite(tcb_array[i].arrival_time) && (int)tcb_array[i].arrival_time <= global_time) {
                                pthread_cond_signal(&tcb_array[i].cond);
                            }
                            if (tcb_array[i].state == BLOCKED && isfinite(tcb_array[i].io_finish_time) && (int)tcb_array[i].io_finish_time <= global_time) {
                                pthread_cond_signal(&tcb_array[i].cond);
                            }
                        }
                        // If CPU is idle and someone is READY now.
                        if (cpu_thread == NULL && !is_queue_empty(ready_queue)) {
                            while (!is_queue_empty(ready_queue)) {
                                thread_tcb_t* h = queue_peek(ready_queue);
                                if (h && h->state == READY) break;
                                dequeue(ready_queue);
                            }
                            if (!is_queue_empty(ready_queue)) {
                                thread_tcb_t *n = queue_peek(ready_queue);
                                pthread_cond_signal(&n->cond);
                            }
                        }
                    }
                }
            }
        }
        my_tcb->state = READY;  // Arrived but do not enqueue for IO
    }

    // For FCFS, we own the CPU before starting IO.
    if (!(cpu_thread && cpu_thread->tid == tid)) {
        if (my_tcb->state != READY) {
            my_tcb->state = READY;
            my_tcb->arrival_time = global_time;
            enqueue(ready_queue, my_tcb);
        }
        while ((cpu_thread != NULL && cpu_thread->tid != tid)
            || (cpu_thread == NULL && (is_queue_empty(ready_queue) || queue_peek(ready_queue)->tid != tid))) {
            pthread_cond_wait(&my_tcb->cond, &scheduler_lock);
        }
        if (cpu_thread == NULL) {
            dequeue(ready_queue);
            cpu_thread = my_tcb;
            my_tcb->state = RUNNING;
        }
    }
    if (!(cpu_thread && cpu_thread->tid == tid)) {
        while ((cpu_thread != NULL && cpu_thread->tid != tid)
            || (cpu_thread == NULL && (is_queue_empty(ready_queue) || queue_peek(ready_queue)->tid != tid))) {
            pthread_cond_wait(&my_tcb->cond, &scheduler_lock);
        }
        if (cpu_thread == NULL) {
            dequeue(ready_queue);
            cpu_thread = my_tcb;
            my_tcb->state = RUNNING;
        }
    }
    int start_time;
    if(global_time > io_busy_until){
        start_time = global_time;
    } else {
        start_time = io_busy_until;
    }
    finish_time = start_time + duration;
    io_busy_until = finish_time; 

    my_tcb->io_finish_time = finish_time;
    my_tcb->state = BLOCKED;

    if (cpu_thread && cpu_thread->tid == tid) { // Release CPU as we blocked for IO
        cpu_thread = NULL;
        if (!is_queue_empty(ready_queue)) {
            while (!is_queue_empty(ready_queue)) {
                thread_tcb_t* h = queue_peek(ready_queue);
                if (h && h->state == READY) break;
                dequeue(ready_queue);
            }
            if (!is_queue_empty(ready_queue)) {
                thread_tcb_t *n = queue_peek(ready_queue);
                if (n) pthread_cond_signal(&n->cond);
            }
        } else {
            bool flag = false;
            for (int i = 0; i < total_threads; ++i) {
                if (tcb_array[i].state == NEW && isfinite(tcb_array[i].arrival_time) && (int)tcb_array[i].arrival_time <= global_time) {
                    pthread_cond_signal(&tcb_array[i].cond);
                    flag = true;
                }
            }
            for (int i = 0; i < total_threads; ++i) {
                if (tcb_array[i].state == BLOCKED && isfinite(tcb_array[i].io_finish_time) && (int)tcb_array[i].io_finish_time <= global_time) {
                    pthread_cond_signal(&tcb_array[i].cond);
                    flag = true;
                }
            }
            if (!flag) {
                bool unknown_new_exists = false;
                for (int i = 0; i < total_threads; ++i) {
                    if (tcb_array[i].state == NEW && !isfinite(tcb_array[i].arrival_time)) {
                        unknown_new_exists = true;
                        break;
                    }
                }
                if (!unknown_new_exists) {
                    float next_time = INFINITY;
                    for (int i = 0; i < total_threads; ++i) {
                        if (tcb_array[i].state == NEW && isfinite(tcb_array[i].arrival_time) && tcb_array[i].arrival_time > global_time && tcb_array[i].arrival_time < next_time) {
                            next_time = tcb_array[i].arrival_time;
                        }
                        if (tcb_array[i].state == BLOCKED && isfinite(tcb_array[i].io_finish_time) && tcb_array[i].io_finish_time > global_time && tcb_array[i].io_finish_time < next_time) {
                            next_time = tcb_array[i].io_finish_time;
                        }
                    }
                    if (isfinite(next_time)) {
                        global_time = (int)next_time;
                        for (int i = 0; i < total_threads; ++i) {
                            if (tcb_array[i].state == NEW && isfinite(tcb_array[i].arrival_time) && (int)tcb_array[i].arrival_time <= global_time) {
                                pthread_cond_signal(&tcb_array[i].cond);
                            }
                            if (tcb_array[i].state == BLOCKED && isfinite(tcb_array[i].io_finish_time) && (int)tcb_array[i].io_finish_time <= global_time) {
                                pthread_cond_signal(&tcb_array[i].cond);
                            }
                        }
                        if (cpu_thread == NULL && !is_queue_empty(ready_queue)) {
                            while (!is_queue_empty(ready_queue)) {
                                thread_tcb_t* h = queue_peek(ready_queue);
                                if (h && h->state == READY) break;
                                dequeue(ready_queue);
                            }
                            if (!is_queue_empty(ready_queue)) {
                                thread_tcb_t *n2 = queue_peek(ready_queue);
                                pthread_cond_signal(&n2->cond);
                            }
                        }
                    }
                }
            }
        }
    }
    queue_remove(ready_queue, tid);   //READY_Q has this as present then remove before blocking.

    while (global_time < my_tcb->io_finish_time) {
        pthread_cond_wait(&my_tcb->cond, &scheduler_lock);
    }

    //Avoiding double-enqueue
    if (my_tcb->state != BLOCKED) {
        my_tcb->io_finish_time = INFINITY;  // set to infinity to clear stale IO duration
        int time_of_return = finish_time;
        pthread_mutex_unlock(&scheduler_lock);
        return time_of_return;
    }

    // After IO is completed, clear IO timer to avoid mislead.
    my_tcb->io_finish_time = INFINITY;

    my_tcb->state = READY;
    my_tcb->arrival_time = global_time;
    enqueue(ready_queue, my_tcb);
    if (cpu_thread == NULL && !is_queue_empty(ready_queue) && queue_peek(ready_queue)->tid == tid) {
        pthread_cond_signal(&my_tcb->cond);
    }
    int time_of_return = finish_time;
    pthread_mutex_unlock(&scheduler_lock);
    return time_of_return;
}

int P(float current_time, int tid, int sem_id) {
    pthread_mutex_lock(&scheduler_lock);

    thread_tcb_t *my_tcb = &tcb_array[tid];
    semaphore_t  *sem = &sem_array[sem_id];

    sem->val--;

    if (sem->val < 0) {
        my_tcb->io_finish_time = INFINITY;
        queue_remove(ready_queue, tid);
        my_tcb->state = BLOCKED;
        enqueue(&sem->wait_queue, my_tcb);
        if (cpu_thread && cpu_thread->tid == tid) {
            cpu_thread = NULL;
            if (!is_queue_empty(ready_queue)) {
                while (!is_queue_empty(ready_queue)) {
                    thread_tcb_t* h = queue_peek(ready_queue);
                    if (h && h->state == READY) break;
                    dequeue(ready_queue);
                }
                if (!is_queue_empty(ready_queue)) {
                    thread_tcb_t *n = queue_peek(ready_queue);
                    pthread_cond_signal(&n->cond);
                }
            } else {
                bool flag = false;
                for (int i = 0; i < total_threads; ++i) {
                    if (tcb_array[i].state == NEW && isfinite(tcb_array[i].arrival_time) && (int)tcb_array[i].arrival_time <= global_time) {
                        pthread_cond_signal(&tcb_array[i].cond);
                        flag = true;
                    }
                }
                for (int i = 0; i < total_threads; ++i) {
                    if (tcb_array[i].state == BLOCKED && isfinite(tcb_array[i].io_finish_time) && (int)tcb_array[i].io_finish_time <= global_time) {
                        pthread_cond_signal(&tcb_array[i].cond);
                        flag = true;
                    }
                }
                if (!flag) {
                    bool unknown_new_exists = false;
                    for (int i = 0; i < total_threads; ++i) {
                        if (tcb_array[i].state == NEW && !isfinite(tcb_array[i].arrival_time)) {
                            unknown_new_exists = true;
                            break;
                        }
                    }
                    if (!unknown_new_exists) {
                        float next_time = INFINITY;
                        for (int i = 0; i < total_threads; ++i) {
                            if (tcb_array[i].state == NEW && isfinite(tcb_array[i].arrival_time) && tcb_array[i].arrival_time > global_time && tcb_array[i].arrival_time < next_time) {
                                next_time = tcb_array[i].arrival_time;
                            }
                            if (tcb_array[i].state == BLOCKED && isfinite(tcb_array[i].io_finish_time) && tcb_array[i].io_finish_time > global_time && tcb_array[i].io_finish_time < next_time) {
                                next_time = tcb_array[i].io_finish_time;
                            }
                        }
                        if (isfinite(next_time)) {
                            global_time = (int)next_time;
                            for (int i = 0; i < total_threads; ++i) {
                                if (tcb_array[i].state == NEW && isfinite(tcb_array[i].arrival_time) && (int)tcb_array[i].arrival_time <= global_time) {
                                    pthread_cond_signal(&tcb_array[i].cond);
                                }
                                if (tcb_array[i].state == BLOCKED && isfinite(tcb_array[i].io_finish_time) && (int)tcb_array[i].io_finish_time <= global_time) {
                                    pthread_cond_signal(&tcb_array[i].cond);
                                }
                            }
                        }
                    }
                }
            }
        }
        while (my_tcb->state == BLOCKED) {
            pthread_cond_wait(&my_tcb->cond, &scheduler_lock);
        }
        int time_of_return = global_time;
        pthread_mutex_unlock(&scheduler_lock);
        return time_of_return;
    }

    int time_of_return = global_time;
    pthread_mutex_unlock(&scheduler_lock);
    return time_of_return;
}

int V(float current_time, int tid, int sem_id) {
    pthread_mutex_lock(&scheduler_lock);

    thread_tcb_t *my_tcb  = &tcb_array[tid];
    semaphore_t  *sem = &sem_array[sem_id];

    int at = (int)ceilf(current_time);
    if (my_tcb->state == NEW && !isfinite(my_tcb->arrival_time)) {
        my_tcb->arrival_time = at;
    }
    if (global_time < at && cpu_thread == NULL && is_queue_empty(ready_queue)) {
        float next_time = at;
        for (int i = 0; i < total_threads; ++i) {
            if (tcb_array[i].state == NEW && isfinite(tcb_array[i].arrival_time) && tcb_array[i].arrival_time > global_time && tcb_array[i].arrival_time < next_time) {
                next_time = tcb_array[i].arrival_time;
            }
            if (tcb_array[i].state == BLOCKED && isfinite(tcb_array[i].io_finish_time) && tcb_array[i].io_finish_time > global_time && tcb_array[i].io_finish_time < next_time) {
                next_time = tcb_array[i].io_finish_time;
            }
        }
        if (isfinite(next_time)) {
            global_time = (int)next_time;
            for (int i = 0; i < total_threads; ++i) {
                if (tcb_array[i].state == NEW && isfinite(tcb_array[i].arrival_time) && (int)tcb_array[i].arrival_time <= global_time) {
                    pthread_cond_signal(&tcb_array[i].cond);
                }
                if (tcb_array[i].state == BLOCKED && isfinite(tcb_array[i].io_finish_time) && (int)tcb_array[i].io_finish_time <= global_time) {
                    pthread_cond_signal(&tcb_array[i].cond);
                }
            }
        }
    }

    sem->val++;
    if (sem->val <= 0 && !is_queue_empty(&sem->wait_queue)) {
        thread_tcb_t *tmp = dequeue(&sem->wait_queue);
        tmp->state = READY;
        tmp->arrival_time = global_time;
        enqueue(ready_queue, tmp);
        pthread_cond_signal(&tmp->cond);  // waking up the next unblocked thread
    }
    my_tcb->state = READY;
    my_tcb->arrival_time = global_time;
    enqueue(ready_queue, my_tcb);
    if (cpu_thread && cpu_thread->tid == tid) {
        cpu_thread = NULL;
    }
    if (!is_queue_empty(ready_queue)) {
        while (!is_queue_empty(ready_queue)) {
            thread_tcb_t* h = queue_peek(ready_queue);
            if (h && h->state == READY) break;
            dequeue(ready_queue);
        }
        if (!is_queue_empty(ready_queue)) {
            thread_tcb_t *n = queue_peek(ready_queue);
            pthread_cond_signal(&n->cond);
        }
    }

    int time_of_return = global_time;
    pthread_mutex_unlock(&scheduler_lock);
    return time_of_return;
}

void end_me(int tid) {
    pthread_mutex_lock(&scheduler_lock);
    tcb_array[tid].state = TERMINATED;
    active_threads--;
    if (cpu_thread != NULL && cpu_thread->tid == tid) {
        cpu_thread = NULL;
    }
    queue_remove(ready_queue, tid);
    while (!is_queue_empty(ready_queue)) {
        thread_tcb_t* h = queue_peek(ready_queue);
        if (h && h->state == READY) break;
        dequeue(ready_queue);
    }
    if (!is_queue_empty(ready_queue)) {
        thread_tcb_t *n = queue_peek(ready_queue);
        pthread_cond_signal(&n->cond);
    } else {
        bool flag = false;
        for (int i = 0; i < total_threads; ++i) {
            if (tcb_array[i].state == NEW && isfinite(tcb_array[i].arrival_time) && (int)tcb_array[i].arrival_time <= global_time) {
                pthread_cond_signal(&tcb_array[i].cond);
                flag = true;
            }
        }
        for (int i = 0; i < total_threads; ++i) {
            if (tcb_array[i].state == BLOCKED && isfinite(tcb_array[i].io_finish_time) && (int)tcb_array[i].io_finish_time <= global_time) {
                pthread_cond_signal(&tcb_array[i].cond);
                flag = true;
            }
        }
        if (!flag) {
            bool unknown_new_exists = false;
            for (int i = 0; i < total_threads; ++i) {
                if (tcb_array[i].state == NEW && !isfinite(tcb_array[i].arrival_time)) {
                    unknown_new_exists = true;
                    break;
                }
            }
            if (!unknown_new_exists) {
                float next_time = INFINITY;
                for (int i = 0; i < total_threads; ++i) {
                    if (tcb_array[i].state == NEW && isfinite(tcb_array[i].arrival_time) && tcb_array[i].arrival_time > global_time && tcb_array[i].arrival_time < next_time) {
                        next_time = tcb_array[i].arrival_time;
                    }
                    if (tcb_array[i].state == BLOCKED && isfinite(tcb_array[i].io_finish_time) && tcb_array[i].io_finish_time > global_time && tcb_array[i].io_finish_time < next_time) {
                        next_time = tcb_array[i].io_finish_time;
                    }
                }
                if (isfinite(next_time)) {
                    global_time = (int)next_time;
                    for (int i = 0; i < total_threads; ++i) {
                        if (tcb_array[i].state == NEW && isfinite(tcb_array[i].arrival_time) && (int)tcb_array[i].arrival_time <= global_time) {
                            pthread_cond_signal(&tcb_array[i].cond);
                        }
                        if (tcb_array[i].state == BLOCKED && isfinite(tcb_array[i].io_finish_time) && (int)tcb_array[i].io_finish_time <= global_time) {
                            pthread_cond_signal(&tcb_array[i].cond);
                        }
                    }
                }
            }
        }
    }
    pthread_mutex_unlock(&scheduler_lock);
}
