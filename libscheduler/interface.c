#include "api.h"
#include "scheduler.h"
#include <math.h>
#include <sched.h>

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
extern enum sch_type sch_no;
extern queue_t* io_queue;

// To move any IO finished BLOCKED threads to ready state
static void funcBlocked2Ready(void) {
    for (int i = 0; i < total_threads; ++i) {
        thread_tcb_t* t = &tcb_array[i];
        if (t->state == BLOCKED && isfinite(t->io_finish_time) && (int)t->io_finish_time <= global_time) {
            t->state = READY;                  
            t->arrival_time = global_time;     
            t->rem_srtf_time = INT_MAX;       
            enqueue(ready_queue, t);
            pthread_cond_signal(&t->cond);     
        }
    }
    pthread_cond_signal(&state_changed_cond);
}

//To remove terminated threads(that are not ready) at the head of ready queue.
static void funcRemoveNonReady(void) {
    while (!is_queue_empty(ready_queue)) {
        thread_tcb_t* h = queue_peek(ready_queue);
        if (h && h->state == READY) break;
        dequeue(ready_queue);
    }
}

//To move new threads whose arrival time is already began to ready queue.
static void funcMoveAlreadyNew2Ready(void) {
    bool flag = false;
    for (int i = 0; i < total_threads; ++i) {
        thread_tcb_t* t = &tcb_array[i];
        if (t->state == NEW && isfinite(t->arrival_time) && (int)t->arrival_time <= global_time) {
            t->state = READY;
            enqueue(ready_queue, t);
            pthread_cond_signal(&t->cond);
            flag = true;
        }
    }
    if (flag) pthread_cond_signal(&state_changed_cond);
}

static thread_tcb_t* funcBestSrtf(void) {
    if(ready_queue == NULL) return NULL;
    node_t* curr = ready_queue->head;
    thread_tcb_t* ans = NULL;
    while (curr) {
        thread_tcb_t* t = curr->tcb;
        if (t && t->state == READY && t->rem_srtf_time != INT_MAX) {
            if (!ans || t->rem_srtf_time < ans->rem_srtf_time ||
                (t->rem_srtf_time == ans->rem_srtf_time && t->tid < ans->tid)) {
                ans = t;
            }
        }
        curr = curr->next;
    }
    return ans;
}

// To check for better READY thread than the current running thread for SRTF.
static thread_tcb_t* FindBetterReadySrtf(int my_remaining, int my_tid) {
    if(ready_queue == NULL) return NULL;
    node_t* curr = ready_queue->head;
    thread_tcb_t* ans = NULL;
    while (curr) {
        thread_tcb_t* t = curr->tcb;
        if (t && t->state == READY && t->rem_srtf_time != INT_MAX) {
            if (t->rem_srtf_time < my_remaining ||
                (t->rem_srtf_time == my_remaining && t->tid < my_tid)) {
                if (!ans || t->rem_srtf_time < ans->rem_srtf_time ||
                    (t->rem_srtf_time == ans->rem_srtf_time && t->tid < ans->tid)) {
                    ans = t;
                }
            }
        }
        curr = curr->next;
    }
    return ans;
}

// To check for any lower tid thread(with some unknown remaining time) already existing as READY than the given thread for SRTF.
static bool CheckLowerTidSrtf(int tid) {
    if(ready_queue == NULL) return false;
    node_t* curr = ready_queue->head;
    while (curr) {
        thread_tcb_t* t = curr->tcb;
        if (t && t->state == READY && t->rem_srtf_time == INT_MAX && (int)t->arrival_time <= global_time && t->tid < tid) {
            return true;
        }
        curr = curr->next;
    }
    return false;
}

static bool SetReadySrtf(thread_tcb_t* t) {
    if (!t) return false;
    thread_tcb_t* b = funcBestSrtf();
    if (!b) return false;
    if (CheckLowerTidSrtf(b->tid)) return false;  // If a lower tid is already existing as READY, defer it.
    return b && b->tid == t->tid;
}

static void funcWakeBestReady(void) {
    if (sch_no == SCH_SRTF) {
        thread_tcb_t* n = funcBestSrtf();
        if (n && !CheckLowerTidSrtf(n->tid)) {
            pthread_cond_signal(&n->cond);
        } else {
            pthread_cond_signal(&state_changed_cond);
        }
        return;
    }
    funcRemoveNonReady();   //FCFS
    thread_tcb_t* n = queue_peek(ready_queue);
    if (n) pthread_cond_signal(&n->cond);
}

// To check for any READY thread with known remaining time already existing.
static bool funcReadyKnownRemTime(void) {
    if(ready_queue == NULL) return false;
    node_t* curr = ready_queue->head;
    while (curr) {
        thread_tcb_t* t = curr->tcb;
        if (t && t->state == READY && t->rem_srtf_time != INT_MAX && (int)t->arrival_time <= global_time) {
            return true;
        }
        curr = curr->next;
    }
    return false;
}

// To check if IO_QUEUE already has a lower tid request at the same request time
static bool CheckIOQueuelowTid(int my_tid, int request_time) {
    if (!io_queue) return false;
    node_t* curr = io_queue->head;
    while (curr) {
        thread_tcb_t* t = curr->tcb;
        if (t && (int)t->arrival_time == request_time && t->tid < my_tid) return true;
        curr = curr->next;
    }
    return false;
}

// To check if this given thread id already present in IO Queue.
static bool CheckIOQueueTid(int tid) {
    if (!io_queue) return false;
    node_t* curr = io_queue->head;
    while (curr) {
        if (curr->tcb && curr->tcb->tid == tid) return true;
        curr = curr->next;
    }
    return false;
}

//To check if there is any lower tid thread already existing with arrival<=t and hasn't enqueued IO
static bool CheckLowTidIOStatus(int my_tid, int t) {
    for (int i = 0; i < my_tid; ++i) {
        thread_tcb_t* x = &tcb_array[i];
        if ((x->state == READY || x->state == NEW) && isfinite(x->arrival_time) && (int)x->arrival_time <= t) {
            if (!CheckIOQueueTid(i)) return true;
        }
    }
    return false;
}

int cpu_me(float current_time, int tid, int remaining_time) {
    if (remaining_time == 0 || tcb_array[tid].state == TERMINATED) {
        return global_time;
    }
    pthread_mutex_lock(&scheduler_lock);
    thread_tcb_t* my_tcb = &tcb_array[tid];


    // To check remaining time for SRTF
    int flagUnknown = (my_tcb->rem_srtf_time == INT_MAX);
    my_tcb->rem_srtf_time = remaining_time;

    if (flagUnknown && remaining_time != INT_MAX) {
        pthread_cond_signal(&state_changed_cond); // signaling for transitions from unknown to known.
    }

    funcBlocked2Ready();  // to push any IO completions already existing into READY state.
    funcMoveAlreadyNew2Ready();  // to push any NEW arrived threads before or at this time.

    if (my_tcb->state == NEW) {
        current_time = ceilf(current_time);
        current_time = (int)current_time;
        my_tcb->arrival_time = current_time;
        pthread_cond_signal(&state_changed_cond);

        if (global_time < my_tcb->arrival_time && cpu_thread == NULL && is_queue_empty(ready_queue)) { //If no thread is running and no thread is ready.
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
                funcBlocked2Ready();
                for (int i = 0; i < total_threads; ++i) {
                    if (tcb_array[i].state == NEW && isfinite(tcb_array[i].arrival_time) && (int)tcb_array[i].arrival_time <= global_time) {
                        pthread_cond_signal(&tcb_array[i].cond);
                    }
                    if (tcb_array[i].state == BLOCKED && isfinite(tcb_array[i].io_finish_time) && (int)tcb_array[i].io_finish_time <= global_time) {
                        pthread_cond_signal(&tcb_array[i].cond);
                    }
                }
                funcMoveAlreadyNew2Ready();
            }
        }

        while (global_time < my_tcb->arrival_time) {
            pthread_cond_wait(&my_tcb->cond, &scheduler_lock);
        }
        my_tcb->state = READY;
        enqueue(ready_queue, my_tcb);

        if (cpu_thread == NULL && !is_queue_empty(ready_queue)) {
            bool next = queue_peek(ready_queue)->tid == tid;
            if(sch_no == SCH_SRTF) {
                next = SetReadySrtf(my_tcb);
            }
            if (next) {
                if (sch_no == SCH_SRTF) {
                    queue_remove(ready_queue, tid);
                } else {
                    dequeue(ready_queue);
                }
                cpu_thread = my_tcb;
                my_tcb->state = RUNNING;
            }
        }
    }

    if (sch_no == SCH_SRTF) {
        while (1) {
            if (cpu_thread == my_tcb) {
                thread_tcb_t* better_now = FindBetterReadySrtf(my_tcb->rem_srtf_time, tid);
                if (!better_now) {
                    break;
                }
                // Do preempt to move self -> READY with arrival now and wake the best thread.
                cpu_thread = NULL;
                my_tcb->state = READY;
                my_tcb->arrival_time = global_time;
                enqueue(ready_queue, my_tcb);
                funcWakeBestReady();
                pthread_cond_signal(&state_changed_cond);
            } 
            else if (cpu_thread == NULL) {
                if (!is_queue_empty(ready_queue) && SetReadySrtf(my_tcb)) {
                    break;
                }
            } 
            else {
                // Means someone else is running  -> keep waiting
            }

            // To wait for either a time/state change or targeted wakeup
            if (!funcReadyKnownRemTime()) {
                pthread_cond_wait(&state_changed_cond, &scheduler_lock);
            } else {
                pthread_cond_wait(&my_tcb->cond, &scheduler_lock);
            }
        }
    } else {
        while ((cpu_thread != NULL && cpu_thread->tid != tid) 
            || (cpu_thread == NULL && (is_queue_empty(ready_queue) || queue_peek(ready_queue)->tid != tid))) {
            pthread_cond_wait(&my_tcb->cond, &scheduler_lock);
        }
    }

    if (cpu_thread == NULL) {
        if (sch_no == SCH_SRTF) {
            // To pick the best known SRTF thread if exists(we will ignore unknowns unless we find a lower-tid unknown already existing.)
            thread_tcb_t* best = funcBestSrtf();
            if (best && !CheckLowerTidSrtf(best->tid)) {
                cpu_thread = best;
                queue_remove(ready_queue, best->tid);
            } else {
                pthread_mutex_unlock(&scheduler_lock);
                sched_yield(); //to let the scheduler know that other threads run.
                pthread_mutex_lock(&scheduler_lock);
            }
        } else {
            cpu_thread = dequeue(ready_queue);
        }

        my_tcb = &tcb_array[tid];

        if (cpu_thread == my_tcb) {
            my_tcb->state = RUNNING;
        }
    }

    // Move to next tick

    // Move to next tick
    global_time++;

    // To update remaining time for SRTF
    if (sch_no == SCH_SRTF) {
        if (tcb_array[tid].rem_srtf_time > 0 && tcb_array[tid].state == RUNNING) {
            tcb_array[tid].rem_srtf_time = remaining_time - 1;
        }
    }

    // To push any IO completions that became due at this new tick.
    funcBlocked2Ready();
    funcMoveAlreadyNew2Ready();

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

    // For SRTF - we preempt only if a strictly better READY thread exists (for next tick)
    // and compare against my remaining updated time.
    if (sch_no == SCH_SRTF) {
        thread_tcb_t* me = &tcb_array[tid];
        int remainticks = remaining_time - 1;
        if (remainticks < 0) remainticks = 0;
        thread_tcb_t* better = FindBetterReadySrtf(remainticks, tid);
        if (better && me->rem_srtf_time > 0) {
            if (cpu_thread && cpu_thread->tid == tid) {
                cpu_thread = NULL;
            }
            me->state = READY;
            me->arrival_time = global_time;
            enqueue(ready_queue, me);
            funcWakeBestReady();
            pthread_cond_signal(&state_changed_cond);
        } 
        else {
            cpu_thread = me;
            me->state = RUNNING;
        }
    }

    int time_of_return = global_time;

    pthread_mutex_unlock(&scheduler_lock);

    if (sch_no == SCH_SRTF) sched_yield();
    return time_of_return;
}

int io_me(float current_time, int tid, int duration) {
    pthread_mutex_lock(&scheduler_lock);

    thread_tcb_t *my_tcb = &tcb_array[tid];
    int finish_time = -1; 

    if (my_tcb->state == NEW) {
        int at = (int)ceilf(current_time);
        my_tcb->arrival_time = at;
        pthread_cond_signal(&state_changed_cond);
    }

    queue_remove(ready_queue, tid);
    my_tcb->state = BLOCKED;
    my_tcb->rem_srtf_time = INT_MAX;

    int request_time = (int)ceilf(current_time);
    my_tcb->arrival_time = request_time;
    enqueue(io_queue, my_tcb);
    pthread_cond_signal(&state_changed_cond);

    if (cpu_thread && cpu_thread->tid == tid) {
        cpu_thread = NULL;
        if (!is_queue_empty(ready_queue)) funcWakeBestReady();
    }

    // Only the head of the io_queue may start IO.
    // If not head, wait to be signaled.
    while(1){
        while (io_queue->head == NULL || queue_peek(io_queue)->tid != tid) {
            pthread_cond_wait(&state_changed_cond, &scheduler_lock);
        }
        
        while (global_time < request_time) {
            pthread_cond_wait(&state_changed_cond, &scheduler_lock);
        }
        if (io_busy_until <= request_time) { // If IO device is idle
            if (CheckLowTidIOStatus(tid, request_time)) { // chance is given to lower tid threads
                pthread_cond_wait(&state_changed_cond, &scheduler_lock);
                continue;
            }
            if (CheckIOQueuelowTid(tid, request_time)) {
                pthread_cond_wait(&state_changed_cond, &scheduler_lock);
                continue;
            }
        }
        break;
    }

    int start_time = 0;
    if(io_busy_until < request_time) {
        start_time = request_time;
    }
    else{
        start_time = io_busy_until;
    }
    finish_time = start_time + duration;
    io_busy_until = finish_time;

    my_tcb->io_finish_time = finish_time;

    dequeue(io_queue);
    if (!is_queue_empty(io_queue)) {
        pthread_cond_signal(&state_changed_cond);
    }
    if (cpu_thread == NULL) {
        funcRemoveNonReady();
        if (is_queue_empty(ready_queue)) {
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
                funcBlocked2Ready();
                funcMoveAlreadyNew2Ready();
                for (int i = 0; i < total_threads; ++i) {
                    if (tcb_array[i].state == NEW && isfinite(tcb_array[i].arrival_time) && (int)tcb_array[i].arrival_time <= global_time) {
                        pthread_cond_signal(&tcb_array[i].cond);
                    }
                    if (tcb_array[i].state == BLOCKED && isfinite(tcb_array[i].io_finish_time) && (int)tcb_array[i].io_finish_time <= global_time) {
                        pthread_cond_signal(&tcb_array[i].cond);
                    }
                }
                pthread_cond_signal(&state_changed_cond);
            }
        }
    }

    while (global_time < my_tcb->io_finish_time) {
        pthread_cond_wait(&my_tcb->cond, &scheduler_lock);
    }

    if (my_tcb->state != BLOCKED) { // After IO completion push to READY as before
        my_tcb->io_finish_time = INFINITY;
        int time_of_return = finish_time;
        pthread_mutex_unlock(&scheduler_lock);
        return time_of_return;
    }

    my_tcb->io_finish_time = INFINITY;
    my_tcb->state = READY;
    my_tcb->arrival_time = global_time;
    enqueue(ready_queue, my_tcb);

    if (cpu_thread == NULL && !is_queue_empty(ready_queue)) {
        if (sch_no == SCH_SRTF) {
            if (SetReadySrtf(my_tcb)) pthread_cond_signal(&my_tcb->cond);
        } else if (queue_peek(ready_queue)->tid == tid) {
            pthread_cond_signal(&my_tcb->cond);
        }
    }

    int time_of_return = finish_time;
    pthread_mutex_unlock(&scheduler_lock);
    return time_of_return;
}

int P(float current_time, int tid, int sem_id) {
    pthread_mutex_lock(&scheduler_lock);

    thread_tcb_t *my_tcb = &tcb_array[tid];
    semaphore_t  *sem = &sem_array[sem_id];

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
                funcBlocked2Ready();
                funcMoveAlreadyNew2Ready();
                for (int i = 0; i < total_threads; ++i) {
                    if (tcb_array[i].state == NEW && isfinite(tcb_array[i].arrival_time) && (int)tcb_array[i].arrival_time <= global_time) {
                        pthread_cond_signal(&tcb_array[i].cond);
                    }
                    if (tcb_array[i].state == BLOCKED && isfinite(tcb_array[i].io_finish_time) && (int)tcb_array[i].io_finish_time <= global_time) {
                        pthread_cond_signal(&tcb_array[i].cond);
                    }
                }
            }
        } else {
            while (global_time < at) {
                pthread_cond_wait(&my_tcb->cond, &scheduler_lock);
            }
        }
        my_tcb->state = READY;
        enqueue(ready_queue, my_tcb);
    }

    sem->val--;
    if (sem->val < 0) {
        my_tcb->io_finish_time = INFINITY;
        my_tcb->rem_srtf_time = INT_MAX;
        queue_remove(ready_queue, tid);
        my_tcb->state = BLOCKED;
        enqueue(&sem->wait_queue, my_tcb);
        if (cpu_thread && cpu_thread->tid == tid) {
            cpu_thread = NULL;
            if (!is_queue_empty(ready_queue)) {
                funcWakeBestReady();
            }
        }

        if (cpu_thread == NULL && is_queue_empty(ready_queue)) {
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
                funcBlocked2Ready();
                funcMoveAlreadyNew2Ready();
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

        while (my_tcb->state == BLOCKED) {
            pthread_cond_wait(&my_tcb->cond, &scheduler_lock);
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
    if (global_time < at) {  // We should start V till it reaches 'at'
        if (cpu_thread == NULL && is_queue_empty(ready_queue)) {
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
                funcBlocked2Ready();
                funcMoveAlreadyNew2Ready();
                for (int i = 0; i < total_threads; ++i) {
                    if (tcb_array[i].state == NEW && isfinite(tcb_array[i].arrival_time) && (int)tcb_array[i].arrival_time <= global_time) {
                        pthread_cond_signal(&tcb_array[i].cond);
                    }
                    if (tcb_array[i].state == BLOCKED && isfinite(tcb_array[i].io_finish_time) && (int)tcb_array[i].io_finish_time <= global_time) {
                        pthread_cond_signal(&tcb_array[i].cond);
                    }
                }
                pthread_cond_signal(&state_changed_cond);
            }
        }
        while (global_time < at) {
            pthread_cond_wait(&my_tcb->cond, &scheduler_lock);
        }
    }

    sem->val++;
    if (sem->val <= 0 && !is_queue_empty(&sem->wait_queue)) {
        thread_tcb_t *tmp = dequeue(&sem->wait_queue);
        tmp->state = READY;
        tmp->arrival_time = global_time;
        enqueue(ready_queue, tmp);
        pthread_cond_signal(&tmp->cond); // wake the next unblocked thread
        pthread_cond_signal(&state_changed_cond);
    }
    my_tcb->state = READY;
    my_tcb->state = READY;
    my_tcb->arrival_time = global_time;
    my_tcb->rem_srtf_time = INT_MAX;
    enqueue(ready_queue, my_tcb);
    if (cpu_thread && cpu_thread->tid == tid) {
        cpu_thread = NULL;
    }
    if (!is_queue_empty(ready_queue)) {
        funcWakeBestReady();
    }
    pthread_cond_signal(&state_changed_cond);

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
    funcRemoveNonReady();
    if (!is_queue_empty(ready_queue)) {
        funcWakeBestReady();
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
            bool flagUnknownNew = false;
            for (int i = 0; i < total_threads; ++i) {
                if (tcb_array[i].state == NEW && !isfinite(tcb_array[i].arrival_time)) {
                    flagUnknownNew = true;
                    break;
                }
            }
            if (!flagUnknownNew) {
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
