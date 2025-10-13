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
extern queue_t mlfq_queues[5];

// To check if there are any runnable threads or not.
static inline bool checkNoRunnable(void) {
    if (sch_no == SCH_MLFQ) {
        return checkMlfqIsEmpty();
    }
    if(is_queue_empty(ready_queue)) return true;
    return false;
}

// To check if any threads are still NEW whose arrival time are unknown.
static inline bool checkUnknownNew(void) {
    for (int i = 0; i < total_threads; ++i) {
        thread_tcb_t* t = &tcb_array[i];
        if (t->state == NEW && !isfinite(t->arrival_time)) {
            return true;
        }
    }
    return false;
}

//To push any IO completions and NEW arrivals that are still pending to READY state.
static void funcBlocked2Ready(void) {
    for (int i = 0; i < total_threads; ++i) {
        thread_tcb_t* t = &tcb_array[i];
        if (t->state == BLOCKED && isfinite(t->io_finish_time) && (int)t->io_finish_time <= global_time) {
            t->state = READY;                  
            t->arrival_time = global_time;     
            t->rem_srtf_time = INT_MAX;       
            if (sch_no == SCH_MLFQ) {
                t->mlfq_level = 0;
                t->mlfq_leftquant = MLFQ[0];
                mlfq_enqueue(t, 0);
                if (cpu_thread == NULL) {
                    thread_tcb_t* tb = mlfq_peek();
                    if (tb) pthread_cond_signal(&tb->cond);
                }
            } else {
                enqueue(ready_queue, t);
                pthread_cond_signal(&t->cond);     
            }
        }
    }
    pthread_cond_signal(&state_changed_cond);
}

//To move NEW threads whose arrival time reached to READY state.
static void funcMoveAlreadyNew2Ready(void) {
    bool flag = false;
    for (int i = 0; i < total_threads; ++i) {
        thread_tcb_t* t = &tcb_array[i];
        if (t->state == NEW && isfinite(t->arrival_time) && (int)t->arrival_time <= global_time) {
            t->state = READY;
            if (sch_no == SCH_MLFQ) {
                t->mlfq_level = 0;   //Level0 initially
                t->mlfq_leftquant = MLFQ[0];
                mlfq_enqueue(t, 0);
                if (cpu_thread == NULL) {
                    thread_tcb_t* p = mlfq_peek();
                    if (p!=NULL) pthread_cond_signal(&p->cond);
                }
            } else {
                enqueue(ready_queue, t);
            }
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

static bool SetReadySrtf(thread_tcb_t* t) {
    if (!t) return false;
    thread_tcb_t* b = funcBestSrtf();
    if (!b) return false;
    return b && b->tid == t->tid;
}

static void funcWakeBestReady(void) {
    if (sch_no == SCH_SRTF) {
        thread_tcb_t* n = funcBestSrtf();
        if (n) {
            pthread_cond_signal(&n->cond);
        } else {
            pthread_cond_signal(&state_changed_cond);
        }
        return;
    }
    if (sch_no == SCH_MLFQ) {
        thread_tcb_t* n = mlfq_peek();
        if (n) {
            pthread_cond_signal(&n->cond);
        } else {
            pthread_cond_signal(&state_changed_cond);
        }
        return;
    }
    // FCFS
    while (!is_queue_empty(ready_queue)) {
        thread_tcb_t* h = queue_peek(ready_queue);
        if (h && h->state == READY) break;
        dequeue(ready_queue);
    }
    thread_tcb_t* n = queue_peek(ready_queue);
    if (n) pthread_cond_signal(&n->cond);
}

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

static bool CheckIOQueueTid(int tid) {
    if (!io_queue) return false;
    node_t* curr = io_queue->head;
    while (curr) {
        if (curr->tcb && curr->tcb->tid == tid) return true;
        curr = curr->next;
    }
    return false;
}

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

    int flagUnknown = (my_tcb->rem_srtf_time == INT_MAX);
    my_tcb->rem_srtf_time = (sch_no == SCH_SRTF ? remaining_time : INT_MAX);

    if (flagUnknown && sch_no == SCH_SRTF && remaining_time != INT_MAX) {
        pthread_cond_signal(&state_changed_cond);
    }
    funcBlocked2Ready();
    funcMoveAlreadyNew2Ready();

    if (my_tcb->state == NEW) {
        current_time = ceilf(current_time);
        current_time = (int)current_time;
        my_tcb->arrival_time = current_time;
        pthread_cond_signal(&state_changed_cond);
        if (global_time < my_tcb->arrival_time && cpu_thread == NULL && checkNoRunnable()==true && checkUnknownNew()==false) {
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
        if (my_tcb->state == NEW) { // To avoid double enqueue
            my_tcb->state = READY;
            if (sch_no == SCH_MLFQ) {
                my_tcb->mlfq_level = 0;
                my_tcb->mlfq_leftquant = MLFQ[0];
                mlfq_enqueue(my_tcb, 0);
            } else {
                enqueue(ready_queue, my_tcb);
            }
        }

        // For FCFS or SRTF, we pick from ready queue immediately.
        if (sch_no != SCH_MLFQ) {
            if (cpu_thread == NULL && !is_queue_empty(ready_queue)) {
                bool next = queue_peek(ready_queue)->tid == tid;
                if(sch_no == SCH_SRTF) {
                    next = SetReadySrtf(my_tcb);
                }
                if (next == true) {
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
    }

    if (sch_no == SCH_MLFQ) {
        while (1) {
            if (cpu_thread == my_tcb) break;
            if (cpu_thread == NULL) {
                thread_tcb_t* p = mlfq_peek();
                if (p && p->tid == tid) break;
            }
            pthread_cond_wait(&my_tcb->cond, &scheduler_lock);
        }

        if (cpu_thread == NULL) {
            thread_tcb_t* tr = mlfq_dequeue();
            cpu_thread = tr;
            tr->state = RUNNING;
            if (tr->mlfq_leftquant <= 0) tr->mlfq_leftquant = MLFQ[tr->mlfq_level];
        }
        global_time++;
        if (cpu_thread == my_tcb) {
            if (my_tcb->mlfq_leftquant <= 0) {
                my_tcb->mlfq_leftquant = MLFQ[my_tcb->mlfq_level];
            }
            my_tcb->mlfq_leftquant -= 1;
        }
        funcBlocked2Ready();
        funcMoveAlreadyNew2Ready();
        if (cpu_thread == NULL) {
            thread_tcb_t* p = mlfq_peek();
            if (p!=NULL) pthread_cond_signal(&p->cond);
        }

        if (cpu_thread == my_tcb) {
            int my_level = my_tcb->mlfq_level;
            int level= checkMlfqLevel();
            if (level >= 0 && level < my_level) {
                cpu_thread = NULL;
                my_tcb->state = READY;
                my_tcb->arrival_time = global_time; // FCFS within each level
                mlfq_enqueue(my_tcb, my_level);
                thread_tcb_t* p = mlfq_peek();
                if (p) pthread_cond_signal(&p->cond);
                for (int i = 0; i < total_threads; ++i) {
                    if (tcb_array[i].state == NEW && isfinite(tcb_array[i].arrival_time)
                        && (int)tcb_array[i].arrival_time <= global_time) {
                        pthread_cond_signal(&tcb_array[i].cond);
                    }
                }
                for (int i = 0; i < total_threads; ++i) {
                    if (tcb_array[i].state == BLOCKED && isfinite(tcb_array[i].io_finish_time)
                        && (int)tcb_array[i].io_finish_time <= global_time) {
                        pthread_cond_signal(&tcb_array[i].cond);
                    }
                }
                int ret = global_time;
                pthread_mutex_unlock(&scheduler_lock);
                if (sch_no == SCH_MLFQ) sched_yield();
                return ret;
            }
        }
        if (cpu_thread == my_tcb) {
            if (remaining_time - 1 > 0 && my_tcb->mlfq_leftquant == 0) {
                cpu_thread = NULL;
                my_tcb->state = READY;
                int new = 0;
                if(my_tcb->mlfq_level < 4) new = my_tcb->mlfq_level + 1;
                else new = 4;
                my_tcb->mlfq_level = new;
                my_tcb->mlfq_leftquant = MLFQ[new];
                my_tcb->arrival_time = global_time;
                mlfq_enqueue(my_tcb, new);
                thread_tcb_t* p = mlfq_peek();
                if (p) pthread_cond_signal(&p->cond);
            } else {
                cpu_thread = my_tcb;
                my_tcb->state = RUNNING;
            }
        }

        int ret = global_time;
        for (int i = 0; i < total_threads; ++i) {
            if (tcb_array[i].state == NEW && isfinite(tcb_array[i].arrival_time)
                && (int)tcb_array[i].arrival_time <= global_time) {
                pthread_cond_signal(&tcb_array[i].cond);
            }
        }
        for (int i = 0; i < total_threads; ++i) {
            if (tcb_array[i].state == BLOCKED && isfinite(tcb_array[i].io_finish_time)
                && (int)tcb_array[i].io_finish_time <= global_time) {
                pthread_cond_signal(&tcb_array[i].cond);
            }
        }
        pthread_mutex_unlock(&scheduler_lock);
        sched_yield(); 
        return ret;
    }
    // FCFS or SRTF
    if (sch_no == SCH_SRTF) {
        while (1) {
            if (cpu_thread == my_tcb) {
                thread_tcb_t* tb = FindBetterReadySrtf(my_tcb->rem_srtf_time, tid);
                if (!tb) {
                    break;
                }
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
            }
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
            thread_tcb_t* tb = funcBestSrtf();
            if (tb) {
                cpu_thread = tb;
                queue_remove(ready_queue, tb->tid);
            } else {
                pthread_mutex_unlock(&scheduler_lock);
                sched_yield();
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

    global_time++;

    if (sch_no == SCH_SRTF) {
        if (tcb_array[tid].rem_srtf_time > 0 && tcb_array[tid].state == RUNNING) {
            tcb_array[tid].rem_srtf_time = remaining_time - 1;
        }
    }

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

    if (sch_no == SCH_SRTF) {
        thread_tcb_t* tr = &tcb_array[tid];
        int remainticks = remaining_time - 1;
        if (remainticks < 0) remainticks = 0;
        thread_tcb_t* tb = FindBetterReadySrtf(remainticks, tid);
        if (tb && tr->rem_srtf_time > 0) {
            if (cpu_thread && cpu_thread->tid == tid) {
                cpu_thread = NULL;
            }
            tr->state = READY;
            tr->arrival_time = global_time;
            enqueue(ready_queue, tr);
            funcWakeBestReady();
            pthread_cond_signal(&state_changed_cond);
        } 
        else {
            cpu_thread = tr;
            tr->state = RUNNING;
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

    if (sch_no == SCH_MLFQ) removeMlfqTid(tid);
    queue_remove(ready_queue, tid);
    my_tcb->state = BLOCKED;
    my_tcb->rem_srtf_time = INT_MAX;

    int request_time = (int)ceilf(current_time);
    my_tcb->arrival_time = request_time;
    enqueue(io_queue, my_tcb);
    pthread_cond_signal(&state_changed_cond);

    if (cpu_thread && cpu_thread->tid == tid) {
        cpu_thread = NULL;
        if (!checkNoRunnable()) funcWakeBestReady();
    }

    while(1){
        while (io_queue->head == NULL || queue_peek(io_queue)->tid != tid) {
            pthread_cond_wait(&state_changed_cond, &scheduler_lock);
        }
        
        while (global_time < request_time) {
            pthread_cond_wait(&state_changed_cond, &scheduler_lock);
        }
        if (io_busy_until <= request_time) {
            if (CheckLowTidIOStatus(tid, request_time)) {
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
        if (checkNoRunnable()==true) {
            float next_time = INFINITY;
            for (int i = 0; i < total_threads; ++i) {
                if (tcb_array[i].state == NEW && isfinite(tcb_array[i].arrival_time)
                    && tcb_array[i].arrival_time > global_time && tcb_array[i].arrival_time < next_time) {
                    next_time = tcb_array[i].arrival_time;
                }
                if (tcb_array[i].state == BLOCKED && isfinite(tcb_array[i].io_finish_time)
                    && tcb_array[i].io_finish_time > global_time && tcb_array[i].io_finish_time < next_time) {
                    next_time = tcb_array[i].io_finish_time;
                }
            }
            if (isfinite(next_time) && !checkUnknownNew()==true) {
                global_time = (int)next_time;
                funcBlocked2Ready();
                funcMoveAlreadyNew2Ready();
                for (int i = 0; i < total_threads; ++i) {
                    if (tcb_array[i].state == NEW && isfinite(tcb_array[i].arrival_time)
                        && (int)tcb_array[i].arrival_time <= global_time) {
                        pthread_cond_signal(&tcb_array[i].cond);
                    }
                    if (tcb_array[i].state == BLOCKED && isfinite(tcb_array[i].io_finish_time)
                        && (int)tcb_array[i].io_finish_time <= global_time) {
                        pthread_cond_signal(&tcb_array[i].cond);
                    }
                }
                pthread_cond_signal(&state_changed_cond);
            }
        } else {
            funcWakeBestReady();
        }
    }

    while (global_time < my_tcb->io_finish_time) {
        pthread_cond_wait(&my_tcb->cond, &scheduler_lock);
    }

    if (my_tcb->state != BLOCKED) {
        my_tcb->io_finish_time = INFINITY;
        int time_of_return = finish_time;
        pthread_mutex_unlock(&scheduler_lock);
        return time_of_return;
    }

    my_tcb->io_finish_time = INFINITY;
    my_tcb->state = READY;
    my_tcb->arrival_time = global_time;
    if (sch_no == SCH_MLFQ) {
        my_tcb->mlfq_level = 0;
        my_tcb->mlfq_leftquant = MLFQ[0];
        mlfq_enqueue(my_tcb, 0);
    } else {
        enqueue(ready_queue, my_tcb);
    }

    if (cpu_thread == NULL) {
        if (sch_no == SCH_MLFQ) {
            thread_tcb_t* p = mlfq_peek();
            if (p) pthread_cond_signal(&p->cond);
        } else if (!is_queue_empty(ready_queue)) {
            if (sch_no == SCH_SRTF) {
                if (SetReadySrtf(my_tcb)) pthread_cond_signal(&my_tcb->cond);
            } else if (queue_peek(ready_queue)->tid == tid) {
                pthread_cond_signal(&my_tcb->cond);
            }
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
        if (global_time < at && cpu_thread == NULL && checkNoRunnable()==true && checkUnknownNew()!=true) {
            float next_time = at;
            for (int i = 0; i < total_threads; ++i) {
                if (tcb_array[i].state == NEW && isfinite(tcb_array[i].arrival_time) && tcb_array[i].arrival_time > global_time && tcb_array[i].arrival_time < next_time) {
                    next_time = tcb_array[i].arrival_time;
                }
                if (tcb_array[i].state == BLOCKED && isfinite(tcb_array[i].io_finish_time) && tcb_array[i].io_finish_time > global_time && tcb_array[i].io_finish_time < next_time) {
                    next_time = tcb_array[i].io_finish_time;
                }
            }
            if (isfinite(next_time) && checkUnknownNew()!=true) {
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
        if (sch_no == SCH_MLFQ) removeMlfqTid(tid);
        queue_remove(ready_queue, tid);
        my_tcb->state = BLOCKED;
        enqueue(&sem->wait_queue, my_tcb);
        if (cpu_thread && cpu_thread->tid == tid) {
            cpu_thread = NULL;
            if (!checkNoRunnable()) {
                funcWakeBestReady();
            }
        }

        if (cpu_thread == NULL && checkNoRunnable()==true) {
            float next_time = INFINITY;
            for (int i = 0; i < total_threads; ++i) {
                if (tcb_array[i].state == NEW && isfinite(tcb_array[i].arrival_time) && tcb_array[i].arrival_time > global_time && tcb_array[i].arrival_time < next_time) {
                    next_time = tcb_array[i].arrival_time;
                }
                if (tcb_array[i].state == BLOCKED && isfinite(tcb_array[i].io_finish_time) && tcb_array[i].io_finish_time > global_time && tcb_array[i].io_finish_time < next_time) {
                    next_time = tcb_array[i].io_finish_time;
                }
            }
            if (isfinite(next_time) && checkUnknownNew()!=true) {
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
    if (global_time < at) {
        if (cpu_thread == NULL && checkNoRunnable()==true && checkUnknownNew()!=true) {
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
        if (sch_no == SCH_MLFQ) {
            tmp->mlfq_leftquant = MLFQ[tmp->mlfq_level];
            mlfq_enqueue(tmp, tmp->mlfq_level);
        } else {
            enqueue(ready_queue, tmp);
        }
        pthread_cond_signal(&tmp->cond);
        pthread_cond_signal(&state_changed_cond);
        if (cpu_thread == NULL) {
            funcWakeBestReady();
        }
    }
    my_tcb->state = READY;
    my_tcb->arrival_time = global_time;
    my_tcb->rem_srtf_time = INT_MAX;
    if (sch_no == SCH_MLFQ) {
        my_tcb->mlfq_level = 0;
        my_tcb->mlfq_leftquant = MLFQ[0];
        mlfq_enqueue(my_tcb, 0);
    } else {
        enqueue(ready_queue, my_tcb);
    }
    if (cpu_thread && cpu_thread->tid == tid) {
        cpu_thread = NULL;
    }
    if (checkNoRunnable()!=true) {
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
    if (sch_no == SCH_MLFQ) removeMlfqTid(tid);
    queue_remove(ready_queue, tid);
    while (!is_queue_empty(ready_queue)) {
        thread_tcb_t* h = queue_peek(ready_queue);
        if (h && h->state == READY) break;
        dequeue(ready_queue);
    }
    if (checkNoRunnable()!=true) {
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
