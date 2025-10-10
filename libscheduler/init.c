#include "api.h"
#include "scheduler.h"

// Interface implementation
// Implement init/finish APIs here...
extern pthread_mutex_t scheduler_lock;
extern pthread_cond_t state_changed_cond;
extern thread_tcb_t* tcb_array;
extern int active_threads;
extern queue_t* ready_queue;
extern semaphore_t sem_array[MAX_NUM_SEM];
extern int total_threads;

void init_scheduler(enum sch_type type, int thread_count) {
    pthread_mutex_init(&scheduler_lock, NULL);
    pthread_cond_init(&state_changed_cond, NULL);

    active_threads = thread_count;
    total_threads = thread_count;

    tcb_array = malloc(sizeof(thread_tcb_t) * thread_count);
    for (int i = 0; i < thread_count; ++i) {
        tcb_array[i].tid = i;
        tcb_array[i].state = NEW;
        pthread_cond_init(&tcb_array[i].cond, NULL);
        tcb_array[i].arrival_time = INFINITY;   // not set until first cpu_me
        tcb_array[i].io_finish_time = INFINITY; // not set until io_me
    }

    ready_queue = malloc(sizeof(queue_t));
    queue_init(ready_queue);
    for (int i = 0; i < MAX_NUM_SEM; ++i) {
        sem_array[i].val = 0; 
        queue_init(&sem_array[i].wait_queue);
    }
    return;
}

void finish_scheduler() {
    free(tcb_array);
    free(ready_queue);
    pthread_mutex_destroy(&scheduler_lock);
    pthread_cond_destroy(&state_changed_cond);
    return;
}
