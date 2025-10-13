#include "scheduler.h"

// Scheduler implementation
// Implement your own functions here...
#include <stdlib.h>
#include <stdio.h>
#include <math.h>
#include <limits.h>

pthread_mutex_t scheduler_lock;     
pthread_cond_t state_changed_cond;  
int global_time = 0;                
thread_tcb_t* tcb_array;            
int active_threads;                 
thread_tcb_t* cpu_thread = NULL;   
queue_t* ready_queue;
semaphore_t sem_array[MAX_NUM_SEM];
int total_threads = 0;
int io_busy_until = 0;
queue_t* io_queue = NULL;
enum sch_type sch_no = SCH_FCFS;
queue_t mlfq_queues[5];

void queue_init(queue_t* q) {
    q->head = NULL;
    q->tail = NULL;
}

void enqueue(queue_t* q, thread_tcb_t* tcb) {
    // Ordering is done by arrival time then tid (for FCFS and SRTF)
    queue_remove(q, tcb->tid);
    node_t* new_node = malloc(sizeof(node_t));
    new_node->tcb = tcb;
    new_node->next = NULL;

    if (q->head == NULL || new_node->tcb->arrival_time < q->head->tcb->arrival_time ||
       (new_node->tcb->arrival_time == q->head->tcb->arrival_time && new_node->tcb->tid < q->head->tcb->tid)) {
        new_node->next = q->head;
        q->head = new_node;
        if (q->tail == NULL) { 
            q->tail = new_node;
        }
        return;
    }

    node_t* current = q->head;
    while (current->next != NULL && (new_node->tcb->arrival_time > current->next->tcb->arrival_time ||
           (new_node->tcb->arrival_time == current->next->tcb->arrival_time && new_node->tcb->tid > current->next->tcb->tid))) {
        current = current->next;
    }

    new_node->next = current->next;
    current->next = new_node;

    if (new_node->next == NULL) {
        q->tail = new_node;
    }
}

thread_tcb_t* dequeue(queue_t* q) {
    if (q->head == NULL) {
        return NULL;
    }
    node_t* temp = q->head;
    thread_tcb_t* tcb = temp->tcb;
    q->head = q->head->next;
    if (q->head == NULL) {
        q->tail = NULL;
    }
    free(temp);
    return tcb;
}

thread_tcb_t* queue_peek(queue_t* q) {
    if (q->head == NULL) {
        return NULL;
    }
    return q->head->tcb;
}

bool is_queue_empty(queue_t* q) {
    if (q == NULL) {
        return true;
    }
    return q->head == NULL;
}

bool queue_remove(queue_t* q, int tid) {
    if (q == NULL || q->head == NULL) return false;
    node_t* prev = NULL;
    node_t* cur = q->head;
    while (cur) {
        if (cur->tcb && cur->tcb->tid == tid) {
            if (prev) prev->next = cur->next; else q->head = cur->next;
            if (cur == q->tail) q->tail = prev;
            free(cur);
            return true;
        }
        prev = cur;
        cur = cur->next;
    }
    return false;
}

void mlfq_enqueue(thread_tcb_t* t, int level) {
    if (level < 0) level = 0; 
    if (level > 4) level = 4;
    t->mlfq_level = level;
    enqueue(&mlfq_queues[level], t);
}

thread_tcb_t* mlfq_peek(void) {
    for (int i = 0; i < 5; ++i) {
        thread_tcb_t* t = queue_peek(&mlfq_queues[i]);
        if (t) return t;
    }
    return NULL;
}

thread_tcb_t* mlfq_dequeue(void) {
    for (int i = 0; i < 5; ++i) {
        if (!is_queue_empty(&mlfq_queues[i])) {
            return dequeue(&mlfq_queues[i]);
        }
    }
    return NULL;
}

bool checkMlfqIsEmpty(void) {
    for (int i = 0; i < 5; ++i) if (!is_queue_empty(&mlfq_queues[i])) return false;
    return true;
}

void removeMlfqTid(int tid) {
    for (int i = 0; i < 5; ++i){
        queue_remove(&mlfq_queues[i], tid);
    }
}

int checkMlfqLevel(void) {
    for (int i = 0; i < 5; ++i) {
        if (!is_queue_empty(&mlfq_queues[i])) return i;
    }
    return -1;
}


