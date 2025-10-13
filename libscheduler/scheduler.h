#pragma once

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <stddef.h>
#include <stdarg.h>
#include <string.h>
#include <math.h>
#include <limits.h>
#include <pthread.h>

#include "api.h"

// Declare your own data structures and functions here...
typedef enum {
    NEW,
    READY,
    RUNNING,
    TERMINATED,
    BLOCKED
} thread_state_t;

typedef struct thread_tcb thread_tcb_t;

//LinkedList queue
typedef struct node {
    thread_tcb_t* tcb;
    struct node* next;
} node_t;
typedef struct {
    node_t* head;
    node_t* tail;
} queue_t;

struct thread_tcb {
    int tid;
    thread_state_t state;
    pthread_cond_t cond;
    float arrival_time;
    float io_finish_time;
    int rem_srtf_time;
    int mlfq_level;           
    int mlfq_leftquant;    
    bool mlfq_new_burst;
};

typedef struct {
    int val;
    queue_t wait_queue;
} semaphore_t;

void queue_init(queue_t* q);
void enqueue(queue_t* q, thread_tcb_t* tcb);
thread_tcb_t* dequeue(queue_t* q);
thread_tcb_t* queue_peek(queue_t* q);
bool is_queue_empty(queue_t* q);
bool queue_remove(queue_t* q, int tid);
void mlfq_enqueue(thread_tcb_t* t, int level);
thread_tcb_t* mlfq_dequeue(void);
thread_tcb_t* mlfq_peek(void);
bool checkMlfqIsEmpty(void);
void removeMlfqTid(int tid);
int checkMlfqLevel(void);


extern int total_threads;
extern int io_busy_until;
extern queue_t* io_queue;
extern enum sch_type sch_no;
extern queue_t mlfq_queues[5];

