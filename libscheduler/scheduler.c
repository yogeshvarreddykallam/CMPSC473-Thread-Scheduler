#include "scheduler.h"

// Scheduler implementation
// Implement your own functions here...
#include <stdlib.h>

pthread_mutex_t scheduler_lock;     
pthread_cond_t state_changed_cond;  
int global_time = 0;                
thread_tcb_t* tcb_array;            
int active_threads;                 
thread_tcb_t* cpu_thread = NULL;   
queue_t* ready_queue;
semaphore_t sem_array[MAX_NUM_SEM];
              

void queue_init(queue_t* q) {
    q->head = NULL;
    q->tail = NULL;
}


void enqueue(queue_t* q, thread_tcb_t* tcb) {
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

int myfunc() {
    // Function for ...

    return 0;
};

int myfunc2() {
    // Function for ...

    return 0;
};

