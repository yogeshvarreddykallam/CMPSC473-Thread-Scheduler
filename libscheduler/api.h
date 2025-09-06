#pragma once

// Scheduler type
enum sch_type {
    SCH_FCFS = 0, // first come first served
    SCH_SRTF = 1, // shortest remaining time first
    SCH_MLFQ = 2, // multi-level feedback queue
};

void init_scheduler(enum sch_type scheduler_type, int thread_count);
void finish_scheduler();

int cpu_me(float current_time, int tid, int remaining_time);
int io_me(float current_time, int tid, int duration);
int P(float current_time, int tid, int sem_id);
int V(float current_time, int tid, int sem_id);
void end_me(int tid);

// MLFQ definitions
static const int MLFQ_TIME_QUANTUM[5] = {5, 10, 15, 20, 25};
// MLFQ_TIME_QUANTUM[0] is the highest, [4] is the lowest level

// Semaphore definitions
#define MAX_NUM_SEM 10 // sem_id from 0 to 9
