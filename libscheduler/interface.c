#include "api.h"
#include "scheduler.h"

// Interface implementation
// Implement APIs here...

int cpu_me(float current_time, int tid, int remaining_time) {

    return 0;
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

    return;
}
