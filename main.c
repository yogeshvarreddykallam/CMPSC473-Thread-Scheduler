#include <sys/stat.h>
#include <libgen.h>

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <stddef.h>
#include <stdarg.h>
#include <string.h>
#include <errno.h>
#include <pthread.h>
#include <time.h>

#include "api.h"

#define MAX_LINE_SIZE 1024
#define MAX_LOG_SIZE 512
#define MAX_LOG_LEN 4096

struct log {
    int64_t ns;
    char buf[MAX_LOG_SIZE];
};

struct thread_struct {
    pthread_t p_t;                    // pthread identifier
    int tid;                          // tid
    char line[MAX_LINE_SIZE];         // tid's operations
    int64_t log_idx;                  // index of log_data to use for log_msg
    struct log log_data[MAX_LOG_LEN]; // tid's log
};

void *thread_start(void *);
int get_line_count(char *file_name);

// Log a message to log_data
void log_msg(struct thread_struct *td, const char *format, ...) {
    struct log *current_slot = &(td->log_data[td->log_idx]);

    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    int64_t ns = (ts.tv_sec * 1000000000UL) + ts.tv_nsec;
    current_slot->ns = ns;

    va_list args;
    va_start(args, format);
    vsnprintf(current_slot->buf, MAX_LOG_SIZE, format, args);
    va_end(args);

    td->log_idx++;
    if (td->log_idx >= MAX_LOG_LEN) {
        fprintf(stderr, "%s: Error, tid: %d, too many logs\n", __func__, td->tid);
        exit(EXIT_FAILURE);
    }
}

// Main function
// Read input file and create threads accordingly
int main(int argc, char **argv) {
    printf("%s: Hello Project 1!\n", __func__);
    if (argc != 3) {
        fprintf(stderr, "Not enough parameters specified. Usage: ./proj1 <scheduler_type> <input_file>\n");
        fprintf(stderr, "  Scheduler type: 0 - First Come, First Served\n");
        fprintf(stderr, "  Scheduler type: 1 - Shortest Remaining Time First\n");
        fprintf(stderr, "  Scheduler type: 2 - Multi-Level Feedback Queue\n");
        exit(EXIT_FAILURE);
    }

    // Get parameters
    int scheduler_type = atoi(argv[1]);
    int num_lines = get_line_count(argv[2]);
    if (num_lines <= 0) {
        fprintf(stderr, "%s: invalid input file.\n", __func__);
        exit(EXIT_FAILURE);
    }

    int num_threads = num_lines;
    printf("%s: Scheduler type: %d, number of threads: %d\n", __func__, scheduler_type, num_threads);

    // Allocate thread_struct
    struct thread_struct *threads;
    threads = (struct thread_struct *)malloc(sizeof(*threads) * num_threads);
    if (!threads) {
        perror("malloc() error");
        exit(EXIT_FAILURE);
    }
    memset(threads, 0, sizeof(*threads) * num_threads);

    // Read each line and save inside threads[].line
    FILE *fp = fopen(argv[2], "r");
    char *buf = (char *)malloc(sizeof(char) * MAX_LINE_SIZE);
    for (int i = 0; i < num_threads; ++i) {
        if (fgets(buf, MAX_LINE_SIZE, fp) == NULL) {
            perror("fgets() error");
            exit(EXIT_FAILURE);
        }
        if (buf[strlen(buf) - 1] == '\n')
            buf[strlen(buf) - 1] = '\0'; // remove newline

        strncpy(threads[i].line, buf, MAX_LINE_SIZE);
    }
    free(buf);
    fclose(fp);

    // Init scheduler
    init_scheduler(scheduler_type, num_threads);

    // Assign tid and create threads using threads[]
    int ret = 0;
    for (int i = 0; i < num_threads; ++i) {
        threads[i].tid = i;
        ret = pthread_create(&(threads[i].p_t), NULL, thread_start, &(threads[i]));
        if (ret) {
            fprintf(stderr, "%s: pthread_create() error!\n", __func__);
            exit(EXIT_FAILURE);
        }
    }

    // Join threads
    for (int i = 0; i < num_threads; ++i) {
        ret = pthread_join(threads[i].p_t, NULL);
        if (ret) {
            fprintf(stderr, "%s: pthread_join() error!\n", __func__);
            exit(EXIT_FAILURE);
        }
    }

    finish_scheduler();

    // Open file for Gantt chart
    FILE *gantt_file = NULL;
    char gantt_filename[512] = {0};
    mkdir("output", 0755);
    strcat(gantt_filename, "output/gantt-");
    strcat(gantt_filename, argv[1]);
    strcat(gantt_filename, "-");
    strcat(gantt_filename, basename(argv[2]));
    gantt_file = fopen(gantt_filename, "w");
    if (gantt_file == NULL) {
        perror("fopen() error");
        exit(EXIT_FAILURE);
    }

    // write the thread logs to gantt_file
    int64_t *cmp_idx = (int64_t *)malloc(sizeof(*cmp_idx) * num_threads);
    memset(cmp_idx, 0, sizeof(int64_t) * num_threads);
    while (true) {
        int64_t current_min_ns = INT64_MAX;
        int current_min_tid = -1;

        // find tid of smallest ns
        for (int i = 0; i < num_threads; ++i) {
            struct thread_struct *cmp_ts = &(threads[i]);
            if (cmp_ts->log_idx <= cmp_idx[i])
                continue; // done for this thread
            if (cmp_ts->log_data[cmp_idx[i]].ns < current_min_ns) {
                current_min_ns = cmp_ts->log_data[cmp_idx[i]].ns;
                current_min_tid = i;
            }
        }

        // written all logs, nothing left
        if (current_min_tid == -1)
            break;

        // write log to file
        char *log = threads[current_min_tid].log_data[cmp_idx[current_min_tid]].buf;
        size_t ret = fwrite(log, sizeof(char), strlen(log), gantt_file);
        if (ret != strlen(log)) {
            fprintf(stderr, "fwrite() failed.\n");
            exit(EXIT_FAILURE);
        }

        // increment the index to next slot
        cmp_idx[current_min_tid]++;
    }
    free(cmp_idx);

    fclose(gantt_file);
    free(threads);

    // sort
    // char sort_command[2048];
    // snprintf(sort_command, 2048, "sort %s > %s-sorted", gantt_filename, gantt_filename);
    // system(sort_command);

    printf("%s: Output file: %s\n", __func__, gantt_filename);
    printf("%s: Bye!\n", __func__);
    return 0;
}

// Thread starting point
// Independently read each line and call C/I/P/V/E
void *thread_start(void *arg) {
    struct thread_struct *my_info = (struct thread_struct *)arg;
    int tid = my_info->tid;

    // read tokens
    char *token = NULL;
    char delim[3] = "\t ";
    char *saveptr;

    // arrival time
    token = strtok_r(my_info->line, delim, &saveptr);
    float arrival_time = atof(token);

    // the first operation (C/I/P/V) call from this thread is the arrival time in input file
    float schedule_time = arrival_time;

    // tid
    token = strtok_r(NULL, delim, &saveptr);
    if (tid != atoi(token)) {
        fprintf(stderr, "%s: tid: %d, incorrect tid\n", __func__, tid);
        exit(EXIT_FAILURE);
    }

    // loop until 'E'
    token = strtok_r(NULL, delim, &saveptr);
    while (token) {
        // save the return value (time) of C/I/P/V
        int ret_time = 0;

        // parse token
        if (token[0] == 'C') {
            int duration = atoi(&(token[1]));
            while (duration >= 0) {
                ret_time = cpu_me(schedule_time, tid, duration);
                // return from cpu_me()
                if (duration > 0)
                    // only print when CPU is actually requested
                    // (if duration is 0, we are just notifying the scheduler)
                    // this tid had cpu from 'ret_time-1' to 'ret_time'
                    log_msg(my_info, "%3d~%3d: T%d, CPU\n", ret_time - 1, ret_time, tid);

                // values for the next cpu_me() call
                schedule_time = ret_time;
                duration = duration - 1;
            }
        } else if (token[0] == 'I') {
            int duration = atoi(&(token[1]));
            ret_time = io_me(schedule_time, tid, duration);
            // return from io_me()
            // this tid finished IO at time 'ret_time'
            log_msg(my_info, "   ~%3d: T%d, Return from IO\n", ret_time, tid);
        } else if (token[0] == 'P') {
            int sem_id = atoi(&(token[1]));
            ret_time = P(schedule_time, tid, sem_id);
            // return from P()
            // this tid finished P at time 'ret_time'
            log_msg(my_info, "   ~%3d: T%d, Return from P%d\n", ret_time, tid, sem_id);
        } else if (token[0] == 'V') {
            int sem_id = atoi(&(token[1]));
            ret_time = V(schedule_time, tid, sem_id);
            // return from V()
            // this tid finished V at time 'ret_time'
            log_msg(my_info, "   ~%3d: T%d, Return from V%d\n", ret_time, tid, sem_id);
        } else if (token[0] == 'E') {
            // this thread is finished, notify scheduler
            end_me(tid);

            // end this thread normally
            return NULL;
        } else {
            fprintf(stderr, "%s: Error, tid: %d, invalid token: %c%c\n", __func__, tid, token[0], token[1]);
            exit(EXIT_FAILURE);
        }

        // get next token
        token = strtok_r(NULL, delim, &saveptr);

        // call the next operation without any time delay
        schedule_time = ret_time;
    }

    // No 'E' found in input file
    fprintf(stderr, "%s: Error, tid: %d, thread finished without 'E' operation\n", __func__, tid);
    exit(EXIT_FAILURE);
}

// From file_name, get the number of lines and do error check
int get_line_count(char *file_name) {
    FILE *fp = fopen(file_name, "r");
    if (!fp) {
        perror("fopen() error");
        exit(EXIT_FAILURE);
    }

    int num_lines = 0;
    char arrival_time[512];
    int temp;
    char *buf = (char *)malloc(sizeof(char) * MAX_LINE_SIZE);
    while (fgets(buf, MAX_LINE_SIZE, fp) != NULL) {
        // Check tid of the input file. tid starts from 0
        sscanf(buf, "%s %d", arrival_time, &temp);
        if (temp != num_lines) {
            exit(EXIT_FAILURE);
        }
        ++num_lines;
    }
    free(buf);

    fclose(fp);
    return num_lines;
}
