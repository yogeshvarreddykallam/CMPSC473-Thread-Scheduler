# CMPSC 473 — Thread Scheduler

A **user-space cooperative thread scheduler** implemented in C for **PSU CMPSC 473 (Operating Systems) — Fall 2025, Project 1**.

The scheduler supports three scheduling algorithms, I/O blocking, semaphore-based synchronization, and produces Gantt chart output for visualization.

## Scheduling Algorithms

| Policy | Description |
|--------|-------------|
| **FCFS** (0) | First Come First Served — non-preemptive, ordered by arrival time |
| **SRTF** (1) | Shortest Remaining Time First — preemptive, always runs shortest remaining burst |
| **MLFQ** (2) | Multi-Level Feedback Queue — 5 priority levels with aging and demotion |

## Features

- Thread control blocks (TCB) with arrival time, burst time, and priority tracking
- Ready queue and I/O wait queue management
- Semaphore implementation with blocking/unblocking support
- Gantt chart output showing CPU allocation across time steps
- Mutex-protected scheduler state for thread-safe operation

## Repository Structure

```
.
├── main.c                      # Entry point — drives simulation from input file
├── Makefile
├── report.pdf                  # Project write-up and analysis
├── libscheduler/
│   ├── scheduler.c / .h        # Core scheduling logic (FCFS, SRTF, MLFQ)
│   ├── init.c                  # Scheduler initialization
│   ├── interface.c             # Public API bridge
│   └── api.h                   # Scheduling API definition
├── sample_input/               # 12 test input files (input_0 … input_11)
└── sample_output/              # Expected Gantt chart outputs per policy
```

## Build & Run

```bash
make

# Usage: ./main <scheduling_policy> <input_file>
#   Policy 0 = FCFS
#   Policy 1 = SRTF
#   Policy 2 = MLFQ

./main 0 sample_input/input_1     # FCFS
./main 1 sample_input/input_3     # SRTF
./main 2 sample_input/input_5     # MLFQ
```

## Input Format

```
<num_threads>
<tid> <arrival_time> <burst_time>
...
<operations>
CPU <tid>
IO  <tid>
SEM_WAIT <tid> <sem_id>
SEM_POST <tid> <sem_id>
```

## Output

Gantt chart per time step:

```
Time 0: CPU=T1, IO=idle
Time 1: CPU=T1, IO=T2
...
```

Outputs are validated against `sample_output/gantt-{policy}-{input}`.

## Report

Full analysis including algorithm comparison and performance metrics: [`report.pdf`](report.pdf)
