/*
 * mpi_join.c
 * MPI-parallel hash-partitioned database join.
 *
 * Algorithm:
 *   1. Every rank reads both CSVs independently (I/O outside MPI).
 *   2. Each rank determines its contiguous chunk of trips.
 *   3. Each rank builds a hash table from employees where id % n == taskid.
 *   4. Each rank bins its trips chunk by id % n and exchanges via Alltoallv.
 *   5. Each rank probes its received trips against its local hash table.
 *   6. Each rank writes matches to its own output file (parallel I/O).
 *   7. Match counts are reduced to rank 0.
 *   8. Rank 0 concatenates per-rank files into a single results file.
 *
 * Build:  mpicc -o mpi_join src/mpi_join.c
 * Run:    mpirun -np <N> ./mpi_join -e <employees.csv> -t <trips.csv>
 */

#include "mpi.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

/* ---- constants ---- */
#define MASTER 0
#define MAX_NAME_LEN 31
#define MAX_DEST_LEN 21
#define MAX_LINE_LEN 256
#define HASH_TABLE_SIZE 2003 /* prime, ~1.6x expected max bucket count */

/* ---- data structures ---- */

typedef struct {
  int id;
  char name[MAX_NAME_LEN];
} Employee;

typedef struct {
  long long timestamp;
  int id;
  char destination[MAX_DEST_LEN];
} Trip;

typedef struct {
  const char *employee_path;
  const char *trips_path;
  int num_procs;
} Args;

/* ---- hash table (chaining) ---- */

typedef struct HashNode {
  Employee employee;
  struct HashNode *next;
} HashNode;

typedef struct {
  HashNode *buckets[HASH_TABLE_SIZE];
} HashTable;

void hash_init(HashTable *ht) { memset(ht->buckets, 0, sizeof(ht->buckets)); }

void hash_insert(HashTable *ht, const Employee *e) {
  unsigned int h = ((unsigned int)e->id) % HASH_TABLE_SIZE;
  HashNode *node = malloc(sizeof(HashNode));
  node->employee = *e;
  node->next = ht->buckets[h];
  ht->buckets[h] = node;
}

Employee *hash_lookup(HashTable *ht, int id) {
  unsigned int h = ((unsigned int)id) % HASH_TABLE_SIZE;
  HashNode *node = ht->buckets[h];
  while (node) {
    if (node->employee.id == id)
      return &node->employee;
    node = node->next;
  }
  return NULL;
}

void hash_free(HashTable *ht) {
  for (int i = 0; i < HASH_TABLE_SIZE; i++) {
    HashNode *node = ht->buckets[i];
    while (node) {
      HashNode *tmp = node;
      node = node->next;
      free(tmp);
    }
    ht->buckets[i] = NULL;
  }
}

/* ---- argument parsing ---- */

void print_usage(const char *prog) {
  fprintf(stderr,
          "Usage: %s -e <employees.csv> -t <trips.csv> -n <num_procs>\n", prog);
  fprintf(stderr, "  -e  Path to employees CSV file\n");
  fprintf(stderr, "  -t  Path to trips CSV file\n");
  fprintf(stderr, "  -n  Number of processors\n");
}

Args parse_args(int argc, char *argv[]) {
  Args args = {NULL, NULL, 0};
  int opt;
  optind = 1;

  while ((opt = getopt(argc, argv, "e:t:n:")) != -1) {
    switch (opt) {
    case 'e':
      args.employee_path = optarg;
      break;
    case 't':
      args.trips_path = optarg;
      break;
    case 'n':
      args.num_procs = atoi(optarg);
      break;
    default:
      print_usage(argv[0]);
      exit(EXIT_FAILURE);
    }
  }

  if (!args.employee_path || !args.trips_path || args.num_procs <= 0) {
    fprintf(stderr, "Error: -e, -t, and -n are all required.\n");
    print_usage(argv[0]);
    exit(EXIT_FAILURE);
  }

  return args;
}

/* ---- CSV loading (with header auto-detect) ---- */

int load_csv(const char *path, size_t record_size,
             int (*parse_row)(char *line, void *record), void **out) {
  FILE *f = fopen(path, "r");
  if (!f) {
    perror(path);
    return -1;
  }

  int capacity = 1024, count = 0;
  char *arr = malloc(capacity * record_size);
  char line[MAX_LINE_LEN];

  /* Auto-detect header */
  if (fgets(line, sizeof(line), f)) {
    char tmp[MAX_LINE_LEN];
    strncpy(tmp, line, MAX_LINE_LEN);
    char *tok = strtok(tmp, ",\n");
    if (tok) {
      char *endptr;
      strtol(tok, &endptr, 10);
      if (endptr == tok) {
        /* header — discard */
      } else {
        if (parse_row(line, arr + count * record_size) == 0)
          count++;
      }
    }
  }

  while (fgets(line, sizeof(line), f)) {
    if (count == capacity) {
      capacity *= 2;
      arr = realloc(arr, capacity * record_size);
    }
    if (parse_row(line, arr + count * record_size) == 0)
      count++;
  }

  fclose(f);
  *out = arr;
  return count;
}

/* ---- row parsers ---- */

int parse_employee(char *line, void *record) {
  Employee *e = (Employee *)record;
  char *tok = strtok(line, ",\n");
  if (!tok)
    return -1;
  e->id = atoi(tok);
  tok = strtok(NULL, ",\n");
  if (!tok)
    return -1;
  strncpy(e->name, tok, MAX_NAME_LEN - 1);
  e->name[MAX_NAME_LEN - 1] = '\0';
  return 0;
}

int parse_trip(char *line, void *record) {
  Trip *t = (Trip *)record;
  char *tok = strtok(line, ",\n");
  if (!tok)
    return -1;
  t->timestamp = atoll(tok);
  tok = strtok(NULL, ",\n");
  if (!tok)
    return -1;
  t->id = atoi(tok);
  tok = strtok(NULL, ",\n");
  if (!tok)
    return -1;
  strncpy(t->destination, tok, MAX_DEST_LEN - 1);
  t->destination[MAX_DEST_LEN - 1] = '\0';
  return 0;
}

/* ======================================================================== */
/* ============================  MAIN  ==================================== */
/* ======================================================================== */

int main(int argc, char *argv[]) {
  int taskid, numtasks;

  /* ---- parse args (all ranks need paths) ---- */
  Args args = parse_args(argc, argv);

  /* ---- every rank loads both CSVs (I/O outside MPI) ---- */
  Employee *employees = NULL;
  Trip *trips = NULL;

  int num_employees = load_csv(args.employee_path, sizeof(Employee),
                               parse_employee, (void **)&employees);
  int num_trips =
      load_csv(args.trips_path, sizeof(Trip), parse_trip, (void **)&trips);

  if (num_employees < 0 || num_trips < 0) {
    fprintf(stderr, "Error loading CSV files.\n");
    free(employees);
    free(trips);
    return 1;
  }

  printf("Loaded %d employees and %d trips.\n", num_employees, num_trips);
  printf("Configured for %d processors.\n", args.num_procs);

  /* ---- MPI init ---- */
  MPI_Init(&argc, &argv);
  MPI_Comm_size(MPI_COMM_WORLD, &numtasks);
  MPI_Comm_rank(MPI_COMM_WORLD, &taskid);

  if (numtasks != args.num_procs) {
    if (taskid == MASTER)
      fprintf(stderr, "Warning: -n %d but running with %d MPI tasks.\n",
              args.num_procs, numtasks);
  }

  /* ---- synchronize and start timing ---- */
  MPI_Barrier(MPI_COMM_WORLD);

  struct timespec t_start, t_end;
  if (taskid == MASTER) {
    clock_gettime(CLOCK_MONOTONIC, &t_start);
  }

  /* ================================================================== */
  /* Phase 1: Determine this rank's trips chunk                         */
  /* ================================================================== */

  int local_count = num_trips / numtasks;
  int leftover = num_trips % numtasks;

  int chunk_start = taskid * local_count;
  if (taskid < leftover) {
    chunk_start += taskid;
    local_count++;
  } else {
    chunk_start += leftover;
  }
  int chunk_end = chunk_start + local_count;

  /* ================================================================== */
  /* Phase 2: Build local employee hash table (id % n == taskid)        */
  /* ================================================================== */

  HashTable ht;
  hash_init(&ht);

  for (int i = 0; i < num_employees; i++) {
    if (employees[i].id % numtasks == taskid) {
      hash_insert(&ht, &employees[i]);
    }
  }

  /* ================================================================== */
  /* Phase 3: Bin trips by destination rank and exchange via Alltoallv   */
  /* ================================================================== */

  /* 3a: Count how many trips from our chunk go to each rank */
  int *send_counts = calloc(numtasks, sizeof(int));
  for (int i = chunk_start; i < chunk_end; i++) {
    int dest = trips[i].id % numtasks;
    send_counts[dest]++;
  }

  /* 3b: Exchange counts so each rank knows how many it will receive */
  int *recv_counts = malloc(numtasks * sizeof(int));
  MPI_Alltoall(send_counts, 1, MPI_INT, recv_counts, 1, MPI_INT,
               MPI_COMM_WORLD);

  /* 3c: Compute send/recv displacements (in units of Trip) */
  int *send_displs = malloc(numtasks * sizeof(int));
  int *recv_displs = malloc(numtasks * sizeof(int));
  send_displs[0] = 0;
  recv_displs[0] = 0;
  for (int i = 1; i < numtasks; i++) {
    send_displs[i] = send_displs[i - 1] + send_counts[i - 1];
    recv_displs[i] = recv_displs[i - 1] + recv_counts[i - 1];
  }

  int total_recv = recv_displs[numtasks - 1] + recv_counts[numtasks - 1];

  /* 3d: Pack trips into send buffer, ordered by destination rank */
  Trip *send_buf = malloc(local_count * sizeof(Trip));
  int *offsets = malloc(numtasks * sizeof(int));
  memcpy(offsets, send_displs, numtasks * sizeof(int));

  for (int i = chunk_start; i < chunk_end; i++) {
    int dest = trips[i].id % numtasks;
    send_buf[offsets[dest]++] = trips[i];
  }

  /* 3e: One-sided MPI RMA (Put) instead of Alltoallv */
  Trip *recv_buf;
  MPI_Win win_recv, win_offset;

  /* Allocate memory for the window that will receive trips */
  MPI_Win_allocate(total_recv * sizeof(Trip), sizeof(Trip), MPI_INFO_NULL,
                   MPI_COMM_WORLD, &recv_buf, &win_recv);

  /* Allocate memory for an atomic offset counter (1 integer per rank) */
  int *local_offset;
  MPI_Win_allocate(sizeof(int), sizeof(int), MPI_INFO_NULL, MPI_COMM_WORLD,
                   &local_offset, &win_offset);

  /* Initialize our local offset to 0 */
  MPI_Win_lock(MPI_LOCK_EXCLUSIVE, taskid, 0, win_offset);
  *local_offset = 0;
  MPI_Win_unlock(taskid, win_offset);

  MPI_Barrier(MPI_COMM_WORLD); /* ensure all windows and offsets are ready */

  /* Start RMA epoch */
  MPI_Win_fence(0, win_recv);
  MPI_Win_fence(0, win_offset);

  for (int i = 0; i < numtasks; i++) {
    int count = send_counts[i];
    if (count > 0) {
      int remote_offset;
      /* Atomically fetch the current offset at destination 'i' and add 'count'
       * to it */
      MPI_Fetch_and_op(&count, &remote_offset, MPI_INT, i, 0, MPI_SUM,
                       win_offset);

      /* We must flush the offset window to ensure remote_offset is returned
       * before Put */
      MPI_Win_flush(i, win_offset);

      /* Put the trips into the destination's window at the reserved offset */
      MPI_Put(&send_buf[offsets[i] - count], count * sizeof(Trip), MPI_BYTE, i,
              remote_offset, count * sizeof(Trip), MPI_BYTE, win_recv);
    }
  }

  /* End RMA epoch — blocks until all Puts from all ranks have completed */
  MPI_Win_fence(0, win_offset);
  MPI_Win_fence(0, win_recv);

  /* ================================================================== */
  /* Phase 4: Local hash-probe join + write matches to per-rank file    */
  /* ================================================================== */

  char filename[64];
  sprintf(filename, "results_rank%d.csv", taskid);
  FILE *out = fopen(filename, "w");

  long long local_match_count = 0;

  for (int i = 0; i < total_recv; i++) {
    Employee *match = hash_lookup(&ht, recv_buf[i].id);
    if (match) {
      fprintf(out, "%d,%s,%lld\n", match->id, recv_buf[i].destination,
              recv_buf[i].timestamp);
      local_match_count++;
    }
  }

  fclose(out);

  /* ---- stop timing ---- */
  MPI_Barrier(MPI_COMM_WORLD);

  if (taskid == MASTER) {
    clock_gettime(CLOCK_MONOTONIC, &t_end);
  }

  /* ---- reduce match counts ---- */
  long long total_match_count = 0;
  MPI_Reduce(&local_match_count, &total_match_count, 1, MPI_LONG_LONG, MPI_SUM,
             MASTER, MPI_COMM_WORLD);

  /* ---- master reports results and concatenates output files ---- */
  if (taskid == MASTER) {
    double elapsed = (t_end.tv_sec - t_start.tv_sec) +
                     (t_end.tv_nsec - t_start.tv_nsec) / 1e9;

    printf("\nTotal matches: %lld\n", total_match_count);
    printf("Join time:     %.4f seconds\n", elapsed);

    /* Concatenate per-rank files into results.csv */
    FILE *combined = fopen("results_mpi.csv", "w");
    fprintf(combined, "id,destination,timestamp\n");

    char buf[MAX_LINE_LEN];
    for (int r = 0; r < numtasks; r++) {
      sprintf(filename, "results_rank%d.csv", r);
      FILE *part = fopen(filename, "r");
      if (part) {
        while (fgets(buf, sizeof(buf), part))
          fputs(buf, combined);
        fclose(part);
        remove(filename); /* clean up temp file */
      }
    }
    fclose(combined);
    printf("Combined results written to results_mpi.csv\n");
  }

  /* ---- cleanup ---- */
  hash_free(&ht);
  free(employees);
  free(trips);
  free(send_buf);
  free(send_counts);
  free(recv_counts);
  free(send_displs);
  free(recv_displs);
  free(offsets);

  MPI_Win_free(&win_recv);
  MPI_Win_free(&win_offset);

  MPI_Finalize();
  return 0;
}
