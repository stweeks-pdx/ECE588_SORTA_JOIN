#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>
#include <errno.h>

#define MAX_NAME_LEN    31
#define MAX_DEST_LEN    21
#define MAX_LINE_LEN   256

typedef struct {
    int  id;
    char name[MAX_NAME_LEN];
} Employee;

typedef struct {
    long long timestamp;
    int       id;
    char      destination[MAX_DEST_LEN];
} Trip;

typedef struct {
    const char *employee_path;
    const char *trips_path;
    int threads;
} Args;

/* ---------- argument parsing ---------- */

void print_usage(const char *prog) {
    fprintf(stderr, "Usage: %s -e <employees.csv> -t <trips.csv> -n <threads>\n", prog);
    fprintf(stderr, "  -e  Path to employees CSV file\n");
    fprintf(stderr, "  -t  Path to trips CSV file\n");
    fprintf(stderr, "  -n  Number of worker threads (>=1). If omitted, defaults to online CPU count.\n");
}

static int clamp_threads(int n) {
    if (n < 1) return 1;
    return n;
}

Args parse_args(int argc, char *argv[]) {
    Args args = { NULL, NULL, 0 };
    int opt;

    while ((opt = getopt(argc, argv, "e:t:n:")) != -1) {
        switch (opt) {
            case 'e':
                args.employee_path = optarg;
                break;
            case 't':
                args.trips_path = optarg;
                break;
            case 'n':
                args.threads = atoi(optarg);
                break;
            default:
                print_usage(argv[0]);
                exit(EXIT_FAILURE);
        }
    }

    if (!args.employee_path || !args.trips_path) {
        fprintf(stderr, "Error: both -e and -t are required.\n");
        print_usage(argv[0]);
        exit(EXIT_FAILURE);
    }

    if (args.threads <= 0) {
        long ncpu = sysconf(_SC_NPROCESSORS_ONLN);
        if (ncpu < 1) ncpu = 1;
        args.threads = (int)ncpu;
    }
    args.threads = clamp_threads(args.threads);

    return args;
}

/* ---------- generic CSV loader ---------- */

int load_csv(const char *path, size_t record_size,
             int (*parse_row)(char *line, void *record),
             void **out)
{
    FILE *f = fopen(path, "r");
    if (!f) { perror(path); return -1; }

    int capacity = 1024, count = 0;
    char *arr = (char *)malloc((size_t)capacity * record_size);
    if (!arr) { perror("malloc"); fclose(f); return -1; }

    char line[MAX_LINE_LEN];

    while (fgets(line, sizeof(line), f)) {
        if (count == capacity) {
            capacity *= 2;
            char *tmp = (char *)realloc(arr, (size_t)capacity * record_size);
            if (!tmp) {
                perror("realloc");
                free(arr);
                fclose(f);
                return -1;
            }
            arr = tmp;
        }
        if (parse_row(line, arr + (size_t)count * record_size) == 0)
            count++;
    }

    fclose(f);
    *out = arr;
    return count;
}

/* ---------- row parsers ---------- */

int parse_employee(char *line, void *record) {
    Employee *e = (Employee *)record;

    char *tok = strtok(line, ",\n");
    if (!tok) return -1;
    e->id = atoi(tok);

    tok = strtok(NULL, ",\n");
    if (!tok) return -1;
    strncpy(e->name, tok, MAX_NAME_LEN - 1);
    e->name[MAX_NAME_LEN - 1] = '\0';

    return 0;
}

int parse_trip(char *line, void *record) {
    Trip *t = (Trip *)record;

    char *tok = strtok(line, ",\n");
    if (!tok) return -1;
    t->timestamp = atoll(tok);

    tok = strtok(NULL, ",\n");
    if (!tok) return -1;
    t->id = atoi(tok);

    tok = strtok(NULL, ",\n");
    if (!tok) return -1;
    strncpy(t->destination, tok, MAX_DEST_LEN - 1);
    t->destination[MAX_DEST_LEN - 1] = '\0';

    return 0;
}

/* ---------- pthread join worker ---------- */

typedef struct {
    int tid;
    int start_idx;   /* inclusive */
    int end_idx;     /* exclusive */
    const Employee *employees;
    int num_employees;
    const Trip *trips;
    int num_trips;
    char tmp_path[256];
    long long matches;
    int err;         /* 0 ok, else errno-style */
} ThreadCtx;

static void *worker_join(void *arg) {
    ThreadCtx *ctx = (ThreadCtx *)arg;

    FILE *f = fopen(ctx->tmp_path, "w");
    if (!f) {
        ctx->err = errno ? errno : 1;
        return NULL;
    }

    long long local = 0;
    for (int i = ctx->start_idx; i < ctx->end_idx; i++) {
        int eid = ctx->employees[i].id;
        for (int j = 0; j < ctx->num_trips; j++) {
            if (eid == ctx->trips[j].id) {
                /* NOTE: no header in temp files */
                fprintf(f, "%d,%s,%lld\n",
                        eid,
                        ctx->trips[j].destination,
                        ctx->trips[j].timestamp);
                local++;
            }
        }
    }

    fclose(f);
    ctx->matches = local;
    ctx->err = 0;
    return NULL;
}

static int concat_file(FILE *out, const char *path) {
    FILE *in = fopen(path, "r");
    if (!in) return -1;

    char buf[1 << 16];
    size_t n;
    while ((n = fread(buf, 1, sizeof(buf), in)) > 0) {
        if (fwrite(buf, 1, n, out) != n) {
            fclose(in);
            return -1;
        }
    }
    fclose(in);
    return 0;
}

/* ---------- main ---------- */

int main(int argc, char *argv[]) {
    Args args = parse_args(argc, argv);

    Employee *employees = NULL;
    Trip     *trips     = NULL;

    int num_employees = load_csv(args.employee_path, sizeof(Employee),
                                 parse_employee, (void **)&employees);
    int num_trips     = load_csv(args.trips_path, sizeof(Trip),
                                 parse_trip, (void **)&trips);

    if (num_employees < 0 || num_trips < 0) {
        free(employees);
        free(trips);
        return 1;
    }

    int nthreads = args.threads;
    if (nthreads > num_employees && num_employees > 0) {
        /* Dont spawn more threads than employees to partition */
        nthreads = num_employees;
        if (nthreads < 1) nthreads = 1;
    }

    pthread_t *threads = (pthread_t *)calloc((size_t)nthreads, sizeof(pthread_t));
    ThreadCtx *ctxs    = (ThreadCtx *)calloc((size_t)nthreads, sizeof(ThreadCtx));
    if (!threads || !ctxs) {
        perror("calloc");
        free(threads);
        free(ctxs);
        free(employees);
        free(trips);
        return 1;
    }

    /* Partition employees into contiguous chunks */
    int base = (num_employees / nthreads);
    int rem  = (num_employees % nthreads);

    struct timespec t_start, t_end;
    clock_gettime(CLOCK_MONOTONIC, &t_start);

    int idx = 0;
    for (int t = 0; t < nthreads; t++) {
        int chunk = base + (t < rem ? 1 : 0);

        ctxs[t].tid = t;
        ctxs[t].start_idx = idx;
        ctxs[t].end_idx   = idx + chunk;
        ctxs[t].employees = employees;
        ctxs[t].num_employees = num_employees;
        ctxs[t].trips = trips;
        ctxs[t].num_trips = num_trips;
        ctxs[t].matches = 0;
        ctxs[t].err = 0;

        /* Temp output per thread */
        snprintf(ctxs[t].tmp_path, sizeof(ctxs[t].tmp_path),
                 "./results.part.%d.csv", t);

        idx += chunk;

        int rc = pthread_create(&threads[t], NULL, worker_join, &ctxs[t]);
        if (rc != 0) {
            fprintf(stderr, "pthread_create failed for thread %d (rc=%d)\n", t, rc);
            /* best-effort join already-started threads */
            for (int k = 0; k < t; k++) pthread_join(threads[k], NULL);
            free(threads);
            free(ctxs);
            free(employees);
            free(trips);
            return 1;
        }
    }

    long long match_count = 0;
    int any_err = 0;
    for (int t = 0; t < nthreads; t++) {
        pthread_join(threads[t], NULL);
        match_count += ctxs[t].matches;
        if (ctxs[t].err != 0) {
            any_err = 1;
            fprintf(stderr, "Thread %d failed writing %s (err=%d)\n",
                    t, ctxs[t].tmp_path, ctxs[t].err);
        }
    }

    /* Write final output by concatenating temp parts */
    FILE *out = fopen("./results.csv", "w");
    if (!out) {
        perror(".test/results.csv");
        any_err = 1;
    } else {
        fprintf(out, "id,destination,timestamp\n");
        for (int t = 0; t < nthreads; t++) {
            if (concat_file(out, ctxs[t].tmp_path) != 0) {
                fprintf(stderr, "Failed concatenating %s\n", ctxs[t].tmp_path);
                any_err = 1;
                break;
            }
        }
        fclose(out);
    }

    /* Cleanup temp files */
    for (int t = 0; t < nthreads; t++) {
        unlink(ctxs[t].tmp_path);
    }

    clock_gettime(CLOCK_MONOTONIC, &t_end);

    double elapsed = (t_end.tv_sec  - t_start.tv_sec) +
                     (t_end.tv_nsec - t_start.tv_nsec) / 1e9;

    printf("Threads used:  %d\n", nthreads);
    printf("Total matches: %lld\n", match_count);
    printf("Join time:     %.4f seconds\n", elapsed);

    free(threads);
    free(ctxs);
    free(employees);
    free(trips);

    return any_err ? 1 : 0;
}
