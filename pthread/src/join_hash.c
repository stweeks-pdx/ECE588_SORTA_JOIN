#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>
#include <errno.h>
#include <stdint.h>
#include <sys/stat.h>
#include <sys/types.h>

#define MAX_NAME_LEN    31
#define MAX_DEST_LEN    21
#define MAX_LINE_LEN   256

/* Set to 1 if your CSVs have a header row, 0 if they do not */
#define SKIP_HEADER 0

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

static void print_usage(const char *prog) {
    fprintf(stderr, "Usage: %s -e <employees.csv> -t <trips.csv> -n <threads>\n", prog);
    fprintf(stderr, "  -e  Path to employees CSV file\n");
    fprintf(stderr, "  -t  Path to trips CSV file\n");
    fprintf(stderr, "  -n  Number of worker threads (>=1). If omitted, defaults to online CPU count.\n");
}

static int clamp_threads(int n) {
    return (n < 1) ? 1 : n;
}

static Args parse_args(int argc, char *argv[]) {
    Args args = { NULL, NULL, 0 };
    int opt;

    while ((opt = getopt(argc, argv, "e:t:n:")) != -1) {
        switch (opt) {
            case 'e': args.employee_path = optarg; break;
            case 't': args.trips_path    = optarg; break;
            case 'n': args.threads       = atoi(optarg); break;
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

/* ---------- filesystem helper ---------- */

static int ensure_dir(const char *dir) {
    struct stat st;
    if (stat(dir, &st) == 0) {
        return S_ISDIR(st.st_mode) ? 0 : -1;
    }
    if (mkdir(dir, 0777) == 0) return 0;
    return (errno == EEXIST) ? 0 : -1;
}

/* ---------- generic CSV loader ---------- */

static int load_csv(const char *path, size_t record_size,
                    int (*parse_row)(char *line, void *record),
                    void **out)
{
    FILE *f = fopen(path, "r");
    if (!f) { perror(path); return -1; }

    int capacity = 1024, count = 0;
    char *arr = (char *)malloc((size_t)capacity * record_size);
    if (!arr) { perror("malloc"); fclose(f); return -1; }

    char line[MAX_LINE_LEN];

#if SKIP_HEADER
    if (!fgets(line, sizeof(line), f)) {
        fclose(f);
        *out = arr;
        return 0;
    }
#endif

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

static int parse_employee(char *line, void *record) {
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

static int parse_trip(char *line, void *record) {
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

/* ---------- hash set for employee IDs ---------- */
/* Open-addressing hash set (linear probing), built once then read-only. */

typedef struct {
    uint32_t *keys;     /* employee ids as uint32_t */
    size_t cap;         /* power of two */
    size_t used;
    int has_sentinel;   /* whether we inserted SENTINEL_KEY as a real key */
} IdSet;

static const uint32_t SENTINEL_KEY = 0xFFFFFFFFu;

static uint32_t mix32(uint32_t x) {
    /* decent 32-bit mix (no crypto needed) */
    x ^= x >> 16;
    x *= 0x7feb352du;
    x ^= x >> 15;
    x *= 0x846ca68bu;
    x ^= x >> 16;
    return x;
}

static size_t next_pow2(size_t x) {
    size_t p = 1;
    while (p < x) p <<= 1;
    return p;
}

static int idset_init(IdSet *s, size_t expected) {
    /* Keep load factor <= ~0.5 for fast probes */
    size_t cap = next_pow2(expected * 2 + 8);
    s->keys = (uint32_t *)malloc(cap * sizeof(uint32_t));
    if (!s->keys) return -1;
    for (size_t i = 0; i < cap; i++) s->keys[i] = SENTINEL_KEY;
    s->cap = cap;
    s->used = 0;
    s->has_sentinel = 0;
    return 0;
}

static void idset_free(IdSet *s) {
    free(s->keys);
    s->keys = NULL;
    s->cap = s->used = 0;
    s->has_sentinel = 0;
}

static void idset_insert(IdSet *s, uint32_t key) {
    if (key == SENTINEL_KEY) {
        s->has_sentinel = 1;
        return;
    }
    size_t mask = s->cap - 1;
    size_t idx = (size_t)mix32(key) & mask;
    while (1) {
        uint32_t cur = s->keys[idx];
        if (cur == SENTINEL_KEY) {
            s->keys[idx] = key;
            s->used++;
            return;
        }
        if (cur == key) return; /* already present */
        idx = (idx + 1) & mask;
    }
}

static int idset_contains(const IdSet *s, uint32_t key) {
    if (key == SENTINEL_KEY) return s->has_sentinel;

    size_t mask = s->cap - 1;
    size_t idx = (size_t)mix32(key) & mask;
    while (1) {
        uint32_t cur = s->keys[idx];
        if (cur == SENTINEL_KEY) return 0; /* miss */
        if (cur == key) return 1;
        idx = (idx + 1) & mask;
    }
}

/* ---------- pthread worker (scan trips) ---------- */

typedef struct {
    int tid;
    int start_idx;    /* inclusive */
    int end_idx;      /* exclusive */
    const Trip *trips;
    int num_trips;
    const IdSet *idset;
    char tmp_path[256];
    long long matches;
    int err;
} ThreadCtx;

static void *worker_scan_trips(void *arg) {
    ThreadCtx *ctx = (ThreadCtx *)arg;

    FILE *f = fopen(ctx->tmp_path, "w");
    if (!f) {
        ctx->err = errno ? errno : 1;
        return NULL;
    }

    long long local = 0;
    for (int i = ctx->start_idx; i < ctx->end_idx; i++) {
        uint32_t tid = (uint32_t)ctx->trips[i].id;
        if (idset_contains(ctx->idset, tid)) {
            fprintf(f, "%d,%s,%lld\n",
                    ctx->trips[i].id,
                    ctx->trips[i].destination,
                    ctx->trips[i].timestamp);
            local++;
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

    const char *out_dir = "../test";
    if (ensure_dir(out_dir) != 0) {
        fprintf(stderr, "Error: output directory '%s' missing and could not be created.\n", out_dir);
        perror("mkdir/stat");
        free(employees);
        free(trips);
        return 1;
    }

    struct timespec t_start, t_end;
    clock_gettime(CLOCK_MONOTONIC, &t_start);

    /* Build hash set of employee IDs (single-threaded, then read-only) */
    IdSet set;
    if (idset_init(&set, (size_t)num_employees) != 0) {
        perror("idset_init");
        free(employees);
        free(trips);
        return 1;
    }
    for (int i = 0; i < num_employees; i++) {
        idset_insert(&set, (uint32_t)employees[i].id);
    }

    int nthreads = args.threads;
    if (nthreads > num_trips && num_trips > 0) nthreads = num_trips;
    if (nthreads < 1) nthreads = 1;

    pthread_t *threads = (pthread_t *)calloc((size_t)nthreads, sizeof(pthread_t));
    ThreadCtx *ctxs    = (ThreadCtx *)calloc((size_t)nthreads, sizeof(ThreadCtx));
    if (!threads || !ctxs) {
        perror("calloc");
        free(threads);
        free(ctxs);
        idset_free(&set);
        free(employees);
        free(trips);
        return 1;
    }

    /* Partition trips into contiguous chunks */
    int base = (num_trips / nthreads);
    int rem  = (num_trips % nthreads);


    int idx = 0;
    for (int t = 0; t < nthreads; t++) {
        int chunk = base + (t < rem ? 1 : 0);

        ctxs[t].tid = t;
        ctxs[t].start_idx = idx;
        ctxs[t].end_idx   = idx + chunk;
        ctxs[t].trips = trips;
        ctxs[t].num_trips = num_trips;
        ctxs[t].idset = &set;
        ctxs[t].matches = 0;
        ctxs[t].err = 0;

        snprintf(ctxs[t].tmp_path, sizeof(ctxs[t].tmp_path),
                 "%s/results.part.%d.csv", out_dir, t);

        idx += chunk;

        int rc = pthread_create(&threads[t], NULL, worker_scan_trips, &ctxs[t]);
        if (rc != 0) {
            fprintf(stderr, "pthread_create failed for thread %d (rc=%d)\n", t, rc);
            for (int k = 0; k < t; k++) pthread_join(threads[k], NULL);
            free(threads);
            free(ctxs);
            idset_free(&set);
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

    /* Write final output */
    FILE *out = fopen("../test/results.csv", "w");
    if (!out) {
        perror("../test/results.csv");
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
    printf("Employees:     %d\n", num_employees);
    printf("Trips:         %d\n", num_trips);
    printf("Total matches: %lld\n", match_count);
    printf("Join time:     %.4f seconds\n", elapsed);

    free(threads);
    free(ctxs);
    idset_free(&set);
    free(employees);
    free(trips);

    return any_err ? 1 : 0;
}
