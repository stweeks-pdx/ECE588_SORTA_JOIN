#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

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
} Args;

/* ---------- argument parsing ---------- */

/*
 * Prints usage instructions to stderr.
 *
 * prog - the name of the executable (typically argv[0])
 */
void print_usage(const char *prog) {
    fprintf(stderr, "Usage: %s -e <employees.csv> -t <trips.csv>\n", prog);
    fprintf(stderr, "  -e  Path to employees CSV file\n");
    fprintf(stderr, "  -t  Path to trips CSV file\n");
}

/*
 * Parses command-line arguments using getopt.
 *
 * argc - argument count from main
 * argv - argument vector from main
 *
 * Returns a populated Args struct with paths to the employee and trips CSVs.
 * Exits with EXIT_FAILURE if required flags are missing or unknown flags
 * are provided.
 */
Args parse_args(int argc, char *argv[]) {
    Args args = { NULL, NULL };
    int opt;

    while ((opt = getopt(argc, argv, "e:t:")) != -1) {
        switch (opt) {
            case 'e':
                args.employee_path = optarg;
                break;
            case 't':
                args.trips_path = optarg;
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

    return args;
}

/* ---------- generic CSV loader ---------- */

/*
 * Loads a CSV file into a dynamically allocated array.
 *
 * path         - path to the CSV file
 * record_size  - sizeof each record (e.g. sizeof(Employee))
 * parse_row    - callback that parses one CSV line into the record at `record`
 *                returns 0 on success, -1 to skip the row
 * out          - set to the allocated array on success
 *
 * Returns the number of records loaded, or -1 on error.
 */
int load_csv(const char *path, size_t record_size,
             int (*parse_row)(char *line, void *record),
             void **out)
{
    FILE *f = fopen(path, "r");
    if (!f) { perror(path); return -1; }

    int capacity = 1024, count = 0;
    char *arr = malloc(capacity * record_size);

    char line[MAX_LINE_LEN];
    fgets(line, sizeof(line), f); /* skip header */

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

/* ---------- row parsers ---------- */

/*
 * Parses a single CSV line into an Employee record.
 * Expected format: id,name
 *
 * line   - null-terminated string containing one CSV row (will be mutated
 *          by strtok)
 * record - pointer to an Employee to populate
 *
 * Returns 0 on success, -1 if the line is malformed and should be skipped.
 */
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

/*
 * Parses a single CSV line into a Trip record.
 * Expected format: timestamp,id,destination
 *
 * line   - null-terminated string containing one CSV row (will be mutated
 *          by strtok)
 * record - pointer to a Trip to populate
 *
 * Returns 0 on success, -1 if the line is malformed and should be skipped.
 */
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

    FILE *out = fopen("../test/results.csv", "w");
    if (!out) {
        perror("../test/results.csv");
        free(employees);
        free(trips);
        return 1;
    }
    fprintf(out, "id,destination,timestamp\n");

    struct timespec t_start, t_end;
    clock_gettime(CLOCK_MONOTONIC, &t_start);

    long long match_count = 0;

    /* Naive nested-loop join: O(n * m) */
    for (int i = 0; i < num_employees; i++) {
        for (int j = 0; j < num_trips; j++) {
            if (employees[i].id == trips[j].id) {
                fprintf(out, "%d,%s,%lld\n",
                        employees[i].id,
                        trips[j].destination,
                        trips[j].timestamp);
                match_count++;
            }
        }
    }

    clock_gettime(CLOCK_MONOTONIC, &t_end);
    fclose(out);

    double elapsed = (t_end.tv_sec  - t_start.tv_sec) +
                     (t_end.tv_nsec - t_start.tv_nsec) / 1e9;

    printf("Total matches: %lld\n", match_count);
    printf("Join time:     %.4f seconds\n", elapsed);

    free(employees);
    free(trips);
    return 0;
}
