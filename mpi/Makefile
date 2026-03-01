CC = mpicc
CFLAGS = -Wall -Wextra -O3

TARGETS = mpi_join

.PHONY: all clean benchmarks

all: $(TARGETS)

mpi_join: src/mpi_join.c
	$(CC) $(CFLAGS) -o $@ $<

clean:
	rm -f $(TARGETS) results_rank*.csv results_mpi.csv

MAX_PROCS ?= 16

benchmarks: all
	@echo "=== Running MPI Alltoallv Benchmark ==="
	bash benchmark_mpi.sh $(MAX_PROCS)
