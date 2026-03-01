CC = mpicc
CFLAGS = -Wall -Wextra -O3

TARGETS = mpi_join mpi_join_rma

.PHONY: all clean benchmarks

all: $(TARGETS)

mpi_join: src/mpi_join.c
	$(CC) $(CFLAGS) -o $@ $<

mpi_join_rma: src/mpi_join_rma.c
	$(CC) $(CFLAGS) -o $@ $<

clean:
	rm -f $(TARGETS) results_rank*.csv results_mpi.csv

MAX_PROCS ?= 16

benchmarks: all
	@echo "=== Running MPI Alltoallv Benchmark ==="
	bash benchmark_mpi.sh $(MAX_PROCS)
	@echo ""
	@echo "=== Running MPI RMA Benchmark ==="
	bash benchmark_rma.sh $(MAX_PROCS)
