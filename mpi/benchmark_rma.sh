#!/bin/bash
# benchmark_mpi.sh
# Runs mpi_join_rma with 1 through 16 processors and produces a speedup table.
#
# Usage: ./benchmark_mpi.sh [max_procs]

MAX_PROCS=${1:-16}
BINARY="./mpi_join_rma"
EMPLOYEES="../dataset/employees.csv"
TRIPS="../dataset/trips.csv"

# Compile
echo "Compiling mpi_join_rma..."
mpicc -o "$BINARY" src/mpi_join_rma.c
if [ $? -ne 0 ]; then
    echo "Compilation failed."
    exit 1
fi

echo ""
echo "Benchmarking 1 to $MAX_PROCS processors..."
echo "Procs | Time (s)   | Speedup"
echo "------+------------+--------"

BASE_TIME=""

for np in $(seq 1 $MAX_PROCS); do
    # Extract just the join time from output
    TIME=$(mpirun --oversubscribe -np "$np" "$BINARY" \
        -e "$EMPLOYEES" -t "$TRIPS" -n "$np" 2>/dev/null \
        | grep "Join time:" | awk '{print $3}')

    if [ "$np" -eq 1 ]; then
        BASE_TIME=$TIME
    fi

    SPEEDUP=$(echo "scale=2; $BASE_TIME / $TIME" | bc 2>/dev/null)

    printf "%5d | %10s | %sx\n" "$np" "$TIME" "$SPEEDUP"
done

echo ""
echo "Done. Base time (1 proc): ${BASE_TIME}s"
