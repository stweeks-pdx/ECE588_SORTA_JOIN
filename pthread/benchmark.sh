#!/usr/bin/env bash
# =============================================================================
# benchmark.sh — Compare speedup of naive, join (threaded), and join_hash
# Usage: ./benchmark.sh -e <employees.csv> -t <trips.csv> \
#                       --src-naive <dir> --src-join <dir>
#
# Compiles all three programs, runs:
#   - naive.c             once (single-threaded baseline)
#   - join.c              for 1..16 threads
#   - join_hash.c         for 1..16 threads
#
# Outputs a summary table and speedup ratios relative to naive.
# =============================================================================

set -euo pipefail

# ---------- defaults ----------
SRC_NAIVE="."
SRC_JOIN="."
EMPLOYEES=""
TRIPS=""
RESULTS_DIR="./bench_results"
MAX_THREADS=16

# ---------- usage ----------
usage() {
    echo "Usage: $0 -e <employees.csv> -t <trips.csv> [--src-naive <dir>] [--src-join <dir>]"
    echo ""
    echo "  -e               Path to employees CSV"
    echo "  -t               Path to trips CSV"
    echo "  --src-naive DIR  Directory containing naive.c (default: .)"
    echo "  --src-join DIR   Directory containing join.c and join_hash.c (default: .)"
    exit 1
}

# ---------- arg parsing ----------
while [[ $# -gt 0 ]]; do
    case "$1" in
        -e) EMPLOYEES="$2"; shift 2 ;;
        -t) TRIPS="$2";     shift 2 ;;
        --src-naive) SRC_NAIVE="$2"; shift 2 ;;
        --src-join)  SRC_JOIN="$2";  shift 2 ;;
        -h|--help) usage ;;
        *) echo "Unknown argument: $1"; usage ;;
    esac
done

[[ -z "$EMPLOYEES" || -z "$TRIPS" ]] && { echo "Error: -e and -t are required."; usage; }
[[ ! -f "$EMPLOYEES" ]] && { echo "Error: employees file not found: $EMPLOYEES"; exit 1; }
[[ ! -f "$TRIPS"     ]] && { echo "Error: trips file not found: $TRIPS"; exit 1; }

[[ ! -f "$SRC_NAIVE/naive.c"    ]] && { echo "Error: source file not found: $SRC_NAIVE/naive.c";    exit 1; }
[[ ! -f "$SRC_JOIN/join.c"      ]] && { echo "Error: source file not found: $SRC_JOIN/join.c";      exit 1; }
[[ ! -f "$SRC_JOIN/join_hash.c" ]] && { echo "Error: source file not found: $SRC_JOIN/join_hash.c"; exit 1; }

# ---------- setup ----------
mkdir -p "$RESULTS_DIR"
# join_hash.c hard-codes ../test for output — create it relative to where we run
mkdir -p "./test"

BIN_NAIVE="$RESULTS_DIR/naive"
BIN_JOIN="$RESULTS_DIR/join"
BIN_HASH="$RESULTS_DIR/join_hash"

echo "============================================================"
echo " Compiling..."
echo "============================================================"

cc -O2 -o "$BIN_NAIVE" "$SRC_NAIVE/naive.c"               && echo "  [OK] naive"
cc -O2 -o "$BIN_JOIN"  "$SRC_JOIN/join.c"      -lpthread  && echo "  [OK] join"
cc -O2 -o "$BIN_HASH"  "$SRC_JOIN/join_hash.c" -lpthread  && echo "  [OK] join_hash"

echo ""

# ---------- helper: extract join time ----------
# Expects output like: "Join time:     0.1234 seconds"
extract_time() {
    grep -E "Join time:|join time:" "$1" | grep -oE '[0-9]+\.[0-9]+' | head -1
}

extract_time_naive() {
    grep -E "Join time:|join time:" "$1" | grep -oE '[0-9]+\.[0-9]+' | head -1
}

# ---------- run naive baseline ----------
echo "============================================================"
echo " Running naive (baseline)..."
echo "============================================================"

NAIVE_LOG="$RESULTS_DIR/naive.log"
"$BIN_NAIVE" -e "$EMPLOYEES" -t "$TRIPS" > "$NAIVE_LOG" 2>&1
NAIVE_TIME=$(extract_time_naive "$NAIVE_LOG")

if [[ -z "$NAIVE_TIME" ]]; then
    echo "Error: could not parse join time from naive output:"
    cat "$NAIVE_LOG"
    exit 1
fi

echo "  naive time: ${NAIVE_TIME}s"
echo ""

# ---------- run join.c and join_hash.c for 1..MAX_THREADS ----------
echo "============================================================"
echo " Running join & join_hash for 1..$MAX_THREADS threads..."
echo "============================================================"

declare -a JOIN_TIMES
declare -a HASH_TIMES

for n in $(seq 1 $MAX_THREADS); do
    # --- join ---
    LOG="$RESULTS_DIR/join_t${n}.log"
    "$BIN_JOIN" -e "$EMPLOYEES" -t "$TRIPS" -n "$n" > "$LOG" 2>&1
    t=$(extract_time "$LOG")
    JOIN_TIMES[$n]="${t:-N/A}"

    # --- join_hash ---
    LOG="$RESULTS_DIR/hash_t${n}.log"
    "$BIN_HASH" -e "$EMPLOYEES" -t "$TRIPS" -n "$n" > "$LOG" 2>&1
    t=$(extract_time "$LOG")
    HASH_TIMES[$n]="${t:-N/A}"

    echo "  threads=$n  join=${JOIN_TIMES[$n]}s  join_hash=${HASH_TIMES[$n]}s"
done

echo ""

# ---------- compute speedup (awk for floating point) ----------
speedup() {
    local base="$1" t="$2"
    if [[ "$t" == "N/A" || -z "$t" ]]; then
        echo "N/A"
    else
        awk "BEGIN { printf \"%.2fx\", $base / $t }"
    fi
}

# ---------- print summary table ----------
echo "============================================================"
echo " RESULTS SUMMARY"
echo " Baseline (naive): ${NAIVE_TIME}s"
echo " Speedup vs naive shown in () — final column is join_hash vs join"
echo "============================================================"
printf "%-10s | %-12s %-12s | %-12s %-12s | %-14s\n" \
    "Threads" "join(s)" "vs naive" "join_hash(s)" "vs naive" "hash vs join"
printf "%.0s-" {1..80}; echo ""

for n in $(seq 1 $MAX_THREADS); do
    jt="${JOIN_TIMES[$n]}"
    ht="${HASH_TIMES[$n]}"
    js=$(speedup "$NAIVE_TIME" "$jt")
    hs=$(speedup "$NAIVE_TIME" "$ht")
    hvj=$(speedup "$jt" "$ht")
    printf "%-10s | %-12s %-12s | %-12s %-12s | %-14s\n" \
        "$n" "$jt" "$js" "$ht" "$hs" "$hvj"
done

echo ""
echo "Logs saved to: $RESULTS_DIR/"

# ---------- optional: save CSV of results ----------
CSV_OUT="$RESULTS_DIR/benchmark_summary.csv"
echo "threads,join_time_s,join_vs_naive,join_hash_time_s,join_hash_vs_naive,hash_vs_join" > "$CSV_OUT"
for n in $(seq 1 $MAX_THREADS); do
    jt="${JOIN_TIMES[$n]}"
    ht="${HASH_TIMES[$n]}"
    js=$(speedup "$NAIVE_TIME" "$jt" | tr -d 'x')
    hs=$(speedup "$NAIVE_TIME" "$ht" | tr -d 'x')
    hvj=$(speedup "$jt" "$ht" | tr -d 'x')
    echo "$n,$jt,$js,$ht,$hs,$hvj" >> "$CSV_OUT"
done
echo "CSV summary saved to: $CSV_OUT"
