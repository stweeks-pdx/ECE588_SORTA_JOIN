#!/usr/bin/env python3
"""
generate_dataset.py
Generates employees.csv and trips.csv for ECE588 parallel database join project.
"""

import csv
import random
import time

# ─── Configuration Constants ────────────────────────────────────────────────
NUM_EMPLOYEES = 20000
NUM_TRIPS = 100000
EMPLOYEES_WITH_TRIPS = 15000       # IDs 0–14999 take trips
INVALID_ID_START = 20000           # IDs 20000–24999 appear in trips but not employees
INVALID_ID_END = 24999
INVALID_TRIP_FRACTION = 0.25       # 25% of trips belong to non-existent employees

SEED = 588                         # reproducible runs; change or remove for variety

# ─── Seed RNG ────────────────────────────────────────────────────────────────
random.seed(SEED)

# ─── 1. Generate employees.csv ──────────────────────────────────────────────
def generate_employees(filepath: str) -> None:
    with open(filepath, "w", newline="") as f:
        writer = csv.writer(f)
        for i in range(NUM_EMPLOYEES):
            name = f"employee{i:05d}"          # 13 chars, fits in 30-char field
            writer.writerow([i, name])
    print(f"Wrote {NUM_EMPLOYEES} rows to {filepath}")

# ─── 2. Generate trips.csv ──────────────────────────────────────────────────
def generate_trips(filepath: str) -> None:
    num_invalid = int(NUM_TRIPS * INVALID_TRIP_FRACTION)   # 25,000
    num_valid = NUM_TRIPS - num_invalid                    # 75,000

    # Build the ID pool and shuffle so IDs are randomly distributed
    valid_ids = random.choices(range(EMPLOYEES_WITH_TRIPS), k=num_valid)
    invalid_ids = random.choices(range(INVALID_ID_START, INVALID_ID_END + 1), k=num_invalid)

    trip_ids = valid_ids + invalid_ids
    random.shuffle(trip_ids)
    
    current_ts = int(time.time() * 1000) 

    with open(filepath, "w", newline="") as f:
        writer = csv.writer(f)
        for j in range(NUM_TRIPS):
            current_ts += random.randint(1, 100)                  # Timestamps: monotonically increasing, with random gaps between entries
            emp_id = trip_ids[j]
            destination = f"destination{j:05d}"                   # 16 chars, fits in 20-char field
            writer.writerow([emp_id, destination, current_ts])
    print(f"Wrote {NUM_TRIPS} rows to {filepath}")

# ─── Main ────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    generate_employees("../employees.csv")
    generate_trips("../trips.csv")
    print("\nDataset generation complete.")
