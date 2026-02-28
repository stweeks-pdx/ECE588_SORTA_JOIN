#!/usr/bin/env python3
"""
verify_dataset.py
Quick verification: average trips per ID for valid and invalid employee IDs.
"""

import csv
from collections import Counter

EMPLOYEES_WITH_TRIPS = 15000       # valid IDs: 0–14999
NUM_EMPLOYEES = 20000              # all employee IDs: 0–19999
INVALID_ID_START = 20000           # invalid IDs: 20000–24999

trips_per_id = Counter()

with open("../trips.csv", newline="") as f:
    reader = csv.reader(f)
    for row in reader:
        emp_id = int(row[1])
        trips_per_id[emp_id] += 1

# Partition IDs
valid_ids   = {eid: trips_per_id[eid] for eid in trips_per_id if eid < EMPLOYEES_WITH_TRIPS}
no_trip_ids = {eid: trips_per_id.get(eid, 0) for eid in range(EMPLOYEES_WITH_TRIPS, NUM_EMPLOYEES)}
invalid_ids = {eid: trips_per_id[eid] for eid in trips_per_id if eid >= INVALID_ID_START}

total_valid   = sum(valid_ids.values())
total_invalid = sum(invalid_ids.values())

print("=== Dataset Verification ===\n")
print(f"Valid IDs (0–{EMPLOYEES_WITH_TRIPS - 1}):")
print(f"  Unique IDs with trips : {len(valid_ids)}")
print(f"  Total trips           : {total_valid}")
print(f"  Avg trips per ID      : {total_valid / len(valid_ids):.2f}\n")

print(f"No-trip employees ({EMPLOYEES_WITH_TRIPS}–{NUM_EMPLOYEES - 1}):")
no_trip_with_trips = sum(1 for v in no_trip_ids.values() if v > 0)
print(f"  IDs with 0 trips      : {len(no_trip_ids) - no_trip_with_trips}")
print(f"  IDs erroneously w/trips: {no_trip_with_trips}\n")

print(f"Invalid IDs ({INVALID_ID_START}–24999):")
print(f"  Unique IDs with trips : {len(invalid_ids)}")
print(f"  Total trips           : {total_invalid}")
print(f"  Avg trips per ID      : {total_invalid / len(invalid_ids):.2f}\n")

print(f"Grand total trips       : {total_valid + total_invalid}")
