# Makefile for dataset generation and verification
# Run from the dataset/ directory

GENSCRIPT = genscript/generate_dataset.py
VERIFYSCRIPT = genscript/verify_dataset.py

.PHONY: all generate verify clean

all: generate verify

generate:
	cd genscript && python3 generate_dataset.py

verify:
	cd genscript && python3 verify_dataset.py

clean:
	rm -f employees.csv trips.csv
