SHELL := /bin/bash

.PHONY: pgo bench-baseline

# Default target
all: pgo

pgo:
	./scripts/build_pgo.sh

bench-baseline:
	cargo bench -p backtester-core --bench bench_core
