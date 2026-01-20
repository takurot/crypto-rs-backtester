#!/bin/bash
set -e

# Default to llvm-profdata found via xcrun if on macOS, otherwise assume it's in PATH
if command -v xcrun &> /dev/null; then
    LLVM_PROFDATA=$(xcrun --find llvm-profdata)
else
    LLVM_PROFDATA=llvm-profdata
fi

if ! command -v "$LLVM_PROFDATA" &> /dev/null; then
    echo "Error: llvm-profdata not found. Please install LLVM tools."
    exit 1
fi

echo "Using llvm-profdata: $LLVM_PROFDATA"

PGO_DATA="/tmp/pgo-data"
rm -rf "$PGO_DATA"
mkdir -p "$PGO_DATA"

echo "Step 1: Instrumentation build..."
# clean specific crate artifacts to ensure rebuild
cargo clean -p backtester-core --release
RUSTFLAGS="-Cprofile-generate=$PGO_DATA" cargo build --release -p backtester-core --bench bench_core

echo "Step 2: Profile generation (Running benchmarks)..."
# Run a representative benchmark to generate profiles.
# Adjust the filter to run enough logic to train the branch predictor.
BENCH_BIN=$(find target/release/deps -name "bench_core-*" -type f ! -name "*.dSYM" -maxdepth 1 -perm +111 | head -n 1)

if [ -z "$BENCH_BIN" ]; then
    echo "Error: Benchmark binary not found."
    exit 1
fi

echo "Running benchmark binary: $BENCH_BIN"
"$BENCH_BIN" --noplot --sample-size 10 bench_event_loop_1m_ticks

echo "Step 3: Merging profiles..."
"$LLVM_PROFDATA" merge -o "$PGO_DATA/merged.profdata" "$PGO_DATA"

echo "Step 4: Optimized build..."
# clean specific crate artifacts to ensure rebuild
cargo clean -p backtester-core --release
RUSTFLAGS="-Cprofile-use=$PGO_DATA/merged.profdata" cargo build --release -p backtester-core --bench bench_core

echo "PGO optimized build complete."
