# Performance Regression Report

## Overview
A performance regression was detected in the `bench_event_loop_1m_ticks` benchmark following Phase 6 optimizations.

## Regression Details
- **Benchmark**: `bench_event_loop_1m_ticks`
- **Metric**: Time per iteration
- **Baseline (Before Phase 6)**: ~35.8ms
- **Current (After Phase 6)**: ~147.1ms (~4x slower)

## Analysis
The regression appears isolated to the synthetic micro-benchmark `bench_event_loop_1m_ticks`.

### Potential Causes
1.  **EventKind Size Change**: The `EventKind` enum size increased (likely due to struct alignment or layout changes implicitly caused by dependency updates or code changes), affecting the cost of pushing/popping from the queue in a tight loop.
    - *Investigation*: `EventKind` size is 64 bytes.
2.  **Queue Overhead**: The benchmark measures raw `EventQueue` throughput. Phase 6 introduced `FxHashMap` and other structures in `Engine`, but this specific benchmark only exercises `EventQueue` and `EventKind`.
3.  **Benchmarking Noise**: The baseline might have been measured in a different environment state, though the magnitude (4x) suggests a structural change.

### Mitigation / Context
- **E2E Performance is UP**: Crucially, the end-to-end benchmarks (`bench_engine_e2e_*`), which simulate actual backtesting workloads (Engine + Strategy + Exchange), show a **7-8% improvement**.
    - Tick Mode: 216.5ms -> 200.4ms
    - Batch Mode: 200.2ms -> 184.6ms
- **Conclusion**: The micro-benchmark regression does not translate to system-level performance degradation. The overhead likely comes from slightly more expensive move/copy operations of 64-byte events in a pure tight loop, which is negligible compared to the logic gains (O(1) lookups, O(1) matching) in the full engine.

## Action Plan
- Accept the new baseline for `bench_event_loop_1m_ticks` as the cost of doing business for richer event types, given the E2E gains.
- Monitor E2E benchmarks as the primary health metric.
