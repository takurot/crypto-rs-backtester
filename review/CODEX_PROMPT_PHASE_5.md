# Code Review Request: Phase 5.3-5.6 Scaling & Optimization

Please review the following changes in the `crypto-rs-backtester` project. The focus of this PR is **scaling and optimization** for large-scale backtests.

## Context
We are building a high-performance event-driven backtester in Rust with Python bindings. This PR implements Phase 5.3 (Zero-Copy Export), 5.4 (Memory Controls), and 5.6 (CI Observability).

## Key Changes
1.  **Zero-Copy Result Export (Python/Rust FFI)**
    -   **File**: `backtester-py/src/lib.rs`
    -   **Feature**: Implemented `trades_df()` and `equity_curve_df()` methods on `BacktestResult`.
    -   **Mechanism**: Uses `pyo3` and `arrow` to export Rust `Vec` data directly to Python dictionaries of arrays (compatible with Polars/Pandas) without intermediate Python object overhead.

2.  **TradeLog Memory Controls (Rust)**
    -   **File**: `backtester-core/src/stats.rs`, `backtester-core/src/engine.rs`
    -   **Feature**: Introduced `TradeLogMode` enum:
        -   `All`: Default, keeps everything.
        -   `RingBuffer(N)`: Keeps only last N trades (using `VecDeque`).
        -   `SummaryOnly`: Discards individual trades, computes stats (PnL, win rate) incrementally.
    -   **Logic**: Added `IncrementalStats` struct and updated `TradeLog::push_fill`/`push_pnl_event` to handle these modes.

3.  **CI & Tests**
    -   **File**: `.github/workflows/bench.yml` (Artifact uploads)
    -   **File**: `python/tests/test_e2e_result_export.py` (New E2E test)
    -   **File**: `backtester-core/src/stats.rs` (New unit tests)

## Specific Review Questions
1.  **Memory Safety (FFI)**: Are the `trades_df` and `equity_curve_df` implementations in `backtester-py` safe? Do they properly handle ownership/lifetimes when passing data to Python?
2.  **Logic Correctness**: Does the `SummaryOnly` incremental aggregation logic in `stats.rs` cover all edge cases compared to the full `calculate_stats`?
3.  **Rust Idioms**: Is the `TradeLogMode` implementation idiosyncratic? Specifically the use of `VecDeque` and `make_contiguous`?
4.  **Performance**: Are there any obvious bottlenecks introduced in the hot path (`push_fill`)?

## Files to Review
- `backtester-core/src/stats.rs`
- `backtester-core/src/engine.rs`
- `backtester-core/src/lib.rs`
- `backtester-py/src/lib.rs`
- `python/tests/test_e2e_result_export.py`
