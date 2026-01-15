# Phase 5.3–5.6 Code Review (Scaling & Optimization)

## Review Plan
- Read `review/CODEX_PROMPT_PHASE_5.md` to scope focus and files.
- Inspect Rust core changes (`stats`, `engine`, exports) for correctness, memory-control behavior, and hot-path performance risk.
- Inspect Python bindings for the claimed zero-copy result export and parity with Rust stats.
- Evaluate the new end-to-end test coverage versus the intended behaviors.

## Findings
- High — SummaryOnly stats never surface: `TradeLogMode::SummaryOnly` only increments counters (`backtester-core/src/stats.rs:82`, `backtester-core/src/stats.rs:129`) but `calculate_stats` always derives figures from stored fills/events (`backtester-core/src/stats.rs:446`) and `Engine::stats` calls that path (`backtester-core/src/engine.rs:206`). With SummaryOnly configured, stats remain zero and realized PnL is never fed into `incremental_stats` because the engine never calls `push_pnl_delta`. The advertised memory-saving mode is non-functional.
- High — RingBuffer data dropped after wrap: `TradeLog::fills()` and `pnl_events()` return only the first slice of the deque (`backtester-core/src/stats.rs:142`, `backtester-core/src/stats.rs:164`), so once the ring buffer wraps, part of the buffered history is silently omitted. `calculate_stats` and the Python exports consume these truncated slices, producing incorrect stats/exports as soon as wrapping occurs (e.g., `backtester-py/src/lib.rs:147`, `backtester-py/src/lib.rs:199`).
- Medium — Python equity/trade exports ignore funding PnL: the exported equity curve is derived solely from fill deltas (`backtester-py/src/lib.rs:147`, `backtester-py/src/lib.rs:199`) and never incorporates `pnl_events` (funding). Rust-side stats do include funding, so Python-visible equity diverges from reported stats when funding is present.
- Medium — “Zero-copy” exports still copy data twice: `trades_df`/`equity_curve_df` build new per-column `Vec`s and hand them to Python, which PyO3 materializes as Python lists (`backtester-py/src/lib.rs:49`). This doubles memory/time for large results and is not Arrow/Polars zero-copy as promised; scaling benefit is lost.
- Low — Coverage gaps: the new e2e test only asserts presence of keys and allows `_len >= 0` (`python/tests/test_e2e_result_export.py:50`, `python/tests/test_e2e_result_export.py:75`). It would not catch the SummaryOnly/ring-buffer/funding discrepancies above.

## Notes
- Tests not run (not requested).
