# Code Review Request: Phase 5 - Zero-Copy Ingestion & Streaming TickSource

## Branch
`feature/phase5-scale-optimization`

## Summary
This PR implements Phase 5 of the Rust Backtester project, focusing on **Scale & Optimization**. The main goals are:

1. **Zero-Copy Data Ingestion**: Enable efficient ingestion of large datasets using the Arrow C Data Interface
2. **Streaming TickSource**: Refactor the Engine to support lazy/streaming tick ingestion instead of pre-loading all data

## Files Changed

### New Files
| File | Description |
|------|-------------|
| `backtester-core/src/tick_source.rs` | `TickSource` trait and `ArrowTickSource` implementation for zero-copy Arrow stream ingestion |
| `backtester-py/src/arrow_utils.rs` | FFI utilities to extract Arrow streams from Python PyCapsule objects |
| `python/tests/test_e2e_arrow_stream_ingestion_smoke.py` | E2E test validating Arrow stream ingestion from Python |

### Modified Files
| File | Description |
|------|-------------|
| `backtester-core/Cargo.toml` | Added `arrow` dependency with `ffi` feature |
| `backtester-core/src/lib.rs` | Exposed `tick_source` module and types |
| `backtester-core/src/engine.rs` | Added `sources` field, `add_tick_source()` method, and lazy ingestion logic in `step()` |
| `backtester-py/Cargo.toml` | Added `arrow` dependency with `pyarrow` feature |
| `backtester-py/src/lib.rs` | Added `run_arrow()` method using streaming ingestion |
| `pyproject.toml` | Added `pyarrow` dependency |

## Key Implementation Details

### 1. ArrowTickSource (`tick_source.rs`)
- Implements `TickSource` trait with `next()`, `peek()`, and `symbol_id()` methods
- Wraps `ArrowArrayStreamReader` from Arrow FFI
- Parses Arrow batches into `Tick` structs on-the-fly
- Handles column name variations (e.g., `ts_exchange` vs `ts_event`)

### 2. Engine Lazy Ingestion (`engine.rs`)
- Added `sources: Vec<Box<dyn TickSource>>` field
- `step()` now checks if any source has a tick ready before the next queued event
- If source tick is earlier, it ingests and schedules `Tick` + `TickDelivery` events
- Applies `feed_latency_ns` when `ts_local` is not provided (zero)

### 3. Python FFI (`arrow_utils.rs`)
- Extracts Arrow stream via `_export_to_c` method (fallback for PyCapsule compatibility)
- Handles ownership transfer to prevent double-free issues

## Review Focus Areas

Please pay special attention to:

1. **Memory Safety** (`arrow_utils.rs`)
   - Is the PyCapsule ownership handling correct?
   - Are there potential use-after-free or double-free issues?

2. **Determinism** (`engine.rs`)
   - Does the lazy ingestion maintain the same deterministic ordering as pre-loading?
   - Is the timestamp comparison logic correct for tie-breaking?

3. **Error Handling**
   - Are schema mismatches handled gracefully in `ArrowTickSource`?
   - Should missing columns trigger a hard error or a default value?

4. **Performance**
   - Is the `peek()` + `next()` pattern efficient?
   - Should we batch-ingest multiple ticks per `step()` call?

5. **API Design**
   - Is `add_tick_source()` the right API for the Engine?
   - Should `run_arrow()` accept multiple streams?

## Test Commands

```bash
# Rust unit tests
cargo test -p backtester-core

# Python E2E test
source .venv/bin/activate
PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1 maturin develop --release
pytest python/tests/test_e2e_arrow_stream_ingestion_smoke.py -v

# Benchmarks
cargo bench -p backtester-core
```

## Benchmark Results

| Benchmark | Mean Time | Throughput |
|-----------|-----------|------------|
| `bench_event_loop_1m_ticks` | 138.5 ms | ~7.2M ticks/sec |
| `bench_orderbook_apply_l2_1m_updates` | 34.6 ms | ~28.9M updates/sec |

## Related Documentation
- `docs/PLAN.md` - Phase 5 roadmap
- `docs/SPEC.md` - Technical specification
