# Implementation Plan

This document outlines the detailed implementation tasks for the Rust-based Tick-level Backtester, derived from `docs/SPEC.md`.

## Status Legend
- [ ] Not Started
- [/] In Progress
- [x] Completed

---

## Current Implementation Snapshot
- Phases 0–4: completed and covered by tests/benches.
- Phase 5.1: Arrow C Stream zero-copy ingestion implemented (`backtester-py/src/arrow_utils.rs`, `run_arrow`), E2E smoke test present.
- Phase 5.2: Streaming `TickSource` + lazy scheduling implemented (`backtester-core/src/tick_source.rs`, engine `sources` with deterministic tie-breakers).
- Phase 5.3–5.6: not started.

## Test Naming & Layout Conventions (Applies to tasks below)
- **Rust unit tests**: co-located in `backtester-core/src/**` using `#[cfg(test)] mod tests { ... }`
  - Naming: `test_<unit>_<behavior>_<expected>()`
- **Rust integration tests**: `backtester-core/tests/test_<area>.rs`
  - Naming: `test_<area>_<scenario>()`
- **Python E2E tests**: `python/tests/test_e2e_<area>.py` (pytest)
  - Naming: `test_e2e_<area>_<scenario>()`
- **Benchmark naming**:
  - Criterion: `bench_<area>_<case>()`
  - Python perf (optional): `test_bench_<area>_<case>()` (using `pytest-benchmark`, marked separately from correctness tests)

## Phase 0: Quality Gates & Tooling Baseline
**Goal**: Establish deterministic test/bench harnesses early so performance and correctness regressions are caught immediately.

### 0.1 Rust Unit/Integration Test Scaffolding
- [x] **0.1.1 Test Harness & Fixtures**
    - Add a shared fixtures module for tiny deterministic event streams (in-memory, no heavy datasets).
    - Define helper builders for `Tick`, `L2Update`, `Order`, and `Event` with explicit timestamps and sequence IDs.
    - **Deliverable**: `cargo test` runs a minimal suite in < 1s.
    - **Suggested tests**:
        - `test_fixtures_smoke_builders()`

- [x] **0.1.2 Determinism Tests**
    - Add tests asserting deterministic ordering for same-timestamp events (stable tie-breakers; no `HashMap` ordering dependence).
    - Add tests asserting reproducibility given the same RNG seed.
    - **Deliverable**: Re-running tests produces bit-identical results (where applicable).
    - **Suggested tests**:
        - `test_event_queue_tiebreak_stable()`
        - `test_reproducible_with_seed()`

### 0.2 Python E2E Test Scaffolding
- [x] **0.2.1 Pytest + Maturin Workflow**
    - Add a `pytest` E2E test target that installs the extension module (e.g., via `maturin develop`) and runs end-to-end backtests.
    - **Deliverable**: A single `pytest` command runs E2E tests locally/CI.
    - **Suggested tests**:
        - `test_e2e_import_and_run_smoke()`
    - **E2E minimal data generation (Polars, generated on-the-fly)**:
        - Create data inside tests (no external files) and pass it as a `LazyFrame`:

```python
import polars as pl

def make_minimal_ticks_lazyframe(*, with_seq: bool = True) -> pl.LazyFrame:
    # Small deterministic stream: 4 ticks, 1 symbol, trade-only.
    ts_exchange = [1_000, 2_000, 3_000, 4_000]
    price = [100_00000000, 101_00000000,  99_00000000, 100_00000000]
    qty =   [  1_00000000,   1_00000000,   1_00000000,   1_00000000]
    side =  [           1,          -1,            1,          -1]
    data = {"ts_exchange": ts_exchange, "price": price, "qty": qty, "side": side}
    if with_seq:
        data["seq"] = list(range(len(ts_exchange)))
    return pl.DataFrame(data).lazy()
```

### 0.3 Benchmark Scaffolding (Rust + Python)
- [x] **0.3.1 Criterion Bench Harness (Rust Core)**
    - Add `criterion` benches for core hot paths (event loop, order book updates).
    - **Deliverable**: `cargo bench` produces a baseline report.
    - **Suggested benches**:
        - `bench_event_loop_1m_ticks()`
        - `bench_orderbook_apply_l2_1m_updates()`

- [x] **0.3.2 Python Integration Bench Harness (Optional)**
    - Add a lightweight benchmark for Rust↔Python batch callback overhead (e.g., `pytest-benchmark` or a dedicated timing script).
    - **Deliverable**: A repeatable per-batch overhead measurement.
    - **Suggested benches/tests**:
        - `test_bench_python_batch_callback_overhead()`

## Phase 1: Core Engine Integration (Rust)
**Goal**: Establish the pure Rust simulation core with L2 matching engine and event loop.

### 1.1 Project Scaffolding
- [x] **1.1.1 Initialize Rust Workspace**
    - Create a new Cargo workspace.
    - Setup crates: `backtester-core` (lib), `backtester-py` (cdylib).
    - Add dependencies: `thiserror`, `serde`, `log`, `rust_decimal` (or `i64` implementation), `arrow`, `polars`.
    - **Deliverable**: Compilable empty workspace.
    - **Verification**:
        - `cargo test` passes (no specific test required yet).

- [x] **1.1.2 Define Core Data Structures** `[Depends on 1.1.1]`
    - Implement `FixedPoint` helper (satoshi precision).
    - Define time axes types and naming: `ts_exchange`, `ts_local`, `ts_sim` (all nanoseconds).
    - Define `Tick` (logical representation for callbacks/logging) with `ts_exchange` and `ts_local`.
    - Define `Order`, `Side`, `OrderType` enums.
    - Implement `OrderState` enum (`PendingNew`, `Open`, `Filled`, etc.).
    - Define `OrderReport` (fills/cancels/rejects) used by strategy callbacks.
    - **Deliverable**: `types.rs` with unit tests for fixed-point arithmetic and basic invariants.
    - **Suggested tests**:
        - `test_fixed_point_roundtrip_io_only()`
        - `test_order_state_machine_basic_invariants()`

- [x] **1.1.3 Deterministic Event Model** `[Depends on 1.1.2]`
    - Define a stable `EventId` / tie-breaker strategy for same-`ts_sim` events.
    - Define an `Event` model that can represent market truth, feed deliveries, order arrivals/ACKs, funding, and timers.
    - **Deliverable**: Unit tests proving stable ordering for same-timestamp events.
    - **Suggested tests**:
        - `test_event_ordering_same_ts_sim_uses_stable_tiebreak()`

### 1.2 Matching Engine (L2)
- [x] **1.2.1 Implement OrderBook L2** `[Depends on 1.1.2]`
    - Create `OrderBook` struct using `BTreeMap<Price, SideQueue>`.
    - Implement `apply_l2_update(price, qty, side)` logic.
    - Implement `get_best_bid/ask`.
    - **Deliverable**: Unit tested L2 OrderBook that correctly updates state from diffs.
    - **Suggested tests**:
        - `test_orderbook_l2_apply_update_and_best_bid_ask()`
        - `test_orderbook_l2_remove_level_with_qty_zero()`

- [x] **1.2.2 Implement ExchangeSimulator** `[Depends on 1.2.1]`
    - Define `ExchangeSimulator` struct.
    - Implement `submit_order` -> generates `OrderID` -> transitions to `PendingNew`.
    - Implement `cancel_order` -> transitions to `PendingCancel`.
    - **Deliverable**: Simulator that accepts orders and tracks their basic lifecycle.
    - **Suggested tests**:
        - `test_exchange_submit_transitions_to_pending_new()`
        - `test_exchange_cancel_transitions_to_pending_cancel()`

- [x] **1.2.3 Conservative Queue Model** `[Depends on 1.2.2]`
    - Implement `QueueModel` trait.
    - Implement `ConservativeQueue` (LIFO logic).
    - Integrate with `ExchangeSimulator`: Check for fills on every market trade event.
    - **Deliverable**: Tests showing user orders get filled only after queue depletion.
    - **Suggested tests**:
        - `test_queue_conservative_user_is_last_in_queue()`

### 1.3 Event Loop
- [x] **1.3.1 Global Event Queue (`ts_sim`)** `[Depends on 1.1.3]`
    - Implement a `BinaryHeap`-based event queue ordered by `ts_sim` with deterministic tie-breakers.
    - Event variants SHOULD cover: market data updates (truth), feed deliveries (strategy view), order arrivals/ACKs, order-update deliveries, funding, timers.
    - **Deliverable**: Mechanism to push multiple streams and pop events in deterministic temporal order.
    - **Suggested tests**:
        - `test_global_event_queue_orders_by_ts_sim_then_tiebreak()`

- [x] **1.3.2 Basic Event Loop** `[Depends on 1.3.1]`
    - Create `Engine` struct.
    - Implement `run()` loop processing events one by one.
    - Dispatch events to `ExchangeSimulator` (market truth) and `Strategy` (feed-delayed view).
    - **Deliverable**: A test harness that runs a predefined sequence and verifies order execution deterministically.
    - **Suggested tests**:
        - `test_engine_run_smoke_deterministic_sequence()`

- [x] **1.3.3 MarketView & Look-ahead Prevention** `[Depends on 1.3.2]`
    - Implement a feed-delayed `MarketView` that the strategy reads (consistent with `ts_local`).
    - Ensure the matching engine uses ground-truth updates at `ts_exchange`, while strategy only sees delivered updates at `ts_local`.
    - **Deliverable**: Unit/integration test that fails if the strategy can observe future (`ts_exchange`) information early.
    - **Suggested tests**:
        - `test_marketview_no_lookahead_with_feed_latency()`

---

## Phase 2: Python Integration
**Goal**: Expose the Rust core to Python via PyO3 and enable Polars data ingestion.

### 2.1 PyO3 Bindings
- [x] **2.1.1 Setup Maturin** `[Depends on 1.1.1]`
    - Configure `pyproject.toml`.
    - Create basic `#[pyclass]` for `Backtester`.
    - **Deliverable**: `maturin develop` installs the package in a python venv.
    - **Suggested tests**:
        - `test_e2e_import_and_run_smoke()`

- [x] **2.1.2 Strategy Interface (FFI)** `[Depends on 2.1.1]`
    - Define `Strategy` trait in Rust.
    - Implement Python wrapper struct that holds a `PyObject` (the Python strategy instance).
    - Support both modes:
        - Tick mode: Rust calls Python `on_tick`.
        - Batch mode: Rust calls Python `on_ticks` and `on_order_updates` (preferred).
    - Add config surface aligned with SPEC (e.g., `python_mode`, `batch_ms`, `seed`).
    - **Deliverable**: Python script can define a class and run in tick or batch mode with deterministic results.
    - **Suggested tests**:
        - `test_e2e_strategy_tick_mode_smoke()`
        - `test_e2e_strategy_batch_mode_smoke()`

### 2.2 Data Ingestion
- [x] **2.2.1 Polars / Arrow Conversion** `[Depends on 1.1.1]`
    - Use `pyo3-polars` to accept Polars data (DataFrame/LazyFrame materialization strategy TBD).
    - Validate schema and aliases per SPEC (`ts_exchange`/`ts_event`, `qty`/`size`, optional `seq`, optional `ts_local`).
    - Implement columnar iteration over Arrow arrays (SoA) and avoid materializing a full `Vec<Tick>` for large datasets.
    - **Deliverable**: Python passes a large Polars dataset, Rust processes it without a large memory spike.
    - **Suggested tests**:
        - `test_schema_accepts_ts_event_alias()`
        - `test_schema_accepts_qty_size_alias()`

- [x] **2.2.2 Batch Processing** `[Depends on 2.2.1]`
    - Buffer delivered ticks and wake Python on configurable conditions (max batch duration, any order update delivery, funding/timer).
    - Ensure batch wakeups preserve determinism with stable ordering.
    - **Deliverable**: Benchmarks showing > 1M ticks/sec throughput with batch mode enabled.
    - **Suggested tests**:
        - `test_batch_wakeup_max_batch_ms()`
        - `test_batch_wakeup_on_order_update_delivery()`

### 2.3 End-to-End (E2E) Tests (Python)
- [x] **2.3.1 Deterministic E2E Run** `[Depends on 2.1.2, 2.2.2]`
    - E2E test: run the same backtest twice with the same seed and assert identical outputs (trades + stats).
    - **Deliverable**: `pytest` E2E suite validates reproducibility.
    - **Suggested tests**:
        - `test_e2e_reproducible_seed()`
    - **Data**: Use `make_minimal_ticks_lazyframe()` (above) to generate the input on-the-fly.

- [x] **2.3.2 Tick vs Batch Equivalence (Sanity)** `[Depends on 2.3.1]`
    - E2E test: for a simple deterministic strategy, compare tick mode vs batch mode outputs (allowing expected minor differences only if explicitly documented).
    - **Deliverable**: Confidence that batch mode does not change semantics.
    - **Suggested tests**:
        - `test_e2e_tick_vs_batch_equivalence()`
    - **Data**: Use `make_minimal_ticks_lazyframe(with_seq=True)` to avoid ambiguous same-timestamp ordering.

- [x] **2.3.3 Look-ahead Bias Guard (E2E)** `[Depends on 1.3.3, 2.3.1]`
    - E2E test: set non-zero feed latency and assert the strategy cannot act on market moves before `ts_local`.
    - **Deliverable**: Regression guard preventing accidental “fast view” exposure in Python API.
    - **Suggested tests**:
        - `test_no_lookahead_with_feed_latency()`
    - **Data generation approach**:
        - Generate ticks at `ts_exchange=[1000, 2000, 3000, 4000]` and set a constant feed latency (e.g., 1000ns).
        - Assert the strategy observes each tick at `ts_local == ts_exchange + 1000`.

---

## Phase 3: Advanced Microstructure
**Goal**: Increase simulation fidelity with latency jitter and advanced queue models.

### 3.1 Latency Models
- [x] **3.1.1 Latency Trait & Jitter** `[Depends on 1.3.2]`
    - Implement `LatencyModel` trait.
    - Implement `LogNormalJitter` using `rand` and `statrs` (or similar).
    - Ensure RNG is owned/seeded by the engine for reproducibility (models MUST NOT keep RNG state internally).
    - Update `ExchangeSimulator` to schedule ACK events in the future based on latency.
    - **Deliverable**: Orders are not "Open" immediately; they appear after delay + reproducible results under fixed seed.
    - **Suggested tests**:
        - `test_latency_lognormal_is_reproducible_under_seed()`

- [x] **3.2 Order State Race Conditions** `[Depends on 3.1.1]`
    - verify `PendingCancel` logic.
    - Create test case: Send Cancel -> Market moves -> Order Fills -> Cancel Rejected.
    - **Deliverable**: Validated robust state machine.
    - **Suggested tests**:
        - `test_pending_cancel_can_fill_before_cancel_ack()`

### 3.2 Advanced Queues
- [x] **3.2.1 Volume Clock Queue** `[Depends on 1.2.3]`
    - Implement `VolumeClockQueue` model.
    - Logic: Track cumulative volume since order entry. Fill when `vol >= queue_pos`.
    - **Deliverable**: More realistic fill rates on L2 data compared to Conservative model.
    - **Suggested tests**:
        - `test_queue_volume_clock_fills_when_cum_volume_exceeds_queue_pos()`

### 3.3 Crypto Specifics
- [x] **3.3.1 Funding Rate Simulation** `[Depends on 1.3.2]`
    - Add `FundingEvent` to data types.
    - Implement periodic funding payment logic in `Account` struct.
    - **Deliverable**: Positions incur funding costs/gains.
    - **Suggested tests**:
        - `test_funding_applied_at_scheduled_ts_exchange()`

---

## Phase 4: Production Readiness
**Goal**: Optimizations, multi-venue support, and reporting.

- [x] **4.1 Multi-Venue Support** `[Depends on 1.3.1]`
    - Update `symbol_id` mapping to support `(Exchange, Symbol)`.
    - Instantiate multiple `ExchangeSimulator` instances within the same deterministic event loop.
    - **Deliverable**: Arbitrage strategy test between two simulated exchanges.
    - **Suggested tests**:
        - `test_multi_venue_event_ordering_by_ts_sim()`
        - `test_arbitrage_two_venues_smoke()`

- [x] **4.2 Result Statistics** `[Depends on 2.1.2]`
    - Implement `TradeLog` collector in Rust.
    - Implement `calculate_stats()` (Sharpe, Drawdown) in Rust.
    - Return stats as a Python Dict or Polars DF.
    - **Deliverable**: Complete backtest report generation.
    - **Suggested tests**:
        - `test_stats_max_drawdown_matches_reference()`
        - `test_stats_total_pnl_fixed_point_consistency()`

- [x] **4.3 Benchmarking & CI**
    - Ensure Criterion benches cover: event loop, order book updates, batch callback overhead (where measurable).
    - Set up CI to run: Rust unit/integration tests + Python E2E tests (and optionally benches on a schedule).
    - **Deliverable**: Automated CI pipeline.
    - **Suggested tests**:
        - `test_ci_runs_rust_and_python_suites_smoke()`

---

## Phase 5: Scale & Optimization (Future)
**Goal**: Achieve true zero-copy ingestion, reduce memory overhead for long backtests, enable fast parameter sweeps, and make performance regressions observable—without breaking determinism / look-ahead prevention / fixed-point money rules.

- [x] **5.1 True Zero-Copy Ingestion (Arrow C Data Interface)** `[Depends on 2.2.1]`
    - Accept an Arrow `RecordBatch` stream from Python (e.g., `pyarrow.RecordBatchReader`) via the Arrow C Data Interface.
    - Implement a Rust-side columnar iterator (SoA) that reads required columns (`ts_exchange`, `price`, `qty`, `side`, optional `seq`, optional `ts_local`, optional `flags`) without materializing a full `Vec<Tick>`.
    - Enforce/validate per-stream ordering by (`ts_exchange`, `seq`) ascending; reject unsorted streams rather than sorting internally (determinism + perf).
    - **Deliverable**: 1GB-class dataset ingestion without a large memory spike; minimal Python↔Rust copying.
    - **Suggested tests**:
        - Rust: `test_tick_source_arrow_schema_aliases_smoke()`
        - Python E2E: `test_e2e_arrow_stream_ingestion_smoke()`

- [x] **5.2 Streaming TickSource + Lazy Event Scheduling** `[Depends on 1.3.1, 5.1]`
    - Introduce a `TickSource` abstraction per `(exchange, symbol)` stream that yields the next market-truth tick.
    - Update the engine to advance each stream lazily (schedule only next truth + next delivery per stream; push subsequent ticks as prior ticks are consumed).
    - Preserve global determinism with stable tie-breakers for same-`ts_sim` events (never rely on stream iteration order).
    - **Deliverable**: Tick scheduling memory becomes \(O(\#streams)\), not \(O(\#ticks)\).
    - **Suggested tests**:
        - Rust integration: `test_engine_streaming_tick_source_equivalence_to_materialized()`
    - **Suggested benches**:
        - `bench_event_loop_streaming_1m_ticks()`

- [x] **5.3 Zero-Copy Result Export (Trades / Equity Curve)** `[Depends on 4.2, 5.1]`
    - Add result export APIs that return Arrow/Polars objects backed by Rust buffers (avoid Python dict/list for large outputs).
    - Provide `BacktestResult.trades_df()` and `BacktestResult.equity_curve_df()` (or equivalent) with a stable schema.
    - **Deliverable**: Large trade logs can be analyzed in Python without a second full copy.
    - **Suggested tests**:
        - Python E2E: `test_e2e_result_trades_df_schema_and_values()`

- [x] **5.4 TradeLog Retention & Memory Controls** `[Depends on 4.2]`
    - Add `TradeLogMode` (e.g., `All`, `RingBuffer(N)`, `SummaryOnly`, `None`) and expose it in the Python API.
    - When `SummaryOnly` is enabled, compute aggregate stats incrementally and ensure the output matches the full-log computation for small deterministic inputs.
    - **Deliverable**: Backtests can run for hours of data without unbounded memory growth.
    - **Suggested tests**:
        - Rust: `test_trade_log_ring_buffer_caps_size()`
        - Rust: `test_stats_summary_only_matches_full_log_for_small_input()`

- [x] **5.5 Parallel Parameter Sweep Runner (Deterministic)** `[Depends on 2.1.2]`
    - Provide a batch API to run many independent backtests (parameter search) in parallel (Rust-side concurrency).
    - Derive per-run seeds deterministically from a base seed and input index; return results in stable input order.
    - **Deliverable**: Parameter sweeps scale across cores without losing reproducibility.
    - **Suggested tests**:
        - Python E2E: `test_e2e_run_many_deterministic_and_ordered()`

- [x] **5.6 Benchmark Regression Observability (CI Artifacts)** `[Depends on 0.3.1, 4.3]`
    - Upload Criterion reports as CI artifacts for each run.
    - Keep scheduled benchmark runs (weekly) for baseline tracking; avoid hard performance gates unless methodology is robust.
    - **Deliverable**: Performance trends are visible and regressions are easier to catch.

---

## Phase 6: Performance Optimizations (Future)
**Goal**: Maximize throughput via compiler tuning, data structure improvements, Python FFI reduction, and CPU-aware optimizations—preserving determinism and correctness.

### 6.1 Compiler & Build Optimizations
- [x] **6.1.1 LTO and Codegen Tuning**
    - Add release profile with:
      - `lto = "fat"` (or `"thin"` for faster builds)
      - `codegen-units = 1`
      - `opt-level = 3`
      - `panic = "abort"`
    - Optional: `RUSTFLAGS="-C target-cpu=native"` for CPU-specific optimizations (note: reduces portability).
    - **Deliverable**: 10-20% throughput improvement with no code changes.
    - **Suggested benches**: Compare `cargo bench` before/after profile changes.

### 6.2 Data Structure Optimizations
- [x] **6.2.1 FxHashMap / Vec for Hot Lookups** `[Depends on 1.2.2]`
    - Replace `BTreeMap` with `FxHashMap` or `Vec` for `exchanges`, `order_symbol_by_id`, `MarketView.last_trade_by_symbol`.
    - For dense sequential IDs (e.g., `order_id`), use `Vec<Option<u32>>` for O(1) direct indexing.
    - For sparse lookups (e.g., `symbol_id` → Exchange), use `FxHashMap` or small `Vec` with linear scan.
    - Preserve deterministic iteration where needed via sorted keys or auxiliary Vec.
    - **Deliverable**: O(1) vs O(log n) lookups; 5-15% improvement for order-heavy workloads.
    - **Suggested tests**:
        - `test_engine_determinism_with_vec_lookup()`

- [x] **6.2.2 Pre-allocate & Reuse Buffers**
    - Pre-allocate `tick_buffer`, `report_buffer`, and other hot `Vec` allocations.
    - Reuse allocations via `Vec::clear()` instead of creating new Vecs per step (e.g., `fills`, `trade_fills` in `Engine::step`).
    - **Deliverable**: Reduced allocation overhead in batch mode and long-running backtests.

- [x] **6.2.3 EventKind Size Optimization**
    - Analyze enum size with `std::mem::size_of` and consider boxing large variants. (Investigated: 64 bytes fits in a cache line, boxing unnecessary)

- [x] **6.2.4 Order Bucket Indexing for Fill Checks** `[Depends on 1.2.3]`
    - Current `ExchangeSimulator::on_trade` scans all orders for each trade tick (O(orders × ticks)).
    - Implement price+side bucket: `HashMap<(i64, Side), Vec<u64>>` mapping (price, opposite_side) → order_ids.
    - On trade, only check orders at matching price level.
    - **Deliverable**: Dramatic reduction in fill-check iterations; 10-50x speedup for order-heavy strategies.
    - **Suggested tests**:
        - `test_order_bucket_fill_equivalence()`

- [x] **6.2.5 Min-Heap for Multi-Symbol Source Selection** `[Depends on 5.2]`
    - Current `Engine::step` scans all sources to find minimum `ts_exchange` (O(N) per step).
    - Use a min-heap of `(ts_exchange, source_idx)` for O(log N) selection.
    - Maintain determinism via stable tie-breaking on `source_idx`.
    - **Deliverable**: Scales better with many symbols (>8).
    - **Suggested benches**:
        - `bench_engine_16_symbols_with_heap_selection()`
    - **Deliverable**: Smaller event queue memory footprint, better cache utilization.

### 6.3 Python FFI Optimizations
- [x] **6.3.1 Default to Arrow Ingestion Path** `[Depends on 5.1]`
    - Make `run_arrow` the default path; deprecate `schedule_ticks_from_python_polars`.
    - Current dict/list-based ingestion is very slow (DataFrame → dict → list → row access).
    - Pass Polars → Arrow C stream directly to `ArrowTickSource`.
    - **Deliverable**: 5-10x faster data ingestion for large datasets.

- [x] **6.3.2 Arrow-Based Tick Batches for Strategy Callbacks** `[Depends on 5.1]`
    - Replace per-tick dict creation with Arrow RecordBatch in `on_ticks`.
    - Python strategy receives columnar arrays (numpy/pyarrow, zero-copy).
    - **Deliverable**: 20-30% reduction in Python FFI overhead.
    - **Suggested tests**:
        - Python E2E: `test_e2e_strategy_arrow_batch_callback()`

- [x] **6.3.3 Structured Tick Objects (dataclass/namedtuple)** `[Depends on 2.1.2]`
    - Use Python `dataclass` or `namedtuple` instead of `dict` for tick objects.
    - Faster instantiation and attribute access.
    - **Deliverable**: Lower per-tick object creation cost.

- [x] **6.3.4 Cache Column Arrays in ArrowTickSource** `[Depends on 5.1]`
    - Current `read_tick_at` calls `column_by_name` and `downcast_ref` per row.
    - Cache column array references once per batch, access via `arr.value(idx)` only.
    - **Deliverable**: Significant reduction in per-tick overhead for Arrow ingestion.
    - **Suggested code location**: `backtester-core/src/tick_source.rs`

### 6.4 Incremental Statistics
- [ ] **6.4.1 Streaming Stats Accumulation** `[Depends on 5.4]`
    - Implement `IncrementalStats` struct that updates on each fill/PnL event.
    - Avoid post-hoc O(n) scan in `calculate_stats()`.
    - **Deliverable**: Constant-time stats retrieval; enables `TradeLogMode::SummaryOnly`.
    - **Suggested tests**:
        - `test_incremental_stats_matches_batch_calculation()`

### 6.5 SIMD & Vectorized Computation
- [ ] **6.5.1 SIMD Stats Calculation (Optional)**
    - Use `std::simd` (nightly) or `wide`/`ultraviolet` crate for equity curve prefix sum, Sharpe/Sortino calculation.
    - **Deliverable**: 2-5x faster stats for large trade logs.
    - **Suggested benches**:
        - `bench_stats_simd_vs_scalar()`

### 6.6 Parallel Execution
- [ ] **6.6.1 Rayon-Based Parallel Sweeps** `[Depends on 5.5]`
    - Use `rayon::par_iter` for multi-core parameter sweeps.
    - Ensure thread-local RNG seeding for determinism.
    - **Deliverable**: Linear speedup with core count.

### 6.7 Cache & Memory Optimizations
- [ ] **6.7.1 Struct Layout Analysis**
    - Profile cache behavior with `perf` or similar tools.
    - Reorder struct fields for better cache locality.
    - Consider `#[repr(C)]` for predictable layout.
    - **Deliverable**: Reduced cache misses in hot loops.
