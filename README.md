### crypto-rs-backtester

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/takurot/crypto-rs-backtester/blob/main/example/colab_backtester_demo.ipynb)

Japanese version: see `README.ja.md`.

A tick-level, high-precision backtester powered by Rust × Python (Polars), designed for researchers. WIP.

- Goal: Combine Python's agility with Rust's deterministic, high-performance simulation to eliminate performance bottlenecks, look-ahead bias, and poor reproducibility.
- Scope: Mainly crypto spot/futures (CEX). Validates microstructure such as multi-exchange, latency, and queue position.

---

### Key Highlights

- Performant by design: Rust event-driven core with fixed-point arithmetic; Python focuses on research interface.
- Zero-copy pipeline: Use Polars (Arrow) in Python and hand off to Rust with minimal copying.
- Microstructure fidelity: Latency modeling, L2/L3 queue logic, and realistic race conditions (e.g., PendingCancel).
- Determinism first: Seeded RNGs, stable tie-breaking for identical timestamps.

See `docs/SPEC.md` for details.

### Features (current state)

- Hybrid architecture: Rust core (event-driven, fixed-point i64) + Python strategy interface.
- Separated timelines: `ts_exchange` (ground truth) / `ts_local` (what the strategy observes) / `ts_sim` (total order of all events).
- Look-ahead prevention: Strategies only see `MarketView` after feed latency. Order arrival and ACK are strictly ordered on `ts_sim`.
- Execution modes: Tick mode (`on_tick`) and Batch mode (`on_ticks` / `on_order_updates`).
- Data ingestion: via Polars LazyFrame (minimal copying), or zero-copy via Arrow C Stream (`run_arrow`).
- Determinism: Fixed RNG seed, stable tie-breakers for simultaneous events, lexicographic assignment of symbol IDs.

See `docs/SPEC.md` for details.

---

### Repository Structure

- `backtester-core/`: Rust simulation core (`src/*.rs`, `tests/`, `benches/`)
- `backtester-py/`: PyO3 wrapper exposing the core to Python
- `python/`: Python package `rust_backtester/` and tests in `python/tests/`
- `docs/`: Specs and plans (`SPEC.md`, `PLAN.md`, etc.)
- Root `Cargo.toml`: Rust workspace, `pyproject.toml`: maturin build

---

### Installation

```bash
pip install crypto-rs-backtester
```

---

### Install & Build (dev)

Prerequisites: Python 3.9+ / Rust toolchain / maturin

```bash
# Virtualenv
python -m venv .venv && source .venv/bin/activate

# Dev install (builds Rust extension)
pip install -e .[dev]

# Alternative: direct build
maturin develop
```

---

### Quickstart (Python)

Required columns: `ts_exchange:Int64`, `price:Int64`, `qty:Int64`, `side:Int8`
Recommended: `seq:Int64` (stable order for same-timestamp), `ts_local:Int64` (if missing, applies `ts_exchange + feed_latency_ns`)
Aliases: `ts_event` ≈ `ts_exchange`, `size` ≈ `qty`

```python
import polars as pl
from rust_backtester import Backtester

# Tiny deterministic dataset (1e-8 fixed point: 100.0 => 100_00000000)
lf = pl.DataFrame({
    "ts_exchange": [1_000, 2_000, 3_000, 4_000],
    "price": [100_00000000, 101_00000000, 99_00000000, 100_00000000],
    "qty":   [  1_00000000,   1_00000000,  1_00000000,   1_00000000],
    "side":  [            1,           -1,           1,           -1],
    "seq": list(range(4)),
}).lazy()

class MyStrategy:
    def on_tick(self, tick: dict, ctx):
        # Example: place a passive order using the received tick
        ctx.submit_order(
            symbol_id=int(tick["symbol_id"]),
            side=1,  # 1=Buy, -1=Sell
            price=int(tick["price"]),
            qty=1_00000000,
        )

bt = Backtester(
    data={"binance:BTC/USDT": lf},
    seed=42,
    python_mode="tick",    # or "batch"
    batch_ms=100,
    feed_latency_ns=1_000,  # applies ts_local = ts_exchange + 1_000 (ns)
)

result = bt.run(MyStrategy())
print(result.stats())
print(result.trades())
```

Batch mode (higher throughput)

```python
class MyBatch:
    def on_ticks(self, ticks: list[dict], ctx):
        for t in ticks:
            ctx.submit_order(symbol_id=t["symbol_id"], side=1, price=t["price"], qty=1_00000000)

bt = Backtester(data={"binance:BTC/USDT": lf}, seed=42, python_mode="batch", batch_ms=50)
res = bt.run(MyBatch())
```

Arrow zero-copy path (for large datasets)

```python
# Pass a PyArrow RecordBatchReader implementing __arrow_c_stream__
res = bt.run_arrow(stream=rb_reader, strategy=MyBatch())
```

---

### Build, Test, Benchmarks

- Python tests: `pytest -q`
  - Benchmarks only: `pytest -m bench -q`
- Rust build: `cargo build -p backtester-core`
- Rust tests: `cargo test -p backtester-core`
- Rust benches: `cargo bench -p backtester-core`

Note: `python/tests/conftest.py` auto-runs `maturin develop` if the extension isn't installed.

---

### Benchmark Configuration (Practical Conditions)

Criterion benches in Rust can be tuned via environment variables (defaults: 4 symbols × 250k ticks each):

- `BACKTEST_BENCH_NSYMBOLS` (default: `4`)
- `BACKTEST_BENCH_TICKS_PER_SYMBOL` (default: `250000`)
- `BACKTEST_BENCH_DT_NS` tick spacing in ns (default: `1000`)
- `BACKTEST_BENCH_SYMBOL_STAGGER_NS` per-symbol start offset in ns (default: `10000`)
- `BACKTEST_BENCH_FEED_LATENCY_NS` (default: `2000000`)
- `BACKTEST_BENCH_ORDER_UPDATE_LATENCY_NS` (default: `1000000`)
- `BACKTEST_BENCH_ORDER_LATENCY_NS` (default: `500000`)
- `BACKTEST_BENCH_SUBMIT_EVERY_N` order submission interval in ticks (default: `256`)
- `BACKTEST_BENCH_MAX_BATCH_NS` batch-mode window in ns (default: `10000000`)

Example:

```bash
# 8 symbols × 500k ticks per symbol, 5ms batch window
BACKTEST_BENCH_NSYMBOLS=8 \
BACKTEST_BENCH_TICKS_PER_SYMBOL=500000 \
BACKTEST_BENCH_MAX_BATCH_NS=5000000 \
cargo bench -p backtester-core --bench bench_core
```

The E2E benches (`bench_engine_e2e_*`) measure both Tick and Batch modes under identical conditions. Synthetic data is deterministic and includes realistic order flow (opposite-side, same-price passive limit orders at a fixed interval).

### Profile-Guided Optimization (PGO)

To build with PGO for maximum performance (Linux/macOS):

```bash
# Requires llvm-profdata (part of LLVM tools)
make pgo
```

This runs a 4-step pipeline:
1. Instrumentation build
2. Profile generation (runs benchmarks)
3. Profile merge
4. Optimized build using profiles

Expected improvement: 5-15% throughput.


---

### Examples (example/)

- `example/colab_backtester_demo.ipynb`
  - Minimal E2E demo notebook. Click the badge above to run in Colab.
  - For local use, start Jupyter from the repo root and open files under `example/`.
- `example/crypto_researcher_adoption_guide.md`
  - Practical guide for researchers: onboarding, data schema, strategy modes (tick/batch), performance tuning.

Note: Large datasets are not bundled. Start with the minimal data generated by tests in `python/tests/` or the Colab demo.

---

### Coding Style & Core Principles

- Rust: edition 2024, `cargo fmt` / `cargo clippy`. Naming: functions/modules `snake_case`, types `CamelCase`.
- Python: PEP 8, 4-space indent. Type hints required for new/changed code.
- Determinism first: fixed RNG seeds, stable ordering. Avoid `f64` for monetary logic (only at I/O boundaries).

---

### Contribution & Commit Guidelines

- Start with `docs/SPEC.md` and `docs/PLAN.md`.
- Conventional Commits: e.g., `feat(core): add queue model`, `chore(fmt): rustfmt`.
- Branches: `feature/...`, `fix/...`, `chore/...`.
- PRs should include What/Why, linked issues, test plan (commands + results), and performance notes if core paths changed. Update `docs/` when APIs or architecture shift.

---

### Status & Roadmap

This project is under active development (WIP). APIs and internals may change.

- Technical spec: `docs/SPEC.md`
- Plan/tests/benches: `docs/PLAN.md`
- Researcher adoption guide: `example/crypto_researcher_adoption_guide.md`
- Colab demo: `example/colab_backtester_demo.ipynb`
