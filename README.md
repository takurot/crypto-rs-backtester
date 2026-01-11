### crypt-rs-backtester

**A research-friendly, tick-level, high-fidelity backtester powered by Rust and Python (Polars).** (WIP / Under Development)

- **Goal**: Combining the agility of Python for research with the high-performance, deterministic simulation of Rust. It aims to solve common HFT backtesting pitfalls: *performance bottlenecks*, *look-ahead bias*, and *lack of reproducibility*.
- **Target**: Cryptocurrency (CEX) focused, enabling microstructure-level verification including multi-venue dynamics, latency modeling, and queue position (matching) logic.

---

### Why use this tool?

- **Performant by Design**: The core is built in Rust using an event-driven architecture and fixed-point arithmetic, allowing Python to focus purely on the research interface.
- **Zero-Copy Data Pipeline**: Leveraging Polars (Apache Arrow) for pre-processing in Python and handing off data to Rust with minimal memory copying.
- **Microstructure Fidelity**: Simulates realistic market conditions such as latency jitter/interpolation, L2/L3 queue models, and the "PendingCancel" race conditions.
- **Determinism First**: Ensures 100% reproducibility via seeded RNGs and stable tie-breaking for events with identical timestamps.

---

### Key Features (Goals & Specifications)

- **Hybrid Architecture**: Rust simulation core + Python strategy/analysis interface.
- **Time Axis Separation**:
  - **`ts_exchange`**: Ground-truth market time (venue timestamp).
  - **`ts_local`**: Time observed by the strategy (reflecting feed latency).
  - **`ts_sim`**: Internal simulation clock ordering all discrete events.
- **Look-ahead Bias Prevention**: Strategies can only access the feed-delayed `MarketView` consistent with their `ts_local`.
- **Python Strategy Execution Modes**:
  - **Tick Mode**: `on_tick` callback (simple and intuitive).
  - **Batch Mode**: `on_ticks` / `on_order_updates` (optimized for higher throughput by reducing FFI overhead).
- **Fixed-Point Arithmetic**: All monetary values (price, qty) use `i64` fixed-point math; `f64` is strictly limited to I/O boundaries.

For more details, see `docs/SPEC.md`.

---

### Status (Important)

**Currently under active development (WIP).** The API, internal architecture, and file structure are subject to change.

- **Technical Specification**: `docs/SPEC.md`
- **Implementation Plan (Tasks/Tests/Benches)**: `docs/PLAN.md`

---

### Usage (Example / Pseudo-code)

```python
import polars as pl
from rust_backtester import Backtester, Strategy, QueueModel, LatencyModel

class MyStrategy(Strategy):
    def on_ticks(self, ticks, ctx):
        # ticks: observations after feed delay (ts_local basis)
        for t in ticks:
            # e.g., Submit order if conditions are met
            pass

# Recommended: Use LazyFrame for zero-copy ingestion
df = pl.scan_parquet("data/btc_usdt/*.parquet")

bt = Backtester(
    data={"binance:BTC/USDT": df},
    python_mode="batch",
    batch_ms=100,
    seed=42,
    latency_model=LatencyModel.log_normal(mean_ms=5, std_ms=2),
    queue_model=QueueModel.volume_clock(),
)

result = bt.run(MyStrategy())
trades = result.trades()
stats = result.stats()
```

*Note: This example illustrates the intended API. Implementation status depends on the progress tracked in `docs/PLAN.md`.*

---

### Contributing / Development

- **Prerequisites**: Read `docs/SPEC.md` → `docs/PLAN.md`.
- **Core Principles**:
  - Never break determinism (seed consistency, stable event ordering).
  - Do not use `f64` for core monetary/accounting logic.
  - Prioritize batching and zero-copy for Python-Rust FFI.
