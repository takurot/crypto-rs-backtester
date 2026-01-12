# crypto-rs-backtester Adoption Guide (for Crypto Researchers)

This guide helps quantitative crypto researchers adopt crypto-rs-backtester quickly and run deterministic, high‑fidelity tick‑level backtests at speed (WIP).

- Hybrid workflow: preprocessing/visualization in Python (Polars/Arrow), simulation core in Rust (deterministic, event‑driven)
- Non‑negotiables: reproducibility (seeded RNG, stable ordering), no look‑ahead, money as fixed‑point `i64`
- References: `docs/SPEC.md` (spec), `docs/PLAN.md` (plan), Colab demo `example/colab_backtester_demo.ipynb`

---

## 1. Setup

Prerequisites
- Python 3.10+
- Rust toolchain (`rustup`)
- maturin (build Python↔Rust extension)

Install

```bash
# 1) Virtualenv
python -m venv .venv && source .venv/bin/activate

# 2) Build tools
pip install -U pip maturin

# 3) Dev install (build Rust extension)
pip install -e .[dev]
# Alternative: maturin develop
```

Quick smoke test

```python
# Python REPL
import polars as pl
from rust_backtester import Backtester

lf = pl.DataFrame({
    'ts_exchange': [1_000, 2_000, 3_000],
    'price': [100_00000000, 101_00000000, 100_50000000],  # 1e-8 fixed-point
    'qty':   [  1_00000000,   1_00000000,   1_00000000],
    'side':  [            1,           -1,            1],  # 1=Buy, -1=Sell, 0=None
}).lazy()

bt = Backtester(data={"binance:BTC/USDT": lf}, seed=42)
print(bt.run_smoke())  # Any numeric output means OK
```

---

## 2. Data Schema (Required/Recommended)

Required columns (Polars DataFrame / LazyFrame)
- `ts_exchange: Int64` – exchange time (ground truth) [ns]
- `price: Int64` – price (fixed‑point 1e‑8; e.g., 100.0 => 100_00000000)
- `qty: Int64` – quantity (same scale)
- `side: Int8` – 1=Buy, −1=Sell, 0=None

Recommended columns
- `seq: Int64` – stable ordering within equal `ts_exchange` (row index used if missing)
- `ts_local: Int64` – strategy‑observed time (after feed latency)

Notes
- For `Backtester.run(...)` (Polars path), if `ts_local` is missing, the engine uses `ts_exchange + feed_latency_ns`.
- For `run_arrow(...)` (Arrow zero‑copy path), providing `ts_local` explicitly is recommended for now.

Inject a constant feed latency

```python
lf = lf.with_columns(
    (pl.col("ts_exchange") + 5_000_000).alias("ts_local")  # +5ms
)
```

---

## 3. Minimal Strategies (Tick / Batch)

Tick mode (easy to debug)

```python
from rust_backtester import Backtester

class MyStrategy:
    def on_tick(self, tick: dict, ctx):
        # tick: {ts_exchange, ts_local, symbol_id, price, qty, side, ...}
        sym = tick["symbol_id"]
        # Example: place a simple passive limit order
        ctx.submit_order(symbol_id=sym, side=1, price=tick["price"], qty=1_00000000)

lf = make_lazyframe_somehow()
bt = Backtester(data={"binance:BTC/USDT": lf}, seed=42, python_mode="tick")
res = bt.run(MyStrategy())
print(res.stats())  # dict
print(res.trades()) # list[dict]
```

Batch mode (high throughput, fast iteration)

```python
class MyBatchStrategy:
    def on_ticks(self, ticks: list[dict], ctx):
        # Process in batches; optionally pass to Polars for vectorized filtering
        for t in ticks:
            ctx.submit_order(symbol_id=t["symbol_id"], side=1, price=t["price"], qty=1_00000000)

bt = Backtester(data={"binance:BTC/USDT": lf}, seed=42, python_mode="batch", batch_ms=100)
res = bt.run(MyBatchStrategy())
```

Notes
- `symbol_id` is numeric (assigned deterministically by sorted keys). Use the tick’s `symbol_id` when submitting orders.
- `side`: 1=Buy, −1=Sell.
- Always pass `price/qty` as 1e‑8 scaled `i64` (floats only at I/O boundaries).

---

## 4. Multi‑venue / Multi‑symbol

```python
data = {
  "binance:BTC/USDT": lf_btc,
  "okx:BTC/USDT":     lf_btc_okx,
}

bt = Backtester(data=data, seed=42, python_mode="batch", batch_ms=50)
res = bt.run(MyBatchStrategy())
```

- Keys (`"exchange:SYMBOL"`) are arbitrary strings; IDs are assigned by lexicographic order for determinism.
- Use `tick["symbol_id"]` to distinguish venues/symbols and evaluate cross‑venue spreads/latency.

---

## 5. Reproducibility (Determinism) check

Example: same seed + same input => identical results

```python
bt1 = Backtester(data=data, seed=42, python_mode="batch")
res1 = bt1.run(MyBatchStrategy())

bt2 = Backtester(data=data, seed=42, python_mode="batch")
res2 = bt2.run(MyBatchStrategy())

assert res1.stats()["total_trades"] == res2.stats()["total_trades"]
# Optionally assert full trade‑log equality
```

Recommendations
- Provide a `seq` column for explicit stable ordering
- Any stochastic models must derive from the engine‑owned seeded RNG

---

## 6. Performance tuning

- Reduce Python call overhead: use `python_mode="batch"` and tune `batch_ms` (e.g., 50–200ms)
- Leverage Polars: select only necessary columns; define filters/derived columns (e.g., `ts_local`) lazily
- Keep loops light in Python; push heavy aggregation to Polars or Rust
- Data hygiene: enforce `Int64/Int8` dtypes, remove nulls/NaNs
- System: minimize other loads when measuring hot single‑threaded paths

---

## 7. Arrow zero‑copy path (advanced)

For large datasets, prefer `run_arrow(stream, strategy)`.

```python
# Example: pass a PyArrow RecordBatchReader exposing __arrow_c_stream__
from rust_backtester import Backtester

bt = Backtester(data={}, seed=42, python_mode="batch", batch_ms=50)
res = bt.run_arrow(stream=rb_reader, strategy=MyBatchStrategy())
```

Notes
- For now, explicitly provide a `ts_local` column (auto application is not yet unified on the Arrow path).
- Multi‑stream ingestion and metadata→`symbol_id` mapping will be expanded.

---

## 8. Common errors and fixes

- `missing ts_exchange/price/qty/side` → verify column names (supports `qty/size`, accepts `ts_event` alias)
- `invalid side` → only 1/−1/0 are valid
- `column lengths mismatch` → ensure equal lengths; watch collect/materialization steps
- No fills? → re‑check price/qty scale (1e‑8) and `side`; also verify `ts_local` ordering

---

## 9. Next steps

- Spec/design: `docs/SPEC.md`
- Implementation plan/tests: `docs/PLAN.md`
- Colab demo: `example/colab_backtester_demo.ipynb`
- Related article (use cases/differentiation): `example/qiita_use_cases_and_differentiation.md`

If you’d like help profiling, tuning batch settings, or validating tick vs batch equivalence, share your data shape/strategy and we can suggest targeted steps.
