# Rust-based Tick-level Backtester Specification

## 1. Overview
This document defines the functional and technical specifications for a high-performance, tick-level crypto backtesting engine. The system is designed to solve the "Two-Language Problem" by combining **Rust** for the high-performance simulation core and **Python** for strategy research and data analysis.

### Key Philosophy
- **Performance First**: Deterministic execution without Garbage Collection (GC) pauses.
- **Zero-Copy Architecture**: Seamless data sharing between Python and Rust using Apache Arrow memory layout.
- **Microstructure Fidelity**: Accurate simulation of latency, queue dynamics, and market microstructure.
- **Hybrid Workflow**: Research in Python, Execution in Rust.

### Terminology & Time Axes (Critical)
- **`ts_exchange`**: Ground-truth venue timestamp in nanoseconds (market time).
- **`ts_local`**: Strategy-observed time in nanoseconds (`ts_exchange + simulated feed latency`).
- **`ts_sim`**: The simulation clock used to order *all* discrete events in the engine (market data, feed deliveries, order arrivals/ACKs, funding, timers). `ts_sim` is always in the same nanosecond timeline.
- **Look-ahead bias prevention**: The strategy MUST only observe market information that has been delivered at or before its current `ts_local`. The engine MUST provide a feed-delayed `MarketView` for strategy access; the exchange matching engine uses the ground-truth book.

### Determinism & Reproducibility (Non-Negotiable)
- A backtest run MUST be reproducible given the same inputs and RNG seed.
- All stochastic components (latency jitter, probabilistic queue models) MUST draw from an explicitly-seeded RNG stream.
- Event ordering MUST be deterministic even when multiple events share the same timestamp (stable tie-breakers; never rely on `HashMap` iteration order).
- Floating-point output in statistics is allowed, but strategy decisions and accounting MUST NOT depend on non-deterministic float behavior.

### Design Goals (Differentiation from Existing Tools)
| Aspect | hftbacktest | NautilusTrader | This System |
|--------|-------------|----------------|-------------|
| Focus | Research | Live Trading | **Pure Backtesting** |
| Python Integration | Numba (debug difficulty) | Complex Config | **Polars (intuitive)** |
| Latency Model | Advanced | Advanced | **Jitter + Interpolation** |
| FFI Overhead | Per-tick callback | Actor overhead | **Batch processing** |

## 2. System Architecture

The system adopts a **Hybrid Event-Driven Architecture**.

### 2.1 High-Level Data Flow
```
┌─────────────────────────────────────────────────────────────────────────┐
│                           Python Layer                                   │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────────────────┐  │
│  │ Polars       │ -> │ LazyFrame    │ -> │ Arrow Memory (Zero-Copy) │  │
│  │ scan_parquet │    │ Optimization │    │ Pointer Handover         │  │
│  └──────────────┘    └──────────────┘    └────────────┬─────────────┘  │
└───────────────────────────────────────────────────────┼─────────────────┘
                                       PyO3 FFI Boundary │
┌───────────────────────────────────────────────────────┼─────────────────┐
│                           Rust Core                   ▼                  │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │              Global Event Queue (ts_sim ordered)                 │   │
│  │    Binance Events ─┬─ Bybit Events ─┬─ OKX Events ───────────>   │   │
│  └────────────────────┴────────────────┴────────────────────────────┘   │
│                                      │                                   │
│         ┌────────────────────────────┼────────────────────────────┐     │
│         ▼                            ▼                            ▼     │
│  ┌─────────────┐       ┌───────────────────┐       ┌────────────────┐  │
│  │ LatencyModel│       │ ExchangeSimulator │       │ Strategy       │  │
│  │ (Feed/Order)│  <->  │ (Matching Engine) │  <->  │ (Rust/Python)  │  │
│  └─────────────┘       └───────────────────┘       └────────────────┘  │
│                                      │                                   │
│                        ┌─────────────┴─────────────┐                    │
│                        ▼                           ▼                    │
│                 ┌─────────────┐            ┌─────────────┐              │
│                 │ Account/OMS │            │ QueueModel  │              │
│                 └─────────────┘            └─────────────┘              │
└─────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Event Processing Flow
1.  **Data Ingestion (Python/Polars)**: Historical tick data is loaded into `Polars.LazyFrame` in Python.
    - Lazy evaluation ensures only necessary columns are loaded (projection pushdown).
2.  **Zero-Copy Handover (PyO3)**: The underlying Arrow memory pointers are passed to the Rust core via `pyo3-polars`.
3.  **Pre-Calculation (Vectorized)**: Indicators amenable to vectorization (e.g., SMA, RSI) are pre-calculated in Rust using Polars expressions.
4.  **Event Simulation (Rust Core)**: The core event loop is a discrete-event simulator.
    -   **Global Event Queue**: Merges *all* events (market data, feed deliveries, order arrivals/ACKs, funding, timers) ordered by `ts_sim` (nanoseconds) with deterministic tie-breakers.
    -   **Market Truth vs Strategy View**: Market data updates the exchange at `ts_exchange`; the strategy receives the same information at `ts_local` via feed-delivery events.
    -   **Batch Processing (Python)**: To minimize FFI overhead, the engine buffers delivered ticks and wakes the Python strategy on configurable conditions (e.g., max batch duration like 100ms, any order-update delivery, funding/timer events). The preferred interface is a batched callback (`on_ticks`) rather than per-tick calls.
5.  **Strategy Execution**:
    -   **Pure Rust**: Strategies written in Rust for maximum speed.
    -   **Python (Tick Mode)**: `on_tick` called per delivered tick (simpler, slower).
    -   **Python (Batch Mode)**: `on_ticks` called with batches of delivered ticks + order updates (fewer FFI calls).
6.  **Result Aggregation**: Trade logs and statistics are returned to Python as Polars DataFrames.

## 3. Core Components & Data Structures

### 3.1 Data Types (Rust)
To ensure precision and memory efficiency.

**Numerical Precision**:
All prices and quantities must use **Fixed-Point Arithmetic** (e.g., `i64` scaled by 1e8) to avoid floating-point errors. `f64` is strictly prohibited for monetary values.

```rust
// Core Tick structure (logical representation for callbacks/logging)
#[repr(C)]
pub struct Tick {
    pub ts_exchange: i64,   // Exchange timestamp (nanoseconds)
    pub ts_local: i64,      // Strategy-observed timestamp (nanoseconds)
    pub symbol_id: u32,     // Mapped integer ID for symbol/venue pair
    pub price: i64,         // Fixed-point (e.g., 100.00 -> 10000000000)
    pub qty: i64,           // Fixed-point
    pub side: i8,           // 1 = Buy, -1 = Sell, 0 = None
    pub flags: u8,          // Bitmask: TRADE=0x01, QUOTE=0x02, LIQUIDATION=0x04, SNAPSHOT=0x08
}

// Order Book Update (L2)
#[repr(C)]
pub struct L2Update {
    pub ts_exchange: i64,
    pub symbol_id: u32,
    pub price: i64,
    pub qty: i64,           // 0 = price level removed
    pub side: i8,
}

// Order Book Update (L3)
#[repr(C)]
pub struct L3Update {
    pub ts_exchange: i64,
    pub symbol_id: u32,
    pub order_id: u64,
    pub price: i64,
    pub qty: i64,
    pub side: i8,
    pub action: u8,         // ADD=0x01, MODIFY=0x02, DELETE=0x03
}
```

> Note: For true zero-copy ingestion, the engine SHOULD iterate Arrow/Polars columns directly (SoA) and avoid materializing a full `Vec<Tick>` for large datasets. The `Tick` struct above is a logical representation for callbacks/logging, not a required on-disk/in-memory row layout.

### 3.2 Order State Machine
```
    ┌─────────────┐
    │   Created   │
    └──────┬──────┘
           │ submit()
           ▼
    ┌─────────────┐     Order Entry Latency
    │ PendingNew  │ ─────────────────────────>
    └──────┬──────┘
           │ ACK received
           ▼
    ┌─────────────┐
    │    Open     │<──────────────────────────┐
    └──────┬──────┘                           │
           │                                   │
     ┌─────┴─────┐                            │
     ▼           ▼                            │
┌────────┐  ┌───────────────┐                 │
│ Filled │  │ PartialFilled │─────────────────┘
└────────┘  └───────────────┘
                   │
     cancel() ─────┼───────────────────────────┐
                   │                           │
                   ▼                           ▼
           ┌──────────────┐            ┌─────────────┐
           │PendingCancel │───────────>│  Cancelled  │
           └──────────────┘ Cancel ACK └─────────────┘
                   │
                   │ Fill during cancel flight
                   ▼
            ┌─────────────┐
            │   Filled    │  (Cancel rejected)
            └─────────────┘
```

**Critical Note**: Orders in `PendingCancel` state may still be filled before the cancel is acknowledged. The engine MUST simulate this race condition.

### 3.3 Modules

#### A. Backtester (Main Engine)
Exposed as a Python Class via `#[pyclass]`.
-   **Responsibility**: Orchestrates data loading, clock management, and the event loop.
-   **State**: Holds references to `ExchangeSimulator`, `Strategy`, and `LatencyModel`.
-   **Clock Management**: 
    - `ts_exchange`: Actual market time (ground truth).
    - `ts_local`: Strategy's perceived time (`ts_exchange + feed_latency`).
    - `ts_sim`: Engine simulation time used to order all discrete events.
    - Strategy logic MUST only access `ts_local` and a feed-delayed `MarketView` consistent with `ts_local`.

#### B. ExchangeSimulator (Matching Engine)
Simulates the order matching logic of a Centralized Exchange (CEX).
-   **MarketDepth Trait**: Abstract interface handling both **L2 (Price Levels)** and **L3 (Order-by-Order)** data.
    -   *L2 Mode*: Updates price levels (bids/asks).
    -   *L3 Mode*: Tracks individual order IDs for deterministic queue position.
-   **Component**: `OrderBook` struct managing the state of the book.
-   **Fee Model**: Configurable maker/taker fees per exchange.

#### C. QueueModel (Microstructure Simulation)
Determines *when* a limit order fills based on its position in the queue.

| Model | Use Case | Description |
|-------|----------|-------------|
| `Conservative` (Default) | Pessimistic testing | User is last in queue (LIFO assumption) |
| `VolumeClock` | L2 data | Fill when `cumulative_trade_volume > queue_size_at_entry` |
| `Probabilistic` | Research | Queue-Reactive model with ML-based fill probability |
| `L3Exact` | L3 data | Exact queue position tracking |

#### D. LatencyModel
Simulates network and processing delays.

| Latency Type | Description | Default Model |
|--------------|-------------|---------------|
| Feed Latency | `ts_local = ts_exchange + Δfeed` | Constant or Log-Normal |
| Order Entry | Signal → Exchange ACK | Poisson distribution |
| Order Response | Exchange ACK → Strategy receipt | Same as Feed Latency |

**Jitter Implementation**:
```rust
pub trait LatencyModel: Send + Sync {
    fn sample_feed_latency(&self, ts_exchange: i64, rng: &mut impl Rng) -> i64;
    fn sample_order_latency(&self, ts_local: i64, rng: &mut impl Rng) -> i64;
}

pub struct LogNormalJitter {
    pub mean_ns: i64,
    pub std_dev_ns: i64,
}
```

**Interpolated Latency (Advanced)**: Load historical pcap data to replay actual network conditions.
The engine MUST own the RNG (seeded) for reproducibility; latency models MUST NOT keep RNG state internally.

#### E. Account & OMS (Order Management System)
Tracks portfolio state.
-   **Balances**: Available vs. Locked assets (per exchange).
-   **Positions**: Average entry price, Unrealized PnL, Realized PnL.
-   **In-Flight Orders**: Strict state tracking for `PendingNew`, `PendingCancel`.
-   **Kill Switch**: Hard-coded position limits and max drawdown triggers.

### 3.4 Crypto-Specific Features

#### Funding Rate (Perpetual Futures)
```rust
pub struct FundingEvent {
    pub ts_exchange: i64,
    pub symbol_id: u32,
    pub rate: i64,  // Fixed-point, e.g., 0.0001 = 10000 (scaled by 1e8)
}
```
- Funding payments occur at configurable intervals (default: 8 hours).
- Applied to open positions at funding timestamp.

#### Liquidation Simulation
- Monitor position health factor based on Mark Price.
- Force-close positions when margin ratio breaches threshold.

## 4. Interfaces

### 4.1 Python API
The user-facing API should look like idiomatic Python.

```python
import polars as pl
from rust_backtester import Backtester, Strategy, Order, Side

class MyStrategy(Strategy):
    def on_tick(self, tick, ctx):
        if tick.price < 50000_00000000:  # Fixed-point: 50000.00
            ctx.submit_order(Order.limit(
                symbol="BTC/USDT",
                side=Side.BUY,
                price=tick.price,
                qty=1_00000000  # 1.0 BTC
            ))
    
    def on_order_update(self, report, ctx):
        # OrderReport covers fills, cancels, and rejects.
        if report.status == "FILLED":
            print(f"Filled: {report.last_fill_qty} @ {report.last_fill_price}")
        elif report.status == "REJECTED":
            print(f"Rejected: {report.reason}")

# Load Data (Lazy evaluation)
df = pl.scan_parquet("data/btc_usdt/*.parquet")

# Configure Backtester
bt = Backtester(
    data={"binance:BTC/USDT": df},
    latency_model=LatencyModel.log_normal(mean_ms=5, std_ms=2),
    queue_model=QueueModel.volume_clock(),
    initial_balance={"USDT": 100000_00000000},
    fees={"binance": FeeModel(maker=0.0001, taker=0.0005)},
    python_mode="batch",   # "tick" or "batch"
    batch_ms=100,
    seed=42,
)

# Run Simulation
result = bt.run(MyStrategy())

# Access Results (Polars DataFrame)
trades_df = result.trades()
equity_curve = result.equity_curve()
stats = result.stats()  # Returns BacktestStats object
```

### 4.2 Result Statistics (Computed in Rust)
All statistics MUST be computed in Rust to avoid Python overhead.

```rust
pub struct BacktestStats {
    pub total_trades: u64,
    pub win_rate: f64,
    pub profit_factor: f64,
    pub sharpe_ratio: f64,      // Annualized
    pub sortino_ratio: f64,
    pub max_drawdown: f64,      // Percentage
    pub max_drawdown_duration: i64, // Nanoseconds
    pub calmar_ratio: f64,
    pub total_pnl: i64,         // Fixed-point
    pub avg_trade_pnl: i64,     // Fixed-point
    pub avg_holding_period: i64, // Nanoseconds
    pub total_fees_paid: i64,   // Fixed-point
}
```

### 4.3 Rust Trait Definitions

```rust
pub trait Strategy: Send {
    fn on_tick(&mut self, tick: &Tick, ctx: &mut Context);
    fn on_ticks(&mut self, ticks: &[Tick], ctx: &mut Context) {
        for tick in ticks {
            self.on_tick(tick, ctx);
        }
    }

    fn on_order_update(&mut self, report: &OrderReport, ctx: &mut Context);
    fn on_order_updates(&mut self, reports: &[OrderReport], ctx: &mut Context) {
        for report in reports {
            self.on_order_update(report, ctx);
        }
    }

    fn on_funding(&mut self, event: &FundingEvent, ctx: &mut Context) {}
}

pub trait LatencyModel: Send + Sync {
    fn sample_feed_latency(&self, ts_exchange: i64, rng: &mut impl Rng) -> i64;
    fn sample_order_latency(&self, ts_local: i64, rng: &mut impl Rng) -> i64;
}

pub trait QueueModel: Send + Sync {
    fn register_order(&mut self, order: &LimitOrder, book_state: &OrderBook);
    fn check_fill(&mut self, order_id: u64, trades: &[Trade]) -> Option<FillResult>;
}

pub trait MarketDepth {
    fn apply_l2(&mut self, update: &L2Update);
    fn apply_l3(&mut self, update: &L3Update);
    fn best_bid(&self) -> Option<(i64, i64)>;  // (price, qty)
    fn best_ask(&self) -> Option<(i64, i64)>;
}
```

## 5. Multi-Venue & Asset Specifics

### 5.1 Crypto Market Fragmentation
-   **Problem**: Same asset (BTC/USDT) trades on Binance, Bybit, OKX with latency diffs.
-   **Solution**: 
    - `symbol_id` maps to unique `(Exchange, Symbol)` tuple.
    - Each exchange has independent `ExchangeSimulator` instance.
    - Market events are ordered by `ts_exchange`; strategy-perceived ordering is by `ts_local`. The engine processes a unified event queue ordered by `ts_sim`.
-   **Cross-Exchange Arbitrage**: Supports simultaneous orders across venues with independent latency models.

### 5.2 Reg NMS & SIP (Optional/Advanced for US Equities)
-   **Slow NBBO**: Simulates the SIP feed latency (e.g., 20-50µs delay).
-   **Direct Feed**: Simulates direct exchange proprietary feeds (faster).
-   **Logic**: 
    - Strategy declares feed subscription type in config.
    - SIP subscribers cannot react to Direct-only price changes until SIP catch-up.
    - Prevents look-ahead bias in equity strategy backtests.

## 6. Implementation Requirements

### 6.1 Integration Stack
| Component | Choice | Rationale |
|-----------|--------|-----------|
| Language | Rust 2021+ | Performance, safety |
| Python Binding | `pyo3` + `maturin` | Mature, well-documented |
| Data Interchange | `polars` + `pyo3-polars` | Zero-copy Arrow |
| Async Runtime | `tokio` (optional) | Future live trading |
| Random | `rand` + `SmallRng` | Reproducible jitter |

### 6.2 Data Input Requirements
Parquet files MUST contain these columns (aliases accepted):

| Column | Type | Description |
|--------|------|-------------|
| `ts_exchange` (or `ts_event`) | int64 | Exchange timestamp (nanoseconds since epoch) |
| `price` | int64 | Scaled fixed-point price |
| `qty` (or `size`) | int64 | Scaled fixed-point quantity |
| `side` | int8 | 1=Buy, -1=Sell, 0=Unknown |

Optional columns:
| Column | Type | Description |
|--------|------|-------------|
| `ts_local` | int64 | Strategy-observed timestamp. If omitted, computed as `ts_exchange + Δfeed`. |
| `flags` | uint8 | Bitmask: TRADE=0x01, QUOTE=0x02, LIQUIDATION=0x04, SNAPSHOT=0x08 |
| `seq` | uint64 | Stable sequence number within a stream. If omitted, row order is used as a tie-breaker. |

Optional columns for L3:
| Column | Type | Description |
|--------|------|-------------|
| `order_id` | uint64 | Unique order identifier |
| `action` | uint8 | ADD/MODIFY/DELETE |

L3 mode is enabled from Python with `depth_mode="l3"` and `l3_data={symbol: lazy_frame}`.
L3 `MODIFY` updates quantity for the same `(side, price, order_id)` queue entry; venue
price changes should be represented as DELETE followed by ADD so queue priority remains explicit.

For performance and determinism, each input stream SHOULD be pre-sorted by (`ts_exchange`, `seq`) ascending. The engine MAY reject unsorted input rather than sorting internally.

### 6.3 Performance Targets

| Metric | Target | Measurement |
|--------|--------|-------------|
| Throughput (Pure Rust) | > 10,000,000 ticks/sec | Single core, no strategy callback |
| Throughput (Python Strategy) | > 1,000,000 ticks/sec | With batch processing |
| Memory Overhead | < 10% | Above Polars DataFrame size |
| Startup Time | < 1 sec | For 1GB Parquet file |
| FFI Call Overhead (Rust↔Python) | < 20µs | Per batched callback (excluding user Python code) |

### 6.4 Error Handling
```rust
#[derive(Debug, thiserror::Error)]
pub enum BacktestError {
    #[error("Data validation failed: {0}")]
    DataValidation(String),
    
    #[error("Insufficient balance: have {have}, need {need}")]
    InsufficientBalance { have: i64, need: i64 },
    
    #[error("Order rejected: {reason}")]
    OrderRejected { order_id: u64, reason: String },
    
    #[error("Invalid price: {price} outside tick size {tick_size}")]
    InvalidPrice { price: i64, tick_size: i64 },
    
    #[error("Position limit exceeded: {current} + {delta} > {limit}")]
    PositionLimit { current: i64, delta: i64, limit: i64 },
}
```

### 6.5 Logging & Debugging
```rust
// Configurable verbosity levels
pub enum LogLevel {
    Silent,     // No output
    Summary,    // Final stats only
    Trades,     // Log all fills
    Orders,     // Log all order events
    Ticks,      // Log every tick (SLOW)
    Debug,      // Full trace
}
```

### 6.6 Scalability & Performance Extensions (Future)
This section specifies forward-looking optimizations that preserve the non-negotiable principles (determinism, fixed-point money, no look-ahead bias) while scaling to larger datasets and parameter sweeps.

#### 6.6.1 True Zero-Copy Ingestion via Arrow C Data Interface
The system SHOULD support a “true zero-copy” ingestion path across the Python↔Rust boundary using the Arrow C Data Interface:

- **Input type**: `ArrowArrayStream` (RecordBatch stream) exported from Python (e.g., `pyarrow.RecordBatchReader`, Polars Arrow export).
- **Rust consumption**: Iterate SoA (columnar) arrays directly to avoid materializing a full `Vec<Tick>`.
- **Schema**: Must validate required columns and aliases (see 6.2). Missing optional columns must be derived deterministically (e.g., `ts_local = ts_exchange + Δfeed` when absent).
- **Ordering requirement**: Each input stream MUST be sorted by (`ts_exchange`, `seq`) ascending. The engine MAY reject unsorted input rather than sorting internally.

#### 6.6.2 Streaming Tick Sources + Lazy Event Scheduling
To reduce startup time and memory footprint for very large datasets, the engine SHOULD support streaming “tick sources”:

- A `TickSource` abstraction yields the *next* market-truth tick per stream without pre-loading all ticks/events.
- The global event loop maintains determinism by:
  - Scheduling only the next tick (truth) + next delivery per stream, then advancing that stream when consumed.
  - Using stable tie-breakers (global `EventId` / `seq`) for events sharing the same `ts_sim`.
- **Goal**: Memory overhead becomes \(O(\#streams)\) for tick scheduling rather than \(O(\#ticks)\).

#### 6.6.3 Zero-Copy Result Export (Trades / Equity Curve / Stats)
For large result sets, the system SHOULD expose results back to Python without building large Python lists/dicts:

- **Trades**: Provide an Arrow/Polars representation (`RecordBatch` / `DataFrame`) backed by Rust-owned Arrow buffers.
- **Equity curve**: Export a time-ordered series (timestamp + equity) as Arrow/Polars.
- **Stats**: Small structs can remain as a Python object, but must not require float-based decisions inside the engine.

#### 6.6.4 Bounded Trade Log Retention Policies
To support long-duration, high-frequency backtests, trade logging SHOULD be configurable:

- `TradeLogMode::All`: keep all fills/events (current behavior for correctness/debuggability).
- `TradeLogMode::RingBuffer(N)`: keep only the last N events (bounded memory).
- `TradeLogMode::SummaryOnly`: do not store per-trade rows; compute aggregate stats incrementally.
- `TradeLogMode::None`: disable trade logging entirely (maximum throughput).

When `SummaryOnly` is enabled, statistics MUST be computed deterministically and should match post-hoc computation for the same input (within defined tolerances for floating output fields).

#### 6.6.5 Parallel Parameter Sweeps (Deterministic)
The system SHOULD support running multiple independent backtests in parallel for parameter searches:

- Each run must remain reproducible under fixed inputs and seed.
- Seed derivation MUST be deterministic (e.g., `seed_run = hash64(base_seed, run_index)`).
- Output ordering MUST be stable (results returned in the same order as the input configurations).

#### 6.6.6 Performance Regression Observability
CI SHOULD publish benchmark results as artifacts (and optionally post summaries on PRs) to detect regressions early:

- Criterion reports uploaded per run.
- Optional scheduled runs (e.g., weekly) for baseline tracking.
- Avoid hard gating on noisy microbenchmarks unless a robust methodology is established.

### 6.7 Build & Compiler Optimizations
The system SHOULD use optimized build profiles for release builds:

```toml
[profile.release]
lto = "thin"           # Link-time optimization
codegen-units = 1      # Single codegen unit for better optimization
opt-level = 3          # Maximum optimization
# panic = "abort"      # UNSAFE with PyO3 (causes interpreter crash on Rust panic)
```

- **Expected impact**: 10-20% throughput improvement over default release profile.
- **Consideration**: Longer compile times; `panic="abort"` must be avoided for Python extension modules.

### 6.8 Data Structure Performance Guidelines
The following data structure choices SHOULD be considered for performance-critical paths:

| Use Case | Recommended | Rationale |
|----------|------------|-----------|
| Symbol → Exchange lookup | `FxHashMap<u32, _>` or `Vec` | O(1) lookup; small key |
| Order ID → Symbol routing | `Vec<Option<u32>>` | O(1) direct index for dense sequential IDs |
| Event queue | `BinaryHeap` | O(log n) push/pop; stable ordering |
| Order book (L2) | `BTreeMap<i64, i64>` | Ordered traversal for best bid/ask |
| Batch buffers | `Vec::with_capacity(N)` | Pre-allocate to avoid resizing |
| Orders by price (fill check) | `HashMap<(price, side), Vec<order_id>>` | O(1) bucket lookup on trade |

**Order Fill Check Optimization**: The `ExchangeSimulator::on_trade` function SHOULD NOT scan all orders for each trade. Instead, maintain a price+side bucket index:
```rust
// Index: (price, opposite_side) -> order_ids
orders_by_price: HashMap<(i64, Side), Vec<u64>>

fn on_trade(&mut self, trade: Tick) -> Vec<OrderReport> {
    let bucket = self.orders_by_price.get(&(trade.price, trade.side.opposite()));
    // Only check orders in the matching bucket
}
```

**Multi-Symbol Source Selection**: When multiple `TickSource` streams are active, the engine SHOULD use a min-heap for O(log N) source selection instead of O(N) linear scan:
```rust
// (ts_exchange, source_idx) for stable ordering
source_heap: BinaryHeap<Reverse<(i64, usize)>>
```

**Determinism Note**: When using `FxHashMap`, iteration order is not deterministic. If deterministic iteration is required (e.g., for logging or reproducibility), sort keys explicitly or maintain a separate ordered `Vec`.

### 6.9 Python FFI Performance
To minimize Python↔Rust boundary overhead:

- **Batch callbacks preferred**: `on_ticks(batch)` over `on_tick(tick)`.
- **Arrow-based data transfer**: Use Arrow RecordBatch for tick data instead of Python dicts.
- **Large result export**: Return trades/equity as Arrow arrays, not Python lists.

```python
# Preferred: Arrow-based callback (future)
def on_ticks_arrow(self, batch: pa.RecordBatch, ctx):
    prices = batch["price"].to_numpy()  # Zero-copy

# Current: Dict-based callback
def on_ticks(self, ticks: list[dict], ctx):
    for tick in ticks:
        price = tick["price"]
```

**Target overhead**: < 20µs per batched callback (excluding user strategy code).

## 7. Testing Requirements

### 7.1 Unit Tests
- Order state machine transitions
- Queue model fill logic
- Fixed-point arithmetic edge cases
- Latency model distribution verification

### 7.2 Integration Tests
- Multi-venue event ordering
- Funding rate application
- Position/PnL calculation accuracy

### 7.3 Regression Tests
- Known profitable strategy must remain profitable
- Known losing strategy must remain losing
- Comparison with reference implementation (hftbacktest)

### 7.4 Benchmark Suite
```rust
// Prefer Criterion benchmarks on stable Rust.
fn bench_event_loop_1m_ticks(c: &mut Criterion) { ... }
fn bench_orderbook_update(c: &mut Criterion) { ... }
fn bench_python_callback_batch(c: &mut Criterion) { ... }
```

## 8. Development Roadmap

> **Source of truth for task-level detail**: `docs/PLAN.md`.
> This section provides a high-level status overview; `PLAN.md` tracks individual sub-tasks, test names, and phase dependencies.

### Implemented

**Phase 0 — Quality Gates & Tooling**
- [x] Rust unit/integration test harness (`cargo test` < 1s baseline)
- [x] Determinism regression tests
- [x] `pytest` + `maturin` E2E workflow
- [x] Criterion benchmark harness

**Phase 1 — Core Engine (MVP)**
- [x] Rust matching engine with L2 order book
- [x] Conservative (price-time) queue model
- [x] Constant latency model (`feed_latency_ns`, `order_update_latency_ns`)
- [x] Global discrete-event loop with deterministic tie-breaking (`ts_sim`)
- [x] MarketView & look-ahead prevention (strategy sees only feed-delayed data)

**Phase 2 — Python Integration**
- [x] PyO3 bindings via `maturin`
- [x] Polars zero-copy data ingestion (Arrow memory handover)
- [x] Python strategy interface (`on_tick` / `on_ticks` / `on_order_update`)
- [x] Result export as Polars-compatible columnar format (Arrow arrays; trades, equity curve)

**Phase 3 — Advanced Microstructure**
- [x] Latency jitter (log-normal / Poisson models)
- [x] Volume-clock queue model
- [x] Funding rate simulation

**Phase 4 — Multi-Venue & Production**
- [x] Global event queue with multi-symbol / multi-venue scheduling
- [x] Multi-venue support (cross-exchange strategies)
- [x] Benchmark suite (Criterion + CI artifact upload)
- [x] Error propagation across FFI boundary (Result / Python exceptions)

**Phase 5 — Scale & Arrow Integration**
- [x] Arrow C Data Interface ingestion (`run_arrow`)
- [x] Streaming `TickSource` + lazy event scheduling
- [x] Arrow-compatible columnar result export (trades, equity curve)
- [x] Trade-log retention policies (ring buffer / all / none)
- [x] Parallel parameter sweep runner (`run_many`, deterministic with Rayon)
- [x] Benchmark regression observability (CI artifacts)

**Phase 6 — Performance Optimizations**
- [x] Compiler tuning (LTO=fat, codegen-units=1; `panic=abort` intentionally skipped — incompatible with PyO3 unwinding)
- [x] FxHashMap for hot lookups
- [x] Arrow-based columnar tick batches for strategy callbacks
- [x] Incremental / streaming statistics accumulation
- [x] SIMD-accelerated stats (Sharpe, drawdown) via `wide` crate
- [x] Rayon-based parallel sweeps
- [x] Cache-friendly struct layouts

**Phase 7 — Advanced Performance (selected items)**
- [x] PGO (Profile-Guided Optimisation) build pipeline
- [x] SmallVec for order buckets (inline storage for small order sets)
- [x] Memory-mapped file ingestion (`MmapFileLoader`)
- [x] Async I/O overlap with threaded pre-fetching (`AsyncBatchIter`)
- [x] Compressed Arrow IPC streams (LZ4/ZSTD via `ipc_compression` feature)
- [x] Cold path annotations (`#[cold]`, `#[inline(always)]` on hot paths)
- [x] Cache prefetching hints
- [x] Auto-tuning batch size

---

### Partially Implemented

| Item | Status | Notes |
|------|--------|-------|
| L3 order-book support | Implemented | `L3Update`, `OrderBookL3`, `L3ExactQueue`, Arrow L3 ingestion, and Python `depth_mode="l3"` opt-in |
| Kill switch & risk limits | Not started | Pre-trade risk checks; requires new `RiskGuard` component |
| Documentation & examples | Partial | README and architecture comments present; API reference and tutorials incomplete |

---

### Explored / Not Adopted

These were benchmarked and reverted due to regression or negligible benefit:

| Item | Outcome |
|------|---------|
| AVX2/AVX-512 tick parsing | ~20% regression (SoA→AoS scatter overhead); reverted |
| SIMD-accelerated EventQueue (4-ary heap) | No measurable improvement vs `std::BinaryHeap`; reverted |
| Loop unrolling for batch processing | ~10% regression vs compiler-optimised loop; reverted |
| Arena allocator for hot paths | Profiling showed `Vec` allocation is not the bottleneck; skipped |
| FxHashMap for order structs | Batch-mode regression; reverted (Vec reuse kept instead) |

---

### Future (Intentionally Out of Current Scope)

- **Interpolated latency from pcap** — Empirical latency modelling from captured network traces
- **Reg NMS / SIP simulation** — US equities market-structure rules
- **MEV simulation hooks** — DeFi/on-chain research extension
- **Live trading adapter** — Running the same strategy code against a live venue

## Appendix A: Fixed-Point Arithmetic

All monetary calculations use `i64` scaled by `10^8` (satoshi precision).
`f64` is allowed ONLY at I/O boundaries (parsing/formatting); core simulation and accounting must remain fixed-point.

```rust
const SCALE: i64 = 100_000_000;  // 1e8

// NOTE: Convenience for I/O only. Do not use f64 in core monetary calculations.
fn to_fixed(f: f64) -> i64 {
    (f * SCALE as f64).round() as i64
}

fn from_fixed(i: i64) -> f64 {
    i as f64 / SCALE as f64
}

// Multiplication: (a * b) / SCALE
fn mul_fixed(a: i64, b: i64) -> i64 {
    ((a as i128 * b as i128) / SCALE as i128) as i64
}
```

## Appendix B: Reference Implementations

- **hftbacktest**: https://github.com/nkaz001/hftbacktest
- **NautilusTrader**: https://github.com/nautechsystems/nautilus_trader
- **rbuilder** (MEV): https://github.com/flashbots/rbuilder
