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

### Phase 1: Core Engine (MVP)
- [ ] Rust matching engine with L2 support
- [ ] Conservative queue model
- [ ] Constant latency model
- [ ] Basic event loop
- [ ] Single venue support

### Phase 2: Python Integration
- [ ] PyO3 bindings
- [ ] Polars zero-copy data ingestion
- [ ] Python strategy interface
- [ ] Result export as DataFrame

### Phase 3: Advanced Microstructure
- [ ] Latency jitter (Log-Normal, Poisson)
- [ ] Volume-clock queue model
- [ ] L3 data support
- [ ] Funding rate simulation

### Phase 4: Multi-Venue & Production
- [ ] Global event queue
- [ ] Cross-exchange arbitrage support
- [ ] Kill switch & risk limits
- [ ] Benchmark suite
- [ ] Documentation & examples

### Phase 5: Extensions (Future)
- [ ] MEV simulation hooks (for DeFi research)
- [ ] Interpolated latency from pcap
- [ ] Reg NMS / SIP simulation
- [ ] Live trading adapter (same strategy code)

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
