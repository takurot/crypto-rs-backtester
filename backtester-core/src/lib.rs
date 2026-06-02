pub mod account;
pub mod engine;
pub mod event;
pub mod event_queue;
pub mod exchange_simulator;
pub mod fixtures;
pub mod io;
pub mod l3_source;
pub mod latency_model;
#[cfg(feature = "numa")]
pub mod numa;
pub mod orderbook_l2;
pub mod orderbook_l3;
pub mod queue_model;
pub mod risk_guard;
pub mod rng;
pub mod stats;
pub mod sweep;
pub mod tick_source;
pub mod types;
pub mod utils;

pub use account::{Account, Position};
pub use engine::{Context, Engine, EngineConfig, EngineError, EngineMode, MarketView, Strategy};
pub use event::{Event, EventId, EventKind};
pub use event_queue::EventQueue;
pub use l3_source::{ArrowL3Source, L3Source};
pub use latency_model::{ConstantLatency, LatencyModel, LogNormalJitter};
pub use orderbook_l2::{MarketDepth, OrderBookL2};
pub use orderbook_l3::OrderBookL3;
pub use rng::make_small_rng;
pub use stats::{
    BacktestStats, CpuStatsBackend, IncrementalStats, StatsBackend, TradeFill, TradeLog,
    TradeLogMode, calculate_stats, calculate_stats_with_backend,
};
pub use sweep::{SweepResult, run_parameter_sweep};
pub use tick_source::{ArrowTickSource, TickSource, TickSourceError};
pub use types::{
    FixedPoint, FundingEvent, L2Update, L3Update, Order, OrderReport, OrderState, OrderType, Side,
    Tick, TsExchangeNs, TsLocalNs, TsSimNs,
};
pub mod tuner;
