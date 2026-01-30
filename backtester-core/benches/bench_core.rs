use std::collections::BTreeMap;
use std::time::Duration;

use backtester_core::engine::{Context, Engine, EngineConfig, EngineMode, Strategy};
use backtester_core::latency_model::ConstantLatency;
use backtester_core::queue_model::ConservativeQueue;
use backtester_core::stats::{
    sharpe_ratio_from_pnl_series, sharpe_ratio_from_pnl_series_parallel,
    sharpe_ratio_from_pnl_series_simd, sortino_ratio_from_pnl_series,
    sortino_ratio_from_pnl_series_parallel, sortino_ratio_from_pnl_series_simd,
};
use backtester_core::tick_source::TickSource;
use backtester_core::types::{Order, OrderType, Tick};
use backtester_core::{EventKind, EventQueue, OrderBookL2, Side, TradeLogMode, fixtures};
use criterion::{Criterion, black_box, criterion_group, criterion_main};

fn bench_event_loop_1m_ticks(c: &mut Criterion) {
    c.bench_function("bench_event_loop_1m_ticks", |b| {
        b.iter(|| {
            let mut q = EventQueue::new();
            for i in 0..1_000_000u64 {
                let ts = i as i64;
                let tick = fixtures::tick_trade(ts, ts, i);
                q.push(fixtures::event_tick(ts, i, tick));
            }

            let mut acc: i64 = 0;
            while let Some(ev) = q.pop() {
                if let EventKind::Tick(t) = ev.kind {
                    acc = acc.wrapping_add(t.price);
                }
            }
            black_box(acc)
        })
    });
}

fn bench_orderbook_apply_l2_1m_updates(c: &mut Criterion) {
    c.bench_function("bench_orderbook_apply_l2_1m_updates", |b| {
        b.iter(|| {
            let mut ob = OrderBookL2::new();
            for i in 0..1_000_000u64 {
                let price = 100_000 + (i as i64 % 10_000);
                let qty = 1_000 + (i as i64 % 1_000);
                let side = if i % 2 == 0 { Side::Buy } else { Side::Sell };
                let u = fixtures::l2_update(i as i64, i, price, qty, side);
                ob.apply_l2(&u);
            }
            black_box(ob.best_bid());
            black_box(ob.best_ask());
        })
    });
}

fn bench_stats_simd_vs_scalar_vs_parallel(c: &mut Criterion) {
    let n = 1_000_000usize;
    let mut pnl = Vec::with_capacity(n);
    for i in 0..n {
        let v = (i as i64 % 200) - 100;
        pnl.push(v * 1_000_000);
    }

    c.bench_function("bench_stats_sharpe_scalar", |b| {
        b.iter(|| black_box(sharpe_ratio_from_pnl_series(&pnl)))
    });
    c.bench_function("bench_stats_sharpe_simd", |b| {
        b.iter(|| black_box(sharpe_ratio_from_pnl_series_simd(&pnl)))
    });
    c.bench_function("bench_stats_sortino_scalar", |b| {
        b.iter(|| black_box(sortino_ratio_from_pnl_series(&pnl)))
    });
    c.bench_function("bench_stats_sortino_simd", |b| {
        b.iter(|| black_box(sortino_ratio_from_pnl_series_simd(&pnl)))
    });
    c.bench_function("bench_stats_sharpe_parallel", |b| {
        b.iter(|| black_box(sharpe_ratio_from_pnl_series_parallel(&pnl)))
    });
    c.bench_function("bench_stats_sortino_parallel", |b| {
        b.iter(|| black_box(sortino_ratio_from_pnl_series_parallel(&pnl)))
    });
}

/// Simple generated streaming source that produces `n` ticks for one symbol
/// without pre-allocating a vector. `ts_local` is 0 to let Engine apply feed latency.
#[derive(Debug, Clone)]
struct GenTickSource {
    symbol_id: u32,
    n: usize,
    i: usize,
    base_ts: i64,
    dt: i64,
    price_base: i64,
    price_span: i64,
    qty: i64,
    next_tick: Option<Tick>,
}

impl GenTickSource {
    fn new(symbol_id: u32, n: usize, base_ts: i64, dt: i64) -> Self {
        let mut s = Self {
            symbol_id,
            n,
            i: 0,
            base_ts,
            dt,
            price_base: 100_00000000,
            price_span: 4, // cycle a few prices to allow fills soon
            qty: 1_00000000,
            next_tick: None,
        };
        s.next_tick = s.compute_next(0);
        s
    }

    fn compute_next(&self, i: usize) -> Option<Tick> {
        if i >= self.n {
            return None;
        }
        let ts_ex = self.base_ts + (i as i64) * self.dt;
        let price = self.price_base + ((i as i64) % self.price_span);
        let side = if i.is_multiple_of(2) {
            Side::Buy
        } else {
            Side::Sell
        };
        Some(Tick {
            ts_exchange: ts_ex,
            ts_local: 0, // let Engine apply feed latency
            seq: i as u64,
            symbol_id: self.symbol_id,
            price,
            qty: self.qty,
            side,
            flags: 0x01,
        })
    }
}

impl TickSource for GenTickSource {
    fn next(&mut self) -> Option<Tick> {
        let current = self.next_tick;
        self.i += 1;
        self.next_tick = self.compute_next(self.i);
        current
    }

    fn peek(&mut self) -> Option<&Tick> {
        self.next_tick.as_ref()
    }

    fn symbol_id(&self) -> u32 {
        self.symbol_id
    }
}

#[derive(Default)]
struct LoadStrategy {
    every_n: usize,
    delivered_counts: BTreeMap<u32, usize>,
}

impl LoadStrategy {
    fn new(every_n: usize) -> Self {
        Self {
            every_n,
            delivered_counts: BTreeMap::new(),
        }
    }
}

impl Strategy for LoadStrategy {
    fn on_tick(&mut self, tick: &Tick, ctx: &mut Context<'_>) {
        let n = self
            .delivered_counts
            .entry(tick.symbol_id)
            .and_modify(|c| *c = c.saturating_add(1))
            .or_insert(1);

        if (*n).is_multiple_of(self.every_n) {
            // Submit an order against the trade with the same price; likely to fill soon.
            let side = match tick.side {
                Side::Buy => Side::Sell,
                Side::Sell => Side::Buy,
                Side::None => Side::Buy,
            };
            ctx.submit_order(Order {
                order_id: 0,
                ts_submit: ctx.ts_local(),
                seq: 0,
                symbol_id: tick.symbol_id,
                side,
                order_type: OrderType::Limit,
                price: tick.price,
                qty: tick.qty, // 1 lot
            });
        }
    }

    fn on_order_update(
        &mut self,
        _report: &backtester_core::types::OrderReport,
        _ctx: &mut Context<'_>,
    ) {
        // No-op: this strategy only loads submission path.
    }
}

#[derive(Clone, Copy, Debug)]
struct BenchConfig {
    nsymbols: u32,
    ticks_per_symbol: usize,
    dt_ns: i64,
    symbol_stagger_ns: i64,
    feed_latency_ns: i64,
    order_update_latency_ns: i64,
    order_latency_ns: i64,
    submit_every_n: usize,
}

impl BenchConfig {
    fn from_env() -> Self {
        fn getenv<T: std::str::FromStr>(k: &str, default: T) -> T {
            std::env::var(k)
                .ok()
                .and_then(|v| v.parse::<T>().ok())
                .unwrap_or(default)
        }
        Self {
            nsymbols: getenv("BACKTEST_BENCH_NSYMBOLS", 4u32),
            ticks_per_symbol: getenv("BACKTEST_BENCH_TICKS_PER_SYMBOL", 250_000usize),
            dt_ns: getenv("BACKTEST_BENCH_DT_NS", 1_000i64),
            symbol_stagger_ns: getenv("BACKTEST_BENCH_SYMBOL_STAGGER_NS", 10_000i64),
            feed_latency_ns: getenv("BACKTEST_BENCH_FEED_LATENCY_NS", 2_000_000i64),
            order_update_latency_ns: getenv("BACKTEST_BENCH_ORDER_UPDATE_LATENCY_NS", 1_000_000i64),
            order_latency_ns: getenv("BACKTEST_BENCH_ORDER_LATENCY_NS", 500_000i64),
            submit_every_n: getenv("BACKTEST_BENCH_SUBMIT_EVERY_N", 256usize),
        }
    }
}

fn bench_engine_e2e_multisymbol_tick(c: &mut Criterion) {
    // Practical default; overridable via env vars (BACKTEST_BENCH_*).
    let cfg = BenchConfig::from_env();

    c.bench_function("bench_engine_e2e_tick_4x250k", |b| {
        b.iter(|| {
            let config = EngineConfig {
                feed_latency_ns: cfg.feed_latency_ns,
                order_update_latency_ns: cfg.order_update_latency_ns,
                mode: EngineMode::Tick,
                max_batch_ns: 0,
                auto_tune: false,
                seed: 42,
                trade_log_mode: TradeLogMode::All,
            };
            let latency_model = ConstantLatency {
                feed_latency_ns: cfg.feed_latency_ns,
                order_latency_ns: cfg.order_latency_ns,
            };
            let strategy = LoadStrategy::new(cfg.submit_every_n);
            let mut eng: Engine<ConservativeQueue, LoadStrategy, ConstantLatency> =
                Engine::new(ConservativeQueue, strategy, config, latency_model);

            for s in 0..cfg.nsymbols {
                // Stagger base_ts per symbol to avoid identical timestamps across sources
                let base_ts = (s as i64) * cfg.symbol_stagger_ns;
                eng.add_tick_source(Box::new(GenTickSource::new(
                    s + 1,
                    cfg.ticks_per_symbol,
                    base_ts,
                    cfg.dt_ns,
                )));
            }

            eng.run();
            black_box(eng.stats().total_trades)
        })
    });
}

fn bench_engine_e2e_multisymbol_batch(c: &mut Criterion) {
    // Same data volume as tick-mode bench but Batch strategy wakeups (default: 10 ms window).
    let cfg = BenchConfig::from_env();
    let max_batch_ns: i64 = std::env::var("BACKTEST_BENCH_MAX_BATCH_NS")
        .ok()
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(10_000_000);

    c.bench_function("bench_engine_e2e_batch_4x250k", |b| {
        b.iter(|| {
            let config = EngineConfig {
                feed_latency_ns: cfg.feed_latency_ns,
                order_update_latency_ns: cfg.order_update_latency_ns,
                mode: EngineMode::Batch,
                max_batch_ns,
                auto_tune: false,
                seed: 42,
                trade_log_mode: TradeLogMode::All,
            };
            let latency_model = ConstantLatency {
                feed_latency_ns: cfg.feed_latency_ns,
                order_latency_ns: cfg.order_latency_ns,
            };
            let strategy = LoadStrategy::new(cfg.submit_every_n);
            let mut eng: Engine<ConservativeQueue, LoadStrategy, ConstantLatency> =
                Engine::new(ConservativeQueue, strategy, config, latency_model);

            for s in 0..cfg.nsymbols {
                let base_ts = (s as i64) * cfg.symbol_stagger_ns;
                eng.add_tick_source(Box::new(GenTickSource::new(
                    s + 1,
                    cfg.ticks_per_symbol,
                    base_ts,
                    cfg.dt_ns,
                )));
            }

            eng.run();
            black_box(eng.stats().total_trades)
        })
    });
}

criterion_group!(
    name = benches;
    config = Criterion::default()
        .sample_size(10)
        .measurement_time(Duration::from_secs(3))
        .warm_up_time(Duration::from_secs(3));
    targets = bench_event_loop_1m_ticks,
              bench_orderbook_apply_l2_1m_updates,
              bench_stats_simd_vs_scalar_vs_parallel,
              bench_engine_e2e_multisymbol_tick,
              bench_engine_e2e_multisymbol_batch
);
criterion_main!(benches);
