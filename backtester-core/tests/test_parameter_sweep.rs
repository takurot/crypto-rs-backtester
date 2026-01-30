use backtester_core::engine::{Context, EngineConfig, EngineMode, Strategy};
use backtester_core::latency_model::ConstantLatency;
use backtester_core::queue_model::ConservativeQueue;
use backtester_core::sweep::run_parameter_sweep;
use backtester_core::types::{Order, OrderReport, OrderType, Side, Tick};
use backtester_core::{EventKind, TradeLogMode};

#[derive(Debug, Clone)]
struct PriceStrategy {
    price: i64,
    submitted: bool,
}

impl PriceStrategy {
    fn new(price: i64) -> Self {
        Self {
            price,
            submitted: false,
        }
    }
}

impl Strategy for PriceStrategy {
    fn on_tick(&mut self, tick: &Tick, ctx: &mut Context<'_>) {
        if self.submitted {
            return;
        }
        self.submitted = true;
        ctx.submit_order(Order {
            order_id: 0,
            ts_submit: ctx.ts_local(),
            seq: 0,
            symbol_id: tick.symbol_id,
            side: Side::Buy,
            order_type: OrderType::Limit,
            price: self.price,
            qty: 1_00000000,
        });
    }

    fn on_order_update(&mut self, _report: &OrderReport, _ctx: &mut Context<'_>) {}
}

#[test]
fn test_parameter_sweep_parallel_ordered() {
    let tick1_truth = Tick {
        ts_exchange: 1_000,
        ts_local: 1_000,
        seq: 0,
        symbol_id: 1,
        price: 100_00000000,
        qty: 1_00000000,
        side: Side::Sell,
        flags: 0x01,
    };
    let tick1_delivery = Tick {
        ts_exchange: tick1_truth.ts_exchange,
        ts_local: tick1_truth.ts_exchange,
        seq: 0,
        symbol_id: tick1_truth.symbol_id,
        price: tick1_truth.price,
        qty: tick1_truth.qty,
        side: tick1_truth.side,
        flags: tick1_truth.flags,
    };

    let tick2_truth = Tick {
        ts_exchange: 2_000,
        ts_local: 2_000,
        seq: 1,
        symbol_id: 1,
        price: 100_00000000,
        qty: 1_00000000,
        side: Side::Sell,
        flags: 0x01,
    };
    let tick2_delivery = Tick {
        ts_exchange: tick2_truth.ts_exchange,
        ts_local: tick2_truth.ts_exchange,
        seq: 1,
        symbol_id: tick2_truth.symbol_id,
        price: tick2_truth.price,
        qty: tick2_truth.qty,
        side: tick2_truth.side,
        flags: tick2_truth.flags,
    };

    let events = vec![
        (tick1_truth.ts_exchange, EventKind::Tick(tick1_truth)),
        (
            tick1_delivery.ts_local,
            EventKind::TickDelivery(tick1_delivery),
        ),
        (tick2_truth.ts_exchange, EventKind::Tick(tick2_truth)),
        (
            tick2_delivery.ts_local,
            EventKind::TickDelivery(tick2_delivery),
        ),
    ];

    let base_config = EngineConfig {
        feed_latency_ns: 0,
        order_update_latency_ns: 0,
        mode: EngineMode::Tick,
        max_batch_ns: 0,
        auto_tune: false,
        seed: 7,
        trade_log_mode: TradeLogMode::SummaryOnly,
    };
    let latency = ConstantLatency {
        feed_latency_ns: 0,
        order_latency_ns: 0,
    };

    let strategies = vec![
        PriceStrategy::new(100_00000000),
        PriceStrategy::new(101_00000000),
    ];

    let results = run_parameter_sweep(ConservativeQueue, latency, base_config, strategies, &events);

    assert_eq!(results.len(), 2);
    assert_eq!(results[0].stats.total_trades, 1);
    assert_eq!(results[1].stats.total_trades, 0);
}
