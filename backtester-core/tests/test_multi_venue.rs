use backtester_core::engine::{EngineConfig, EngineMode, Strategy};
use backtester_core::latency_model::ConstantLatency;
use backtester_core::queue_model::ConservativeQueue;
use backtester_core::types::{L2Update, Order, OrderReport, OrderType, Side, Tick};
use backtester_core::{Engine, EventKind};

const SYMBOL_A: u32 = 1; // e.g. binance:BTC/USDT
const SYMBOL_B: u32 = 2; // e.g. bybit:BTC/USDT

#[derive(Debug, Default)]
struct RecordingStrategy {
    seen_symbol_ids: Vec<u32>,
    reports: Vec<OrderReport>,
    submitted: bool,
}

impl Strategy for RecordingStrategy {
    fn on_tick(&mut self, tick: &Tick, ctx: &mut backtester_core::Context<'_>) {
        self.seen_symbol_ids.push(tick.symbol_id);

        // For the L2-isolation test: submit exactly one order on the first tick.
        if self.submitted {
            return;
        }
        self.submitted = true;
        ctx.submit_order(Order {
            order_id: 0,
            ts_submit: ctx.ts_local(),
            seq: 0,
            symbol_id: SYMBOL_A,
            side: Side::Buy,
            order_type: OrderType::Limit,
            price: 100_00000000,
            qty: 1_00000000,
        });
    }

    fn on_order_update(&mut self, report: &OrderReport, _ctx: &mut backtester_core::Context<'_>) {
        self.reports.push(*report);
    }
}

#[test]
fn test_multi_venue_event_ordering_by_ts_sim() {
    let config = EngineConfig {
        feed_latency_ns: 0,
        order_update_latency_ns: 0,
        mode: EngineMode::Tick,
        max_batch_ns: 0,
        seed: 42,
        ..Default::default()
    };
    let latency = ConstantLatency {
        feed_latency_ns: 0,
        order_latency_ns: 0,
    };

    let strategy = RecordingStrategy::default();
    let mut eng: Engine<ConservativeQueue, RecordingStrategy, ConstantLatency> =
        Engine::new(ConservativeQueue, strategy, config, latency);

    // Same ts_sim, pushed in deterministic order: B then A.
    let tick_b = Tick {
        ts_exchange: 1_000,
        ts_local: 1_000,
        seq: 0,
        symbol_id: SYMBOL_B,
        price: 110_00000000,
        qty: 1_00000000,
        side: Side::Buy,
        flags: 0x01,
    };
    let tick_a = Tick {
        ts_exchange: 1_000,
        ts_local: 1_000,
        seq: 0,
        symbol_id: SYMBOL_A,
        price: 100_00000000,
        qty: 1_00000000,
        side: Side::Buy,
        flags: 0x01,
    };
    eng.push_event(1_000, EventKind::TickDelivery(tick_b));
    eng.push_event(1_000, EventKind::TickDelivery(tick_a));

    eng.run();
    assert_eq!(eng.strategy().seen_symbol_ids, vec![SYMBOL_B, SYMBOL_A]);
}

#[test]
fn test_arbitrage_two_venues_smoke() {
    #[derive(Debug, Default)]
    struct ArbStrategy {
        last_a: Option<i64>,
        last_b: Option<i64>,
        submitted: bool,
        reports: Vec<OrderReport>,
    }

    impl Strategy for ArbStrategy {
        fn on_tick(&mut self, tick: &Tick, ctx: &mut backtester_core::Context<'_>) {
            match tick.symbol_id {
                SYMBOL_A => self.last_a = Some(tick.price),
                SYMBOL_B => self.last_b = Some(tick.price),
                _ => {}
            }

            if self.submitted {
                return;
            }
            let (Some(pa), Some(pb)) = (self.last_a, self.last_b) else {
                return;
            };

            // Buy cheap on A, sell rich on B.
            if pa < pb {
                self.submitted = true;
                ctx.submit_order(Order {
                    order_id: 0,
                    ts_submit: ctx.ts_local(),
                    seq: 0,
                    symbol_id: SYMBOL_A,
                    side: Side::Buy,
                    order_type: OrderType::Limit,
                    price: pa,
                    qty: 1_00000000,
                });
                ctx.submit_order(Order {
                    order_id: 0,
                    ts_submit: ctx.ts_local(),
                    seq: 1,
                    symbol_id: SYMBOL_B,
                    side: Side::Sell,
                    order_type: OrderType::Limit,
                    price: pb,
                    qty: 1_00000000,
                });
            }
        }

        fn on_order_update(
            &mut self,
            report: &OrderReport,
            _ctx: &mut backtester_core::Context<'_>,
        ) {
            self.reports.push(*report);
        }
    }

    let config = EngineConfig {
        feed_latency_ns: 0,
        order_update_latency_ns: 0,
        mode: EngineMode::Tick,
        max_batch_ns: 0,
        seed: 42,
        ..Default::default()
    };
    let latency = ConstantLatency {
        feed_latency_ns: 0,
        order_latency_ns: 0,
    };

    let strategy = ArbStrategy::default();
    let mut eng: Engine<ConservativeQueue, ArbStrategy, ConstantLatency> =
        Engine::new(ConservativeQueue, strategy, config, latency);

    // ts=1_000: make B visible first, then A, so the second callback can see both.
    let truth_b = Tick {
        ts_exchange: 1_000,
        ts_local: 1_000,
        seq: 0,
        symbol_id: SYMBOL_B,
        price: 110_00000000,
        qty: 1_00000000,
        side: Side::Buy,
        flags: 0x01,
    };
    let delivery_b = truth_b;
    let truth_a = Tick {
        ts_exchange: 1_000,
        ts_local: 1_000,
        seq: 0,
        symbol_id: SYMBOL_A,
        price: 100_00000000,
        qty: 1_00000000,
        side: Side::Buy,
        flags: 0x01,
    };
    let delivery_a = truth_a;

    eng.push_event(1_000, EventKind::Tick(truth_b));
    eng.push_event(1_000, EventKind::TickDelivery(delivery_b));
    eng.push_event(1_000, EventKind::Tick(truth_a));
    eng.push_event(1_000, EventKind::TickDelivery(delivery_a));

    // ts=2_000: fill both legs with aggressive trades.
    let fill_a = Tick {
        ts_exchange: 2_000,
        ts_local: 2_000,
        seq: 1,
        symbol_id: SYMBOL_A,
        price: 100_00000000,
        qty: 1_00000000,
        side: Side::Sell,
        flags: 0x01,
    };
    let fill_b = Tick {
        ts_exchange: 2_000,
        ts_local: 2_000,
        seq: 1,
        symbol_id: SYMBOL_B,
        price: 110_00000000,
        qty: 1_00000000,
        side: Side::Buy,
        flags: 0x01,
    };
    eng.push_event(2_000, EventKind::Tick(fill_a));
    eng.push_event(2_000, EventKind::Tick(fill_b));

    eng.run();

    // Positions should reflect the filled legs.
    assert_eq!(eng.account().position_qty(SYMBOL_A), 1_00000000);
    assert_eq!(eng.account().position_qty(SYMBOL_B), -1_00000000);

    // Both orders should have produced fill reports.
    assert_eq!(eng.strategy().reports.len(), 2);
    assert!(
        eng.strategy()
            .reports
            .iter()
            .all(|r| r.status == backtester_core::types::OrderState::Filled)
    );
}

#[test]
fn test_multi_venue_l2_updates_do_not_leak_between_symbols() {
    let config = EngineConfig {
        feed_latency_ns: 0,
        order_update_latency_ns: 0,
        mode: EngineMode::Tick,
        max_batch_ns: 0,
        seed: 42,
        ..Default::default()
    };
    let latency = ConstantLatency {
        feed_latency_ns: 0,
        order_latency_ns: 0,
    };

    let strategy = RecordingStrategy::default();
    let mut eng: Engine<ConservativeQueue, RecordingStrategy, ConstantLatency> =
        Engine::new(ConservativeQueue, strategy, config, latency);

    // Update only SYMBOL_B's book at price=100 with a large visible queue.
    let u_b = L2Update {
        ts_exchange: 500,
        seq: 0,
        symbol_id: SYMBOL_B,
        price: 100_00000000,
        qty: 10_00000000,
        side: Side::Buy,
    };
    eng.push_event(500, EventKind::L2Update(u_b));

    // Deliver a tick for SYMBOL_A so the strategy submits a BUY @ 100.
    let t_a = Tick {
        ts_exchange: 1_000,
        ts_local: 1_000,
        seq: 0,
        symbol_id: SYMBOL_A,
        price: 100_00000000,
        qty: 1_00000000,
        side: Side::Buy,
        flags: 0x01,
    };
    eng.push_event(1_000, EventKind::TickDelivery(t_a));

    // Next trade against SYMBOL_A should fill immediately if its book is isolated (qty_ahead=0).
    let fill_a = Tick {
        ts_exchange: 2_000,
        ts_local: 2_000,
        seq: 1,
        symbol_id: SYMBOL_A,
        price: 100_00000000,
        qty: 1_00000000,
        side: Side::Sell,
        flags: 0x01,
    };
    eng.push_event(2_000, EventKind::Tick(fill_a));

    eng.run();

    assert_eq!(eng.account().position_qty(SYMBOL_A), 1_00000000);
    assert_eq!(eng.strategy().reports.len(), 1);
}
