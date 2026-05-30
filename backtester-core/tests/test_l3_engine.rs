use backtester_core::event::EventKind;
use backtester_core::l3_source::L3Source;
use backtester_core::latency_model::ConstantLatency;
use backtester_core::queue_model::L3ExactQueue;
use backtester_core::tick_source::{TickSource, TickSourceError};
use backtester_core::{
    Context, Engine, EngineConfig, L2Update, L3Update, Order, OrderType, Side, Strategy, Tick,
};

const SYMBOL_ID: u32 = 1;

#[derive(Default)]
struct SubmitBuyOnce {
    submitted: bool,
}

impl Strategy for SubmitBuyOnce {
    type Error = &'static str;

    fn on_tick(&mut self, tick: &Tick, ctx: &mut Context<'_>) -> Result<(), Self::Error> {
        if !self.submitted {
            self.submitted = true;
            ctx.submit_order(Order {
                order_id: 0,
                ts_submit: 0,
                seq: 0,
                symbol_id: tick.symbol_id,
                side: Side::Buy,
                order_type: OrderType::Limit,
                price: tick.price,
                qty: 3,
            });
        }
        Ok(())
    }

    fn on_order_update(
        &mut self,
        _report: &backtester_core::OrderReport,
        _ctx: &mut Context<'_>,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

fn l3_update(seq: u64, order_id: u64, qty: i64, action: u8) -> L3Update {
    L3Update {
        ts_exchange: 500,
        seq,
        symbol_id: SYMBOL_ID,
        order_id,
        price: 100,
        qty,
        side: Side::Buy,
        action,
    }
}

fn l3_add(order_id: u64, qty: i64) -> L3Update {
    l3_update(0, order_id, qty, backtester_core::orderbook_l3::L3_ADD)
}

fn tick_with_seq(ts: i64, seq: u64, side: Side, qty: i64) -> Tick {
    Tick {
        ts_exchange: ts,
        ts_local: ts,
        seq,
        symbol_id: SYMBOL_ID,
        price: 100,
        qty,
        side,
        flags: 0x01,
    }
}

fn tick(ts: i64, side: Side, qty: i64) -> Tick {
    tick_with_seq(ts, 0, side, qty)
}

struct VecTickSource {
    ticks: Vec<Tick>,
    idx: usize,
}

impl VecTickSource {
    fn new(ticks: Vec<Tick>) -> Self {
        Self { ticks, idx: 0 }
    }
}

impl TickSource for VecTickSource {
    fn peek(&mut self) -> Result<Option<&Tick>, TickSourceError> {
        Ok(self.ticks.get(self.idx))
    }

    fn next(&mut self) -> Result<Option<Tick>, TickSourceError> {
        let tick = self.ticks.get(self.idx).copied();
        if tick.is_some() {
            self.idx += 1;
        }
        Ok(tick)
    }

    fn symbol_id(&self) -> u32 {
        SYMBOL_ID
    }
}

struct VecL3Source {
    updates: Vec<L3Update>,
    idx: usize,
}

impl VecL3Source {
    fn new(updates: Vec<L3Update>) -> Self {
        Self { updates, idx: 0 }
    }
}

impl L3Source for VecL3Source {
    fn peek(&mut self) -> Result<Option<&L3Update>, TickSourceError> {
        Ok(self.updates.get(self.idx))
    }

    fn next(&mut self) -> Result<Option<L3Update>, TickSourceError> {
        let update = self.updates.get(self.idx).copied();
        if update.is_some() {
            self.idx += 1;
        }
        Ok(update)
    }

    fn symbol_id(&self) -> u32 {
        SYMBOL_ID
    }
}

#[test]
fn engine_routes_l3_updates_to_l3_exact_queue_model() {
    let mut engine = Engine::new(
        L3ExactQueue,
        SubmitBuyOnce::default(),
        EngineConfig::default(),
        ConstantLatency {
            feed_latency_ns: 0,
            order_latency_ns: 0,
        },
    );

    engine.push_event(500, EventKind::L3Update(l3_add(20, 5)));
    engine.push_event(1_000, EventKind::TickDelivery(tick(1_000, Side::Buy, 1)));
    engine.push_event(2_000, EventKind::Tick(tick(2_000, Side::Sell, 6)));

    engine.run().expect("engine run");

    let fills = engine.trade_log().fills_vec();
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].qty, 1);
    assert!(!fills[0].is_taker);
}

#[test]
fn l3_delete_after_submit_advances_active_queue_state() {
    let mut engine = Engine::new(
        L3ExactQueue,
        SubmitBuyOnce::default(),
        EngineConfig::default(),
        ConstantLatency {
            feed_latency_ns: 0,
            order_latency_ns: 0,
        },
    );

    engine.push_event(500, EventKind::L3Update(l3_add(20, 5)));
    engine.push_event(1_000, EventKind::TickDelivery(tick(1_000, Side::Buy, 1)));
    engine.push_event(
        1_500,
        EventKind::L3Update(l3_update(
            0,
            20,
            0,
            backtester_core::orderbook_l3::L3_DELETE,
        )),
    );
    engine.push_event(2_000, EventKind::Tick(tick(2_000, Side::Sell, 1)));

    engine.run().expect("engine run");

    let fills = engine.trade_log().fills_vec();
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].qty, 1);
}

#[test]
fn l3_and_tick_sources_at_same_timestamp_follow_feed_seq_order() {
    let mut engine = Engine::new(
        L3ExactQueue,
        SubmitBuyOnce::default(),
        EngineConfig::default(),
        ConstantLatency {
            feed_latency_ns: 0,
            order_latency_ns: 0,
        },
    );

    engine.add_l3_source(Box::new(VecL3Source::new(vec![
        l3_update(0, 20, 5, backtester_core::orderbook_l3::L3_ADD),
        L3Update {
            ts_exchange: 2_000,
            seq: 0,
            symbol_id: SYMBOL_ID,
            order_id: 20,
            price: 100,
            qty: 0,
            side: Side::Buy,
            action: backtester_core::orderbook_l3::L3_DELETE,
        },
    ])));
    engine.add_tick_source(Box::new(VecTickSource::new(vec![
        tick_with_seq(1_000, 0, Side::Buy, 1),
        tick_with_seq(2_000, 1, Side::Sell, 1),
    ])));

    engine.run().expect("engine run");

    let fills = engine.trade_log().fills_vec();
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].qty, 1);
}

#[test]
fn engine_rejects_mixed_l2_and_l3_depth_for_same_symbol() {
    let mut engine = Engine::new(
        L3ExactQueue,
        SubmitBuyOnce::default(),
        EngineConfig::default(),
        ConstantLatency {
            feed_latency_ns: 0,
            order_latency_ns: 0,
        },
    );

    engine.push_event(500, EventKind::L3Update(l3_add(20, 5)));
    engine.push_event(
        600,
        EventKind::L2Update(L2Update {
            ts_exchange: 600,
            seq: 0,
            symbol_id: SYMBOL_ID,
            price: 100,
            qty: 10,
            side: Side::Buy,
        }),
    );

    let err = engine.run().expect_err("mixed L2/L3 depth must fail");
    assert!(err.to_string().contains("mixed L2/L3"));
}
