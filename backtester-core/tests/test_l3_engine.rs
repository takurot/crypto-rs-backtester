use backtester_core::event::EventKind;
use backtester_core::latency_model::ConstantLatency;
use backtester_core::queue_model::L3ExactQueue;
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

fn l3_add(order_id: u64, qty: i64) -> L3Update {
    L3Update {
        ts_exchange: 500,
        symbol_id: SYMBOL_ID,
        order_id,
        price: 100,
        qty,
        side: Side::Buy,
        action: backtester_core::orderbook_l3::L3_ADD,
    }
}

fn tick(ts: i64, side: Side, qty: i64) -> Tick {
    Tick {
        ts_exchange: ts,
        ts_local: ts,
        seq: 0,
        symbol_id: SYMBOL_ID,
        price: 100,
        qty,
        side,
        flags: 0x01,
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
