//! Tiny deterministic fixtures for unit/integration tests and benches.
//!
//! Keep fixtures in-memory and small; avoid external datasets.

use crate::event::{Event, EventId, EventKind};
use crate::types::{L2Update, Order, OrderType, Side, Tick};

pub const SYMBOL_ID_BTC_USDT: u32 = 1;

pub fn tick_trade(ts_exchange: i64, ts_local: i64, seq: u64) -> Tick {
    Tick {
        ts_exchange,
        ts_local,
        seq,
        symbol_id: SYMBOL_ID_BTC_USDT,
        price: 100_00000000,
        qty: 1_00000000,
        side: Side::Buy,
        flags: 0x01, // TRADE
    }
}

pub fn l2_update(ts_exchange: i64, seq: u64, price: i64, qty: i64, side: Side) -> L2Update {
    L2Update {
        ts_exchange,
        seq,
        symbol_id: SYMBOL_ID_BTC_USDT,
        price,
        qty,
        side,
    }
}

pub fn order_limit(
    order_id: u64,
    ts_submit: i64,
    seq: u64,
    price: i64,
    qty: i64,
    side: Side,
) -> Order {
    Order {
        order_id,
        ts_submit,
        seq,
        symbol_id: SYMBOL_ID_BTC_USDT,
        side,
        order_type: OrderType::Limit,
        price,
        qty,
    }
}

pub fn event_tick(ts_sim: i64, seq: u64, tick: Tick) -> Event {
    Event {
        id: EventId { ts_sim, seq },
        kind: EventKind::Tick(tick),
    }
}

pub fn event_l2(ts_sim: i64, seq: u64, update: L2Update) -> Event {
    Event {
        id: EventId { ts_sim, seq },
        kind: EventKind::L2Update(update),
    }
}

pub fn event_order(ts_sim: i64, seq: u64, order: Order) -> Event {
    Event {
        id: EventId { ts_sim, seq },
        kind: EventKind::Order(order),
    }
}

pub fn tiny_deterministic_tick_stream() -> Vec<Tick> {
    vec![
        Tick {
            ts_exchange: 1_000,
            ts_local: 1_000,
            seq: 0,
            symbol_id: SYMBOL_ID_BTC_USDT,
            price: 100_00000000,
            qty: 1_00000000,
            side: Side::Buy,
            flags: 0x01,
        },
        Tick {
            ts_exchange: 2_000,
            ts_local: 2_000,
            seq: 1,
            symbol_id: SYMBOL_ID_BTC_USDT,
            price: 101_00000000,
            qty: 1_00000000,
            side: Side::Sell,
            flags: 0x01,
        },
        Tick {
            ts_exchange: 3_000,
            ts_local: 3_000,
            seq: 2,
            symbol_id: SYMBOL_ID_BTC_USDT,
            price: 99_00000000,
            qty: 1_00000000,
            side: Side::Buy,
            flags: 0x01,
        },
        Tick {
            ts_exchange: 4_000,
            ts_local: 4_000,
            seq: 3,
            symbol_id: SYMBOL_ID_BTC_USDT,
            price: 100_00000000,
            qty: 1_00000000,
            side: Side::Sell,
            flags: 0x01,
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fixtures_smoke_builders() {
        let t = tick_trade(1_000, 1_234, 7);
        assert_eq!(t.ts_exchange, 1_000);
        assert_eq!(t.ts_local, 1_234);
        assert_eq!(t.seq, 7);
        assert_eq!(t.symbol_id, SYMBOL_ID_BTC_USDT);
        assert_eq!(t.side, Side::Buy);

        let u = l2_update(2_000, 9, 123, 456, Side::Sell);
        assert_eq!(u.ts_exchange, 2_000);
        assert_eq!(u.seq, 9);
        assert_eq!(u.symbol_id, SYMBOL_ID_BTC_USDT);
        assert_eq!(u.price, 123);
        assert_eq!(u.qty, 456);
        assert_eq!(u.side, Side::Sell);

        let o = order_limit(42, 3_000, 1, 999, 888, Side::Buy);
        assert_eq!(o.order_id, 42);
        assert_eq!(o.ts_submit, 3_000);
        assert_eq!(o.seq, 1);
        assert_eq!(o.symbol_id, SYMBOL_ID_BTC_USDT);
        assert_eq!(o.price, 999);
        assert_eq!(o.qty, 888);
        assert_eq!(o.side, Side::Buy);

        let e = event_tick(4_000, 123, t);
        assert_eq!(e.id.ts_sim, 4_000);
        assert_eq!(e.id.seq, 123);
        match e.kind {
            EventKind::Tick(_) => {}
            _ => panic!("expected Tick event"),
        }

        let stream = tiny_deterministic_tick_stream();
        assert_eq!(stream.len(), 4);
        assert_eq!(stream[0].seq, 0);
        assert_eq!(stream[3].seq, 3);
    }
}
