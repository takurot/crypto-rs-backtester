use crate::orderbook_l2::MarketDepth;
use crate::types::{Order, Side, Tick};

/// Queue model for simulating passive fill probability / queue position.
///
/// Phase 1 scope: a deterministic conservative model driven by market trade ticks.
pub trait QueueModel {
    type State: Clone + Copy + core::fmt::Debug + PartialEq + Eq;

    fn register_order(&mut self, order: &Order, book: &dyn MarketDepth) -> Self::State;

    fn check_fill(
        &mut self,
        order: &Order,
        remaining_qty: i64,
        trade: &Tick,
        state: &mut Self::State,
    ) -> i64;
}

/// Queue model that never fills (useful for tests that only care about state transitions).
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopQueue;

impl QueueModel for NoopQueue {
    type State = ();

    fn register_order(&mut self, _order: &Order, _book: &dyn MarketDepth) -> Self::State {
        // unit
    }

    fn check_fill(
        &mut self,
        _order: &Order,
        _remaining_qty: i64,
        _trade: &Tick,
        _state: &mut Self::State,
    ) -> i64 {
        0
    }
}

/// Conservative queue model: treat the user as last in queue at their price level.
///
/// Implementation notes (Phase 1, L2-only):
/// - We snapshot the visible level quantity at order entry as "qty ahead".
/// - On each market trade at that price against the order side, we deplete `qty_ahead` first.
/// - Only the remaining trade volume can fill the user order.
#[derive(Debug, Default, Clone, Copy)]
pub struct ConservativeQueue;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConservativeQueueState {
    pub qty_ahead: i64,
}

impl QueueModel for ConservativeQueue {
    type State = ConservativeQueueState;

    fn register_order(&mut self, order: &Order, book: &dyn MarketDepth) -> Self::State {
        Self::State {
            qty_ahead: book.level_qty(order.side, order.price),
        }
    }

    fn check_fill(
        &mut self,
        order: &Order,
        remaining_qty: i64,
        trade: &Tick,
        state: &mut Self::State,
    ) -> i64 {
        if remaining_qty <= 0 {
            return 0;
        }
        if trade.symbol_id != order.symbol_id {
            return 0;
        }
        if trade.price != order.price {
            return 0;
        }
        if trade.qty <= 0 {
            return 0;
        }

        let is_against = matches!(
            (order.side, trade.side),
            (Side::Buy, Side::Sell) | (Side::Sell, Side::Buy)
        );
        if !is_against {
            return 0;
        }

        let mut available = trade.qty;
        if state.qty_ahead > 0 {
            let d = state.qty_ahead.min(available);
            state.qty_ahead -= d;
            available -= d;
        }

        available.min(remaining_qty)
    }
}

/// Volume-clock queue model (Phase 3.2.1):
/// - Snapshot visible quantity at entry as "queue position" in base units.
/// - Track cumulative aggressive trade volume at the order price against the order side.
/// - Only volume *past* `queue_pos` can fill the user order.
#[derive(Debug, Default, Clone, Copy)]
pub struct VolumeClockQueue;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VolumeClockQueueState {
    pub queue_pos: i64,
    pub cum_volume: i64,
    pub claimed: i64,
}

impl QueueModel for VolumeClockQueue {
    type State = VolumeClockQueueState;

    fn register_order(&mut self, order: &Order, book: &dyn MarketDepth) -> Self::State {
        Self::State {
            queue_pos: book.level_qty(order.side, order.price),
            cum_volume: 0,
            claimed: 0,
        }
    }

    fn check_fill(
        &mut self,
        order: &Order,
        remaining_qty: i64,
        trade: &Tick,
        state: &mut Self::State,
    ) -> i64 {
        if remaining_qty <= 0 {
            return 0;
        }
        if trade.symbol_id != order.symbol_id {
            return 0;
        }
        if trade.price != order.price {
            return 0;
        }
        if trade.qty <= 0 {
            return 0;
        }

        let is_against = matches!(
            (order.side, trade.side),
            (Side::Buy, Side::Sell) | (Side::Sell, Side::Buy)
        );
        if !is_against {
            return 0;
        }

        state.cum_volume = state.cum_volume.saturating_add(trade.qty);

        // Only volume strictly past the queue ahead can fill the user.
        let past = state.cum_volume.saturating_sub(state.queue_pos);
        if past <= state.claimed {
            return 0;
        }

        let available = past - state.claimed;
        let fill = available.min(remaining_qty).max(0);
        state.claimed = state.claimed.saturating_add(fill);
        fill
    }
}

/// L3-exact queue model: uses per-order queue position from an L3 book.
///
/// `register_order` queries `book.qty_ahead(order.order_id)` so the initial
/// position is exact (not conservative). Fill depletion is identical to
/// `ConservativeQueue`.
#[derive(Debug, Default, Clone, Copy)]
pub struct L3ExactQueue;

impl QueueModel for L3ExactQueue {
    type State = ConservativeQueueState;

    fn register_order(&mut self, order: &Order, book: &dyn MarketDepth) -> Self::State {
        Self::State {
            qty_ahead: book.qty_ahead(order.side, order.price, order.order_id),
        }
    }

    fn check_fill(
        &mut self,
        order: &Order,
        remaining_qty: i64,
        trade: &Tick,
        state: &mut Self::State,
    ) -> i64 {
        if remaining_qty <= 0 {
            return 0;
        }
        if trade.symbol_id != order.symbol_id {
            return 0;
        }
        if trade.price != order.price {
            return 0;
        }
        if trade.qty <= 0 {
            return 0;
        }

        let is_against = matches!(
            (order.side, trade.side),
            (Side::Buy, Side::Sell) | (Side::Sell, Side::Buy)
        );
        if !is_against {
            return 0;
        }

        let mut available = trade.qty;
        if state.qty_ahead > 0 {
            let d = state.qty_ahead.min(available);
            state.qty_ahead -= d;
            available -= d;
        }

        available.min(remaining_qty)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::fixtures;
    use crate::orderbook_l2::OrderBookL2;
    use crate::types::{Order, OrderType, Side, Tick};

    #[test]
    fn test_queue_volume_clock_fills_when_cum_volume_exceeds_queue_pos() {
        // Visible queue ahead at entry = 10.
        let mut book = OrderBookL2::new();
        book.apply_l2(&fixtures::l2_update(1_000, 0, 100, 10, Side::Buy));

        let order = Order {
            order_id: 1,
            ts_submit: 1_000,
            seq: 0,
            symbol_id: fixtures::SYMBOL_ID_BTC_USDT,
            side: Side::Buy,
            order_type: OrderType::Limit,
            price: 100,
            qty: 3,
        };

        let mut qm = VolumeClockQueue;
        let mut state = qm.register_order(&order, &book);

        // cum = 10 => not filled
        let t1 = Tick {
            ts_exchange: 2_000,
            ts_local: 2_000,
            seq: 1,
            symbol_id: fixtures::SYMBOL_ID_BTC_USDT,
            price: 100,
            qty: 10,
            side: Side::Sell,
            flags: 0x01,
        };
        assert_eq!(qm.check_fill(&order, 3, &t1, &mut state), 0);

        // cum = 11 => fill starts (exceeds queue_pos)
        let t2 = Tick {
            ts_exchange: 3_000,
            ts_local: 3_000,
            seq: 2,
            symbol_id: fixtures::SYMBOL_ID_BTC_USDT,
            price: 100,
            qty: 1,
            side: Side::Sell,
            flags: 0x01,
        };
        assert_eq!(qm.check_fill(&order, 3, &t2, &mut state), 1);

        // More volume past the queue fills the rest.
        let t3 = Tick {
            ts_exchange: 4_000,
            ts_local: 4_000,
            seq: 3,
            symbol_id: fixtures::SYMBOL_ID_BTC_USDT,
            price: 100,
            qty: 5,
            side: Side::Sell,
            flags: 0x01,
        };
        assert_eq!(qm.check_fill(&order, 2, &t3, &mut state), 2);
    }

    #[test]
    fn test_l3_exact_fills_earlier_than_conservative_when_orders_cancelled() {
        // L3 book: 5 units ahead (some prior orders were cancelled, L3 knows)
        // L2 book: 10 units ahead (stale — doesn't reflect cancellations)
        use crate::orderbook_l3::{L3_ADD, L3_DELETE, OrderBookL3};
        use crate::types::L3Update;

        let mut book_l3 = OrderBookL3::new();
        // Two orders originally in queue; order 10 gets cancelled before user submits
        book_l3.apply_l3(&L3Update {
            ts_exchange: 0,
            symbol_id: fixtures::SYMBOL_ID_BTC_USDT,
            order_id: 10,
            price: 100,
            qty: 5,
            side: Side::Buy,
            action: L3_ADD,
        });
        book_l3.apply_l3(&L3Update {
            ts_exchange: 0,
            symbol_id: fixtures::SYMBOL_ID_BTC_USDT,
            order_id: 20,
            price: 100,
            qty: 5,
            side: Side::Buy,
            action: L3_ADD,
        });
        // order 10 is cancelled
        book_l3.apply_l3(&L3Update {
            ts_exchange: 0,
            symbol_id: fixtures::SYMBOL_ID_BTC_USDT,
            order_id: 10,
            price: 100,
            qty: 0,
            side: Side::Buy,
            action: L3_DELETE,
        });
        // L3 level: only order 20 (qty=5) remains
        assert_eq!(book_l3.level_qty(Side::Buy, 100), 5);

        let order = Order {
            order_id: 99, // user's order (not in L3 book)
            ts_submit: 1_000,
            seq: 0,
            symbol_id: fixtures::SYMBOL_ID_BTC_USDT,
            side: Side::Buy,
            order_type: OrderType::Limit,
            price: 100,
            qty: 3,
        };

        // L3Exact: qty_ahead = 5 (L3 book is accurate)
        let mut qm_l3 = L3ExactQueue;
        let mut state_l3 = qm_l3.register_order(&order, &book_l3);
        assert_eq!(state_l3.qty_ahead, 5);

        // L2 book still shows stale 10 units
        let mut book_l2 = OrderBookL2::new();
        book_l2.apply_l2(&fixtures::l2_update(1_000, 0, 100, 10, Side::Buy));
        let mut qm_c = ConservativeQueue;
        let mut state_c = qm_c.register_order(&order, &book_l2);
        assert_eq!(state_c.qty_ahead, 10);

        // Trade of 6 units arrives
        let trade = Tick {
            ts_exchange: 2_000,
            ts_local: 2_000,
            seq: 1,
            symbol_id: fixtures::SYMBOL_ID_BTC_USDT,
            price: 100,
            qty: 6,
            side: Side::Sell,
            flags: 0x01,
        };

        // L3Exact: qty_ahead=5, trade=6 → 1 unit past queue → fill=1
        let fill_l3 = qm_l3.check_fill(&order, 3, &trade, &mut state_l3);
        // Conservative: qty_ahead=10, trade=6 → 0 units past queue → no fill
        let fill_c = qm_c.check_fill(&order, 3, &trade, &mut state_c);

        assert!(
            fill_l3 > fill_c,
            "L3Exact fill ({fill_l3}) must be > Conservative fill ({fill_c})"
        );
        assert_eq!(fill_l3, 1);
        assert_eq!(fill_c, 0);
    }
}
