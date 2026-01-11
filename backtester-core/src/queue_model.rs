use crate::orderbook_l2::OrderBookL2;
use crate::types::{Order, Side, Tick};

/// Queue model for simulating passive fill probability / queue position.
///
/// Phase 1 scope: a deterministic conservative model driven by market trade ticks.
pub trait QueueModel {
    type State: Clone + Copy + core::fmt::Debug + PartialEq + Eq;

    fn register_order(&mut self, order: &Order, book: &OrderBookL2) -> Self::State;

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

    fn register_order(&mut self, _order: &Order, _book: &OrderBookL2) -> Self::State {
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

    fn register_order(&mut self, order: &Order, book: &OrderBookL2) -> Self::State {
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
