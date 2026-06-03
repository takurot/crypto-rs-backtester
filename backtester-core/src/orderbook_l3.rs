use std::collections::{BTreeMap, VecDeque};

use crate::orderbook_l2::MarketDepth;
use crate::types::{L3Update, Side};

// L3 order book action codes.
pub const L3_ADD: u8 = 0x01;
pub const L3_MODIFY: u8 = 0x02;
pub const L3_DELETE: u8 = 0x03;

/// Per-side L3 order book: maps price → queue of (order_id, qty).
///
/// Maintains exact queue order so `qty_ahead` returns the total qty of all
/// resting orders that arrived *before* a given order_id.
#[derive(Debug, Default, Clone)]
pub struct OrderBookL3 {
    bids: BTreeMap<i64, VecDeque<(u64, i64)>>, // price → [(order_id, qty)]
    asks: BTreeMap<i64, VecDeque<(u64, i64)>>,
}

impl OrderBookL3 {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn apply_l3(&mut self, update: &L3Update) {
        let side_map = match update.side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
            Side::None => return,
        };

        match update.action {
            L3_ADD => {
                side_map
                    .entry(update.price)
                    .or_default()
                    .push_back((update.order_id, update.qty));
            }
            L3_MODIFY => {
                if let Some(queue) = side_map.get_mut(&update.price)
                    && let Some(entry) = queue.iter_mut().find(|(id, _)| *id == update.order_id)
                {
                    entry.1 = update.qty;
                }
            }
            L3_DELETE => {
                if let Some(queue) = side_map.get_mut(&update.price) {
                    queue.retain(|(id, _)| *id != update.order_id);
                    if queue.is_empty() {
                        side_map.remove(&update.price);
                    }
                }
            }
            _ => {}
        }
    }

    /// Total resting qty at this price level (sum of all order qtys).
    pub fn level_qty(&self, side: Side, price: i64) -> i64 {
        let side_map = match side {
            Side::Buy => &self.bids,
            Side::Sell => &self.asks,
            Side::None => return 0,
        };
        side_map
            .get(&price)
            .map(|q| q.iter().map(|(_, qty)| qty).sum())
            .unwrap_or(0)
    }

    /// Qty of all orders that arrived strictly *before* `order_id` at this level.
    ///
    /// Returns `level_qty` if `order_id` is not in the queue (conservative fallback).
    pub fn qty_ahead(&self, side: Side, price: i64, order_id: u64) -> i64 {
        let side_map = match side {
            Side::Buy => &self.bids,
            Side::Sell => &self.asks,
            Side::None => return 0,
        };
        let Some(queue) = side_map.get(&price) else {
            return 0;
        };
        let mut ahead = 0i64;
        for (id, qty) in queue {
            if *id == order_id {
                return ahead;
            }
            ahead = ahead.saturating_add(*qty);
        }
        // order_id not found — return total level qty as conservative fallback
        ahead
    }

    pub fn best_bid(&self) -> Option<(i64, i64)> {
        self.bids
            .iter()
            .next_back()
            .map(|(p, q)| (*p, q.iter().map(|(_, qty)| qty).sum()))
    }

    pub fn best_ask(&self) -> Option<(i64, i64)> {
        self.asks
            .iter()
            .next()
            .map(|(p, q)| (*p, q.iter().map(|(_, qty)| qty).sum()))
    }
}

impl MarketDepth for OrderBookL3 {
    fn level_qty(&self, side: Side, price: i64) -> i64 {
        OrderBookL3::level_qty(self, side, price)
    }

    fn qty_ahead(&self, side: Side, price: i64, order_id: u64) -> i64 {
        OrderBookL3::qty_ahead(self, side, price, order_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn add(price: i64, order_id: u64, qty: i64, side: Side) -> L3Update {
        L3Update {
            ts_exchange: 0,
            symbol_id: 1,
            price,
            qty,
            order_id,
            side,
            action: L3_ADD,
        }
    }

    fn delete(price: i64, order_id: u64, side: Side) -> L3Update {
        L3Update {
            ts_exchange: 0,
            symbol_id: 1,
            price,
            qty: 0,
            order_id,
            side,
            action: L3_DELETE,
        }
    }

    fn modify(price: i64, order_id: u64, qty: i64, side: Side) -> L3Update {
        L3Update {
            ts_exchange: 0,
            symbol_id: 1,
            price,
            qty,
            order_id,
            side,
            action: L3_MODIFY,
        }
    }

    #[test]
    fn test_add_orders_and_level_qty() {
        let mut book = OrderBookL3::new();
        book.apply_l3(&add(100, 1, 10, Side::Buy));
        book.apply_l3(&add(100, 2, 5, Side::Buy));
        assert_eq!(book.level_qty(Side::Buy, 100), 15);
    }

    #[test]
    fn test_delete_removes_order_and_updates_level_qty() {
        let mut book = OrderBookL3::new();
        book.apply_l3(&add(100, 1, 10, Side::Buy));
        book.apply_l3(&add(100, 2, 5, Side::Buy));
        book.apply_l3(&delete(100, 1, Side::Buy));
        assert_eq!(book.level_qty(Side::Buy, 100), 5);
    }

    #[test]
    fn test_modify_updates_qty_in_place() {
        let mut book = OrderBookL3::new();
        book.apply_l3(&add(100, 1, 10, Side::Buy));
        book.apply_l3(&modify(100, 1, 3, Side::Buy));
        assert_eq!(book.level_qty(Side::Buy, 100), 3);
    }

    #[test]
    fn test_qty_ahead_returns_sum_of_orders_before_given_id() {
        let mut book = OrderBookL3::new();
        // Queue order: [order 1 (qty=10), order 2 (qty=5), order 3 (qty=3)]
        book.apply_l3(&add(100, 1, 10, Side::Buy));
        book.apply_l3(&add(100, 2, 5, Side::Buy));
        book.apply_l3(&add(100, 3, 3, Side::Buy));

        // order 1 is first — nothing ahead
        assert_eq!(book.qty_ahead(Side::Buy, 100, 1), 0);
        // order 2 has order 1 (qty=10) ahead
        assert_eq!(book.qty_ahead(Side::Buy, 100, 2), 10);
        // order 3 has orders 1+2 (qty=15) ahead
        assert_eq!(book.qty_ahead(Side::Buy, 100, 3), 15);
    }

    #[test]
    fn test_qty_ahead_after_delete_reflects_cancellation() {
        let mut book = OrderBookL3::new();
        book.apply_l3(&add(100, 1, 10, Side::Buy));
        book.apply_l3(&add(100, 2, 5, Side::Buy));
        book.apply_l3(&add(100, 3, 3, Side::Buy));
        // Cancel order 1 (which was ahead of order 3)
        book.apply_l3(&delete(100, 1, Side::Buy));

        // Now order 3 only has order 2 (qty=5) ahead
        assert_eq!(book.qty_ahead(Side::Buy, 100, 3), 5);
    }

    #[test]
    fn test_qty_ahead_unknown_order_id_returns_level_qty() {
        let mut book = OrderBookL3::new();
        book.apply_l3(&add(100, 1, 10, Side::Buy));
        // order_id=99 not in queue — conservative fallback
        assert_eq!(book.qty_ahead(Side::Buy, 100, 99), 10);
    }

    #[test]
    fn test_best_bid_ask() {
        let mut book = OrderBookL3::new();
        book.apply_l3(&add(100, 1, 10, Side::Buy));
        book.apply_l3(&add(101, 2, 5, Side::Buy));
        book.apply_l3(&add(103, 3, 7, Side::Sell));
        book.apply_l3(&add(105, 4, 3, Side::Sell));
        assert_eq!(book.best_bid(), Some((101, 5)));
        assert_eq!(book.best_ask(), Some((103, 7)));
    }

    #[test]
    fn test_delete_last_order_removes_price_level() {
        let mut book = OrderBookL3::new();
        book.apply_l3(&add(100, 1, 10, Side::Buy));
        book.apply_l3(&delete(100, 1, Side::Buy));
        assert_eq!(book.level_qty(Side::Buy, 100), 0);
        assert_eq!(book.best_bid(), None);
    }
}
