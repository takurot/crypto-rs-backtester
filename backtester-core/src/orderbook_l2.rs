use std::collections::BTreeMap;

use crate::types::{L2Update, Side};

/// Unified read interface for L2 and L3 depth snapshots.
pub trait MarketDepth: Send {
    /// Total resting quantity at a price level.
    fn level_qty(&self, side: Side, price: i64) -> i64;

    /// Quantity ahead of a specific order at a price level.
    ///
    /// L2 books do not know individual order IDs, so they conservatively return
    /// the full level quantity.
    fn qty_ahead(&self, side: Side, price: i64, order_id: u64) -> i64;

    /// Ordered venue orders ahead at a price level. L2 books do not expose this.
    fn orders_ahead(&self, _side: Side, _price: i64, _order_id: u64) -> Vec<(u64, i64)> {
        Vec::new()
    }
}

/// Minimal L2 order book (price level) representation.
///
/// Scaffolding for benches; production semantics will expand in later phases.
#[derive(Debug, Default, Clone)]
pub struct OrderBookL2 {
    bids: BTreeMap<i64, i64>, // price -> qty
    asks: BTreeMap<i64, i64>, // price -> qty
}

impl OrderBookL2 {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn apply_l2(&mut self, update: &L2Update) {
        let book = match update.side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
            Side::None => return,
        };

        if update.qty == 0 {
            book.remove(&update.price);
        } else {
            book.insert(update.price, update.qty);
        }
    }

    pub fn level_qty(&self, side: Side, price: i64) -> i64 {
        match side {
            Side::Buy => *self.bids.get(&price).unwrap_or(&0),
            Side::Sell => *self.asks.get(&price).unwrap_or(&0),
            Side::None => 0,
        }
    }

    pub fn best_bid(&self) -> Option<(i64, i64)> {
        self.bids.iter().next_back().map(|(p, q)| (*p, *q))
    }

    pub fn best_ask(&self) -> Option<(i64, i64)> {
        self.asks.iter().next().map(|(p, q)| (*p, *q))
    }
}

impl MarketDepth for OrderBookL2 {
    fn level_qty(&self, side: Side, price: i64) -> i64 {
        OrderBookL2::level_qty(self, side, price)
    }

    fn qty_ahead(&self, side: Side, price: i64, _order_id: u64) -> i64 {
        OrderBookL2::level_qty(self, side, price)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixtures;
    use crate::types::Side;

    #[test]
    fn test_orderbook_l2_apply_update_and_best_bid_ask() {
        let mut ob = OrderBookL2::new();
        assert_eq!(ob.best_bid(), None);
        assert_eq!(ob.best_ask(), None);

        ob.apply_l2(&fixtures::l2_update(1_000, 0, 100, 10, Side::Buy));
        ob.apply_l2(&fixtures::l2_update(1_000, 1, 101, 11, Side::Buy));
        assert_eq!(ob.best_bid(), Some((101, 11)));

        ob.apply_l2(&fixtures::l2_update(1_000, 2, 105, 7, Side::Sell));
        ob.apply_l2(&fixtures::l2_update(1_000, 3, 103, 9, Side::Sell));
        assert_eq!(ob.best_ask(), Some((103, 9)));
    }

    #[test]
    fn test_orderbook_l2_remove_level_with_qty_zero() {
        let mut ob = OrderBookL2::new();
        ob.apply_l2(&fixtures::l2_update(1_000, 0, 101, 11, Side::Buy));
        assert_eq!(ob.best_bid(), Some((101, 11)));

        ob.apply_l2(&fixtures::l2_update(1_000, 1, 101, 0, Side::Buy));
        assert_eq!(ob.best_bid(), None);
    }
}
