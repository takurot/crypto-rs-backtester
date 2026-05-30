use std::collections::{BTreeMap, VecDeque};

use crate::orderbook_l2::MarketDepth;
use crate::types::{L3Update, Side};

pub const L3_ADD: u8 = 0x01;
pub const L3_MODIFY: u8 = 0x02;
pub const L3_DELETE: u8 = 0x03;

/// Per-side L3 order book: price -> FIFO queue of (venue order_id, qty).
#[derive(Debug, Default, Clone)]
pub struct OrderBookL3 {
    bids: BTreeMap<i64, VecDeque<(u64, i64)>>,
    asks: BTreeMap<i64, VecDeque<(u64, i64)>>,
}

impl OrderBookL3 {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn apply_l3(&mut self, update: &L3Update) -> Result<(), &'static str> {
        if update.side == Side::None {
            return Err("invalid L3 side");
        }

        match update.action {
            L3_ADD => {
                if update.qty <= 0 {
                    return Err("L3 ADD qty must be positive");
                }
                let queue = self
                    .side_map_mut(update.side)
                    .entry(update.price)
                    .or_default();
                queue.push_back((update.order_id, update.qty));
            }
            L3_MODIFY => {
                if update.qty < 0 {
                    return Err("L3 MODIFY qty must be non-negative");
                }
                let side_map = self.side_map_mut(update.side);
                if let Some(queue) = side_map.get_mut(&update.price) {
                    if update.qty == 0 {
                        queue.retain(|(id, _)| *id != update.order_id);
                    } else if let Some((_, qty)) =
                        queue.iter_mut().find(|(id, _)| *id == update.order_id)
                    {
                        *qty = update.qty;
                    }
                    if queue.is_empty() {
                        side_map.remove(&update.price);
                    }
                }
            }
            L3_DELETE => {
                let side_map = self.side_map_mut(update.side);
                if let Some(queue) = side_map.get_mut(&update.price) {
                    queue.retain(|(id, _)| *id != update.order_id);
                    if queue.is_empty() {
                        side_map.remove(&update.price);
                    }
                }
            }
            _ => return Err("invalid L3 action"),
        }
        Ok(())
    }

    pub fn level_qty(&self, side: Side, price: i64) -> i64 {
        self.side_map(side)
            .and_then(|levels| levels.get(&price))
            .map(|queue| queue.iter().map(|(_, qty)| *qty).sum())
            .unwrap_or(0)
    }

    pub fn qty_ahead(&self, side: Side, price: i64, order_id: u64) -> i64 {
        self.orders_ahead(side, price, order_id)
            .into_iter()
            .map(|(_, qty)| qty)
            .sum()
    }

    pub fn orders_ahead(&self, side: Side, price: i64, order_id: u64) -> Vec<(u64, i64)> {
        let Some(queue) = self.side_map(side).and_then(|levels| levels.get(&price)) else {
            return Vec::new();
        };

        let mut ahead = Vec::new();
        for (id, qty) in queue {
            if *id == order_id {
                return ahead;
            }
            ahead.push((*id, *qty));
        }

        // Simulated user orders are not present in the venue L3 feed; the exact
        // queue-ahead snapshot is therefore the full resting level at entry.
        ahead
    }

    pub fn best_bid(&self) -> Option<(i64, i64)> {
        self.bids
            .iter()
            .next_back()
            .map(|(price, queue)| (*price, queue.iter().map(|(_, qty)| *qty).sum()))
    }

    pub fn best_ask(&self) -> Option<(i64, i64)> {
        self.asks
            .iter()
            .next()
            .map(|(price, queue)| (*price, queue.iter().map(|(_, qty)| *qty).sum()))
    }

    fn side_map(&self, side: Side) -> Option<&BTreeMap<i64, VecDeque<(u64, i64)>>> {
        match side {
            Side::Buy => Some(&self.bids),
            Side::Sell => Some(&self.asks),
            Side::None => None,
        }
    }

    fn side_map_mut(&mut self, side: Side) -> &mut BTreeMap<i64, VecDeque<(u64, i64)>> {
        match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
            Side::None => unreachable!("validated before side_map_mut"),
        }
    }
}

impl MarketDepth for OrderBookL3 {
    fn level_qty(&self, side: Side, price: i64) -> i64 {
        OrderBookL3::level_qty(self, side, price)
    }

    fn qty_ahead(&self, side: Side, price: i64, order_id: u64) -> i64 {
        OrderBookL3::qty_ahead(self, side, price, order_id)
    }

    fn orders_ahead(&self, side: Side, price: i64, order_id: u64) -> Vec<(u64, i64)> {
        OrderBookL3::orders_ahead(self, side, price, order_id)
    }
}
