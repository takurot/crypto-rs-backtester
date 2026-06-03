use likely_stable::{likely, unlikely};
use rustc_hash::FxHashMap;
use smallvec::SmallVec;
use std::collections::BTreeMap;

use crate::orderbook_l2::{MarketDepth, OrderBookL2};
use crate::orderbook_l3::OrderBookL3;
use crate::queue_model::QueueModel;
use crate::types::{L2Update, L3Update, Order, OrderReport, OrderState, OrderType, Side, Tick};

#[derive(Debug, Clone)]
struct LiveOrder<S> {
    order: Order,
    state: OrderState,
    filled_qty: i64,
    remaining_qty: i64,
    queue_state: S,
}

/// Minimal exchange simulator (matching engine scaffolding).
///
/// Phase 1 scope:
/// - L2 book maintenance
/// - basic order lifecycle tracking
/// - queue-model-driven passive fills on market trade ticks
#[derive(Debug)]
pub struct ExchangeSimulator<Q: QueueModel> {
    book: OrderBookL2,
    book_l3: Option<OrderBookL3>,
    queue_model: Q,
    orders: BTreeMap<u64, LiveOrder<Q::State>>,
    /// Optimization: Index active orders by (price, side) to avoid scanning all orders on every trade.
    buckets: FxHashMap<(i64, Side), SmallVec<[u64; 4]>>,
}

impl<Q: QueueModel> ExchangeSimulator<Q> {
    pub fn new(queue_model: Q) -> Self {
        Self {
            book: OrderBookL2::new(),
            book_l3: None,
            queue_model,
            orders: BTreeMap::new(),
            buckets: FxHashMap::default(),
        }
    }

    pub fn new_l3(queue_model: Q) -> Self {
        Self {
            book: OrderBookL2::new(),
            book_l3: Some(OrderBookL3::new()),
            queue_model,
            orders: BTreeMap::new(),
            buckets: FxHashMap::default(),
        }
    }

    pub fn apply_l2_update(&mut self, update: &L2Update) {
        self.book.apply_l2(update);
    }

    pub fn apply_l3_update(&mut self, update: &L3Update) {
        if let Some(book_l3) = &mut self.book_l3 {
            book_l3.apply_l3(update);
        }
    }

    /// Submit an order to the exchange.
    ///
    /// Returns the generated order_id and transitions the order to `PendingNew`.
    pub fn submit_order(&mut self, order: Order) -> u64 {
        let order_id = order.order_id;
        debug_assert!(order_id != 0, "order_id must be assigned by the engine");

        let queue_state = if let Some(book_l3) = &self.book_l3 {
            self.queue_model
                .register_order(&order, book_l3 as &dyn MarketDepth)
        } else {
            self.queue_model
                .register_order(&order, &self.book as &dyn MarketDepth)
        };

        self.buckets
            .entry((order.price, order.side))
            .or_default()
            .push(order_id);

        self.orders.insert(
            order_id,
            LiveOrder {
                order,
                state: OrderState::PendingNew,
                filled_qty: 0,
                remaining_qty: order.qty,
                queue_state,
            },
        );
        order_id
    }

    /// Exchange ACK for a new order: `PendingNew` -> `Open`.
    ///
    /// Returns an `OrderReport` with status `Open` so the engine can deliver
    /// it to the strategy, letting the strategy store the order_id for later
    /// cancellation.
    pub fn ack_new(&mut self, order_id: u64) -> Result<OrderReport, &'static str> {
        let o = self.orders.get_mut(&order_id).ok_or("unknown order_id")?;
        if o.state != OrderState::PendingNew {
            return Err("order is not PendingNew");
        }
        o.state = OrderState::Open;
        Ok(OrderReport {
            order_id: o.order.order_id,
            symbol_id: o.order.symbol_id,
            status: OrderState::Open,
            last_fill_qty: 0,
            last_fill_price: 0,
            filled_qty: 0,
            remaining_qty: o.remaining_qty,
            reason: None,
        })
    }

    /// Request cancel: `Open|PartiallyFilled` -> `PendingCancel`.
    pub fn cancel_order(&mut self, order_id: u64) -> Result<(), &'static str> {
        let o = self.orders.get_mut(&order_id).ok_or("unknown order_id")?;
        if !o.state.can_transition_to(OrderState::PendingCancel) {
            return Err("order cannot transition to PendingCancel");
        }
        o.state = OrderState::PendingCancel;
        Ok(())
    }

    /// Exchange ACK for cancel:
    /// - `PendingCancel` -> `Cancelled`
    ///
    /// If the order filled during the cancel flight, this returns an error to model
    /// a "cancel rejected" outcome (Phase 3.2).
    pub fn ack_cancel(&mut self, order_id: u64) -> Result<OrderReport, &'static str> {
        let o = self.orders.get_mut(&order_id).ok_or("unknown order_id")?;
        match o.state {
            OrderState::PendingCancel => {
                o.state = OrderState::Cancelled;
                Ok(OrderReport {
                    order_id: o.order.order_id,
                    symbol_id: o.order.symbol_id,
                    status: OrderState::Cancelled,
                    last_fill_qty: 0,
                    last_fill_price: 0,
                    filled_qty: o.filled_qty,
                    remaining_qty: o.remaining_qty,
                    reason: None,
                })
            }
            OrderState::Filled => Err("cancel rejected: already filled"),
            _ => Err("order is not PendingCancel"),
        }
    }

    pub fn get_order_state(&self, order_id: u64) -> Option<OrderState> {
        self.orders.get(&order_id).map(|o| o.state)
    }

    pub fn get_order(&self, order_id: u64) -> Option<Order> {
        self.orders.get(&order_id).map(|o| o.order)
    }

    pub fn get_filled_qty(&self, order_id: u64) -> Option<i64> {
        self.orders.get(&order_id).map(|o| o.filled_qty)
    }

    pub fn get_remaining_qty(&self, order_id: u64) -> Option<i64> {
        self.orders.get(&order_id).map(|o| o.remaining_qty)
    }

    /// Immediately fill a market order at the current best bid/ask.
    ///
    /// Must be called on a `PendingNew` market order.  Returns `Some(report)` where
    /// `report.status` is `Filled`, `PartiallyFilled`, or `Rejected`.
    /// Returns `None` only if `order_id` is not found.
    ///
    /// `fallback_price`: used when the L2 book has no relevant side.  When provided,
    /// the order fills at that price for its full requested qty (no depth constraint).
    /// When `None` and the book is empty, the order is `Rejected`.
    pub fn fill_market_immediately(
        &mut self,
        order_id: u64,
        fallback_price: Option<i64>,
    ) -> Option<OrderReport> {
        let o = self.orders.get_mut(&order_id)?;
        debug_assert_eq!(o.order.order_type, OrderType::Market);
        debug_assert_eq!(o.state, OrderState::PendingNew);

        let best = match o.order.side {
            Side::Buy => self
                .book_l3
                .as_ref()
                .and_then(|b| b.best_ask())
                .or_else(|| self.book.best_ask()),
            Side::Sell => self
                .book_l3
                .as_ref()
                .and_then(|b| b.best_bid())
                .or_else(|| self.book.best_bid()),
            Side::None => None,
        };

        // Use L3 or L2 best price when available; fall back to provided price when book is empty.
        // i64::MAX as available_qty: fill the entire order with no depth constraint.
        let effective = best.or_else(|| fallback_price.map(|p| (p, i64::MAX)));

        if let Some((price, available_qty)) = effective {
            let fill_qty = o.order.qty.min(available_qty);
            o.filled_qty = fill_qty;
            o.remaining_qty = o.order.qty - fill_qty;
            let status = if o.remaining_qty == 0 {
                o.state = OrderState::Filled;
                OrderState::Filled
            } else {
                o.state = OrderState::PartiallyFilled;
                OrderState::PartiallyFilled
            };
            Some(OrderReport {
                order_id: o.order.order_id,
                symbol_id: o.order.symbol_id,
                status,
                last_fill_qty: fill_qty,
                last_fill_price: price,
                filled_qty: o.filled_qty,
                remaining_qty: o.remaining_qty,
                reason: None,
            })
        } else {
            o.state = OrderState::Rejected;
            Some(OrderReport {
                order_id: o.order.order_id,
                symbol_id: o.order.symbol_id,
                status: OrderState::Rejected,
                last_fill_qty: 0,
                last_fill_price: 0,
                filled_qty: 0,
                remaining_qty: o.order.qty,
                reason: Some("book empty"),
            })
        }
    }

    /// Cancel a partially-filled market order immediately after a partial fill.
    ///
    /// Transitions `PartiallyFilled → Cancelled` and returns a `Cancelled` report so the
    /// engine can deliver it to the strategy.  Must only be called on an order in
    /// `PartiallyFilled` state.  Returns `None` only if `order_id` is not found.
    pub fn force_cancel_partial_fill(&mut self, order_id: u64) -> Option<OrderReport> {
        let o = self.orders.get_mut(&order_id)?;
        debug_assert_eq!(o.state, OrderState::PartiallyFilled);
        o.state = OrderState::Cancelled;
        Some(OrderReport {
            order_id: o.order.order_id,
            symbol_id: o.order.symbol_id,
            status: OrderState::Cancelled,
            last_fill_qty: 0,
            last_fill_price: 0,
            filled_qty: o.filled_qty,
            remaining_qty: o.remaining_qty,
            reason: Some("market order partial fill: remaining qty cancelled"),
        })
    }

    /// Remove an order from the active set (typically called when terminal).
    pub fn remove_order(&mut self, order_id: u64) {
        if let Some(o) = self.orders.remove(&order_id) {
            let key = (o.order.price, o.order.side);
            if let Some(bucket) = self.buckets.get_mut(&key)
                && let Some(idx) = bucket.iter().position(|&id| id == order_id)
            {
                bucket.remove(idx);
            }
        }
    }

    /// Process a market trade tick and generate order reports for any fills.
    #[inline(always)]
    pub fn on_trade(&mut self, trade: Tick, reports: &mut Vec<OrderReport>) {
        let queue_model = &mut self.queue_model;

        // We only check orders that match the trade's price and have the opposite side of the trade initiator?
        // Wait, Tick side is the AGGRESSOR side.
        // If Trade is Buy, it matched against Sells.
        // So we check our Sell orders.
        // If Trade is Sell, it matched against Buys.
        // So we check our Buy orders.
        // And we check at the trade price.

        let maker_side = match trade.side {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
            Side::None => return,
        };

        if let Some(bucket) = self.buckets.get(&(trade.price, maker_side)) {
            // Iterate a copy of ids to avoid borrowing conflicts with self.orders
            // (bucket is borrowed from self.buckets)
            // But we can't borrow self.orders mutably while bucket is borrowed.
            // So we must copy the IDs.
            // Optimization: SmallVec or just collect to Reusable Buffer?
            // For now, simple Vec clone. It's u64s.
            let order_ids: SmallVec<[u64; 4]> = bucket.clone();

            for order_id in order_ids {
                // Get mutable reference to order
                // Note: remove_order might have been called recursively? No, on_trade doesn't call remove_order.
                // But we must handle if order was removed? No, we just cloned existing IDs.

                let Some(o) = self.orders.get_mut(&order_id) else {
                    continue;
                };

                if unlikely(o.remaining_qty <= 0) {
                    continue;
                }
                if !matches!(
                    o.state,
                    OrderState::Open | OrderState::PartiallyFilled | OrderState::PendingCancel
                ) {
                    continue;
                }

                let fill_qty =
                    queue_model.check_fill(&o.order, o.remaining_qty, &trade, &mut o.queue_state);
                if likely(fill_qty <= 0) {
                    continue;
                }

                o.filled_qty += fill_qty;
                o.remaining_qty -= fill_qty;
                let status = if o.remaining_qty == 0 {
                    o.state = OrderState::Filled;
                    OrderState::Filled
                } else {
                    o.state = OrderState::PartiallyFilled;
                    OrderState::PartiallyFilled
                };

                reports.push(OrderReport {
                    order_id: o.order.order_id,
                    symbol_id: o.order.symbol_id,
                    status,
                    last_fill_qty: fill_qty,
                    last_fill_price: trade.price,
                    filled_qty: o.filled_qty,
                    remaining_qty: o.remaining_qty,
                    reason: None,
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::fixtures;
    use crate::queue_model::{ConservativeQueue, NoopQueue};
    use crate::types::{Order, OrderState, OrderType, Side, Tick};

    #[test]
    fn test_exchange_submit_transitions_to_pending_new() {
        let mut ex = ExchangeSimulator::new(NoopQueue);
        let order = Order {
            order_id: 1,
            ts_submit: 1_000,
            seq: 0,
            symbol_id: fixtures::SYMBOL_ID_BTC_USDT,
            side: Side::Buy,
            order_type: OrderType::Limit,
            price: 100,
            qty: 1,
        };

        let id = ex.submit_order(order);
        assert_eq!(ex.get_order_state(id), Some(OrderState::PendingNew));
    }

    #[test]
    fn test_exchange_cancel_transitions_to_pending_cancel() {
        let mut ex = ExchangeSimulator::new(NoopQueue);
        let order = Order {
            order_id: 1,
            ts_submit: 1_000,
            seq: 0,
            symbol_id: fixtures::SYMBOL_ID_BTC_USDT,
            side: Side::Buy,
            order_type: OrderType::Limit,
            price: 100,
            qty: 1,
        };

        let id = ex.submit_order(order);
        ex.ack_new(id).expect("ack");
        ex.cancel_order(id).expect("cancel");
        assert_eq!(ex.get_order_state(id), Some(OrderState::PendingCancel));
    }

    #[test]
    fn test_queue_conservative_user_is_last_in_queue() {
        let mut ex = ExchangeSimulator::new(ConservativeQueue);

        // Visible bid queue ahead at price=100 is 10. User places a bid of qty=3 at the same price.
        ex.apply_l2_update(&fixtures::l2_update(1_000, 0, 100, 10, Side::Buy));

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
        let id = ex.submit_order(order);
        ex.ack_new(id).expect("ack");

        // Trade volume first depletes the queue ahead, then can fill the user order.
        let t1 = Tick {
            ts_exchange: 2_000,
            ts_local: 2_000,
            seq: 1,
            symbol_id: fixtures::SYMBOL_ID_BTC_USDT,
            price: 100,
            qty: 9,
            side: Side::Sell,
            flags: 0x01,
        };
        let mut reports = Vec::new();
        ex.on_trade(t1, &mut reports);
        assert!(reports.is_empty());
        assert_eq!(ex.get_filled_qty(id), Some(0));
        assert_eq!(ex.get_remaining_qty(id), Some(3));

        let t2 = Tick {
            ts_exchange: 3_000,
            ts_local: 3_000,
            seq: 2,
            symbol_id: fixtures::SYMBOL_ID_BTC_USDT,
            price: 100,
            qty: 2,
            side: Side::Sell,
            flags: 0x01,
        };
        ex.on_trade(t2, &mut reports);
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].status, OrderState::PartiallyFilled);
        assert_eq!(reports[0].last_fill_qty, 1);
        assert_eq!(reports[0].filled_qty, 1);
        assert_eq!(reports[0].remaining_qty, 2);
        reports.clear();

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
        ex.on_trade(t3, &mut reports);
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].status, OrderState::Filled);
        assert_eq!(reports[0].last_fill_qty, 2);
        assert_eq!(reports[0].filled_qty, 3);
        assert_eq!(reports[0].remaining_qty, 0);
        assert_eq!(ex.get_order_state(id), Some(OrderState::Filled));
    }

    #[test]
    fn test_pending_cancel_can_fill_before_cancel_ack() {
        let mut ex = ExchangeSimulator::new(ConservativeQueue);

        let order = Order {
            order_id: 1,
            ts_submit: 1_000,
            seq: 0,
            symbol_id: fixtures::SYMBOL_ID_BTC_USDT,
            side: Side::Buy,
            order_type: OrderType::Limit,
            price: 100,
            qty: 1,
        };
        let id = ex.submit_order(order);
        ex.ack_new(id).expect("ack");

        ex.cancel_order(id).expect("cancel");
        assert_eq!(ex.get_order_state(id), Some(OrderState::PendingCancel));

        // The order can still fill while cancel is in-flight.
        let trade = Tick {
            ts_exchange: 2_000,
            ts_local: 2_000,
            seq: 1,
            symbol_id: fixtures::SYMBOL_ID_BTC_USDT,
            price: 100,
            qty: 1,
            side: Side::Sell,
            flags: 0x01,
        };
        let mut reports = Vec::new();
        ex.on_trade(trade, &mut reports);
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].status, OrderState::Filled);
        assert_eq!(ex.get_order_state(id), Some(OrderState::Filled));

        // Then the cancel is rejected.
        assert_eq!(
            ex.ack_cancel(id).unwrap_err(),
            "cancel rejected: already filled"
        );
        assert_eq!(ex.get_order_state(id), Some(OrderState::Filled));
    }

    // ── Market order tests ───────────────────────────────────────────────────

    fn market_buy(symbol_id: u32, qty: i64) -> Order {
        Order {
            order_id: 1,
            ts_submit: 1_000,
            seq: 0,
            symbol_id,
            side: Side::Buy,
            order_type: OrderType::Market,
            price: 0,
            qty,
        }
    }

    fn market_sell(symbol_id: u32, qty: i64) -> Order {
        Order {
            order_id: 2,
            ts_submit: 1_000,
            seq: 0,
            symbol_id,
            side: Side::Sell,
            order_type: OrderType::Market,
            price: 0,
            qty,
        }
    }

    #[test]
    fn test_market_buy_fills_at_best_ask() {
        let mut ex = ExchangeSimulator::new(NoopQueue);
        ex.apply_l2_update(&fixtures::l2_update(1_000, 0, 105, 10, Side::Sell));

        let order = market_buy(fixtures::SYMBOL_ID_BTC_USDT, 5);
        let id = ex.submit_order(order);

        let report = ex
            .fill_market_immediately(id, None)
            .expect("fill_market_immediately should succeed");

        assert_eq!(report.status, OrderState::Filled);
        assert_eq!(report.last_fill_price, 105);
        assert_eq!(report.last_fill_qty, 5);
        assert_eq!(report.filled_qty, 5);
        assert_eq!(report.remaining_qty, 0);
    }

    #[test]
    fn test_market_sell_fills_at_best_bid() {
        let mut ex = ExchangeSimulator::new(NoopQueue);
        ex.apply_l2_update(&fixtures::l2_update(1_000, 0, 100, 8, Side::Buy));

        let order = market_sell(fixtures::SYMBOL_ID_BTC_USDT, 3);
        let id = ex.submit_order(order);

        let report = ex
            .fill_market_immediately(id, None)
            .expect("fill_market_immediately should succeed");

        assert_eq!(report.status, OrderState::Filled);
        assert_eq!(report.last_fill_price, 100);
        assert_eq!(report.last_fill_qty, 3);
        assert_eq!(report.remaining_qty, 0);
    }

    #[test]
    fn test_market_buy_partial_fill_when_book_depth_insufficient() {
        let mut ex = ExchangeSimulator::new(NoopQueue);
        // Only 3 available at best ask; order wants 10
        ex.apply_l2_update(&fixtures::l2_update(1_000, 0, 105, 3, Side::Sell));

        let order = market_buy(fixtures::SYMBOL_ID_BTC_USDT, 10);
        let id = ex.submit_order(order);

        let report = ex
            .fill_market_immediately(id, None)
            .expect("fill_market_immediately should succeed");

        assert_eq!(report.status, OrderState::PartiallyFilled);
        assert_eq!(report.last_fill_qty, 3);
        assert_eq!(report.filled_qty, 3);
        assert_eq!(report.remaining_qty, 7);
    }

    #[test]
    fn test_market_order_rejected_when_book_empty() {
        let mut ex = ExchangeSimulator::new(NoopQueue);
        // No asks in book

        let order = market_buy(fixtures::SYMBOL_ID_BTC_USDT, 1);
        let id = ex.submit_order(order);

        let report = ex
            .fill_market_immediately(id, None)
            .expect("fill_market_immediately should succeed");

        assert_eq!(report.status, OrderState::Rejected);
        assert_eq!(report.last_fill_qty, 0);
        assert!(report.reason.is_some());
    }

    #[test]
    fn test_market_buy_uses_l3_book_best_ask() {
        use crate::orderbook_l3::L3_ADD;
        use crate::types::L3Update;

        let mut ex = ExchangeSimulator::new_l3(NoopQueue);
        // Add an ask at price 200 with qty 5 to the L3 book; L2 book is empty.
        let upd = L3Update {
            ts_exchange: 1_000,
            order_id: 42,
            price: 200,
            qty: 5,
            symbol_id: fixtures::SYMBOL_ID_BTC_USDT,
            side: Side::Sell,
            action: L3_ADD,
        };
        ex.apply_l3_update(&upd);

        let order = market_buy(fixtures::SYMBOL_ID_BTC_USDT, 3);
        let id = ex.submit_order(order);

        let report = ex
            .fill_market_immediately(id, None)
            .expect("fill_market_immediately should succeed");

        assert_eq!(report.status, OrderState::Filled);
        assert_eq!(report.last_fill_price, 200);
        assert_eq!(report.last_fill_qty, 3);
    }

    #[test]
    fn test_market_sell_uses_l3_book_best_bid() {
        use crate::orderbook_l3::L3_ADD;
        use crate::types::L3Update;

        let mut ex = ExchangeSimulator::new_l3(NoopQueue);
        // Add a bid at price 150 with qty 10 to the L3 book; L2 book is empty.
        let upd = L3Update {
            ts_exchange: 1_000,
            order_id: 99,
            price: 150,
            qty: 10,
            symbol_id: fixtures::SYMBOL_ID_BTC_USDT,
            side: Side::Buy,
            action: L3_ADD,
        };
        ex.apply_l3_update(&upd);

        let order = market_sell(fixtures::SYMBOL_ID_BTC_USDT, 4);
        let id = ex.submit_order(order);

        let report = ex
            .fill_market_immediately(id, None)
            .expect("fill_market_immediately should succeed");

        assert_eq!(report.status, OrderState::Filled);
        assert_eq!(report.last_fill_price, 150);
        assert_eq!(report.last_fill_qty, 4);
    }
}
