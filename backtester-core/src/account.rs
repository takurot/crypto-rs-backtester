use crate::types::{FixedPoint, FundingEvent, Order, Side, TsExchangeNs};
use std::collections::BTreeMap;

/// Minimal account/position model (Phase 3 scaffolding).
///
/// This tracks:
/// - Position quantity (base units, fixed-point scaled)
/// - Average entry price (quote units, fixed-point scaled)
/// - Funding PnL (quote units, fixed-point scaled)
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Position {
    /// Base asset position size (scaled by 1e8). Positive = long, negative = short.
    pub qty: i64,
    /// Average entry price (scaled by 1e8).
    pub avg_price: i64,
    /// Timestamp of last position increase/open.
    pub last_open_ts: TsExchangeNs,
    /// Accumulated funding PnL (scaled by 1e8). Positive = received, negative = paid.
    pub funding_pnl: i64,
}

#[derive(Debug, Default)]
pub struct Account {
    positions: BTreeMap<u32, Position>,
    total_funding_pnl: i64,
}

fn clamp_i128_to_i64(v: i128) -> i64 {
    v.clamp(i64::MIN as i128, i64::MAX as i128) as i64
}

impl Account {
    pub fn position(&self, symbol_id: u32) -> Option<&Position> {
        self.positions.get(&symbol_id)
    }

    pub fn position_qty(&self, symbol_id: u32) -> i64 {
        self.positions.get(&symbol_id).map(|p| p.qty).unwrap_or(0)
    }

    pub fn total_funding_pnl(&self) -> i64 {
        self.total_funding_pnl
    }

    /// Process a fill and return the realized PnL delta (quote, scaled).
    pub fn on_fill(&mut self, order: &Order, fill_qty: i64, fill_price: i64) -> i64 {
        let delta_qty = match order.side {
            Side::Buy => fill_qty,
            Side::Sell => -fill_qty,
            Side::None => 0,
        };
        if delta_qty == 0 {
            return 0;
        }

        let p = self.positions.entry(order.symbol_id).or_default();

        // Open / add to position in the same direction.
        if p.qty == 0 || p.qty.signum() == delta_qty.signum() {
            if p.qty == 0 {
                p.last_open_ts = order.ts_submit; // Approximation if exchange timestamp not known here
                // Note: engine::process_fill usually knows exchange ts, but Account doesn't see fill ts here.
                // We should probably take ts_fill as arg. For now use order ts as approximation for new pos.
            }
            let new_qty = p.qty.saturating_add(delta_qty);
            let abs_old = p.qty.abs() as i128;
            let abs_delta = delta_qty.abs() as i128;
            let new_abs = abs_old + abs_delta;
            if new_abs > 0 {
                let weighted = (p.avg_price as i128 * abs_old) + (fill_price as i128 * abs_delta);
                p.avg_price = clamp_i128_to_i64(weighted / new_abs);
            } else {
                p.avg_price = 0;
            }
            p.qty = new_qty;
            return 0;
        }

        // Reduce / close / flip.
        let abs_old = p.qty.abs() as i128;
        let abs_delta = delta_qty.abs() as i128;
        let close_abs = abs_old.min(abs_delta);

        let pnl_per_unit: i128 = if p.qty > 0 {
            (fill_price - p.avg_price) as i128
        } else {
            (p.avg_price - fill_price) as i128
        };
        let pnl_i128 = (pnl_per_unit.saturating_mul(close_abs)) / FixedPoint::SCALE as i128;
        let pnl_delta = clamp_i128_to_i64(pnl_i128);

        let new_qty = p.qty.saturating_add(delta_qty);
        if new_qty == 0 {
            p.qty = 0;
            p.avg_price = 0;
        } else if new_qty.signum() == p.qty.signum() {
            p.qty = new_qty;
        } else {
            // Flipped position
            p.qty = new_qty;
            p.avg_price = fill_price;
            p.last_open_ts = order.ts_submit; // See note above on ts
        }

        pnl_delta
    }

    /// Apply a funding event using the given mark price.
    ///
    /// Funding cashflow convention:
    /// - Positive rate => longs pay shorts.
    /// - Negative rate => shorts pay longs.
    ///
    /// Returns the funding cashflow (quote, scaled by 1e8): positive = received, negative = paid.
    pub fn apply_funding(&mut self, event: &FundingEvent, mark_price: i64) -> i64 {
        let p = match self.positions.get_mut(&event.symbol_id) {
            Some(p) => p,
            None => return 0,
        };
        if p.qty == 0 || event.rate == 0 || mark_price == 0 {
            return 0;
        }

        let scale = FixedPoint::SCALE as i128;
        let qty = p.qty as i128;
        let price = mark_price as i128;
        let rate = event.rate as i128;

        // quote_scaled = qty_scaled * price_scaled / SCALE
        let notional = (qty.saturating_mul(price)) / scale;
        // payment_scaled = notional_scaled * rate_scaled / SCALE
        let payment = (notional.saturating_mul(rate)) / scale;

        // Convert payment to PnL: positive payment means "long pays" under positive rate,
        // so PnL is the negative of payment.
        let pnl = -payment;

        let pnl_i64 = pnl.clamp(i64::MIN as i128, i64::MAX as i128) as i64;
        p.funding_pnl = p.funding_pnl.saturating_add(pnl_i64);
        self.total_funding_pnl = self.total_funding_pnl.saturating_add(pnl_i64);
        pnl_i64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::fixtures;
    use crate::types::{Order, OrderType};

    #[test]
    fn test_funding_applied_at_scheduled_ts_exchange() {
        let mut acct = Account::default();

        // Long 1.0 (scaled) at mark price 100.00 (scaled).
        let order = Order {
            order_id: 1,
            ts_submit: 1_000,
            seq: 0,
            symbol_id: fixtures::SYMBOL_ID_BTC_USDT,
            side: Side::Buy,
            order_type: OrderType::Limit,
            price: 100_00000000,
            qty: 1_00000000,
        };
        acct.on_fill(&order, 1_00000000, 100_00000000);

        let event = FundingEvent {
            ts_exchange: 3_000,
            symbol_id: fixtures::SYMBOL_ID_BTC_USDT,
            rate: 10_000, // 0.0001
        };

        let pnl = acct.apply_funding(&event, 100_00000000);
        // Notional=100.00, rate=0.0001 => pay 0.01
        assert_eq!(pnl, -1_000_000);
        assert_eq!(acct.total_funding_pnl(), -1_000_000);
        assert_eq!(acct.position_qty(fixtures::SYMBOL_ID_BTC_USDT), 1_00000000);
    }
}
