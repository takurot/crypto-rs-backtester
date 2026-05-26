use crate::types::{Order, Side};

/// Pre-trade risk guard enforcing per-session limits.
///
/// Tracks its own running PnL so it works regardless of `TradeLogMode`.
/// All monetary values are fixed-point i64 (scaled by 1e8).
#[derive(Debug, Clone, Copy)]
pub struct RiskGuard {
    /// Absolute maximum open orders (0 = unlimited).
    pub max_open_orders: usize,
    /// Absolute maximum position size in base units (0 = unlimited).
    pub max_position: i64,
    /// Minimum cumulative PnL before kill switch fires; must be <= 0 to have effect (0 = disabled).
    pub max_loss: i64,

    killed: bool,
    running_pnl: i64,
}

impl RiskGuard {
    pub fn new(max_open_orders: usize, max_position: i64, max_loss: i64) -> Self {
        Self {
            max_open_orders,
            max_position,
            max_loss,
            killed: false,
            running_pnl: 0,
        }
    }

    /// Check whether the order should be rejected before submission.
    ///
    /// Returns `None` if the order passes all limits, or `Some(reason)` on failure.
    pub fn check_order(
        &self,
        order: &Order,
        open_order_count: usize,
        current_position_qty: i64,
    ) -> Option<&'static str> {
        if self.killed {
            return Some("risk: kill_switch active");
        }
        if self.max_open_orders > 0 && open_order_count >= self.max_open_orders {
            return Some("risk: max_open_orders exceeded");
        }
        if self.max_position > 0 {
            let delta: i128 = match order.side {
                Side::Buy => order.qty as i128,
                Side::Sell => -(order.qty as i128),
                Side::None => 0,
            };
            let proposed = current_position_qty as i128 + delta;
            if proposed.unsigned_abs() > self.max_position as u128 {
                return Some("risk: max_position exceeded");
            }
        }
        None
    }

    /// Update the running PnL and fire the kill switch if the threshold is breached.
    ///
    /// `max_loss` is only active when it is strictly negative; zero means disabled and positive
    /// values are treated as disabled (checked at construction via `EngineConfig` validation).
    pub fn update_pnl(&mut self, delta: i64) {
        self.running_pnl = self.running_pnl.saturating_add(delta);
        if !self.killed && self.max_loss < 0 && self.running_pnl <= self.max_loss {
            self.killed = true;
        }
    }

    pub fn is_killed(&self) -> bool {
        self.killed
    }
}

impl Default for RiskGuard {
    fn default() -> Self {
        Self::new(0, 0, 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Order, OrderType, Side};

    fn make_order(side: Side, qty: i64) -> Order {
        Order {
            order_id: 1,
            ts_submit: 0,
            seq: 0,
            symbol_id: 1,
            side,
            order_type: OrderType::Limit,
            price: 100_00000000,
            qty,
        }
    }

    #[test]
    fn test_check_order_passes_when_no_limits() {
        let g = RiskGuard::default();
        assert!(g.check_order(&make_order(Side::Buy, 1_00000000), 100, 0).is_none());
    }

    #[test]
    fn test_max_open_orders_rejects_at_limit() {
        let g = RiskGuard::new(2, 0, 0);
        // 1 open order — OK
        assert!(g.check_order(&make_order(Side::Buy, 1_00000000), 1, 0).is_none());
        // 2 open orders — reject
        assert!(g.check_order(&make_order(Side::Buy, 1_00000000), 2, 0).is_some());
    }

    #[test]
    fn test_max_position_rejects_when_exceeded() {
        let g = RiskGuard::new(0, 1_00000000, 0);
        // Buy 2 units with flat position → proposed=2 > 1 → reject
        assert!(g.check_order(&make_order(Side::Buy, 2_00000000), 0, 0).is_some());
        // Buy 1 unit with flat position → proposed=1 == 1 → OK
        assert!(g.check_order(&make_order(Side::Buy, 1_00000000), 0, 0).is_none());
    }

    #[test]
    fn test_kill_switch_fires_on_max_loss() {
        let mut g = RiskGuard::new(0, 0, -1);
        assert!(!g.is_killed());
        g.update_pnl(-1);
        assert!(g.is_killed());
    }

    #[test]
    fn test_killed_rejects_all_orders() {
        let mut g = RiskGuard::new(0, 0, -1);
        g.update_pnl(-100_00000000);
        assert!(g.check_order(&make_order(Side::Buy, 1_00000000), 0, 0).is_some());
    }

    #[test]
    fn test_max_loss_zero_means_unlimited() {
        let mut g = RiskGuard::new(0, 0, 0);
        g.update_pnl(-1_000_000_000_000);
        assert!(!g.is_killed());
    }

    #[test]
    fn test_positive_max_loss_never_triggers() {
        // Positive max_loss is treated as disabled (guard is < 0 check).
        let mut g = RiskGuard::new(0, 0, 1);
        g.update_pnl(-100_00000000);
        assert!(!g.is_killed(), "positive max_loss must not trigger");
    }
}
