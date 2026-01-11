use std::collections::BTreeMap;

use crate::types::{FixedPoint, Side, TsExchangeNs};

/// A single executed fill (logical trade log row).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TradeFill {
    pub ts_exchange: TsExchangeNs,
    pub symbol_id: u32,
    pub order_id: u64,
    /// Side of the *strategy's* order which got filled.
    pub side: Side,
    pub price: i64,
    pub qty: i64,
}

#[derive(Debug, Default, Clone)]
pub struct TradeLog {
    fills: Vec<TradeFill>,
}

impl TradeLog {
    pub fn push_fill(&mut self, fill: TradeFill) {
        self.fills.push(fill);
    }

    pub fn fills(&self) -> &[TradeFill] {
        &self.fills
    }
}

/// Aggregated backtest statistics (Phase 4.2).
///
/// Notes:
/// - Monetary values remain fixed-point `i64` (scaled by 1e8).
/// - `f64` is used only for non-monetary summary ratios.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BacktestStats {
    pub total_trades: u64,
    pub win_rate: f64,
    pub profit_factor: f64,
    pub sharpe_ratio: f64,
    pub sortino_ratio: f64,
    /// Max drawdown in percent (0..=100).
    pub max_drawdown: f64,
    /// Duration (ns) from the peak to the trough of the max drawdown window.
    pub max_drawdown_duration: i64,
    pub calmar_ratio: f64,
    pub total_pnl: i64,
    pub avg_trade_pnl: i64,
    pub avg_holding_period: i64,
    pub total_fees_paid: i64,
}

impl Default for BacktestStats {
    fn default() -> Self {
        Self {
            total_trades: 0,
            win_rate: 0.0,
            profit_factor: 0.0,
            sharpe_ratio: 0.0,
            sortino_ratio: 0.0,
            max_drawdown: 0.0,
            max_drawdown_duration: 0,
            calmar_ratio: 0.0,
            total_pnl: 0,
            avg_trade_pnl: 0,
            avg_holding_period: 0,
            total_fees_paid: 0,
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct PosState {
    qty: i64,
    avg_price: i64,
}

fn clamp_i128_to_i64(v: i128) -> i64 {
    v.clamp(i64::MIN as i128, i64::MAX as i128) as i64
}

/// Return realized PnL deltas (quote, scaled) per fill using an average-cost model.
///
/// Each entry is `(ts_exchange, pnl_delta)`, where `pnl_delta` is 0 for pure opens/adds,
/// and non-zero when the fill reduces or closes an existing position.
pub fn pnl_deltas_from_fills(fills: &[TradeFill]) -> Vec<(TsExchangeNs, i64)> {
    let mut state_by_symbol: BTreeMap<u32, PosState> = BTreeMap::new();
    let mut deltas: Vec<(TsExchangeNs, i64)> = Vec::with_capacity(fills.len());

    for f in fills {
        let qty = f.qty;
        if qty <= 0 {
            deltas.push((f.ts_exchange, 0));
            continue;
        }

        let delta_qty: i64 = match f.side {
            Side::Buy => qty,
            Side::Sell => -qty,
            Side::None => 0,
        };
        if delta_qty == 0 {
            deltas.push((f.ts_exchange, 0));
            continue;
        }

        let s = state_by_symbol.entry(f.symbol_id).or_default();

        // Open / add to position in the same direction.
        if s.qty == 0 || s.qty.signum() == delta_qty.signum() {
            let new_qty = s.qty.saturating_add(delta_qty);
            let abs_old = s.qty.abs() as i128;
            let abs_delta = delta_qty.abs() as i128;
            let new_abs = abs_old + abs_delta;
            if new_abs > 0 {
                let weighted = (s.avg_price as i128 * abs_old) + (f.price as i128 * abs_delta);
                s.avg_price = clamp_i128_to_i64(weighted / new_abs);
            } else {
                s.avg_price = 0;
            }
            s.qty = new_qty;
            deltas.push((f.ts_exchange, 0));
            continue;
        }

        // Reduce / close / flip.
        let abs_old = s.qty.abs() as i128;
        let abs_delta = delta_qty.abs() as i128;
        let close_abs = abs_old.min(abs_delta);

        let pnl_per_unit: i128 = if s.qty > 0 {
            (f.price - s.avg_price) as i128
        } else {
            (s.avg_price - f.price) as i128
        };
        let pnl_i128 = (pnl_per_unit.saturating_mul(close_abs)) / FixedPoint::SCALE as i128;
        let pnl_delta_i64 = clamp_i128_to_i64(pnl_i128);

        let new_qty = s.qty.saturating_add(delta_qty);
        if new_qty == 0 {
            s.qty = 0;
            s.avg_price = 0;
        } else if new_qty.signum() == s.qty.signum() {
            // Reduced but still same direction: keep avg_price.
            s.qty = new_qty;
        } else {
            // Flipped: leftover opens at this fill price.
            s.qty = new_qty;
            s.avg_price = f.price;
        }

        deltas.push((f.ts_exchange, pnl_delta_i64));
    }

    deltas
}

pub fn equity_curve_from_pnl_deltas(deltas: &[(TsExchangeNs, i64)]) -> Vec<(TsExchangeNs, i64)> {
    let mut equity: i64 = 0;
    let mut curve: Vec<(TsExchangeNs, i64)> = Vec::with_capacity(deltas.len());
    for (ts, d) in deltas {
        equity = equity.saturating_add(*d);
        curve.push((*ts, equity));
    }
    curve
}

/// Compute max drawdown (%) and its duration from a time-ordered equity curve.
///
/// - `max_drawdown` is reported as percent (0..=100).
/// - `duration` is `ts_trough - ts_peak` for the drawdown window.
pub fn max_drawdown_pct_and_duration(equity_curve: &[(TsExchangeNs, i64)]) -> (f64, i64) {
    let Some(&(mut peak_ts, mut peak_eq)) = equity_curve.first() else {
        return (0.0, 0);
    };

    let mut max_dd: f64 = 0.0;
    let mut max_dd_dur: i64 = 0;

    for &(ts, eq) in equity_curve.iter().skip(1) {
        if eq > peak_eq {
            peak_eq = eq;
            peak_ts = ts;
            continue;
        }
        if peak_eq <= 0 {
            continue;
        }
        let dd = (peak_eq - eq) as f64 / peak_eq as f64;
        if dd > max_dd {
            max_dd = dd;
            max_dd_dur = ts.saturating_sub(peak_ts);
        }
    }

    (max_dd * 100.0, max_dd_dur)
}

pub fn sharpe_ratio_from_pnl_series(pnl: &[i64]) -> f64 {
    let n = pnl.len();
    if n < 2 {
        return 0.0;
    }
    let mean = pnl.iter().map(|&x| x as f64).sum::<f64>() / n as f64;
    let var = pnl
        .iter()
        .map(|&x| {
            let d = x as f64 - mean;
            d * d
        })
        .sum::<f64>()
        / (n as f64 - 1.0);
    let std = var.sqrt();
    if std == 0.0 {
        return 0.0;
    }
    mean / std
}

/// Compute basic stats from fills plus a total funding PnL (quote, scaled).
pub fn calculate_stats(trade_log: &TradeLog, total_funding_pnl: i64) -> BacktestStats {
    let fills = trade_log.fills();
    let deltas = pnl_deltas_from_fills(fills);
    let mut trade_pnl: i64 = 0;
    let mut gross_profit: i64 = 0;
    let mut gross_loss: i64 = 0; // negative

    // Use pnl deltas as a basic "return" series.
    let mut pnl_series: Vec<i64> = Vec::with_capacity(deltas.len());
    for (_ts, d) in &deltas {
        trade_pnl = trade_pnl.saturating_add(*d);
        pnl_series.push(*d);
        if *d > 0 {
            gross_profit = gross_profit.saturating_add(*d);
        } else if *d < 0 {
            gross_loss = gross_loss.saturating_add(*d);
        }
    }

    let mut curve = equity_curve_from_pnl_deltas(&deltas);
    if total_funding_pnl != 0 {
        let ts = curve.last().map(|(ts, _)| *ts).unwrap_or(0);
        let eq = curve.last().map(|(_, e)| *e).unwrap_or(0);
        curve.push((ts, eq.saturating_add(total_funding_pnl)));
    }
    let (max_dd, max_dd_dur) = max_drawdown_pct_and_duration(&curve);

    let profit_factor = if gross_loss < 0 {
        gross_profit as f64 / (-gross_loss) as f64
    } else {
        0.0
    };

    let sharpe = sharpe_ratio_from_pnl_series(&pnl_series);

    let total_trades = fills.len() as u64;
    let avg_trade_pnl = if total_trades > 0 {
        trade_pnl / total_trades as i64
    } else {
        0
    };

    BacktestStats {
        total_trades,
        win_rate: 0.0,
        profit_factor,
        sharpe_ratio: sharpe,
        sortino_ratio: 0.0,
        max_drawdown: max_dd,
        max_drawdown_duration: max_dd_dur,
        calmar_ratio: 0.0,
        total_pnl: trade_pnl.saturating_add(total_funding_pnl),
        avg_trade_pnl,
        avg_holding_period: 0,
        total_fees_paid: 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stats_total_pnl_fixed_point_consistency() {
        let fills = vec![
            TradeFill {
                ts_exchange: 1_000,
                symbol_id: 1,
                order_id: 1,
                side: Side::Buy,
                price: 100_00000000,
                qty: 1_00000000,
            },
            TradeFill {
                ts_exchange: 2_000,
                symbol_id: 1,
                order_id: 2,
                side: Side::Sell,
                price: 101_00000000,
                qty: 1_00000000,
            },
        ];
        let mut log = TradeLog::default();
        for f in fills {
            log.push_fill(f);
        }
        let stats = calculate_stats(&log, 0);

        // +1.00 quote PnL (scaled by 1e8).
        assert_eq!(stats.total_pnl, 1_00000000);
    }

    #[test]
    fn test_stats_max_drawdown_matches_reference() {
        // Equity: 100 -> 120 (peak) -> 90 (trough) => DD = 25%
        let curve = vec![(0_i64, 100), (10, 120), (20, 90), (30, 130)];
        let (dd, dur) = max_drawdown_pct_and_duration(&curve);
        assert!((dd - 25.0).abs() < 1e-12, "dd={dd}");
        assert_eq!(dur, 10);
    }
}
