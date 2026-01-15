use std::collections::BTreeMap;
use std::collections::VecDeque;

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

/// Arbitrary PnL event (e.g. funding payment).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PnlEvent {
    pub ts_exchange: TsExchangeNs,
    pub pnl: i64,
}

/// Trade log retention mode for memory control (Phase 5.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TradeLogMode {
    /// Keep all fills/events (default).
    #[default]
    All,
    /// Keep only the last N fills (ring buffer).
    RingBuffer(usize),
    /// Keep no fills, only incrementally compute summary stats.
    SummaryOnly,
    /// Disable logging entirely (fastest, no stats available).
    None,
}

/// Incremental summary statistics for SummaryOnly mode.
#[derive(Debug, Default, Clone, Copy)]
pub struct IncrementalStats {
    pub total_trades: u64,
    pub total_pnl: i64,
    pub gross_profit: i64,
    pub gross_loss: i64,
    pub win_count: u64,
}

#[derive(Debug, Clone)]
pub struct TradeLog {
    mode: TradeLogMode,
    fills: Vec<TradeFill>,
    fills_ring: VecDeque<TradeFill>,
    /// We keep Funding events in history/ring? 
    /// For simplicity, store *all* PnL events (trade deltas + funding) in a simplified log 
    /// for accurate Equity Curve / MaxDD even if fills are dropped.
    pnl_history: Vec<(TsExchangeNs, i64)>, 
    /// Incremental stats for SummaryOnly / RingBuffer modes.
    incremental: IncrementalStats,
}

impl Default for TradeLog {
    fn default() -> Self {
        Self::new(TradeLogMode::All)
    }
}

impl TradeLog {
    pub fn new(mode: TradeLogMode) -> Self {
        Self {
            mode,
            fills: Vec::new(),
            fills_ring: VecDeque::new(),
            pnl_history: Vec::new(),
            incremental: IncrementalStats::default(),
        }
    }

    pub fn mode(&self) -> TradeLogMode {
        self.mode
    }

    pub fn push_fill(&mut self, fill: TradeFill) {
        match self.mode {
            TradeLogMode::All => {
                self.fills.push(fill);
                self.update_incremental_fill();
            }
            TradeLogMode::RingBuffer(cap) => {
                if self.fills_ring.len() >= cap {
                    self.fills_ring.pop_front();
                }
                self.fills_ring.push_back(fill);
                self.update_incremental_fill();
            }
            TradeLogMode::SummaryOnly => {
                self.update_incremental_fill();
            }
            TradeLogMode::None => {}
        }
    }

    fn update_incremental_fill(&mut self) {
        self.incremental.total_trades += 1;
    }

    pub fn push_pnl_event(&mut self, event: PnlEvent) {
        // PnlEvent (Funding) is also a PnL entry.
        if self.mode != TradeLogMode::None {
             if self.mode != TradeLogMode::SummaryOnly {
                 // For All/RingBuffer, we might want to keep explicit PnlEvent objects if needed.
                 // But for stats, `pnl_history` is sufficient. 
                 // If we need to export "Funding Events" specifically, we might need a vector for them.
                 // Given the review, let's focus on Correct Stats first.
                 // We will simply append to pnl_history.
                 // Note: If we need strictly "Funding Events" list, we might need `pnl_events` vec back.
                 // Let's keep `pnl_history` as the Source of Truth for PnL.
             }
             self.pnl_history.push((event.ts_exchange, event.pnl));
        }
        self.update_incremental_pnl(event.pnl);
    }

    /// Update stats from a realized PnL delta (from trade).
    pub fn push_pnl_delta(&mut self, ts: TsExchangeNs, pnl: i64) {
        if self.mode != TradeLogMode::None {
            self.pnl_history.push((ts, pnl));
        }
        self.update_incremental_pnl(pnl);
    }

    fn update_incremental_pnl(&mut self, pnl: i64) {
        if self.mode == TradeLogMode::None { return; }
        
        self.incremental.total_pnl = self.incremental.total_pnl.saturating_add(pnl);
        if pnl > 0 {
            self.incremental.gross_profit = self.incremental.gross_profit.saturating_add(pnl);
            self.incremental.win_count += 1;
        } else if pnl < 0 {
            self.incremental.gross_loss = self.incremental.gross_loss.saturating_add(pnl);
        }
    }

    pub fn fills_iter(&self) -> Box<dyn Iterator<Item = &TradeFill> + '_> {
        match self.mode {
            TradeLogMode::All => Box::new(self.fills.iter()),
            TradeLogMode::RingBuffer(_) => Box::new(self.fills_ring.iter()),
            _ => Box::new(std::iter::empty()),
        }
    }

    pub fn fills_vec(&self) -> Vec<TradeFill> {
        self.fills_iter().copied().collect()
    }

    // Used for full PnL reconstruction
    pub fn pnl_history(&self) -> &[(TsExchangeNs, i64)] {
        &self.pnl_history
    }

    pub fn incremental_stats(&self) -> &IncrementalStats {
        &self.incremental
    }

    pub fn len(&self) -> usize {
        match self.mode {
            TradeLogMode::All => self.fills.len(),
            TradeLogMode::RingBuffer(_) => self.fills_ring.len(),
            // For SummaryOnly, we track count in incremental stats
            TradeLogMode::SummaryOnly => self.incremental.total_trades as usize,
            TradeLogMode::None => 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
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
    last_open_ts: TsExchangeNs,
}

fn clamp_i128_to_i64(v: i128) -> i64 {
    v.clamp(i64::MIN as i128, i64::MAX as i128) as i64
}

/// Information about a realized PnL delta and its holding period.
pub struct PnlDelta {
    pub ts_exchange: TsExchangeNs,
    pub pnl: i64,
    pub holding_period: i64,
}

/// Return realized PnL deltas (quote, scaled) per fill using an average-cost model.
///
/// Each entry is `PnlDelta`, where `pnl` is 0 for pure opens/adds,
/// and non-zero when the fill reduces or closes an existing position.
pub fn pnl_deltas_from_fills<'a>(fills: impl Iterator<Item = &'a TradeFill>) -> Vec<PnlDelta> {
    let mut state_by_symbol: BTreeMap<u32, PosState> = BTreeMap::new();
    let mut deltas: Vec<PnlDelta> = Vec::new();

    for f in fills {
        let qty = f.qty;
        if qty <= 0 {
            deltas.push(PnlDelta {
                ts_exchange: f.ts_exchange,
                pnl: 0,
                holding_period: 0,
            });
            continue;
        }

        let delta_qty: i64 = match f.side {
            Side::Buy => qty,
            Side::Sell => -qty,
            Side::None => 0,
        };
        if delta_qty == 0 {
            deltas.push(PnlDelta {
                ts_exchange: f.ts_exchange,
                pnl: 0,
                holding_period: 0,
            });
            continue;
        }

        let s = state_by_symbol.entry(f.symbol_id).or_default();

        // Open / add to position in the same direction.
        if s.qty == 0 || s.qty.signum() == delta_qty.signum() {
            if s.qty == 0 {
                s.last_open_ts = f.ts_exchange;
            }
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
            deltas.push(PnlDelta {
                ts_exchange: f.ts_exchange,
                pnl: 0,
                holding_period: 0,
            });
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

        let holding_period = if pnl_delta_i64 != 0 {
            f.ts_exchange.saturating_sub(s.last_open_ts)
        } else {
            0
        };

        let new_qty = s.qty.saturating_add(delta_qty);
        if new_qty == 0 {
            s.qty = 0;
            s.avg_price = 0;
            // Position closed, next fill will reset last_open_ts.
        } else if new_qty.signum() == s.qty.signum() {
            // Reduced but still same direction: keep avg_price and last_open_ts.
            s.qty = new_qty;
        } else {
            // Flipped: leftover opens at this fill price.
            s.qty = new_qty;
            s.avg_price = f.price;
            s.last_open_ts = f.ts_exchange;
        }

        deltas.push(PnlDelta {
            ts_exchange: f.ts_exchange,
            pnl: pnl_delta_i64,
            holding_period,
        });
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

pub fn sortino_ratio_from_pnl_series(pnl: &[i64]) -> f64 {
    let n = pnl.len();
    if n < 2 {
        return 0.0;
    }
    let mean = pnl.iter().map(|&x| x as f64).sum::<f64>() / n as f64;
    let downside_var = pnl
        .iter()
        .map(|&x| {
            let d = x as f64 - 0.0; // target return 0
            if d < 0.0 { d * d } else { 0.0 }
        })
        .sum::<f64>()
        / (n as f64);
    let downside_std = downside_var.sqrt();
    if downside_std == 0.0 {
        return 0.0;
    }
    mean / downside_std
}

/// Compute full time-ordered PnL history (trades + events) from the log.
pub fn full_pnl_history(trade_log: &TradeLog) -> Vec<(TsExchangeNs, i64)> {
    if trade_log.mode() == TradeLogMode::None {
        return Vec::new();
    }
    // Return clone of full history.
    trade_log.pnl_history().to_vec()
}

/// Compute basic stats from fills and PnL events (e.g. funding).
pub fn calculate_stats(trade_log: &TradeLog) -> BacktestStats {
    let inc = trade_log.incremental_stats();

    // If we have no pnl history (SummaryOnly or None), return incremental-derived stats.
    if trade_log.mode() == TradeLogMode::SummaryOnly || trade_log.mode() == TradeLogMode::None || trade_log.pnl_history().is_empty() {
        let win_rate = if inc.total_trades > 0 {
            inc.win_count as f64 / inc.total_trades as f64
        } else {
            0.0
        };
        let profit_factor = if inc.gross_loss < 0 {
            inc.gross_profit as f64 / (-inc.gross_loss) as f64
        } else if inc.gross_profit > 0 {
            f64::INFINITY
        } else {
            0.0
        };
        let avg_trade_pnl = if inc.total_trades > 0 {
            inc.total_pnl / inc.total_trades as i64
        } else {
            0
        };

        return BacktestStats {
            total_trades: inc.total_trades,
            win_rate,
            profit_factor,
            sharpe_ratio: 0.0,
            sortino_ratio: 0.0,
            max_drawdown: 0.0,
            max_drawdown_duration: 0,
            calmar_ratio: 0.0,
            total_pnl: inc.total_pnl,
            avg_trade_pnl,
            avg_holding_period: 0, 
            total_fees_paid: 0,
        };
    }

    let all_pnl_deltas = full_pnl_history(trade_log);

    // Calculate other non-PnL stats (holding period etc) which require access to fills
    // Re-running pnl_deltas_from_fills slightly inefficient but clean separation.
    let fills_iter = trade_log.fills_iter();
    let trade_deltas = pnl_deltas_from_fills(fills_iter);

    let mut total_holding_time: i64 = 0;
    let mut num_closed_trades: u64 = 0;

    for d in &trade_deltas {
        if d.holding_period > 0 {
            total_holding_time = total_holding_time.saturating_add(d.holding_period);
            num_closed_trades += 1;
        }
    }

    let mut total_pnl: i64 = 0;
    let mut gross_profit: i64 = 0;
    let mut gross_loss: i64 = 0;
    let mut pnl_series: Vec<i64> = Vec::with_capacity(all_pnl_deltas.len());
    let mut win_count = 0;

    for (_ts, d) in &all_pnl_deltas {
        total_pnl = total_pnl.saturating_add(*d);
        pnl_series.push(*d);
        if *d > 0 {
            gross_profit = gross_profit.saturating_add(*d);
        } else if *d < 0 {
            gross_loss = gross_loss.saturating_add(*d);
        }
    }

    win_count = inc.win_count; // Use incremental logic for consistency

    let curve = equity_curve_from_pnl_deltas(&all_pnl_deltas);
    let (max_dd_pct, max_dd_dur) = max_drawdown_pct_and_duration(&curve);

    let total_trades = inc.total_trades;

    let win_rate = if total_trades > 0 {
        win_count as f64 / total_trades as f64
    } else {
        0.0
    };

    let profit_factor = if gross_loss < 0 {
        gross_profit as f64 / (-gross_loss) as f64
    } else if gross_profit > 0 {
        f64::INFINITY
    } else {
        0.0
    };

    let sharpe = sharpe_ratio_from_pnl_series(&pnl_series);
    let sortino = sortino_ratio_from_pnl_series(&pnl_series);

    let calmar = if max_dd_pct > 0.0 {
        let pnl_units = total_pnl as f64 / FixedPoint::SCALE as f64;
        pnl_units / (max_dd_pct / 100.0)
    } else {
        0.0
    };

    let avg_trade_pnl = if total_trades > 0 {
        total_pnl / total_trades as i64
    } else {
        0
    };

    let avg_holding_period = if num_closed_trades > 0 {
        total_holding_time / num_closed_trades as i64
    } else {
        0
    };

    BacktestStats {
        total_trades,
        win_rate,
        profit_factor,
        sharpe_ratio: sharpe,
        sortino_ratio: sortino,
        max_drawdown: max_dd_pct,
        max_drawdown_duration: max_dd_dur,
        calmar_ratio: calmar,
        total_pnl,
        avg_trade_pnl,
        avg_holding_period,
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
        // Manually push PnL delta (simulating Engine)
        log.push_pnl_delta(2_000, 1_00000000); 

        let stats = calculate_stats(&log);
        assert_eq!(stats.total_pnl, 1_00000000);
    }

    #[test]
    fn test_stats_max_drawdown_matches_reference() {
        let curve = vec![(0_i64, 100), (10, 120), (20, 90), (30, 130)];
        let (dd, dur) = max_drawdown_pct_and_duration(&curve);
        assert!((dd - 25.0).abs() < 1e-12, "dd={dd}");
        assert_eq!(dur, 10);
    }

    #[test]
    fn test_stats_funding_pnl_timeseries() {
        let mut log = TradeLog::default();
        log.push_pnl_event(PnlEvent {
            ts_exchange: 10,
            pnl: 100_00000000,
        });
        log.push_pnl_event(PnlEvent {
            ts_exchange: 20,
            pnl: -20_00000000,
        });
        log.push_pnl_event(PnlEvent {
            ts_exchange: 30,
            pnl: 50_00000000,
        });

        let stats = calculate_stats(&log);
        assert_eq!(stats.total_pnl, 130_00000000);
        assert!((stats.max_drawdown - 20.0).abs() < 1e-12);
        assert_eq!(stats.max_drawdown_duration, 10);
    }

    #[test]
    fn test_stats_win_rate_and_profit_factor() {
        let mut log = TradeLog::default();
        log.push_fill(TradeFill {
            ts_exchange: 10,
            symbol_id: 1,
            order_id: 1,
            side: Side::Buy,
            price: 100_00000000,
            qty: 1_00000000,
        });
        log.push_fill(TradeFill {
            ts_exchange: 11,
            symbol_id: 1,
            order_id: 2,
            side: Side::Sell,
            price: 110_00000000,
            qty: 1_00000000,
        });
        log.push_pnl_delta(11, 10_00000000);

        log.push_fill(TradeFill {
            ts_exchange: 20,
            symbol_id: 1,
            order_id: 3,
            side: Side::Buy,
            price: 100_00000000,
            qty: 1_00000000,
        });
        log.push_fill(TradeFill {
            ts_exchange: 21,
            symbol_id: 1,
            order_id: 4,
            side: Side::Sell,
            price: 95_00000000,
            qty: 1_00000000,
        });
        log.push_pnl_delta(21, -5_00000000);

        let stats = calculate_stats(&log);
        assert_eq!(stats.total_trades, 4);
        assert_eq!(stats.win_rate, 0.25);
        assert_eq!(stats.profit_factor, 2.0);
    }

    #[test]
    fn test_trade_log_ring_buffer_caps_size() {
        let cap = 3;
        let mut log = TradeLog::new(TradeLogMode::RingBuffer(cap));
        for i in 0..5 {
            log.push_fill(TradeFill {
                ts_exchange: i * 1000,
                symbol_id: 1,
                order_id: i as u64,
                side: Side::Buy,
                price: 100_00000000,
                qty: 1_00000000,
            });
        }
        assert_eq!(log.len(), cap);
        let fills = log.fills_vec();
        assert_eq!(fills.len(), cap);
        assert_eq!(fills[0].order_id, 2);
    }

    #[test]
    fn test_stats_summary_only_matches_full_log_for_small_input() {
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
                price: 110_00000000, // +10 PnL
                qty: 1_00000000,
            },
            TradeFill {
                ts_exchange: 3_000,
                symbol_id: 1,
                order_id: 3,
                side: Side::Buy,
                price: 100_00000000,
                qty: 1_00000000,
            },
            TradeFill {
                ts_exchange: 4_000,
                symbol_id: 1,
                order_id: 4,
                side: Side::Sell,
                price: 95_00000000, // -5 PnL
                qty: 1_00000000,
            },
        ];

        // Run with All mode to get reference stats.
        let mut log_all = TradeLog::new(TradeLogMode::All);
        for f in &fills {
            log_all.push_fill(*f);
        }
        log_all.push_pnl_delta(2_000, 10_00000000);
        log_all.push_pnl_delta(4_000, -5_00000000);

        let stats_all = calculate_stats(&log_all);

        // Run with SummaryOnly mode
        let mut log_summary = TradeLog::new(TradeLogMode::SummaryOnly);
        for f in &fills {
            log_summary.push_fill(*f);
        }
        log_summary.push_pnl_delta(2_000, 10_00000000);
        log_summary.push_pnl_delta(4_000, -5_00000000);

        let inc_stats = log_summary.incremental_stats();

        assert_eq!(inc_stats.total_pnl, stats_all.total_pnl);
        assert_eq!(inc_stats.total_trades, stats_all.total_trades);
        assert_eq!(inc_stats.win_count, 1);
    }
}
