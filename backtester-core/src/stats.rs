use std::collections::BTreeMap;
use std::collections::VecDeque;

use rayon::prelude::*;
use wide::f64x4;

use crate::types::{FixedPoint, Side, TsExchangeNs};

/// A single executed fill (logical trade log row).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct TradeFill {
    pub ts_exchange: TsExchangeNs,
    pub order_id: u64,
    pub price: i64,
    pub qty: i64,
    pub symbol_id: u32,
    /// Side of the *strategy's* order which got filled.
    pub side: Side,
}

/// Arbitrary PnL event (e.g. funding payment).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
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
#[derive(Debug, Default, Clone)]
pub struct IncrementalStats {
    pub total_trades: u64,
    pub total_pnl: i64,
    pub gross_profit: i64,
    pub gross_loss: i64,
    pub win_count: u64,
    total_holding_period: i64,
    closed_trades: u64,
    pnl_count: u64,
    pnl_mean: f64,
    pnl_m2: f64,
    downside_sq_sum: f64,
    equity: i64,
    peak_equity: i64,
    peak_ts: TsExchangeNs,
    max_drawdown_pct: f64,
    max_drawdown_duration: i64,
    pos_state: BTreeMap<u32, PosState>,
}

impl IncrementalStats {
    fn on_fill(&mut self, fill: &TradeFill) {
        self.total_trades += 1;

        let qty = fill.qty;
        if qty <= 0 {
            return;
        }

        let delta_qty: i64 = match fill.side {
            Side::Buy => qty,
            Side::Sell => -qty,
            Side::None => 0,
        };
        if delta_qty == 0 {
            return;
        }

        let s = self.pos_state.entry(fill.symbol_id).or_default();

        // Open / add to position in the same direction.
        if s.qty == 0 || s.qty.signum() == delta_qty.signum() {
            if s.qty == 0 {
                s.last_open_ts = fill.ts_exchange;
            }
            let new_qty = s.qty.saturating_add(delta_qty);
            let abs_old = s.qty.abs() as i128;
            let abs_delta = delta_qty.abs() as i128;
            let new_abs = abs_old + abs_delta;
            if new_abs > 0 {
                let weighted = (s.avg_price as i128 * abs_old) + (fill.price as i128 * abs_delta);
                s.avg_price = clamp_i128_to_i64(weighted / new_abs);
            } else {
                s.avg_price = 0;
            }
            s.qty = new_qty;
            return;
        }

        // Reduce / close / flip.
        let abs_old = s.qty.abs() as i128;
        let abs_delta = delta_qty.abs() as i128;
        let close_abs = abs_old.min(abs_delta);

        let pnl_per_unit: i128 = if s.qty > 0 {
            (fill.price - s.avg_price) as i128
        } else {
            (s.avg_price - fill.price) as i128
        };
        let pnl_i128 = (pnl_per_unit.saturating_mul(close_abs)) / FixedPoint::SCALE as i128;
        let pnl_delta_i64 = clamp_i128_to_i64(pnl_i128);

        if pnl_delta_i64 != 0 {
            let holding_period = fill.ts_exchange.saturating_sub(s.last_open_ts);
            self.total_holding_period = self.total_holding_period.saturating_add(holding_period);
            self.closed_trades += 1;
        }

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
            s.avg_price = fill.price;
            s.last_open_ts = fill.ts_exchange;
        }
    }

    fn on_pnl(&mut self, ts: TsExchangeNs, pnl: i64, is_trade: bool) {
        self.total_pnl = self.total_pnl.saturating_add(pnl);
        if is_trade {
            if pnl > 0 {
                self.gross_profit = self.gross_profit.saturating_add(pnl);
                self.win_count += 1;
            } else if pnl < 0 {
                self.gross_loss = self.gross_loss.saturating_add(pnl);
            }
        }

        // PnL series stats (Sharpe/Sortino).
        self.pnl_count += 1;
        let pnl_f = pnl as f64;
        let delta = pnl_f - self.pnl_mean;
        self.pnl_mean += delta / self.pnl_count as f64;
        let delta2 = pnl_f - self.pnl_mean;
        self.pnl_m2 += delta * delta2;
        if pnl < 0 {
            self.downside_sq_sum += pnl_f * pnl_f;
        }

        // Equity curve + max drawdown tracking.
        self.equity = self.equity.saturating_add(pnl);
        if self.pnl_count == 1 {
            self.peak_equity = self.equity;
            self.peak_ts = ts;
            return;
        }

        if self.equity > self.peak_equity {
            self.peak_equity = self.equity;
            self.peak_ts = ts;
            return;
        }

        if self.peak_equity <= 0 {
            return;
        }

        let dd = (self.peak_equity - self.equity) as f64 / self.peak_equity as f64;
        let dd_pct = (dd * 100.0).min(100.0);
        if dd_pct > self.max_drawdown_pct {
            self.max_drawdown_pct = dd_pct;
            self.max_drawdown_duration = ts.saturating_sub(self.peak_ts);
        }
    }

    fn sharpe_ratio(&self) -> f64 {
        if self.pnl_count < 2 {
            return 0.0;
        }
        let var = self.pnl_m2 / (self.pnl_count as f64 - 1.0);
        let std = var.sqrt();
        if std == 0.0 {
            return 0.0;
        }
        self.pnl_mean / std
    }

    fn sortino_ratio(&self) -> f64 {
        if self.pnl_count < 2 {
            return 0.0;
        }
        let downside_var = self.downside_sq_sum / self.pnl_count as f64;
        let downside_std = downside_var.sqrt();
        if downside_std == 0.0 {
            return 0.0;
        }
        self.pnl_mean / downside_std
    }

    fn avg_holding_period(&self) -> i64 {
        if self.closed_trades > 0 {
            self.total_holding_period / self.closed_trades as i64
        } else {
            0
        }
    }
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
                self.update_incremental_fill(&fill);
            }
            TradeLogMode::RingBuffer(cap) => {
                if self.fills_ring.len() >= cap {
                    self.fills_ring.pop_front();
                }
                self.fills_ring.push_back(fill);
                self.update_incremental_fill(&fill);
            }
            TradeLogMode::SummaryOnly => {
                self.update_incremental_fill(&fill);
            }
            TradeLogMode::None => {}
        }
    }

    fn update_incremental_fill(&mut self, fill: &TradeFill) {
        self.incremental.on_fill(fill);
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
        self.update_incremental_pnl(event.ts_exchange, event.pnl, false);
    }

    /// Update stats from a realized PnL delta (from trade).
    pub fn push_pnl_delta(&mut self, ts: TsExchangeNs, pnl: i64) {
        if self.mode != TradeLogMode::None {
            self.pnl_history.push((ts, pnl));
        }
        self.update_incremental_pnl(ts, pnl, true);
    }

    fn update_incremental_pnl(&mut self, ts: TsExchangeNs, pnl: i64, is_trade: bool) {
        if self.mode == TradeLogMode::None {
            return;
        }
        self.incremental.on_pnl(ts, pnl, is_trade);
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

/// SIMD-friendly (unrolled) equity curve prefix sum.
pub fn equity_curve_from_pnl_deltas_simd(
    deltas: &[(TsExchangeNs, i64)],
) -> Vec<(TsExchangeNs, i64)> {
    let mut equity: i64 = 0;
    let mut curve: Vec<(TsExchangeNs, i64)> = Vec::with_capacity(deltas.len());

    let mut i = 0;
    while i + 4 <= deltas.len() {
        let (ts0, d0) = deltas[i];
        let (ts1, d1) = deltas[i + 1];
        let (ts2, d2) = deltas[i + 2];
        let (ts3, d3) = deltas[i + 3];

        let e0 = equity.saturating_add(d0);
        let e1 = e0.saturating_add(d1);
        let e2 = e1.saturating_add(d2);
        let e3 = e2.saturating_add(d3);

        curve.push((ts0, e0));
        curve.push((ts1, e1));
        curve.push((ts2, e2));
        curve.push((ts3, e3));

        equity = e3;
        i += 4;
    }

    for (ts, d) in &deltas[i..] {
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
            max_dd = dd.min(1.0);
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

fn simd_reduce_sum(v: f64x4) -> f64 {
    let arr: [f64; 4] = v.into();
    arr.into_iter().sum()
}

fn simd_sum_f64_from_i64(pnl: &[i64]) -> f64 {
    let mut sum_vec = f64x4::splat(0.0);
    let mut chunks = pnl.chunks_exact(4);
    for chunk in &mut chunks {
        let v = f64x4::from([
            chunk[0] as f64,
            chunk[1] as f64,
            chunk[2] as f64,
            chunk[3] as f64,
        ]);
        sum_vec += v;
    }
    let mut sum = simd_reduce_sum(sum_vec);
    for &x in chunks.remainder() {
        sum += x as f64;
    }
    sum
}

fn simd_sum_sq_diff(pnl: &[i64], mean: f64) -> f64 {
    let mut sum_vec = f64x4::splat(0.0);
    let mean_vec = f64x4::splat(mean);
    let mut chunks = pnl.chunks_exact(4);
    for chunk in &mut chunks {
        let v = f64x4::from([
            chunk[0] as f64,
            chunk[1] as f64,
            chunk[2] as f64,
            chunk[3] as f64,
        ]);
        let d = v - mean_vec;
        sum_vec += d * d;
    }
    let mut sum = simd_reduce_sum(sum_vec);
    for &x in chunks.remainder() {
        let d = x as f64 - mean;
        sum += d * d;
    }
    sum
}

fn simd_sum_downside_sq(pnl: &[i64]) -> f64 {
    let mut sum_vec = f64x4::splat(0.0);
    let zero = f64x4::splat(0.0);
    let mut chunks = pnl.chunks_exact(4);
    for chunk in &mut chunks {
        let v = f64x4::from([
            chunk[0] as f64,
            chunk[1] as f64,
            chunk[2] as f64,
            chunk[3] as f64,
        ]);
        let neg = v.min(zero);
        sum_vec += neg * neg;
    }
    let mut sum = simd_reduce_sum(sum_vec);
    for &x in chunks.remainder() {
        let d = x as f64;
        if d < 0.0 {
            sum += d * d;
        }
    }
    sum
}

/// SIMD-accelerated Sharpe ratio calculation (mean/std over PnL series).
pub fn sharpe_ratio_from_pnl_series_simd(pnl: &[i64]) -> f64 {
    let n = pnl.len();
    if n < 2 {
        return 0.0;
    }
    let mean = simd_sum_f64_from_i64(pnl) / n as f64;
    let var = simd_sum_sq_diff(pnl, mean) / (n as f64 - 1.0);
    let std = var.sqrt();
    if std == 0.0 {
        return 0.0;
    }
    mean / std
}

/// SIMD-accelerated Sortino ratio calculation (mean / downside std).
pub fn sortino_ratio_from_pnl_series_simd(pnl: &[i64]) -> f64 {
    let n = pnl.len();
    if n < 2 {
        return 0.0;
    }
    let mean = simd_sum_f64_from_i64(pnl) / n as f64;
    let downside_var = simd_sum_downside_sq(pnl) / n as f64;
    let downside_std = downside_var.sqrt();
    if downside_std == 0.0 {
        return 0.0;
    }
    mean / downside_std
}

/// Parallel Sharpe ratio calculation using Rayon.
pub fn sharpe_ratio_from_pnl_series_parallel(pnl: &[i64]) -> f64 {
    let n = pnl.len();
    if n < 2 {
        return 0.0;
    }

    let (sum, sum_sq) = pnl
        .par_iter()
        .map(|&x| {
            let val = x as f64;
            (val, val * val)
        })
        .reduce(|| (0.0, 0.0), |acc, val| (acc.0 + val.0, acc.1 + val.1));

    let mean = sum / n as f64;
    // Variance = E[X^2] - (E[X])^2
    // Var(sample) = (Sum(x^2) - n*mean^2) / (n - 1)
    let numerator = sum_sq - (n as f64 * mean * mean);
    // Ensure non-negative due to float precision
    let var = numerator.max(0.0) / (n as f64 - 1.0);
    let std = var.sqrt();

    if std == 0.0 {
        return 0.0;
    }
    mean / std
}

/// Parallel Sortino ratio calculation using Rayon.
pub fn sortino_ratio_from_pnl_series_parallel(pnl: &[i64]) -> f64 {
    let n = pnl.len();
    if n < 2 {
        return 0.0;
    }

    let (sum, downside_sq_sum) = pnl
        .par_iter()
        .map(|&x| {
            let val = x as f64;
            let d = if val < 0.0 { val * val } else { 0.0 };
            (val, d)
        })
        .reduce(|| (0.0, 0.0), |acc, val| (acc.0 + val.0, acc.1 + val.1));

    let mean = sum / n as f64;
    let downside_var = downside_sq_sum / n as f64;
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
    if trade_log.mode() == TradeLogMode::None {
        return BacktestStats::default();
    }

    let total_trades = inc.total_trades;
    let win_rate = if total_trades > 0 {
        inc.win_count as f64 / total_trades as f64
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

    let pnl_history_len = trade_log.pnl_history().len();
    let (sharpe, sortino) = if pnl_history_len > 10_000 {
        // Use parallel implementation for large datasets
        let pnl_vec: Vec<i64> = trade_log
            .pnl_history()
            .iter()
            .map(|&(_, pnl)| pnl)
            .collect();
        (
            sharpe_ratio_from_pnl_series_parallel(&pnl_vec),
            sortino_ratio_from_pnl_series_parallel(&pnl_vec),
        )
    } else {
        (inc.sharpe_ratio(), inc.sortino_ratio())
    };
    let max_dd_pct = inc.max_drawdown_pct;
    let max_dd_dur = inc.max_drawdown_duration;

    let calmar = if max_dd_pct > 0.0 {
        let pnl_units = inc.total_pnl as f64 / FixedPoint::SCALE as f64;
        pnl_units / (max_dd_pct / 100.0)
    } else {
        0.0
    };

    let avg_trade_pnl = if total_trades > 0 {
        inc.total_pnl / total_trades as i64
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
        total_pnl: inc.total_pnl,
        avg_trade_pnl,
        avg_holding_period: inc.avg_holding_period(),
        total_fees_paid: 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn calculate_stats_batch(trade_log: &TradeLog) -> BacktestStats {
        if trade_log.mode() == TradeLogMode::None {
            return BacktestStats::default();
        }

        let fills = trade_log.fills_vec();
        let total_trades = fills.len() as u64;
        let trade_deltas = pnl_deltas_from_fills(fills.iter());

        let all_pnl_deltas = full_pnl_history(trade_log);

        let mut total_holding_time: i64 = 0;
        let mut num_closed_trades: u64 = 0;

        for d in &trade_deltas {
            if d.holding_period > 0 {
                total_holding_time = total_holding_time.saturating_add(d.holding_period);
                num_closed_trades += 1;
            }
        }

        let mut total_pnl: i64 = 0;
        let mut pnl_series: Vec<i64> = Vec::with_capacity(all_pnl_deltas.len());

        for (_ts, d) in &all_pnl_deltas {
            total_pnl = total_pnl.saturating_add(*d);
            pnl_series.push(*d);
        }

        let mut gross_profit: i64 = 0;
        let mut gross_loss: i64 = 0;
        let mut win_count: u64 = 0;
        for d in &trade_deltas {
            if d.pnl > 0 {
                gross_profit = gross_profit.saturating_add(d.pnl);
                win_count += 1;
            } else if d.pnl < 0 {
                gross_loss = gross_loss.saturating_add(d.pnl);
            }
        }

        let curve = equity_curve_from_pnl_deltas(&all_pnl_deltas);
        let (max_dd_pct, max_dd_dur) = max_drawdown_pct_and_duration(&curve);

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
    fn test_stats_max_drawdown_clamped_to_100() {
        let curve = vec![(0_i64, 10), (10, -5)];
        let (dd, dur) = max_drawdown_pct_and_duration(&curve);
        assert!((dd - 100.0).abs() < 1e-12, "dd={dd}");
        assert_eq!(dur, 10);
    }

    #[test]
    fn test_stats_max_drawdown_peak_nonpositive_is_zero() {
        let mut log = TradeLog::new(TradeLogMode::SummaryOnly);
        log.push_pnl_event(PnlEvent {
            ts_exchange: 10,
            pnl: -10_00000000,
        });
        log.push_pnl_event(PnlEvent {
            ts_exchange: 20,
            pnl: -5_00000000,
        });

        let stats = calculate_stats(&log);
        assert_eq!(stats.max_drawdown, 0.0);
        assert_eq!(stats.max_drawdown_duration, 0);
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
    fn test_stats_win_rate_excludes_funding_pnl() {
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
        log.push_pnl_event(PnlEvent {
            ts_exchange: 12,
            pnl: 5_00000000,
        });

        let stats = calculate_stats(&log);
        assert_eq!(stats.total_trades, 2);
        assert_eq!(stats.win_rate, 0.5);
        assert!(stats.profit_factor.is_infinite());
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

    #[test]
    fn test_incremental_stats_matches_batch_calculation() {
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

        let mut log_all = TradeLog::new(TradeLogMode::All);
        for f in &fills {
            log_all.push_fill(*f);
        }
        log_all.push_pnl_delta(2_000, 10_00000000);
        log_all.push_pnl_delta(4_000, -5_00000000);

        let expected = calculate_stats_batch(&log_all);

        let mut log_summary = TradeLog::new(TradeLogMode::SummaryOnly);
        for f in &fills {
            log_summary.push_fill(*f);
        }
        log_summary.push_pnl_delta(2_000, 10_00000000);
        log_summary.push_pnl_delta(4_000, -5_00000000);

        let actual = calculate_stats(&log_summary);

        assert_eq!(actual.total_trades, expected.total_trades);
        assert_eq!(actual.total_pnl, expected.total_pnl);
        assert_eq!(actual.avg_trade_pnl, expected.avg_trade_pnl);
        assert_eq!(actual.avg_holding_period, expected.avg_holding_period);
        assert_eq!(actual.max_drawdown_duration, expected.max_drawdown_duration);
        assert!((actual.win_rate - expected.win_rate).abs() < 1e-12);
        assert!((actual.profit_factor - expected.profit_factor).abs() < 1e-12);
        assert!((actual.sharpe_ratio - expected.sharpe_ratio).abs() < 1e-12);
        assert!((actual.sortino_ratio - expected.sortino_ratio).abs() < 1e-12);
        assert!((actual.max_drawdown - expected.max_drawdown).abs() < 1e-12);
        assert!((actual.calmar_ratio - expected.calmar_ratio).abs() < 1e-12);
    }

    #[test]
    fn test_incremental_stats_matches_batch_with_partials_and_flips() {
        let fills = vec![
            TradeFill {
                ts_exchange: 1_000,
                symbol_id: 1,
                order_id: 1,
                side: Side::Buy,
                price: 100_00000000,
                qty: 2_00000000,
            },
            TradeFill {
                ts_exchange: 2_000,
                symbol_id: 1,
                order_id: 2,
                side: Side::Sell,
                price: 110_00000000,
                qty: 1_00000000,
            },
            TradeFill {
                ts_exchange: 3_000,
                symbol_id: 1,
                order_id: 3,
                side: Side::Buy,
                price: 105_00000000,
                qty: 1_00000000,
            },
            TradeFill {
                ts_exchange: 4_000,
                symbol_id: 1,
                order_id: 4,
                side: Side::Sell,
                price: 95_00000000,
                qty: 3_00000000,
            },
            TradeFill {
                ts_exchange: 5_000,
                symbol_id: 1,
                order_id: 5,
                side: Side::Buy,
                price: 90_00000000,
                qty: 1_00000000,
            },
        ];

        let deltas = pnl_deltas_from_fills(fills.iter());

        let mut log_all = TradeLog::new(TradeLogMode::All);
        for f in &fills {
            log_all.push_fill(*f);
        }
        for d in &deltas {
            log_all.push_pnl_delta(d.ts_exchange, d.pnl);
        }
        let expected = calculate_stats_batch(&log_all);

        let mut log_summary = TradeLog::new(TradeLogMode::SummaryOnly);
        for f in &fills {
            log_summary.push_fill(*f);
        }
        for d in &deltas {
            log_summary.push_pnl_delta(d.ts_exchange, d.pnl);
        }
        let actual = calculate_stats(&log_summary);

        assert_eq!(actual.total_trades, expected.total_trades);
        assert_eq!(actual.total_pnl, expected.total_pnl);
        assert_eq!(actual.avg_trade_pnl, expected.avg_trade_pnl);
        assert_eq!(actual.avg_holding_period, expected.avg_holding_period);
        assert_eq!(actual.max_drawdown_duration, expected.max_drawdown_duration);
        assert!((actual.win_rate - expected.win_rate).abs() < 1e-12);
        assert!((actual.profit_factor - expected.profit_factor).abs() < 1e-12);
        assert!((actual.sharpe_ratio - expected.sharpe_ratio).abs() < 1e-12);
        assert!((actual.sortino_ratio - expected.sortino_ratio).abs() < 1e-12);
        assert!((actual.max_drawdown - expected.max_drawdown).abs() < 1e-12);
        assert!((actual.calmar_ratio - expected.calmar_ratio).abs() < 1e-12);
    }

    #[test]
    fn test_simd_sharpe_sortino_matches_scalar() {
        let pnl = vec![
            -5_000_000, 10_000_000, -2_500_000, 7_500_000, 0, 1_250_000, -3_750_000, 9_000_000,
            -1_000_000, 4_000_000, 2_000_000,
        ];

        let sharpe_scalar = sharpe_ratio_from_pnl_series(&pnl);
        let sortino_scalar = sortino_ratio_from_pnl_series(&pnl);

        let sharpe_simd = sharpe_ratio_from_pnl_series_simd(&pnl);
        let sortino_simd = sortino_ratio_from_pnl_series_simd(&pnl);

        assert!((sharpe_simd - sharpe_scalar).abs() < 1e-10);
        assert!((sortino_simd - sortino_scalar).abs() < 1e-10);
    }

    #[test]
    fn test_equity_curve_simd_matches_scalar() {
        let deltas: Vec<(TsExchangeNs, i64)> = (0..17)
            .map(|i| {
                let pnl = match i % 4 {
                    0 => 5_000_000,
                    1 => -3_000_000,
                    2 => 2_000_000,
                    _ => -1_000_000,
                };
                (1_000 + i as i64 * 10, pnl)
            })
            .collect();

        let scalar = equity_curve_from_pnl_deltas(&deltas);
        let simd = equity_curve_from_pnl_deltas_simd(&deltas);

        assert_eq!(simd, scalar);
    }

    #[test]
    fn test_stats_parallel_matches_sequential() {
        use rand::Rng;
        use rand::SeedableRng;
        use rand::rngs::StdRng;

        let mut rng = StdRng::seed_from_u64(42);
        let mut pnl_series = Vec::with_capacity(100_000);
        for _ in 0..100_000 {
            // Random PnL between -1000 and +1000
            pnl_series.push(rng.gen_range(-1000..=1000));
        }

        let scalar_sharpe = sharpe_ratio_from_pnl_series(&pnl_series);
        let parallel_sharpe = sharpe_ratio_from_pnl_series_parallel(&pnl_series);

        assert!(
            (scalar_sharpe - parallel_sharpe).abs() < 1e-9,
            "Sharpe mismatch: scalar={}, parallel={}",
            scalar_sharpe,
            parallel_sharpe
        );

        let scalar_sortino = sortino_ratio_from_pnl_series(&pnl_series);
        let parallel_sortino = sortino_ratio_from_pnl_series_parallel(&pnl_series);

        assert!(
            (scalar_sortino - parallel_sortino).abs() < 1e-9,
            "Sortino mismatch: scalar={}, parallel={}",
            scalar_sortino,
            parallel_sortino
        );
    }
}
