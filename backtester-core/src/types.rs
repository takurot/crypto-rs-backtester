/// Time axis: exchange (ground truth) timestamp in nanoseconds.
pub type TsExchangeNs = i64;
/// Time axis: local (strategy-observed) timestamp in nanoseconds.
pub type TsLocalNs = i64;
/// Time axis: simulation clock timestamp in nanoseconds.
pub type TsSimNs = i64;

/// Fixed-point value with satoshi precision (1e-8).
///
/// Design rule: `f64` is allowed only at I/O boundaries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct FixedPoint(i64);

impl FixedPoint {
    pub const SCALE: i64 = 100_000_000;

    pub fn from_scaled_i64(v: i64) -> Self {
        Self(v)
    }

    pub fn as_scaled_i64(self) -> i64 {
        self.0
    }

    /// Convert from `f64` to fixed-point. I/O only.
    pub fn from_f64_io_only(v: f64) -> Self {
        Self((v * Self::SCALE as f64).round() as i64)
    }

    /// Convert to `f64`. I/O only.
    pub fn to_f64_io_only(self) -> f64 {
        self.0 as f64 / Self::SCALE as f64
    }

    /// Multiply two fixed-point values: (a*b)/SCALE
    pub fn mul_scaled(self, rhs: Self) -> Self {
        let v = (self.0 as i128 * rhs.0 as i128) / Self::SCALE as i128;
        Self(v.clamp(i64::MIN as i128, i64::MAX as i128) as i64)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i8)]
pub enum Side {
    Buy = 1,
    Sell = -1,
    None = 0,
}

impl Side {
    pub fn as_i8(self) -> i8 {
        self as i8
    }
}

impl TryFrom<i8> for Side {
    type Error = &'static str;

    fn try_from(value: i8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Buy),
            -1 => Ok(Self::Sell),
            0 => Ok(Self::None),
            _ => Err("invalid Side (expected 1, -1, or 0)"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum OrderType {
    Limit = 0,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum OrderState {
    Created = 0,
    PendingNew = 1,
    Open = 2,
    PartiallyFilled = 3,
    PendingCancel = 4,
    Cancelled = 5,
    Filled = 6,
    Rejected = 7,
}

impl OrderState {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Cancelled | Self::Filled | Self::Rejected)
    }

    pub fn can_transition_to(self, next: Self) -> bool {
        use OrderState::*;
        matches!(
            (self, next),
            // Create/submit
            (Created, PendingNew)
                // ACK/reject
                | (PendingNew, Open)
                | (PendingNew, Rejected)
                // Normal lifecycle
                | (Open, PartiallyFilled)
                | (Open, Filled)
                | (Open, PendingCancel)
                | (PartiallyFilled, PartiallyFilled)
                | (PartiallyFilled, Filled)
                | (PartiallyFilled, PendingCancel)
                // Cancel-in-flight can still fill
                | (PendingCancel, Cancelled)
                | (PendingCancel, Filled)
        )
    }
}

/// A minimal order update message for strategy callbacks (fills/cancels/rejects).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct OrderReport {
    pub order_id: u64,
    pub last_fill_qty: i64,
    pub last_fill_price: i64,
    pub filled_qty: i64,
    pub remaining_qty: i64,
    pub reason: Option<&'static str>,
    pub symbol_id: u32,
    pub status: OrderState,
}

/// Funding payment event (perpetual futures).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct FundingEvent {
    pub ts_exchange: TsExchangeNs,
    /// Fixed-point rate (scaled by 1e8). e.g. 0.0001 => 10_000.
    pub rate: i64,
    pub symbol_id: u32,
}

/// Tick (trade/quote) logical representation for callbacks/logging.
///
/// Note: `seq` is included to support deterministic ordering within a stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct Tick {
    pub ts_exchange: TsExchangeNs,
    pub ts_local: TsLocalNs,
    pub seq: u64,
    pub price: i64,
    pub qty: i64,
    pub symbol_id: u32,
    pub side: Side,
    pub flags: u8,
}

/// L2 order book update (price level).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct L2Update {
    pub ts_exchange: TsExchangeNs,
    pub seq: u64,
    pub price: i64,
    pub qty: i64, // 0 = remove level
    pub symbol_id: u32,
    pub side: Side,
}

/// A minimal limit order representation (scaffolding).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct Order {
    pub order_id: u64,
    pub ts_submit: TsLocalNs,
    pub seq: u64,
    pub price: i64,
    pub qty: i64,
    pub symbol_id: u32,
    pub side: Side,
    pub order_type: OrderType,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stats::TradeFill;

    #[test]
    fn test_fixed_point_roundtrip_io_only() {
        let x = FixedPoint::from_scaled_i64(123 * FixedPoint::SCALE);
        let y = FixedPoint::from_f64_io_only(x.to_f64_io_only());
        assert_eq!(x, y);
    }

    #[test]
    fn test_order_state_machine_basic_invariants() {
        assert!(OrderState::Filled.is_terminal());
        assert!(OrderState::Cancelled.is_terminal());
        assert!(OrderState::Rejected.is_terminal());
        assert!(!OrderState::Open.is_terminal());

        assert!(OrderState::PendingNew.can_transition_to(OrderState::Open));
        assert!(OrderState::Open.can_transition_to(OrderState::PendingCancel));
        assert!(OrderState::PendingCancel.can_transition_to(OrderState::Cancelled));

        assert!(!OrderState::Filled.can_transition_to(OrderState::Open));
    }

    #[cfg(target_pointer_width = "64")]
    #[test]
    fn test_struct_layout_sizes_for_cache() {
        use std::mem::size_of;

        assert_eq!(size_of::<Tick>(), 48);
        assert_eq!(size_of::<L2Update>(), 40);
        assert_eq!(size_of::<Order>(), 48);
        assert_eq!(size_of::<OrderReport>(), 64);
        assert_eq!(size_of::<TradeFill>(), 40);
    }
}
