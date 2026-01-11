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

/// Tick (trade/quote) logical representation for callbacks/logging.
///
/// Note: `seq` is included to support deterministic ordering within a stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Tick {
    pub ts_exchange: i64,
    pub ts_local: i64,
    pub seq: u64,
    pub symbol_id: u32,
    pub price: i64,
    pub qty: i64,
    pub side: Side,
    pub flags: u8,
}

/// L2 order book update (price level).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct L2Update {
    pub ts_exchange: i64,
    pub seq: u64,
    pub symbol_id: u32,
    pub price: i64,
    pub qty: i64, // 0 = remove level
    pub side: Side,
}

/// A minimal limit order representation (scaffolding).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Order {
    pub order_id: u64,
    pub ts_submit: i64,
    pub seq: u64,
    pub symbol_id: u32,
    pub side: Side,
    pub price: i64,
    pub qty: i64,
}

