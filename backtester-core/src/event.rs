use crate::types::{L2Update, Order, Tick};

/// Stable identifier for ordering events at the same simulated timestamp.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EventId {
    pub ts_sim: i64,
    pub seq: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventKind {
    Tick(Tick),
    L2Update(L2Update),
    Order(Order),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Event {
    pub id: EventId,
    pub kind: EventKind,
}

impl Event {
    pub fn ts_sim(&self) -> i64 {
        self.id.ts_sim
    }

    pub fn seq(&self) -> u64 {
        self.id.seq
    }
}
