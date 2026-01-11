use crate::types::{FundingEvent, L2Update, Order, OrderReport, Tick, TsSimNs};

/// Stable identifier for ordering events at the same simulated timestamp.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EventId {
    pub ts_sim: TsSimNs,
    pub seq: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventKind {
    /// Market truth (exchange-time) tick/trade.
    Tick(Tick),
    /// Feed-delivered tick/trade (strategy-observed).
    TickDelivery(Tick),
    L2Update(L2Update),
    /// Strategy order request arriving at the exchange.
    Order(Order),
    /// Exchange ACK for a new order (PendingNew -> Open).
    OrderAck {
        order_id: u64,
    },
    /// Strategy cancel request arriving at the exchange.
    OrderCancel {
        order_id: u64,
    },
    /// Exchange ACK for cancel (PendingCancel -> Cancelled).
    OrderCancelAck {
        order_id: u64,
    },
    /// Strategy-facing order update (fills/cancels/rejects).
    OrderReport(OrderReport),
    Funding(FundingEvent),
    Timer {
        timer_id: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Event {
    pub id: EventId,
    pub kind: EventKind,
}

impl Event {
    pub fn ts_sim(&self) -> TsSimNs {
        self.id.ts_sim
    }

    pub fn seq(&self) -> u64 {
        self.id.seq
    }
}
