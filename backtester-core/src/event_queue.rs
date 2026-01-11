use std::cmp::Ordering;
use std::collections::BinaryHeap;

use crate::event::Event;

#[derive(Debug, Clone, Copy)]
struct QueuedEvent(Event);

impl PartialEq for QueuedEvent {
    fn eq(&self, other: &Self) -> bool {
        self.0.id == other.0.id
    }
}

impl Eq for QueuedEvent {}

impl PartialOrd for QueuedEvent {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for QueuedEvent {
    fn cmp(&self, other: &Self) -> Ordering {
        // `BinaryHeap` is a max-heap; reverse ordering so that the smallest
        // (earliest) (ts_sim, seq) pops first.
        other.0.id.cmp(&self.0.id)
    }
}

/// Deterministic global event queue ordered by (`ts_sim`, `seq`).
#[derive(Debug, Default)]
pub struct EventQueue {
    heap: BinaryHeap<QueuedEvent>,
}

impl EventQueue {
    pub fn new() -> Self {
        Self {
            heap: BinaryHeap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.heap.len()
    }

    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

    pub fn peek(&self) -> Option<&Event> {
        self.heap.peek().map(|qe| &qe.0)
    }

    pub fn push(&mut self, event: Event) {
        self.heap.push(QueuedEvent(event));
    }

    pub fn pop(&mut self) -> Option<Event> {
        self.heap.pop().map(|qe| qe.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::EventKind;
    use crate::fixtures;

    #[test]
    fn test_global_event_queue_orders_by_ts_sim_then_tiebreak() {
        let mut q = EventQueue::new();

        // Mix timestamps and seq values; ordering must be by (ts_sim asc, seq asc).
        q.push(fixtures::event_tick(
            2_000,
            0,
            fixtures::tick_trade(2_000, 2_000, 0),
        ));
        q.push(fixtures::event_tick(
            1_000,
            2,
            fixtures::tick_trade(1_000, 1_000, 2),
        ));
        q.push(fixtures::event_tick(
            1_000,
            1,
            fixtures::tick_trade(1_000, 1_000, 1),
        ));
        q.push(fixtures::event_tick(
            3_000,
            0,
            fixtures::tick_trade(3_000, 3_000, 0),
        ));

        let popped: Vec<(i64, u64)> = (0..4)
            .map(|_| {
                let e = q.pop().expect("event");
                (e.ts_sim(), e.seq())
            })
            .collect();

        assert_eq!(popped, vec![(1_000, 1), (1_000, 2), (2_000, 0), (3_000, 0)]);
    }

    #[test]
    fn test_event_queue_tiebreak_stable() {
        let ts_sim = 1_000;

        // Intentionally push out-of-order seq values; pop must be ordered by seq.
        let e2 = fixtures::event_tick(ts_sim, 2, fixtures::tick_trade(ts_sim, ts_sim, 2));
        let e0 = fixtures::event_tick(ts_sim, 0, fixtures::tick_trade(ts_sim, ts_sim, 0));
        let e1 = fixtures::event_tick(ts_sim, 1, fixtures::tick_trade(ts_sim, ts_sim, 1));

        let mut q = EventQueue::new();
        q.push(e2);
        q.push(e0);
        q.push(e1);

        let popped: Vec<u64> = (0..3).map(|_| q.pop().expect("event").seq()).collect();
        assert_eq!(popped, vec![0, 1, 2]);

        // Smoke: verify kinds are preserved.
        let mut q2 = EventQueue::new();
        q2.push(fixtures::event_tick(10, 0, fixtures::tick_trade(10, 10, 0)));
        match q2.pop().expect("event").kind {
            EventKind::Tick(_) => {}
            other => panic!("expected Tick event, got: {other:?}"),
        }
    }

    #[test]
    fn test_event_ordering_same_ts_sim_uses_stable_tiebreak() {
        let ts_sim = 42;

        let mut q = EventQueue::new();
        q.push(fixtures::event_tick(
            ts_sim,
            2,
            fixtures::tick_trade(ts_sim, ts_sim, 2),
        ));
        q.push(fixtures::event_tick(
            ts_sim,
            0,
            fixtures::tick_trade(ts_sim, ts_sim, 0),
        ));
        q.push(fixtures::event_tick(
            ts_sim,
            1,
            fixtures::tick_trade(ts_sim, ts_sim, 1),
        ));

        let popped: Vec<u64> = (0..3).map(|_| q.pop().expect("event").seq()).collect();
        assert_eq!(popped, vec![0, 1, 2]);
    }
}
