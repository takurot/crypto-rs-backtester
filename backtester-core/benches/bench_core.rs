use std::time::Duration;

use backtester_core::{EventKind, EventQueue, OrderBookL2, Side, fixtures};
use criterion::{Criterion, black_box, criterion_group, criterion_main};

fn bench_event_loop_1m_ticks(c: &mut Criterion) {
    c.bench_function("bench_event_loop_1m_ticks", |b| {
        b.iter(|| {
            let mut q = EventQueue::new();
            for i in 0..1_000_000u64 {
                let ts = i as i64;
                let tick = fixtures::tick_trade(ts, ts, i);
                q.push(fixtures::event_tick(ts, i, tick));
            }

            let mut acc: i64 = 0;
            while let Some(ev) = q.pop() {
                match ev.kind {
                    EventKind::Tick(t) => {
                        acc = acc.wrapping_add(t.price);
                    }
                    _ => {}
                }
            }
            black_box(acc)
        })
    });
}

fn bench_orderbook_apply_l2_1m_updates(c: &mut Criterion) {
    c.bench_function("bench_orderbook_apply_l2_1m_updates", |b| {
        b.iter(|| {
            let mut ob = OrderBookL2::new();
            for i in 0..1_000_000u64 {
                let price = 100_000 + (i as i64 % 10_000);
                let qty = 1_000 + (i as i64 % 1_000);
                let side = if i % 2 == 0 { Side::Buy } else { Side::Sell };
                let u = fixtures::l2_update(i as i64, i, price, qty, side);
                ob.apply_l2(&u);
            }
            black_box(ob.best_bid());
            black_box(ob.best_ask());
        })
    });
}

criterion_group!(
    name = benches;
    config = Criterion::default()
        .sample_size(10)
        .measurement_time(Duration::from_secs(1));
    targets = bench_event_loop_1m_ticks, bench_orderbook_apply_l2_1m_updates
);
criterion_main!(benches);
