use backtester_core::engine::{Context, Engine, EngineConfig, EngineMode, Strategy};
use backtester_core::latency_model::ConstantLatency;
use backtester_core::queue_model::ConservativeQueue;
use backtester_core::tick_source::TickSource;
use backtester_core::types::{OrderReport, Tick};

#[derive(Default)]
struct FastStrategy;
impl Strategy for FastStrategy {
    fn on_tick(&mut self, _: &Tick, _: &mut Context<'_>) {}
    fn on_order_update(&mut self, _: &OrderReport, _: &mut Context<'_>) {}
}


struct VecTickSource {
    ticks: Vec<Tick>,
    idx: usize,
}
impl VecTickSource {
    fn new(n: usize) -> Self {
        let ticks = (0..n)
            .map(|i| Tick {
                ts_exchange: i as i64 * 1_000_000,
                ts_local: 0,
                seq: i as u64,
                price: 100,
                qty: 1,
                symbol_id: 1,
                side: backtester_core::types::Side::Buy,
                flags: 0,
            })
            .collect();
        Self { ticks, idx: 0 }
    }
}
impl TickSource for VecTickSource {
    fn peek(&mut self) -> Option<&Tick> {
        self.ticks.get(self.idx)
    }
    fn next(&mut self) -> Option<Tick> {
        let t = self.ticks.get(self.idx).cloned();
        if t.is_some() {
            self.idx += 1;
        }
        t
    }
    fn symbol_id(&self) -> u32 {
        1
    }
}

#[test]
fn test_engine_increases_batch_size_when_fast() {
    let config = EngineConfig {
        mode: EngineMode::Batch,
        max_batch_ns: 1_000_000,
        ..Default::default()
    };

    let latency = ConstantLatency {
        feed_latency_ns: 0,
        order_latency_ns: 0,
    };
    // Tuner updates every 10 batches.
    // If we have very fast strategy, batch size should increase.
    
    // We need enough ticks to trigger multiple batches.
    // Initially batch is 100us. Ticks are 1ms apart (see VecTickSource).
    // So 1 tick per batch?
    // Wait, if ticks are 1ms apart, and batch is 100us, we process 1 tick, then advance time.
    // To fill a batch we need ticks closer than batch size.
    // Let's make ticks 1us apart.
    
    let n_ticks = 50000;
    let mut source = VecTickSource::new(n_ticks);
    // Overwrite ts to be dense
    for i in 0..n_ticks {
        source.ticks[i].ts_exchange = i as i64 * 1000; // 1us apart
    }

    let mut engine = Engine::new(ConservativeQueue, FastStrategy, config, latency);
    engine.add_tick_source(Box::new(source));

    let initial_batch = engine.config().max_batch_ns;
    // tuner initializes current_batch_ns to min (100_000).
    // If EngineConfig passed 1_000_000, that's just the LIMIT.
    // Wait, Engine does: config.max_batch_ns = tuner.current_batch_ns() AFTER first update.
    // But initially engine.config.max_batch_ns is what we passed (1_000_000).
    // Tuner.current is 100_000.
    // So after first batch (update), config.max_batch_ns will snap to 100_000? 
    // Yes, Tuner starts at min.
    
    engine.run();

    let final_batch = engine.config().max_batch_ns;
    println!("Initial limit: {}, Final batch: {}", initial_batch, final_batch);
    
    // It should have snapped to 100k, then increased.
    // 5000 ticks. 1us apart = 5ms total data.
    // If batch is 100us, we have 50 batches.
    // Tuner updates every 10 batches. So 5 updates.
    // Increase is ~100us per update.
    // Should end up around 500us - 600us.
    
    assert!(final_batch > 100_000, "Batch size should increase from min for fast strategy");
}

