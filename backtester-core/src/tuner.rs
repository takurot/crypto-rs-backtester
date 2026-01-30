/// Auto-tuner for batch size optimization.
///
/// Uses a simplified AIMD (Additive Increase, Multiplicative Decrease) algorithm
/// to maximize batch size (throughput) while keeping latency per tick under a target threshold.
#[derive(Debug)]
pub struct BatchTuner {
    /// Minimum batch size duration (ns).
    min_batch_ns: i64,
    /// Maximum allowed batch size duration (ns).
    max_batch_ns_limit: i64,
    /// Target latency per tick (ns). If processing takes longer than this per tick, we decrease batch size.
    target_latency_per_tick_ns: f64,
    /// Current batch size (ns).
    current_batch_ns: i64,
    /// Sample counter for stability.
    sample_count: u64,
}

impl BatchTuner {
    pub fn new(min_batch_ns: i64, max_batch_ns_limit: i64, target_latency_per_tick_ns: f64) -> Self {
        Self {
            min_batch_ns,
            max_batch_ns_limit,
            target_latency_per_tick_ns,
            current_batch_ns: min_batch_ns,
            sample_count: 0,
        }
    }

    pub fn current_batch_ns(&self) -> i64 {
        self.current_batch_ns
    }

    /// Record a batch execution.
    ///
    /// * `batch_duration_ns`: Total wall-clock time spent processing the batch.
    /// * `num_ticks`: Number of ticks processed in the batch.
    pub fn record_batch(&mut self, batch_duration_ns: i64, num_ticks: u64) {
        if num_ticks == 0 {
            return;
        }

        self.sample_count += 1;
        
        let latency_per_tick = batch_duration_ns as f64 / num_ticks as f64;

        // Tuning interval: adjust every 10 batches to avoid jitter.
        if !self.sample_count.is_multiple_of(10) {
            return;
        }

        if latency_per_tick < self.target_latency_per_tick_ns {
            // Latency is good, try increasing batch size to improve throughput (Additive Increase).
            // Increase by 10% or at least 100us.
            let increase = (self.current_batch_ns / 10).max(100_000);
            self.current_batch_ns = (self.current_batch_ns + increase).min(self.max_batch_ns_limit);
        } else {
            // Latency is too high, decrease batch size (Multiplicative Decrease).
            self.current_batch_ns = (self.current_batch_ns as f64 * 0.8) as i64;
            self.current_batch_ns = self.current_batch_ns.max(self.min_batch_ns);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batch_tuner_increases_when_latency_low() {
        let mut tuner = BatchTuner::new(1_000, 10_000_000, 100.0); // 100ns per tick target
        assert_eq!(tuner.current_batch_ns(), 1_000);

        // Record 10 batches with low latency (50ns per tick)
        for _ in 0..10 {
            tuner.record_batch(5_000, 100);
        }

        // Should increase
        assert!(tuner.current_batch_ns() > 1_000);
        // Step is ~100us (100_000ns)
        assert_eq!(tuner.current_batch_ns(), 1_000 + 100_000);
    }

    #[test]
    fn test_batch_tuner_decreases_when_latency_high() {
        let mut tuner = BatchTuner::new(100_000, 10_000_000, 100.0);
        // Force initial state
        tuner.current_batch_ns = 1_000_000;

        // Record 10 batches with high latency (200ns per tick)
        for _ in 0..10 {
            tuner.record_batch(20_000, 100);
        }

        // Should decrease (0.8x)
        assert!(tuner.current_batch_ns() < 1_000_000);
        assert_eq!(tuner.current_batch_ns(), 800_000);
    }

    #[test]
    fn test_batch_tuner_clamps_limits() {
        let mut tuner = BatchTuner::new(1_000, 2_000, 100.0);
        
        // Try to increase beyond max
        for _ in 0..100 {
            tuner.record_batch(1_000, 100); // 10ns latency (very fast)
        }
        assert_eq!(tuner.current_batch_ns(), 2_000);

         // Try to decrease below min
         tuner.current_batch_ns = 1_000;
         for _ in 0..100 {
             tuner.record_batch(1_000_000, 1); // 1ms latency (very slow)
         }
         assert_eq!(tuner.current_batch_ns(), 1_000);
    }
}
