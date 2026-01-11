use rand::Rng;

/// Simulates network / processing latency jitter.
///
/// Design rule:
/// - The engine MUST own and seed the RNG for reproducibility.
/// - Latency models MUST NOT keep RNG state internally; consume randomness via `rng`.
pub trait LatencyModel: Send + Sync {
    fn sample_feed_latency(&self, ts_exchange: i64, rng: &mut impl Rng) -> i64;
    fn sample_order_latency(&self, ts_local: i64, rng: &mut impl Rng) -> i64;
}

/// Constant latency model (useful for deterministic tests and as a baseline).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConstantLatency {
    pub feed_latency_ns: i64,
    pub order_latency_ns: i64,
}

impl LatencyModel for ConstantLatency {
    fn sample_feed_latency(&self, _ts_exchange: i64, _rng: &mut impl Rng) -> i64 {
        self.feed_latency_ns.max(0)
    }

    fn sample_order_latency(&self, _ts_local: i64, _rng: &mut impl Rng) -> i64 {
        self.order_latency_ns.max(0)
    }
}

/// Log-normal jitter model for positive latencies.
///
/// `mean_ns` and `std_dev_ns` are the mean and standard deviation of the *log-normal*
/// distribution, in nanoseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogNormalJitter {
    pub mean_ns: i64,
    pub std_dev_ns: i64,
}

impl LogNormalJitter {
    fn sample_lognormal_ns(&self, rng: &mut impl Rng) -> i64 {
        let mean_ns = self.mean_ns.max(0) as f64;
        let std_dev_ns = self.std_dev_ns.max(0) as f64;

        // Degenerate cases
        if mean_ns == 0.0 {
            return 0;
        }
        if std_dev_ns == 0.0 {
            return self.mean_ns.max(0);
        }

        // Convert (mean, std) of log-normal to (mu, sigma) of underlying normal.
        // sigma^2 = ln(1 + (s^2)/(m^2))
        // mu = ln(m) - 0.5*sigma^2
        let var_ratio = (std_dev_ns * std_dev_ns) / (mean_ns * mean_ns);
        let sigma2 = (1.0 + var_ratio).ln();
        let sigma = sigma2.sqrt();
        let mu = mean_ns.ln() - 0.5 * sigma2;

        // Sample standard normal via Box-Muller.
        // u1 in (0,1], u2 in [0,1)
        let mut u1: f64 = rng.r#gen();
        if u1 == 0.0 {
            u1 = f64::MIN_POSITIVE;
        }
        let u2: f64 = rng.r#gen();
        let z0 = (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos();

        let x = (mu + sigma * z0).exp();
        if !x.is_finite() || x >= i64::MAX as f64 {
            return i64::MAX;
        }
        (x.round() as i64).max(0)
    }
}

impl LatencyModel for LogNormalJitter {
    fn sample_feed_latency(&self, _ts_exchange: i64, rng: &mut impl Rng) -> i64 {
        self.sample_lognormal_ns(rng)
    }

    fn sample_order_latency(&self, _ts_local: i64, rng: &mut impl Rng) -> i64 {
        self.sample_lognormal_ns(rng)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::rng::make_small_rng;

    #[test]
    fn test_latency_lognormal_is_reproducible_under_seed() {
        let m = LogNormalJitter {
            mean_ns: 1_000_000,
            std_dev_ns: 250_000,
        };

        let mut rng1 = make_small_rng(42);
        let mut rng2 = make_small_rng(42);

        for _ in 0..32 {
            let a = m.sample_order_latency(123, &mut rng1);
            let b = m.sample_order_latency(123, &mut rng2);
            assert_eq!(a, b);
            assert!(a >= 0);
        }
    }
}
