use rand::SeedableRng;
use rand::rngs::SmallRng;

/// Create a reproducible RNG from a user-provided seed.
///
/// Design rule: the engine owns the RNG; models should consume randomness
/// via a mutable `&mut impl Rng` passed in.
pub fn make_small_rng(seed: u64) -> SmallRng {
    SmallRng::seed_from_u64(seed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::RngCore;

    #[test]
    fn test_reproducible_with_seed() {
        let mut rng1 = make_small_rng(42);
        let mut rng2 = make_small_rng(42);

        // A few draws are enough to validate reproducibility.
        assert_eq!(rng1.next_u64(), rng2.next_u64());
        assert_eq!(rng1.next_u64(), rng2.next_u64());
        assert_eq!(rng1.next_u64(), rng2.next_u64());
    }
}
