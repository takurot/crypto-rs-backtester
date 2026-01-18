use rayon::prelude::*;

use crate::engine::{Engine, EngineConfig, Strategy};
use crate::event::EventKind;
use crate::latency_model::LatencyModel;
use crate::queue_model::QueueModel;
use crate::stats::BacktestStats;
use crate::types::TsSimNs;

#[derive(Debug, Clone)]
pub struct SweepResult {
    pub stats: BacktestStats,
}

/// Run a deterministic parameter sweep in parallel using rayon.
///
/// - Results are returned in the same order as input strategies.
/// - Each run derives its seed from `base_config.seed + index`.
pub fn run_parameter_sweep<Q, S, L>(
    queue_model: Q,
    latency_model: L,
    base_config: EngineConfig,
    strategies: Vec<S>,
    events: &[(TsSimNs, EventKind)],
) -> Vec<SweepResult>
where
    Q: QueueModel + Clone + Send + Sync,
    S: Strategy + Send,
    L: LatencyModel + Clone + Send + Sync,
{
    let base_seed = base_config.seed;

    strategies
        .into_par_iter()
        .enumerate()
        .map(|(index, strategy)| {
            let mut config = base_config;
            config.seed = base_seed.wrapping_add(index as u64);

            let mut engine: Engine<Q, S, L> =
                Engine::new(queue_model.clone(), strategy, config, latency_model.clone());

            for &(ts, kind) in events {
                engine.push_event(ts, kind);
            }

            engine.run();

            SweepResult {
                stats: engine.stats(),
            }
        })
        .collect()
}
