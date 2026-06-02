//! NUMA-aware memory allocation guide (Phase 7.7.1).
//!
//! # Overview
//!
//! On multi-socket (NUMA) systems, memory latency is significantly lower when
//! a thread accesses memory allocated on the same NUMA node as the core it runs
//! on.  For large Arrow buffers (>1 GiB) in parallel parameter sweeps this can
//! reduce cross-socket cache-line transfers and improve throughput by 20–40%.
//!
//! # Enabling jemalloc (recommended)
//!
//! jemalloc provides NUMA-aware arena allocation out of the box.  Add the
//! following to your **binary or Python extension crate** (not to this library):
//!
//! ```toml
//! # Cargo.toml (binary/extension crate)
//! [dependencies]
//! tikv-jemallocator = "0.6"   # or use backtester-core with feature = "numa"
//! ```
//!
//! Then set the global allocator in `main.rs` / `lib.rs`:
//!
//! ```rust,ignore
//! #[global_allocator]
//! static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;
//! ```
//!
//! Configure jemalloc's NUMA-aware arenas at process start via the environment
//! variable (one arena per NUMA node keeps allocations local):
//!
//! ```sh
//! # Example: 2-socket system with 16 cores per socket → 2 arenas
//! MALLOC_CONF="narenas:2,metadata_thp:auto" ./my_backtester
//! ```
//!
//! # Thread pinning
//!
//! For maximum benefit, pin rayon worker threads to specific NUMA nodes using
//! the `hwloc2` crate or the `numactl` CLI wrapper:
//!
//! ```sh
//! numactl --cpunodebind=0 --membind=0 ./my_backtester   # pin to node 0
//! numactl --interleave=all ./my_backtester               # interleave (baseline)
//! ```
//!
//! # When to enable
//!
//! Enable NUMA-aware allocation only when **all** of the following are true:
//! - Running on a multi-socket server (two or more NUMA nodes)
//! - Arrow input buffers exceed ~512 MiB per sweep worker
//! - Profiling confirms cross-socket memory traffic (via `numastat` or `perf`)
//!
//! On single-socket workstations (including Apple Silicon) jemalloc still
//! reduces fragmentation, but the NUMA benefit is zero.
//!
//! # Feature flag
//!
//! Enabling the `numa` feature in `backtester-core` makes `tikv-jemallocator`
//! available as a dependency.  The global allocator is **not** set automatically
//! (that is the responsibility of the consuming binary or extension crate).

/// Compile-time marker that confirms the `numa` feature is active and
/// `tikv-jemallocator` is available for the consuming crate.
#[cfg(feature = "numa")]
pub use tikv_jemallocator::Jemalloc;

#[cfg(test)]
mod tests {
    #[test]
    fn numa_module_compiles() {
        // Smoke test: the module compiles and all public items are accessible.
        let _ = std::mem::size_of::<()>();
    }

    #[cfg(feature = "numa")]
    #[test]
    fn jemalloc_type_is_re_exported() {
        // Verify the re-export resolves when the `numa` feature is active.
        let _ = std::mem::size_of::<super::Jemalloc>();
    }
}
