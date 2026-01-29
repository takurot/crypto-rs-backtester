/// Prefetches data from the given address into the CPU cache.
///
/// This uses the `prefetcht0` instruction on x86_64 and `prfm` on aarch64.
/// It hints to the processor that the data at `ptr` will be read soon.
/// This is a non-blocking hint and does not affect program correctness.
///
/// # Safety
///
/// This function is safe to call with any pointer, including null or dangling pointers,
/// as prefetch instructions do not cause faults on invalid addresses.
#[inline(always)]
pub fn prefetch_read_data<T>(ptr: *const T) {
    // We casts to *const i8 because intrinsics usually take *const i8
    let p = ptr as *const i8;

    #[cfg(target_arch = "x86_64")]
    unsafe {
        use core::arch::x86_64::{_MM_HINT_T0, _mm_prefetch};
        _mm_prefetch(p, _MM_HINT_T0);
    }

    #[cfg(target_arch = "aarch64")]
    unsafe {
        use core::arch::asm;
        // prfm: Prefetch Memory
        // pld: Preload Data
        // l1: L1 Cache
        // keep: Temporal (keep in cache)
        asm!("prfm pldl1keep, [{}]", in(reg) p);
    }
}
