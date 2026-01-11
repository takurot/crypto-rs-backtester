# Code Review Prompt for crypto-rs-backtester

Use this prompt to guide reviews across Rust core, PyO3 wrapper, and Python package.

## Goals
- Uphold determinism and reproducibility end-to-end (seeded RNG, stable ordering).
- Maintain performance in hot paths; avoid accidental Python overhead in core loops.
- Keep FFI boundary clear, safe, and well-tested across tick vs batch modes.
- Preserve money-safety (no f64 for monetary amounts) and correctness.

## Reviewer Checklist

Architecture & Boundaries
- Are responsibilities well separated between `backtester-core` (engine), `backtester-py` (FFI), and `python/` (package/tests)?
- Are FFI surfaces minimal and stable? Are Python types converted to primitive Rust types early, with zero/low-copy where possible?
- Is determinism preserved (sorted keys, stable symbol_id mapping, fixed seeds)? Any hidden nondeterminism (hash maps, iteration order)?

Correctness & Safety
- Are monetary values integers (i64/i128) not f64? Any conversions that may overflow or lose precision?
- Are error cases handled with meaningful messages on both sides (Rust errors -> Python exceptions)?
- Thread-safety or aliasing concerns at the FFI boundary? Any `unsafe` blocks justified and minimal?

Performance
- Hot loops avoid Python callbacks unless explicitly in tick mode? Batch mode pathways reduce per-tick Python overhead?
- Data marshaling: are large arrays/lists handled efficiently (avoid repeated Python->Rust conversions)? Any opportunities to stream or lazily load?
- Latency/queue models and event scheduling avoid unnecessary allocations or cloning?

Testing & Benchmarks
- Do new changes include unit tests for Rust and Python as appropriate? Are e2e tests added when crossing FFI?
- Are seeds fixed, and tests assert equivalence (tick vs batch) where relevant?
- Any performance-sensitive change comes with benchmark notes (Criterion or `pytest -m bench`).

Style & Maintenance
- Rust code follows edition 2024 idioms; clippy clean or justified; formatted with rustfmt.
- Python code follows PEP 8, with type hints in new/changed code.
- Public APIs documented in code and/or `docs/` when architectural changes occur.

## Review Flow
1. Identify the scope of change (core, FFI, Python) and read related tests first.
2. Trace data flow across boundaries (tick ingestion -> engine -> strategy callbacks -> stats/trades). Validate determinism.
3. Scan for potential perf regressions (loops, conversions, allocations). Suggest targeted benchmarks.
4. Validate error handling and messages at the boundary.
5. Confirm style/lints and test coverage. Request missing tests where needed.

## Suggested Comments (copy/paste)
- Determinism: “Consider sorting keys or replacing HashMap with BTreeMap here to stabilize iteration order.”
- Money safety: “Avoid f64 here; prefer fixed-point integer type consistent with the rest of the codebase.”
- FFI overhead: “This per-tick Python call may dominate runtime in large backtests; can we batch or move loop into Rust?”
- Error clarity: “Surface this as a Python ValueError with field name for easier debugging.”
- Bench ask: “Please add a Criterion bench for X or a `pytest -m bench` microbench to quantify the change.”

