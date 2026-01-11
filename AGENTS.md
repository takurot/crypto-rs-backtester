# Repository Guidelines

## Project Structure & Module Organization
- `backtester-core/`: Rust simulation core (`src/*.rs`, `tests/`, `benches/`).
- `backtester-py/`: PyO3 wrapper exposing the core to Python (`src/lib.rs`).
- `python/`: Python package (`rust_backtester/`) and tests (`python/tests/`).
- `docs/`: Design and plans (`SPEC.md`, `PLAN.md`, research notes).
- Root `Cargo.toml`: Rust workspace; `pyproject.toml`: Python build via maturin.

## Build, Test, and Development Commands
- Env setup: `python -m venv .venv && source .venv/bin/activate`
- Python dev install (builds Rust ext): `pip install -e .[dev]`
- Alt (direct build): `maturin develop` (uses `backtester-py/Cargo.toml`).
- Run Python tests: `pytest -q` (benches: `pytest -m bench -q`).
- Build Rust core: `cargo build -p backtester-core`
- Rust tests: `cargo test -p backtester-core`
- Rust benches: `cargo bench -p backtester-core`

## Coding Style & Naming Conventions
- Rust: use edition 2024 idioms; `snake_case` for functions/modules, `CamelCase` for types. Format with `cargo fmt`; lint with `cargo clippy` (treat warnings seriously).
- Python: PEP 8, 4‑space indents, type hints required for new/changed code. `snake_case` for functions/modules, `CamelCase` for classes, UPPER_CASE for constants.
- Core rule: keep determinism (seeded RNG, stable ordering); avoid `f64` for monetary logic.

## Testing Guidelines
- Frameworks: Rust `cargo test`; Python `pytest` (tests live in `python/tests/test_*.py`).
- Add unit tests for new components and an e2e test if behavior crosses the FFI boundary.
- Ensure reproducibility: fix RNG seeds and assert equality across tick vs batch where relevant.
- Benchmark changes that may affect performance (Criterion benches or `pytest -m bench`).

## Commit & Pull Request Guidelines
- Commit style: Conventional Commits (e.g., `feat(core): add queue model`, `chore(fmt): rustfmt`).
- Branches: `feature/...`, `fix/...`, or `chore/...` mapped to the change type.
- PRs must include: what/why, linked issues, test plan (commands + results), and performance notes if core paths changed. Update `docs/` when APIs or architecture shift.

