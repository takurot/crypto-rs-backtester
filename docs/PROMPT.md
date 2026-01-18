# Implementation Playbook (AI Agent)

Use this file as the default execution rules for implementing tasks/PRs in this repository.

## Inputs (What you are given)
- A PR identifier (e.g., **PR-01**) or a request to implement a subset of tasks.

## Primary References (Always read these first)
- **Specification**: `docs/SPEC.md`
- **Implementation tasks (incl. suggested tests/bench names)**: `docs/PLAN.md`
- **This playbook (process & rules)**: `docs/PROMPT.md`

---

## Non‑Negotiable Project Principles

### 1) Determinism & Reproducibility
- A run **MUST** be reproducible given identical inputs and RNG seed.
- All stochastic components **MUST** use an explicitly seeded RNG owned by the engine (do not hide RNG state inside models).
- Event ordering **MUST** be deterministic even when timestamps are equal (stable tie‑breakers).
- Never rely on `HashMap` iteration order for anything observable.

### 2) Look‑ahead Bias Prevention
- The strategy **MUST** only observe information delivered at or before **`ts_local`**.
- The exchange simulator uses market truth at **`ts_exchange`**; the strategy reads a feed‑delayed **`MarketView`**.
- If you touch time handling, add/extend both unit tests and Python E2E tests that prove “no look‑ahead”.

### 3) Fixed‑Point Monetary Arithmetic
- Prices/quantities/fees/PnL **MUST** be fixed‑point `i64` in core logic.
- `f64` is allowed **only** at I/O boundaries (parsing/formatting), never in core accounting or decision logic.

### 4) Performance Fundamentals
- Prefer **batching** across the Rust↔Python boundary (`on_ticks` / `on_order_updates`) over per‑tick callbacks.
- Prefer Arrow/Polars **columnar (SoA) iteration**; avoid materializing full `Vec<Tick>` for large datasets.

### 5) Safety
- Do **not** delete untracked files without explicit permission (exception: macOS `._*` files).
- Keep diffs focused and avoid unrelated refactors during feature work.

---

## Standard Implementation Workflow

### 0) Pre‑flight
- Read `docs/SPEC.md` and the relevant section(s) in `docs/PLAN.md`.
- Clarify scope: what exactly is in/out for the requested PR/tasks.
- Create a short checklist of tasks you will complete (map to `docs/PLAN.md`).

### 1) Branching
- Branch name: `feature/<pr-id>-<short-description>`
  - Example: `feature/pr-02-polars-integration`
- Branch off `main`.

### 2) Environment Setup (macOS/Linux)
This repo is WIP; some files may not exist yet. Use conditional setup:

#### Python (only if Python packaging exists / tests are present)
```bash
python3 -m venv .venv
source .venv/bin/activate

# If pyproject.toml exists and defines extras:
pip install -U pip
pip install -e ".[dev]" maturin
```

#### Rust
```bash
cargo --version
rustc --version
```

### 3) TDD (Test‑Driven Development)
- **Red**: write a failing test first (Rust unit/integration, and/or Python E2E depending on the layer).
- **Green**: implement the minimum code to pass.
- **Refactor**: clean up; keep determinism and fixed‑point rules intact.

Use the test naming/layout conventions already documented in `docs/PLAN.md`.

### 4) Run Tests (Frequently)
- Rust:
```bash
cargo test
```

- Python (if available):
```bash
source .venv/bin/activate
maturin develop --extras dev
pytest python/tests
```

### 5) Benchmarks (When relevant)
- Rust Criterion benchmarks:
```bash
cargo bench
```

If a Python benchmark script exists in the future, run it and compare against a baseline. Treat numbers as noisy; focus on regressions.

### 6) Code Quality
- Rust:
```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
```

- Python (if present):
```bash
ruff check python/
ruff format python/
```

### 7) Update Documentation
- Update `docs/PLAN.md` checkboxes and add notes for any newly discovered follow‑ups/risks.
- Only update `docs/SPEC.md` if you are changing the intended behavior/contract.

### 8) Commits & PR
- Commit message format: `<type>(<scope>): <description>`
  - types: `feat`, `fix`, `test`, `docs`, `refactor`, `chore`, `ci`
  - example: `test(engine): add no-lookahead regression test`
- Keep commits small and logically separated.

If GitHub CLI is used:
```bash
gh pr create --title "<PR-ID>: <Title>" --body "<Description>"
gh pr checks
```

---

## Minimal E2E Data Generation (Python/Polars)
E2E tests should generate tiny deterministic datasets on-the-fly (no external files).

```python
import polars as pl

def make_minimal_ticks_lazyframe(*, with_seq: bool = True) -> pl.LazyFrame:
    # 4 ticks, deterministic order, trade-only.
    ts_exchange = [1_000, 2_000, 3_000, 4_000]
    price = [100_00000000, 101_00000000,  99_00000000, 100_00000000]
    qty =   [  1_00000000,   1_00000000,   1_00000000,   1_00000000]
    side =  [           1,          -1,            1,          -1]
    data = {"ts_exchange": ts_exchange, "price": price, "qty": qty, "side": side}
    if with_seq:
        data["seq"] = list(range(len(ts_exchange)))
    return pl.DataFrame(data).lazy()

def make_minimal_ticks_alias_columns_lazyframe() -> pl.LazyFrame:
    # Alias columns to validate schema compatibility (`ts_event`, `size`).
    lf = make_minimal_ticks_lazyframe(with_seq=True)
    return lf.rename({"ts_exchange": "ts_event", "qty": "size"})
```

For look-ahead tests, set a constant feed latency and assert the strategy’s observed time matches:
`ts_local == ts_exchange + latency`.

---

## Checklist (Before you call a PR “done”)
- [ ] Implemented the requested tasks with minimal diffs.
- [ ] Added/updated tests (Rust and/or Python) with deterministic assertions.
- [ ] Verified no look‑ahead bias via tests (if time handling was touched).
- [ ] All tests pass (`cargo test`, plus `pytest` if present).
- [ ] Benchmarks checked when performance-sensitive code changed.
- [ ] `fmt/clippy` (and `ruff` if present) are clean.
- [ ] `docs/PLAN.md` updated with progress and notes.

---

## Agentic Workflow

1. **Consultation**: Refer to `CODEX_CLI.md` and discuss implementation details/strategy with `codex`.
2. **Implementation**: Create a feature branch and develop using TDD (Test-Driven Development).
3. **Benchmarking**: After implementation, refer to `README.md` to run benchmarks and save results in `benchmarks`.
4. **Pull Request**: Commit and push changes, then create a PR.
5. **Code Review**: After creating the PR, refer to `CODEX_CLI.md` to request a code review from `codex`.
