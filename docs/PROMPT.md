# Implementation Playbook (AI Agent)

Use this file as the default execution rules for implementing GitHub Issues in this repository.

## Inputs (What you are given)
- A GitHub Issue number (e.g., **Issue #95**).

## Quick Start — One-Shot Issue Implementation

Paste the following prompt, replacing `<N>` with the Issue number:

```
Plan the detailed implementation for Issue #<N>, then create a branch from the latest main and implement using TDD.
Use appropriate Skills. When problems arise, check learned Skills for past examples.
After implementation, run a sub-agent code review and address all findings.
Always run tests and E2E tests for sufficient verification.
Then commit, push, and create a PR. Handle failures until CI is all-green.
Review the implementation and evaluate whether all items in the Issue are addressed.
If there are gaps, plan and re-implement/verify for each gap. If no gaps, merge.
```

The agent will automatically follow the full workflow described in this file.

## Primary References (Always read these first)
- **Specification**: `docs/SPEC.md`
- **Implementation tasks (incl. suggested tests/bench names)**: `docs/PLAN.md`
- **This playbook (process & rules)**: `docs/PROMPT.md`
- **GitHub Issue**: `gh issue view <N>` — fetch the full issue body, acceptance criteria, and comments before starting.

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

### 0) Pre‑flight & Planning
- Fetch the issue: `gh issue view <N>` — read the full body, acceptance criteria, and all comments.
- Read `docs/SPEC.md` and any relevant section(s) in `docs/PLAN.md`.
- Run `/plan` skill (or use the **planner** agent) to produce a concrete task list before touching code.
- Clarify scope: what exactly is in/out for this issue.
- If the issue references past work, check `learned` Skills (`/instinct-status`) and project memory for applicable patterns or pitfalls.

### 1) Branching
- Fetch and reset to latest `main` first:
  ```bash
  git fetch origin
  git checkout main && git reset --hard origin/main
  ```
- Branch name: `feature/issue-<N>-<short-description>`
  - Example: `feature/issue-95-l3-codex-fixes`
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

### 8) Code Review (Sub-agent)
- After all tests pass, invoke the **rust-reviewer** sub-agent (or `/rust-review` skill) on the diff.
- Address every CRITICAL and HIGH finding before proceeding.
- Fix MEDIUM findings where straightforward.

### 9) Commits & PR
- Commit message format: `<type>(<scope>): <description> (#<N>)`
  - types: `feat`, `fix`, `test`, `docs`, `refactor`, `chore`, `ci`
  - example: `fix(engine): correct fill_market_immediately L3 fallback (#95)`
- Keep commits small and logically separated.

Create the PR and link it to the issue:
```bash
gh pr create \
  --title "fix: <short description> (#<N>)" \
  --body "Closes #<N>\n\n## Summary\n- …\n\n## Test plan\n- [ ] cargo test\n- [ ] pytest python/tests"
```

Wait for CI and handle failures:
```bash
gh pr checks --watch
# If red: read logs, fix, push again
```

### 10) Issue Verification & Merge
- Re-read the original issue (`gh issue view <N>`) and confirm every acceptance criterion is met.
- If any criterion is unmet, return to step 0 (re-plan for that gap) and iterate.
- When all criteria are met and CI is green, merge via squash:
  ```bash
  gh pr merge --squash --auto
  ```
- Delete the feature branch after merge.

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
- [ ] Issue body re-read; all acceptance criteria addressed.
- [ ] Implemented the requested tasks with minimal diffs.
- [ ] Added/updated tests (Rust and/or Python) with deterministic assertions.
- [ ] Verified no look‑ahead bias via tests (if time handling was touched).
- [ ] All tests pass (`cargo test`, plus `pytest` if present).
- [ ] Benchmarks checked when performance-sensitive code changed.
- [ ] `fmt/clippy` (and `ruff` if present) are clean.
- [ ] Code review sub-agent run; CRITICAL/HIGH findings addressed.
- [ ] CI green (`gh pr checks`).
- [ ] `docs/PLAN.md` updated with progress and notes.
- [ ] Feature branch deleted after merge.

---

## Agentic Workflow (Issue-centric)

1. **Fetch Issue** — `gh issue view <N>` to load acceptance criteria into context.
2. **Consult past work** — run `/instinct-status` and check `learned` Skills for any prior patterns matching this issue's domain.
3. **Plan** — use `/plan` skill or **planner** agent; produce a task list before touching code.
4. **Branch** — `git checkout -b feature/issue-<N>-<short-description>` from latest `main`.
5. **TDD** — write failing tests, implement, refactor; repeat until green.
6. **Code review** — invoke **rust-reviewer** sub-agent (or `/rust-review`); address findings.
7. **PR** — `gh pr create …`; monitor CI with `gh pr checks --watch`.
8. **Verify & merge** — re-read the issue, confirm all criteria met, then `gh pr merge --squash --auto`.
9. **Clean up** — delete the feature branch.
