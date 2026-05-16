import polars as pl
import pytest

import rust_backtester


def make_ticks(n: int) -> pl.LazyFrame:
    ts = list(range(1_000, 1_000 + n * 1_000, 1_000))
    price = [100_00000000] * n
    qty = [1_00000000] * n
    side = [1 if i % 2 == 0 else -1 for i in range(n)]
    seq = list(range(n))
    return (
        pl.DataFrame({"ts_exchange": ts, "price": price, "qty": qty, "side": side, "seq": seq})
        .with_columns(pl.col("side").cast(pl.Int8))
        .lazy()
    )


class _RecordSymbolIdStrategy:
    """Records symbol_ids observed in on_tick callbacks."""

    def __init__(self) -> None:
        self.symbol_ids: list[int] = []

    def on_tick(self, tick, ctx) -> None:  # noqa: ANN001
        self.symbol_ids.append(tick.symbol_id)

    def on_order_update(self, _report, _ctx) -> None:  # noqa: ANN001
        pass


# ---------------------------------------------------------------------------
# Explicit symbol_map respected
# ---------------------------------------------------------------------------

def test_e2e_symbol_map_explicit_ids_respected() -> None:
    """symbol_map overrides alphabetical assignment."""
    lf_a = make_ticks(3)
    lf_b = make_ticks(3)
    strat_a = _RecordSymbolIdStrategy()

    # Without symbol_map: alphabetical → "a" gets 1, "b" gets 2
    bt_default = rust_backtester.Backtester(
        data={"sym_a": lf_a, "sym_b": lf_b}, seed=42
    )
    bt_default.run(strat_a)

    # With explicit symbol_map: "sym_a" → 10, "sym_b" → 20
    strat_b2 = _RecordSymbolIdStrategy()
    bt_explicit = rust_backtester.Backtester(
        data={"sym_a": make_ticks(3), "sym_b": make_ticks(3)},
        seed=42,
        symbol_map={"sym_a": 10, "sym_b": 20},
    )
    bt_explicit.run(strat_b2)

    default_ids = set(strat_a.symbol_ids)
    explicit_ids = set(strat_b2.symbol_ids)

    # Default assigns 1 and 2 (alphabetical)
    assert default_ids == {1, 2}, f"Expected {{1, 2}}, got {default_ids}"
    # Explicit map assigns 10 and 20
    assert explicit_ids == {10, 20}, f"Expected {{10, 20}}, got {explicit_ids}"


def test_e2e_symbol_map_single_symbol_custom_id() -> None:
    """Single-symbol dict with explicit id."""
    strat = _RecordSymbolIdStrategy()
    bt = rust_backtester.Backtester(
        data={"btc": make_ticks(4)},
        seed=42,
        symbol_map={"btc": 99},
    )
    bt.run(strat)
    assert all(sid == 99 for sid in strat.symbol_ids), (
        f"Expected all symbol_ids to be 99, got {strat.symbol_ids}"
    )


# ---------------------------------------------------------------------------
# Fallback: no symbol_map → alphabetical (backward compatible)
# ---------------------------------------------------------------------------

def test_e2e_symbol_map_fallback_alphabetical() -> None:
    """Omitting symbol_map uses alphabetical order (backward compatible)."""
    strat = _RecordSymbolIdStrategy()
    bt = rust_backtester.Backtester(
        data={"zzz": make_ticks(2), "aaa": make_ticks(2)},
        seed=42,
    )
    bt.run(strat)
    ids = set(strat.symbol_ids)
    # "aaa" → 1, "zzz" → 2 (alphabetical)
    assert ids == {1, 2}, f"Expected {{1, 2}}, got {ids}"


# ---------------------------------------------------------------------------
# Validation errors
# ---------------------------------------------------------------------------

def test_e2e_symbol_map_missing_symbol_raises() -> None:
    """symbol_map that omits a data key must raise ValueError."""
    with pytest.raises((ValueError, Exception)):
        rust_backtester.Backtester(
            data={"sym_a": make_ticks(2), "sym_b": make_ticks(2)},
            seed=42,
            symbol_map={"sym_a": 1},  # sym_b is missing
        )


def test_e2e_symbol_map_id_zero_raises() -> None:
    """symbol_id=0 must raise ValueError."""
    with pytest.raises((ValueError, Exception)):
        rust_backtester.Backtester(
            data={"sym": make_ticks(2)},
            seed=42,
            symbol_map={"sym": 0},
        )


# ---------------------------------------------------------------------------
# run_many() path
# ---------------------------------------------------------------------------

def test_e2e_symbol_map_run_many_respects_map() -> None:
    """symbol_map is respected in the parallel run_many() path."""
    bt = rust_backtester.Backtester(
        data={"asset": make_ticks(4)},
        seed=42,
        symbol_map={"asset": 42},
    )
    strats = [_RecordSymbolIdStrategy(), _RecordSymbolIdStrategy()]
    bt.run_many(strats)
    # After run_many, strats have recorded symbol_ids
    for s in strats:
        assert all(sid == 42 for sid in s.symbol_ids), (
            f"run_many: expected all symbol_ids=42, got {s.symbol_ids}"
        )
