"""E2E tests for run_arrow with multi-symbol dict support (Issue #35)."""

import pyarrow as pa
import polars as pl
import pytest

from rust_backtester import Backtester


def _make_frame(ts_start: int, n: int, price: int) -> pl.DataFrame:
    ts = [ts_start + i * 1_000 for i in range(n)]
    return pl.DataFrame(
        {
            "ts_exchange": ts,
            "price": [price] * n,
            "qty": [1_00000000] * n,
            "side": [1] * n,
            "seq": list(range(n)),
        }
    ).with_columns(pl.col("side").cast(pl.Int8))


def _reader(df: pl.DataFrame) -> pa.RecordBatchReader:
    table = df.to_arrow()
    return pa.RecordBatchReader.from_batches(table.schema, table.to_batches())


class TickCollector:
    def __init__(self) -> None:
        self.symbol_ids: list[int] = []

    def on_tick(self, tick, ctx) -> None:  # noqa: ANN001
        self.symbol_ids.append(tick.symbol_id)

    def on_order_update(self, _report, _ctx) -> None:  # noqa: ANN001
        pass


def test_e2e_run_arrow_dict_two_symbols() -> None:
    """Dict-form run_arrow assigns distinct symbol_ids to each stream."""
    btc_df = _make_frame(ts_start=1_000, n=3, price=100_00000000)
    eth_df = _make_frame(ts_start=2_000, n=2, price=50_00000000)

    bt = Backtester(data={}, seed=42)
    strat = TickCollector()
    bt.run_arrow({"BTC": _reader(btc_df), "ETH": _reader(eth_df)}, strat)

    assert len(strat.symbol_ids) == 5, f"Expected 5 ticks, got {len(strat.symbol_ids)}"
    btc_id = min(strat.symbol_ids)  # "BTC" sorts before "ETH" → id 1
    eth_id = max(strat.symbol_ids)  # "ETH" → id 2
    assert btc_id != eth_id, "BTC and ETH must have distinct symbol_ids"
    assert strat.symbol_ids.count(btc_id) == 3
    assert strat.symbol_ids.count(eth_id) == 2


def test_e2e_run_arrow_dict_single_symbol() -> None:
    """Dict-form with one key works and ticks carry that symbol's id."""
    df = _make_frame(ts_start=1_000, n=4, price=100_00000000)
    bt = Backtester(data={}, seed=42)
    strat = TickCollector()
    bt.run_arrow({"BTC": _reader(df)}, strat)

    assert len(strat.symbol_ids) == 4
    assert len(set(strat.symbol_ids)) == 1, "All ticks should share one symbol_id"


def test_e2e_run_arrow_single_stream_backward_compat() -> None:
    """Single-stream form (pre-#35 API) continues to work with symbol_id=1."""
    df = _make_frame(ts_start=1_000, n=4, price=100_00000000)
    bt = Backtester(data={}, seed=42)
    strat = TickCollector()
    bt.run_arrow(_reader(df), strat)

    assert len(strat.symbol_ids) == 4
    assert all(sid == 1 for sid in strat.symbol_ids), (
        "Backward-compat path must use symbol_id=1"
    )


def test_e2e_run_arrow_dict_symbol_map_override() -> None:
    """symbol_map is respected when using dict-form run_arrow."""
    df = _make_frame(ts_start=1_000, n=2, price=100_00000000)
    bt = Backtester(data={}, seed=42, symbol_map={"BTC": 99})
    strat = TickCollector()
    bt.run_arrow({"BTC": _reader(df)}, strat)

    assert all(sid == 99 for sid in strat.symbol_ids), (
        f"symbol_map override not respected: {strat.symbol_ids}"
    )


def test_e2e_run_arrow_empty_dict_raises() -> None:
    """Empty dict raises ValueError — no streams to process."""
    bt = Backtester(data={}, seed=42)
    with pytest.raises(ValueError, match="stream dict must not be empty"):
        bt.run_arrow({}, TickCollector())


def test_e2e_run_arrow_dict_symbol_map_missing_key_raises() -> None:
    """symbol_map missing a dict key raises ValueError with clear message."""
    df = _make_frame(ts_start=1_000, n=2, price=100_00000000)
    bt = Backtester(data={}, seed=42, symbol_map={"OTHER": 5})
    with pytest.raises(ValueError, match="symbol_map is missing entry"):
        bt.run_arrow({"BTC": _reader(df)}, TickCollector())


def test_e2e_run_arrow_dict_invalid_stream_raises_with_symbol_name() -> None:
    """A non-stream value in the dict raises an error that names the bad symbol."""
    bt = Backtester(data={}, seed=42)
    with pytest.raises((ValueError, TypeError), match="BTC"):
        bt.run_arrow({"BTC": "not_a_stream"}, TickCollector())


def test_e2e_run_arrow_dict_malformed_schema_raises_with_symbol_name() -> None:
    """A dict stream with a missing required column names the offending symbol."""
    bad_df = pl.DataFrame(
        {
            "ts_exchange": [1_000, 2_000],
            "qty": [1_00000000, 1_00000000],
            "side": [1, -1],
            "seq": [0, 1],
        }
    ).with_columns(pl.col("side").cast(pl.Int8))
    bt = Backtester(data={}, seed=42)
    with pytest.raises(ValueError, match="BTC"):
        bt.run_arrow({"BTC": _reader(bad_df)}, TickCollector())
