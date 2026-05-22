import pyarrow as pa
import polars as pl
import pytest

from rust_backtester import Backtester


class NoopStrategy:
    def on_tick(self, tick, ctx):
        pass


class RaisingTickStrategy:
    def on_tick(self, tick, ctx):
        raise ValueError("strategy tick failed")


class RaisingBatchStrategy:
    def on_ticks(self, ticks, ctx):
        raise TypeError("strategy batch failed")


def _reader_from_frame(df: pl.DataFrame) -> pa.RecordBatchReader:
    table = df.to_arrow()
    return pa.RecordBatchReader.from_batches(table.schema, table.to_batches())


def _valid_frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "ts_exchange": [1_000, 2_000],
            "price": [100_00000000, 101_00000000],
            "qty": [1_00000000, 1_00000000],
            "side": [1, -1],
            "seq": [0, 1],
        }
    ).with_columns(pl.col("side").cast(pl.Int8))


def test_e2e_run_arrow_missing_required_column_raises_value_error():
    df = _valid_frame().drop("price")
    backtester = Backtester(data={}, seed=42)

    with pytest.raises(ValueError, match="missing price"):
        backtester.run_arrow(_reader_from_frame(df), NoopStrategy())


def test_e2e_run_arrow_invalid_side_raises_value_error():
    df = _valid_frame().with_columns(pl.lit(2, dtype=pl.Int8).alias("side"))
    backtester = Backtester(data={}, seed=42)

    with pytest.raises(ValueError, match="invalid side"):
        backtester.run_arrow(_reader_from_frame(df), NoopStrategy())


def test_e2e_strategy_tick_exception_is_returned_to_python():
    backtester = Backtester(data={}, seed=42)

    with pytest.raises(ValueError, match="strategy tick failed"):
        backtester.run_arrow(_reader_from_frame(_valid_frame()), RaisingTickStrategy())


def test_e2e_strategy_batch_exception_is_returned_to_python():
    backtester = Backtester(data={}, python_mode="batch", batch_ms=1, seed=42)

    with pytest.raises(TypeError, match="strategy batch failed"):
        backtester.run_arrow(_reader_from_frame(_valid_frame()), RaisingBatchStrategy())


def test_taker_fee_bps_nonzero_is_accepted():
    # taker_fee_bps is now implemented (market orders); non-zero values are valid.
    bt = Backtester(data={}, taker_fee_bps=5)
    assert bt is not None
