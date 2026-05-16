import pyarrow as pa
import polars as pl

from rust_backtester import Backtester


class _Recorder:
    def __init__(self) -> None:
        self.ticks: list[dict[str, int]] = []

    def on_tick(self, tick: dict, ctx) -> None:  # noqa: ANN001
        self.ticks.append(tick)


def test_e2e_arrow_stream_accepts_non_int64_integer_columns() -> None:
    df = pl.DataFrame(
        {
            "ts_exchange": [1_000, 2_000],
            "price": [100, 101],
            "qty": [10, 20],
            "side": [1, -1],
            "seq": [10, 11],
        },
        schema={
            "ts_exchange": pl.Int32,
            "price": pl.Int32,
            "qty": pl.UInt64,
            "side": pl.Int8,
            "seq": pl.UInt64,
        },
    )

    table = df.to_arrow()
    reader = pa.RecordBatchReader.from_batches(table.schema, table.to_batches())
    bt = Backtester(data={"ignored": df.lazy()}, seed=42)
    strategy = _Recorder()

    bt.run_arrow(reader, strategy)

    assert [tick["ts_exchange"] for tick in strategy.ticks] == [1_000, 2_000]
    assert [tick["price"] for tick in strategy.ticks] == [100, 101]
    assert [tick["qty"] for tick in strategy.ticks] == [10, 20]
    assert [tick["seq"] for tick in strategy.ticks] == [10, 11]
