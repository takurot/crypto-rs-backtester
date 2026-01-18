import polars as pl

import rust_backtester


def make_minimal_ticks_alias_ts_event(*, with_seq: bool = True) -> pl.LazyFrame:
    ts_event = [1_000, 2_000, 3_000, 4_000]
    price = [100_00000000, 101_00000000, 99_00000000, 100_00000000]
    qty = [1_00000000, 1_00000000, 1_00000000, 1_00000000]
    side = [1, -1, 1, -1]
    data = {"ts_event": ts_event, "price": price, "qty": qty, "side": side}
    if with_seq:
        data["seq"] = list(range(len(ts_event)))
    return pl.DataFrame(data).with_columns(pl.col("side").cast(pl.Int8)).lazy()


def make_minimal_ticks_alias_size(*, with_seq: bool = True) -> pl.LazyFrame:
    ts_exchange = [1_000, 2_000, 3_000, 4_000]
    price = [100_00000000, 101_00000000, 99_00000000, 100_00000000]
    size = [1_00000000, 1_00000000, 1_00000000, 1_00000000]
    side = [1, -1, 1, -1]
    data: dict[str, list[int]] = {
        "ts_exchange": ts_exchange,
        "price": price,
        "size": size,
        "side": side,
    }
    if with_seq:
        data["seq"] = list(range(len(ts_exchange)))
    return pl.DataFrame(data).lazy()


class _Recorder:
    def __init__(self) -> None:
        self.ticks: list[dict[str, int]] = []

    def on_tick(self, tick: dict, ctx) -> None:  # noqa: ANN001
        self.ticks.append(tick)


def test_schema_accepts_ts_event_alias() -> None:
    lf = make_minimal_ticks_alias_ts_event(with_seq=True)
    bt = rust_backtester.Backtester(
        data={"binance:BTC/USDT": lf},
        seed=42,
        python_mode="tick",
        batch_ms=100,
        feed_latency_ns=0,
    )
    strat = _Recorder()
    bt.run(strat)

    assert [t["ts_exchange"] for t in strat.ticks] == [1_000, 2_000, 3_000, 4_000]


def test_schema_accepts_qty_size_alias() -> None:
    lf = make_minimal_ticks_alias_size(with_seq=True)
    bt = rust_backtester.Backtester(
        data={"binance:BTC/USDT": lf},
        seed=42,
        python_mode="tick",
        batch_ms=100,
        feed_latency_ns=0,
    )
    strat = _Recorder()
    bt.run(strat)

    assert [t["qty"] for t in strat.ticks] == [1_00000000, 1_00000000, 1_00000000, 1_00000000]

