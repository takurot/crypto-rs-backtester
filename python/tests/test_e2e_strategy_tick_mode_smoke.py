import polars as pl

import rust_backtester


def make_minimal_ticks_lazyframe(*, with_seq: bool = True) -> pl.LazyFrame:
    ts_exchange = [1_000, 2_000, 3_000, 4_000]
    price = [100_00000000, 101_00000000, 99_00000000, 100_00000000]
    qty = [1_00000000, 1_00000000, 1_00000000, 1_00000000]
    side = [1, -1, 1, -1]
    data = {"ts_exchange": ts_exchange, "price": price, "qty": qty, "side": side}
    if with_seq:
        data["seq"] = list(range(len(ts_exchange)))
    return pl.DataFrame(data).with_columns(pl.col("side").cast(pl.Int8)).lazy()


class _RecordingStrategy:
    def __init__(self) -> None:
        self.ticks: list[dict[str, int]] = []

    def on_tick(self, tick: dict, ctx) -> None:  # noqa: ANN001
        # tick is a dict of primitive fields.
        self.ticks.append(tick)


def test_e2e_strategy_tick_mode_smoke() -> None:
    lf = make_minimal_ticks_lazyframe(with_seq=True)
    feed_latency_ns = 1_000

    bt = rust_backtester.Backtester(
        data={"binance:BTC/USDT": lf},
        seed=42,
        python_mode="tick",
        batch_ms=100,
        feed_latency_ns=feed_latency_ns,
    )
    strat = _RecordingStrategy()
    bt.run(strat)

    assert [t["ts_exchange"] for t in strat.ticks] == [1_000, 2_000, 3_000, 4_000]
    assert [t["ts_local"] for t in strat.ticks] == [2_000, 3_000, 4_000, 5_000]

