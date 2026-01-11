import polars as pl

import rust_backtester


def make_minimal_ticks_lazyframe(*, with_seq: bool = True) -> pl.LazyFrame:
    ts_exchange = [1_000, 2_000, 3_000, 4_000]
    price = [100_00000000, 101_00000000, 99_00000000, 100_00000000]
    qty = [1_00000000, 1_00000000, 1_00000000, 1_00000000]
    side = [1, -1, 1, -1]
    data: dict[str, list[int]] = {
        "ts_exchange": ts_exchange,
        "price": price,
        "qty": qty,
        "side": side,
    }
    if with_seq:
        data["seq"] = list(range(len(ts_exchange)))
    return pl.DataFrame(data).lazy()


class _TickRecorder:
    def __init__(self) -> None:
        self.ticks: list[dict] = []

    def on_tick(self, tick: dict, ctx) -> None:  # noqa: ANN001
        self.ticks.append(tick)

    def on_ticks(self, ticks: list[dict], ctx) -> None:  # noqa: ANN001
        self.ticks.extend(ticks)


def test_e2e_tick_vs_batch_equivalence() -> None:
    lf = make_minimal_ticks_lazyframe(with_seq=True)

    bt_tick = rust_backtester.Backtester(
        data={"binance:BTC/USDT": lf},
        seed=42,
        python_mode="tick",
        batch_ms=100,
        feed_latency_ns=0,
    )
    s_tick = _TickRecorder()
    bt_tick.run(s_tick)

    bt_batch = rust_backtester.Backtester(
        data={"binance:BTC/USDT": lf},
        seed=42,
        python_mode="batch",
        batch_ms=1,
        feed_latency_ns=0,
    )
    s_batch = _TickRecorder()
    bt_batch.run(s_batch)

    assert s_tick.ticks == s_batch.ticks

