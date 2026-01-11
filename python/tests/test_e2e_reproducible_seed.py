import polars as pl

import rust_backtester


def make_ticks(*, ts_exchange: list[int], price: int, qty: int, side: list[int]) -> pl.LazyFrame:
    assert len(ts_exchange) == len(side)
    data: dict[str, list[int]] = {
        "ts_exchange": ts_exchange,
        "price": [price for _ in ts_exchange],
        "qty": [qty for _ in ts_exchange],
        "side": side,
        "seq": list(range(len(ts_exchange))),
    }
    return pl.DataFrame(data).lazy()


class _Recorder:
    def __init__(self) -> None:
        self.ticks: list[dict] = []
        self.reports: list[dict] = []
        self.submitted = False

    def on_tick(self, tick: dict, ctx) -> None:  # noqa: ANN001
        self.ticks.append(tick)

        if not self.submitted:
            self.submitted = True
            ctx.submit_order(
                symbol_id=int(tick["symbol_id"]),
                side=1,  # buy
                price=int(tick["price"]),
                qty=int(tick["qty"]),
            )

    def on_order_update(self, report: dict, ctx) -> None:  # noqa: ANN001
        self.reports.append(report)


def test_e2e_reproducible_seed() -> None:
    lf = make_ticks(
        ts_exchange=[1_000, 2_000, 3_000, 4_000],
        price=100_00000000,
        qty=1_00000000,
        side=[1, -1, 1, -1],
    )

    bt = rust_backtester.Backtester(
        data={"binance:BTC/USDT": lf},
        seed=42,
        python_mode="tick",
        batch_ms=100,
        feed_latency_ns=0,
    )

    s1 = _Recorder()
    bt.run(s1)
    s2 = _Recorder()
    bt.run(s2)

    assert s1.ticks == s2.ticks
    assert s1.reports == s2.reports

