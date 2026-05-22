import polars as pl

import rust_backtester


def make_ticks(*, ts_exchange: list[int], price: int, qty: int, side: list[int], with_seq: bool = True) -> pl.LazyFrame:
    assert len(ts_exchange) == len(side)
    data: dict[str, list[int]] = {
        "ts_exchange": ts_exchange,
        "price": [price for _ in ts_exchange],
        "qty": [qty for _ in ts_exchange],
        "side": side,
    }
    if with_seq:
        data["seq"] = list(range(len(ts_exchange)))
    return pl.DataFrame(data).with_columns(pl.col("side").cast(pl.Int8)).lazy()


class _Recorder:
    def __init__(self) -> None:
        self.ticks: list = []
        self.reports: list = []
        self.submitted = False

    def on_tick(self, tick, ctx) -> None:  # noqa: ANN001
        # Store tick as tuple for easy comparison
        self.ticks.append((tick.ts_exchange, tick.symbol_id, tick.price, tick.qty, tick.side))

        if not self.submitted:
            self.submitted = True
            ctx.submit_order(
                symbol_id=tick.symbol_id,
                side=1,  # buy
                price=tick.price,
                qty=tick.qty,
            )

    def on_order_update(self, report, ctx) -> None:  # noqa: ANN001
        # Store report as tuple for easy comparison
        self.reports.append((report.order_id, report.symbol_id, report.status))


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


def test_run_many_same_ts_multi_symbol_order_is_deterministic() -> None:
    """run_many must deliver ticks in the same order across identical runs (Issue #56).

    Two symbols share ts_exchange=1_000 to stress-test tie-breaking in parse_polars_data.
    Both strategies in the run_many call must observe the same tick sequence.
    """
    lf_a = make_ticks(ts_exchange=[1_000, 2_000], price=100_00000000, qty=1_00000000, side=[1, -1])
    lf_b = make_ticks(ts_exchange=[1_000, 3_000], price=200_00000000, qty=1_00000000, side=[-1, 1])

    bt = rust_backtester.Backtester(
        data={"symbol_a": lf_a, "symbol_b": lf_b},
        seed=42,
        python_mode="tick",
        batch_ms=100,
        feed_latency_ns=0,
    )

    s1 = _Recorder()
    s2 = _Recorder()
    results = bt.run_many([s1, s2])  # noqa: F841

    # Both strategies receive identical pre-loaded event streams — tick orders must match.
    assert s1.ticks == s2.ticks, (
        f"run_many tick order non-deterministic: s1={s1.ticks!r}, s2={s2.ticks!r}"
    )

