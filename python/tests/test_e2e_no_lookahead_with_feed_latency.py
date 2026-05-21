import polars as pl

import rust_backtester


def make_ticks(*, feed_latency_ns: int) -> pl.LazyFrame:
    # Three ticks: the middle tick would fill if the order incorrectly arrived "too early".
    ts_exchange = [1_000, 2_000, 3_000]
    price = [100_00000000, 100_00000000, 100_00000000]
    qty = [1_00000000, 1_00000000, 1_00000000]
    # Buy, then Sell (would fill), then Sell (should fill after correct ordering).
    side = [1, -1, -1]
    data: dict[str, list[int]] = {
        "ts_exchange": ts_exchange,
        "price": price,
        "qty": qty,
        "side": side,
        "seq": list(range(len(ts_exchange))),
        # Intentionally omit ts_local to exercise engine computation: ts_local = ts_exchange + feed_latency
    }
    return pl.DataFrame(data).with_columns(pl.col("side").cast(pl.Int8)).lazy()


class _LookaheadGuard:
    def __init__(self) -> None:
        self.tick_ctx_ts_local: list[int] = []
        self.order_update_ctx_ts_local: list[int] = []
        self.reports: list = []
        self.submitted = False

    def on_tick(self, tick, ctx) -> None:  # noqa: ANN001
        self.tick_ctx_ts_local.append(int(ctx.ts_local()))

        if not self.submitted:
            self.submitted = True
            ctx.submit_order(
                symbol_id=tick.symbol_id,
                side=1,  # buy
                price=tick.price,
                qty=tick.qty,
            )

    def on_order_update(self, report, ctx) -> None:  # noqa: ANN001
        self.order_update_ctx_ts_local.append(int(ctx.ts_local()))
        self.reports.append(report)


def test_no_lookahead_with_feed_latency() -> None:
    feed_latency_ns = 1_000
    lf = make_ticks(feed_latency_ns=feed_latency_ns)

    bt = rust_backtester.Backtester(
        data={"binance:BTC/USDT": lf},
        seed=42,
        python_mode="tick",
        batch_ms=100,
        feed_latency_ns=feed_latency_ns,
    )
    strat = _LookaheadGuard()
    bt.run(strat)

    # Strategy observes each tick at ts_local = ts_exchange + feed_latency.
    assert strat.tick_ctx_ts_local == [2_000, 3_000, 4_000]

    # If the engine incorrectly allowed the order to arrive before the truth tick at ts_exchange=2_000,
    # we'd see a fill delivered at ts_local=3_000. Correct behavior fills on ts_exchange=3_000 => delivery at 4_000.
    assert strat.order_update_ctx_ts_local == [4_000]
    assert any(r.status == "Filled" for r in strat.reports)


def test_explicit_zero_order_update_latency_is_respected() -> None:
    """Explicit order_update_latency_ns=0 must NOT fall back to feed_latency_ns.

    Backward-compatibility guard: callers that explicitly pass 0 should still
    get zero-latency order updates regardless of feed_latency_ns.
    """
    feed_latency_ns = 1_000
    lf = make_ticks(feed_latency_ns=feed_latency_ns)

    bt = rust_backtester.Backtester(
        data={"binance:BTC/USDT": lf},
        seed=42,
        python_mode="tick",
        batch_ms=100,
        feed_latency_ns=feed_latency_ns,
        order_update_latency_ns=0,
    )
    strat = _LookaheadGuard()
    bt.run(strat)

    # With explicit zero latency, order fills arrive at exchange time (no added delay).
    assert strat.order_update_ctx_ts_local == [3_000]
    assert any(r.status == "Filled" for r in strat.reports)

