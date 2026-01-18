import polars as pl

import rust_backtester


def make_ticks(ts_exchange: list[int], *, with_seq: bool = True) -> pl.LazyFrame:
    # Constant price/qty; side alternates for realism.
    price = [100_00000000 for _ in ts_exchange]
    qty = [1_00000000 for _ in ts_exchange]
    side = [1 if i % 2 == 0 else -1 for i in range(len(ts_exchange))]
    data = {
        "ts_exchange": ts_exchange,
        "price": price,
        "qty": qty,
        "side": side,
    }
    if with_seq:
        data["seq"] = list(range(len(ts_exchange)))
    return pl.DataFrame(data).with_columns(pl.col("side").cast(pl.Int8)).lazy()


class _BatchRecorder:
    def __init__(self) -> None:
        self.tick_batch_sizes: list[int] = []
        self.tick_batch_ctx_ts_local: list[int] = []

    def on_ticks(self, ticks: list[dict], ctx) -> None:  # noqa: ANN001
        self.tick_batch_sizes.append(len(ticks))
        self.tick_batch_ctx_ts_local.append(int(ctx.ts_local()))


def test_batch_wakeup_max_batch_ms() -> None:
    # batch_ms=1 => max_batch_ns=1_000_000
    lf = make_ticks([1_000, 1_500_000, 2_000_000], with_seq=True)

    bt = rust_backtester.Backtester(
        data={"binance:BTC/USDT": lf},
        seed=42,
        python_mode="batch",
        batch_ms=1,
        feed_latency_ns=0,
    )
    strat = _BatchRecorder()
    bt.run(strat)

    # First wake at 1_000 + 1_000_000 = 1_001_000 flushes tick#0 only.
    # Second wake at 1_500_000 + 1_000_000 = 2_500_000 flushes tick#1 and tick#2.
    assert strat.tick_batch_sizes == [1, 2]
    assert strat.tick_batch_ctx_ts_local[0] == 1_001_000
    assert strat.tick_batch_ctx_ts_local[1] == 2_500_000


class _OrderUpdateWakeRecorder:
    def __init__(self) -> None:
        self.tick_wake_ts: list[int] = []
        self.order_update_wake_ts: list[int] = []
        self.seen_reports: list[dict] = []

    def on_ticks(self, ticks: list[dict], ctx) -> None:  # noqa: ANN001
        self.tick_wake_ts.append(int(ctx.ts_local()))

        # Submit a buy order on the first wakeup only.
        if len(self.tick_wake_ts) == 1:
            ctx.submit_order(
                symbol_id=1,
                side=1,
                price=int(ticks[0]["price"]),
                qty=int(ticks[0]["qty"]),
            )

    def on_order_updates(self, reports: list[dict], ctx) -> None:  # noqa: ANN001
        self.order_update_wake_ts.append(int(ctx.ts_local()))
        self.seen_reports.extend(reports)


def test_batch_wakeup_on_order_update_delivery() -> None:
    # First tick at 1_000, second tick at 1_500_000 fills the submitted order.
    lf = make_ticks([1_000, 1_500_000], with_seq=True)

    bt = rust_backtester.Backtester(
        data={"binance:BTC/USDT": lf},
        seed=42,
        python_mode="batch",
        batch_ms=1,
        feed_latency_ns=0,
    )
    strat = _OrderUpdateWakeRecorder()
    bt.run(strat)

    # First wake is time-based at 1_001_000.
    assert strat.tick_wake_ts[0] == 1_001_000

    # Without order-update wake, the next time-based wake would be 2_500_000.
    # We expect an order update wake at 1_500_000 instead.
    assert strat.order_update_wake_ts == [1_500_000]
    assert any(r.get("status") == "Filled" for r in strat.seen_reports)

