import polars as pl

import rust_backtester


def make_ticks(with_seq: bool = True) -> pl.LazyFrame:
    # Deterministic 3 ticks designed to:
    # - Fill a BUY @ 100 on tick#1 (sell trade at 100)
    # - Then fill a SELL @ 101 on tick#2 (buy trade at 101), realizing +1.00 quote PnL
    ts_exchange = [1_000, 2_000, 3_000]
    price = [100_00000000, 100_00000000, 101_00000000]
    qty = [1_00000000, 1_00000000, 1_00000000]
    side = [1, -1, 1]
    data = {
        "ts_exchange": ts_exchange,
        "price": price,
        "qty": qty,
        "side": side,
    }
    if with_seq:
        data["seq"] = list(range(len(ts_exchange)))
    return pl.DataFrame(data).with_columns(pl.col("side").cast(pl.Int8)).lazy()


class _RoundTripStrategy:
    def __init__(self) -> None:
        self.submitted_buy = False
        self.submitted_sell = False

    def on_tick(self, tick: dict, ctx) -> None:  # noqa: ANN001
        if not self.submitted_buy:
            self.submitted_buy = True
            ctx.submit_order(
                symbol_id=int(tick["symbol_id"]),
                side=1,  # buy
                price=int(tick["price"]),
                qty=int(tick["qty"]),
            )

    def on_order_update(self, report: dict, ctx) -> None:  # noqa: ANN001
        # After the BUY is filled, submit a SELL @ 101 to close and realize PnL.
        if report.get("status") == "Filled" and not self.submitted_sell:
            self.submitted_sell = True
            ctx.submit_order(
                symbol_id=int(report["symbol_id"]),
                side=-1,  # sell
                price=101_00000000,
                qty=1_00000000,
            )


def test_e2e_stats_basic_round_trip_pnl() -> None:
    lf = make_ticks()
    bt = rust_backtester.Backtester(
        data={"binance:BTC/USDT": lf},
        seed=42,
        python_mode="tick",
        batch_ms=100,
        feed_latency_ns=0,
    )

    result = bt.run(_RoundTripStrategy())

    trades = result.trades()
    stats = result.stats()

    assert len(trades) == 2
    assert stats["total_trades"] == 2
    assert stats["total_pnl"] == 1_00000000

