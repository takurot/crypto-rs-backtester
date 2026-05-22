"""E2E tests for market order fills (Issue #61)."""

import polars as pl
import pytest
from rust_backtester import Backtester


def make_ticks(*, with_ask: bool = True) -> pl.LazyFrame:
    """4 ticks: first seeds an L2 ask (via price/side), rest are trades."""
    ts_exchange = [1_000, 2_000, 3_000, 4_000]
    # prices: ask at 101 (implicit via trade), then normal trades
    price = [101_00000000, 101_00000000, 100_00000000, 100_00000000]
    qty = [1_00000000, 1_00000000, 1_00000000, 1_00000000]
    side = [1, -1, 1, -1]  # buy, sell, buy, sell
    data = {
        "ts_exchange": ts_exchange,
        "price": price,
        "qty": qty,
        "side": side,
        "seq": list(range(4)),
    }
    return pl.DataFrame(data).lazy()


class MarketBuyStrategy:
    """Submits a market buy on the first tick."""

    def __init__(self):
        self.submitted = False
        self.reports = []

    def on_tick(self, tick, ctx):
        if not self.submitted:
            self.submitted = True
            ctx.submit_order(
                symbol_id=tick.symbol_id,
                side=1,
                price=0,
                qty=1_00000000,
                order_type="market",
            )

    def on_order_update(self, report, ctx):
        self.reports.append(report)


class MarketSellStrategy:
    """Submits a market sell on the first tick."""

    def __init__(self):
        self.submitted = False
        self.reports = []

    def on_tick(self, tick, ctx):
        if not self.submitted:
            self.submitted = True
            ctx.submit_order(
                symbol_id=tick.symbol_id,
                side=-1,
                price=0,
                qty=1_00000000,
                order_type="market",
            )

    def on_order_update(self, report, ctx):
        self.reports.append(report)


class LimitBuyStrategy:
    """Submits a limit buy (default order_type) for regression check."""

    def __init__(self):
        self.submitted = False
        self.reports = []

    def on_tick(self, tick, ctx):
        if not self.submitted:
            self.submitted = True
            ctx.submit_order(
                symbol_id=tick.symbol_id,
                side=1,
                price=tick.price,
                qty=1_00000000,
            )

    def on_order_update(self, report, ctx):
        self.reports.append(report)


def make_bt(**kwargs) -> Backtester:
    lf = make_ticks()
    return Backtester(data={"BTC": lf}, feed_latency_ns=0, **kwargs)


def test_e2e_market_order_fills_immediately():
    """Market buy should produce a single Filled report — no Open step."""
    strat = MarketBuyStrategy()
    result = make_bt().run(strat)
    reports = strat.reports
    assert len(reports) == 1, f"expected 1 report, got {len(reports)}: {reports}"
    assert reports[0].status == "Filled"
    assert reports[0].last_fill_qty == 1_00000000
    assert reports[0].remaining_qty == 0


def test_e2e_market_order_taker_flag_and_fee():
    """Market fill should be tagged is_taker=True and taker_fee_bps applied."""
    strat = MarketBuyStrategy()
    result = make_bt(taker_fee_bps=5).run(strat)

    df_dict = result.trades_df()
    # is_taker column should contain True for the market fill.
    import pyarrow as pa

    is_taker_arr = pa.chunked_array([df_dict["is_taker"]])
    fee_arr = pa.chunked_array([df_dict["fee"]])
    assert is_taker_arr[0].as_py() is True
    assert fee_arr[0].as_py() > 0  # some fee was charged


def test_e2e_market_order_fills_at_last_trade_price_when_book_empty():
    """Market order without L2 data falls back to last trade price and fills."""

    class EarlyMarketBuyStrategy:
        def __init__(self):
            self.submitted = False
            self.reports = []

        def on_tick(self, tick, ctx):
            if not self.submitted:
                self.submitted = True
                ctx.submit_order(
                    symbol_id=tick.symbol_id,
                    side=1,
                    price=0,
                    qty=1_00000000,
                    order_type="market",
                )

        def on_order_update(self, report, ctx):
            self.reports.append(report)

    lf = pl.DataFrame(
        {
            "ts_exchange": [1_000, 2_000],
            "price": [100_00000000, 101_00000000],
            "qty": [1_00000000, 1_00000000],
            "side": [1, -1],
            "seq": [0, 1],
        }
    ).lazy()
    bt = Backtester(data={"BTC": lf}, feed_latency_ns=0)
    strat = EarlyMarketBuyStrategy()
    bt.run(strat)

    reports = strat.reports
    assert len(reports) == 1
    # Fills at the last trade price (fallback when no L2 data).
    assert reports[0].status == "Filled"
    assert reports[0].last_fill_price == 100_00000000


def test_e2e_market_order_invalid_order_type_raises():
    """Unknown order_type string should propagate as ValueError from bt.run()."""

    class BadTypeStrategy:
        def __init__(self):
            self.submitted = False

        def on_tick(self, tick, ctx):
            if not self.submitted:
                self.submitted = True
                # Let the ValueError propagate (no try/except).
                ctx.submit_order(
                    symbol_id=tick.symbol_id,
                    side=1,
                    price=0,
                    qty=1_00000000,
                    order_type="foobar",
                )

        def on_order_update(self, report, ctx):
            pass

    lf = make_ticks()
    bt = Backtester(data={"BTC": lf}, feed_latency_ns=0)
    with pytest.raises(ValueError, match="unknown order_type"):
        bt.run(BadTypeStrategy())


def test_e2e_limit_order_still_works_after_market_order_impl():
    """Regression: existing limit order flow should remain unaffected."""
    strat = LimitBuyStrategy()
    result = make_bt().run(strat)
    reports = strat.reports
    # Limit orders go through Open → Filled lifecycle, so at least 2 reports expected.
    statuses = [r.status for r in reports]
    assert "Open" in statuses, f"expected Open in {statuses}"
    assert "Filled" in statuses or "PartiallyFilled" in statuses, f"no fill in {statuses}"


def test_e2e_taker_fee_bps_accepted_without_error():
    """taker_fee_bps=5 should no longer raise ValueError."""
    bt = Backtester(data={"BTC": make_ticks()}, feed_latency_ns=0, taker_fee_bps=5)
    # Just constructing should not raise.
    assert bt is not None
