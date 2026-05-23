"""E2E tests for market order (immediate taker fill) support (Issue #61)."""
from __future__ import annotations

import polars as pl
import pytest
import rust_backtester


def make_ticks_with_book() -> pl.LazyFrame:
    """4 trade ticks at stable prices for a predictable order book state."""
    return pl.DataFrame(
        {
            "ts_exchange": [1_000, 2_000, 3_000, 4_000],
            "price": [100_00000000, 100_00000000, 100_00000000, 100_00000000],
            "qty": [1_00000000, 1_00000000, 1_00000000, 1_00000000],
            "side": [1, -1, 1, -1],
            "seq": [0, 1, 2, 3],
        }
    ).lazy()


class MarketBuyStrategy:
    """Submit a market buy on the first tick."""

    def __init__(self) -> None:
        self.reports: list[dict] = []

    def on_tick(self, tick, ctx) -> None:
        if tick.ts_exchange == 1_000:
            ctx.submit_order(
                symbol_id=1,
                side=1,
                price=0,
                qty=1_00000000,
                order_type="market",
            )

    def on_order_update(self, report, ctx) -> None:
        self.reports.append(
            {
                "status": report.status,
                "last_fill_qty": report.last_fill_qty,
                "last_fill_price": report.last_fill_price,
                "is_open": report.status == "Open",
            }
        )


class MarketSellStrategy:
    """Submit a market sell on the first tick."""

    def __init__(self) -> None:
        self.reports: list[dict] = []

    def on_tick(self, tick, ctx) -> None:
        if tick.ts_exchange == 1_000:
            ctx.submit_order(
                symbol_id=1,
                side=-1,
                price=0,
                qty=1_00000000,
                order_type="market",
            )

    def on_order_update(self, report, ctx) -> None:
        self.reports.append({"status": report.status})


def _make_bt(taker_fee_bps: int = 0) -> rust_backtester.Backtester:
    return rust_backtester.Backtester(
        data={"BTC": make_ticks_with_book()},
        feed_latency_ns=0,
        seed=42,
        symbol_map={"BTC": 1},
        taker_fee_bps=taker_fee_bps,
    )


def test_market_buy_fills_immediately_no_open_report() -> None:
    """Market order must not produce an Open report — only fill or reject."""
    strat = MarketBuyStrategy()
    _make_bt().run(strat)

    statuses = [r["status"] for r in strat.reports]
    assert "Open" not in statuses, f"Market order produced unexpected Open report: {statuses}"


def test_market_buy_fills_on_first_tick() -> None:
    """Market buy fills within the same engine step as submission."""
    strat = MarketBuyStrategy()
    result = _make_bt().run(strat)

    trades = result.trades()
    assert len(trades) >= 1, "Expected at least one fill"
    fill = trades[0]
    assert fill["is_taker"] is True, "Market fill must be is_taker=True"
    assert fill["qty"] == 1_00000000


def test_market_sell_fills_on_first_tick() -> None:
    strat = MarketSellStrategy()
    result = _make_bt().run(strat)

    trades = result.trades()
    assert len(trades) >= 1, "Expected at least one fill"
    fill = trades[0]
    assert fill["is_taker"] is True


def test_market_order_taker_fee_deducted() -> None:
    """taker_fee_bps must be applied to market fills."""
    strat = MarketBuyStrategy()
    result = _make_bt(taker_fee_bps=10).run(strat)  # 0.1% fee

    trades = result.trades()
    assert len(trades) >= 1
    fill = trades[0]
    # fee = floor(price * qty * 10 / (1e8 * 10_000)) in fixed-point
    expected_fee = (100_00000000 * 1_00000000 * 10) // (100_000_000 * 10_000)
    assert fill["fee"] == expected_fee, f"Expected fee={expected_fee}, got {fill['fee']}"


def test_market_order_invalid_order_type_raises() -> None:
    """Passing an unknown order_type string must raise ValueError."""

    class BadOrderTypeStrategy:
        def on_tick(self, tick, ctx) -> None:
            ctx.submit_order(
                symbol_id=1, side=1, price=0, qty=1_00000000, order_type="foo"
            )

    with pytest.raises(Exception):
        _make_bt().run(BadOrderTypeStrategy())


def test_taker_fee_bps_accepted_in_constructor() -> None:
    """taker_fee_bps != 0 must no longer raise ValueError (market orders implemented)."""
    bt = rust_backtester.Backtester(
        data={"BTC": make_ticks_with_book()},
        feed_latency_ns=0,
        seed=42,
        symbol_map={"BTC": 1},
        taker_fee_bps=5,
    )
    assert bt is not None


def test_limit_orders_still_work_after_market_order_support() -> None:
    """Existing limit order behavior must be unchanged."""

    class LimitStrategy:
        def __init__(self) -> None:
            self.reports: list[str] = []

        def on_tick(self, tick, ctx) -> None:
            if tick.ts_exchange == 1_000:
                ctx.submit_order(
                    symbol_id=1, side=1, price=100_00000000, qty=1_00000000
                )

        def on_order_update(self, report, ctx) -> None:
            self.reports.append(report.status)

    strat = LimitStrategy()
    _make_bt().run(strat)

    assert "Open" in strat.reports, "Limit order must produce Open report"
