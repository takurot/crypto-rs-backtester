"""E2E tests for cancel_order via PyContext (Issue #59)."""
from __future__ import annotations

import polars as pl
import pytest
import rust_backtester


def make_ticks() -> pl.LazyFrame:
    """4 ticks, deterministic, trade-only."""
    return pl.DataFrame(
        {
            "ts_exchange": [1_000, 2_000, 3_000, 4_000],
            "price": [100_00000000, 100_00000000, 100_00000000, 100_00000000],
            "qty": [1_00000000, 1_00000000, 1_00000000, 1_00000000],
            "side": [1, -1, 1, -1],
            "seq": [0, 1, 2, 3],
        }
    ).lazy()


class CancelOnSecondTickStrategy:
    """Submit a buy limit below market on tick-1, cancel it on tick-2.

    The buy is placed at 90_00000000, well below the traded price of 100_00000000,
    so it will not fill before the cancel arrives.
    """

    def __init__(self) -> None:
        self.submitted_order_id: int | None = None
        self.reports: list[dict] = []

    def on_tick(self, tick, ctx) -> None:
        if tick.ts_exchange == 1_000:
            ctx.submit_order(
                symbol_id=1,
                side=1,  # buy limit far below market — will not fill
                price=90_00000000,
                qty=1_00000000,
            )
        elif tick.ts_exchange == 2_000 and self.submitted_order_id is not None:
            ctx.cancel_order(self.submitted_order_id)

    def on_order_update(self, report, ctx) -> None:
        self.reports.append(
            {
                "order_id": report.order_id,
                "status": report.status,
            }
        )
        if report.status == "Open":
            self.submitted_order_id = report.order_id


def test_e2e_cancel_order_cancels_pending_order() -> None:
    """Submit a buy limit order then cancel it; expect a Cancelled report."""
    strat = CancelOnSecondTickStrategy()
    bt = rust_backtester.Backtester(data={"BTC": make_ticks()}, feed_latency_ns=0, seed=42)
    bt.run(strat)

    statuses = [r["status"] for r in strat.reports]
    assert "Cancelled" in statuses, f"Expected Cancelled in {statuses}"


def test_e2e_cancel_order_no_fill_after_cancel() -> None:
    """After cancellation the order must not fill on subsequent ticks."""
    strat = CancelOnSecondTickStrategy()
    bt = rust_backtester.Backtester(data={"BTC": make_ticks()}, feed_latency_ns=0, seed=42)
    result = bt.run(strat)

    # No fills should be recorded because the order was cancelled before any fills.
    trades = result.trades()
    assert len(trades) == 0, f"Expected no fills, got {trades}"
