"""E2E tests for client_order_id correlation via ctx.submit_order (Issue #104)."""
from __future__ import annotations

import polars as pl
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


class TwoOrdersOneCallbackStrategy:
    """Submit two buy limits below market in a single callback, each with its own
    client_order_id, to verify that fills/acks can be correlated back to the
    submission that produced them.
    """

    def __init__(self) -> None:
        self.returned_ids: list[int] = []
        self.reports: list[dict] = []

    def on_tick(self, tick, ctx) -> None:
        if tick.ts_exchange == 1_000:
            rid1 = ctx.submit_order(
                symbol_id=1,
                side=1,
                price=90_00000000,
                qty=1_00000000,
                client_order_id=101,
            )
            rid2 = ctx.submit_order(
                symbol_id=1,
                side=1,
                price=89_00000000,
                qty=1_00000000,
                client_order_id=202,
            )
            self.returned_ids = [rid1, rid2]

    def on_order_update(self, report, ctx) -> None:
        self.reports.append(
            {
                "order_id": report.order_id,
                "client_order_id": report.client_order_id,
                "status": report.status,
            }
        )


def test_e2e_submit_order_returns_client_order_id() -> None:
    """submit_order echoes back the caller-supplied client_order_id immediately."""
    strat = TwoOrdersOneCallbackStrategy()
    bt = rust_backtester.Backtester(
        data={"BTC": make_ticks()},
        feed_latency_ns=0,
        seed=42,
        symbol_map={"BTC": 1},
    )
    bt.run(strat)

    assert strat.returned_ids == [101, 202]


def test_e2e_client_order_id_correlates_order_updates() -> None:
    """on_order_update reports echo the client_order_id of the originating order,
    allowing a strategy to correlate reports with submissions made in the same
    callback.
    """
    strat = TwoOrdersOneCallbackStrategy()
    bt = rust_backtester.Backtester(
        data={"BTC": make_ticks()},
        feed_latency_ns=0,
        seed=42,
        symbol_map={"BTC": 1},
    )
    bt.run(strat)

    open_reports = [r for r in strat.reports if r["status"] == "Open"]
    assert len(open_reports) == 2

    client_ids_by_order_id = {r["order_id"]: r["client_order_id"] for r in open_reports}
    assert set(client_ids_by_order_id.values()) == {101, 202}
    # Each order_id must map to exactly one client_order_id (no collisions).
    assert len(client_ids_by_order_id) == 2


class DefaultClientOrderIdStrategy:
    """Submit a single order without specifying client_order_id."""

    def __init__(self) -> None:
        self.returned_id: int | None = None
        self.reports: list[dict] = []

    def on_tick(self, tick, ctx) -> None:
        if tick.ts_exchange == 1_000:
            self.returned_id = ctx.submit_order(
                symbol_id=1,
                side=1,
                price=90_00000000,
                qty=1_00000000,
            )

    def on_order_update(self, report, ctx) -> None:
        self.reports.append(
            {"order_id": report.order_id, "client_order_id": report.client_order_id}
        )


def test_e2e_client_order_id_defaults_to_zero() -> None:
    """When client_order_id is not provided, it defaults to 0 (unset) and is
    echoed as 0 in order update reports.
    """
    strat = DefaultClientOrderIdStrategy()
    bt = rust_backtester.Backtester(
        data={"BTC": make_ticks()},
        feed_latency_ns=0,
        seed=42,
        symbol_map={"BTC": 1},
    )
    bt.run(strat)

    assert strat.returned_id == 0
    assert len(strat.reports) >= 1
    assert all(r["client_order_id"] == 0 for r in strat.reports)
