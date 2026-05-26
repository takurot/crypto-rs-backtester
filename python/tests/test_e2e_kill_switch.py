"""E2E tests for Issue #66: kill switch and pre-trade risk limits."""
import polars as pl
import pytest

import rust_backtester


def make_ticks_for_kill_switch() -> pl.LazyFrame:
    """8 ticks: 4 buys, 4 sells, prices 100-103."""
    return (
        pl.DataFrame(
            {
                "ts_exchange": list(range(1_000, 9_000, 1_000)),
                "price": [100_00000000] * 8,
                "qty": [1_00000000] * 8,
                "side": [1, -1, 1, -1, 1, -1, 1, -1],
                "seq": list(range(8)),
            }
        )
        .with_columns(pl.col("side").cast(pl.Int8))
        .lazy()
    )


class _OrderSpamStrategy:
    """Tries to submit orders on every tick."""

    def __init__(self) -> None:
        self.orders_submitted = 0
        self.rejections: list[str] = []
        self.last_order_id: int | None = None

    def on_tick(self, tick, ctx) -> None:  # noqa: ANN001
        ctx.submit_order(tick.symbol_id, 1, tick.price, 1_00000000)
        self.orders_submitted += 1

    def on_order_update(self, report, _ctx) -> None:  # noqa: ANN001
        if report.status == "Rejected":
            self.rejections.append(report.reason or "")


class _BigPositionStrategy:
    """Submits large buy orders trying to exceed max_position."""

    def __init__(self) -> None:
        self.submitted = 0
        self.rejections: list[str] = []

    def on_tick(self, tick, ctx) -> None:  # noqa: ANN001
        # Each tick, submit 2 units — will trigger max_position=3 quickly.
        ctx.submit_order(tick.symbol_id, 1, tick.price, 2_00000000)
        self.submitted += 1

    def on_order_update(self, report, _ctx) -> None:  # noqa: ANN001
        if report.status == "Rejected":
            self.rejections.append(report.reason or "")


class _LossyStrategy:
    """Buys once; a non-zero maker fee creates a small realized loss that triggers max_loss.

    The sell submitted after the buy fill is at a price that never matches the tick stream
    (ticks are all at 100, sell is at 99), so no infinite-loop risk. The only thing that
    matters for the kill switch is the fee deducted on the buy fill.
    """

    def __init__(self) -> None:
        self._step = 0
        self.rejections: list[str] = []

    def on_tick(self, tick, ctx) -> None:  # noqa: ANN001
        if self._step == 0:
            # Buy limit at market price; fee deduction creates a small net loss.
            ctx.submit_order(tick.symbol_id, 1, tick.price, 1_00000000)
        self._step += 1

    def on_order_update(self, report, ctx) -> None:  # noqa: ANN001
        if report.status == "Rejected":
            self.rejections.append(report.reason or "")
        elif report.status in ("PartiallyFilled", "Filled"):
            # Attempt to sell after the fill; will be rejected if kill switch already fired.
            ctx.submit_order(report.symbol_id, -1, report.last_fill_price - 1_00000000, 1_00000000)


# ---------------------------------------------------------------------------
# max_open_orders limit
# ---------------------------------------------------------------------------

def test_max_open_orders_rejects_excess() -> None:
    """Orders beyond max_open_orders should be rejected."""
    bt = rust_backtester.Backtester(
        data={"sym": make_ticks_for_kill_switch()},
        seed=42,
        max_open_orders=2,
    )
    strat = _OrderSpamStrategy()
    bt.run(strat)
    assert len(strat.rejections) > 0, "expected some rejections"


def test_max_open_orders_rejection_reason() -> None:
    """Rejection reason should mention risk."""
    bt = rust_backtester.Backtester(
        data={"sym": make_ticks_for_kill_switch()},
        seed=42,
        max_open_orders=1,
    )
    strat = _OrderSpamStrategy()
    bt.run(strat)
    assert any("risk" in r.lower() or "open_orders" in r.lower() for r in strat.rejections)


def test_max_open_orders_zero_means_unlimited() -> None:
    """max_open_orders=0 (default) means no limit."""
    bt = rust_backtester.Backtester(
        data={"sym": make_ticks_for_kill_switch()},
        seed=42,
        max_open_orders=0,
    )
    strat = _OrderSpamStrategy()
    bt.run(strat)
    # With no limit, no rejections from risk
    risk_rejections = [r for r in strat.rejections if "risk" in r.lower()]
    assert len(risk_rejections) == 0


# ---------------------------------------------------------------------------
# max_position limit
# ---------------------------------------------------------------------------

def test_max_position_rejects_when_exceeded() -> None:
    """Order that would exceed max_position should be rejected."""
    bt = rust_backtester.Backtester(
        data={"sym": make_ticks_for_kill_switch()},
        seed=42,
        max_position=1_00000000,  # max 1 unit
    )
    strat = _BigPositionStrategy()
    bt.run(strat)
    assert len(strat.rejections) > 0, "expected rejections when max_position exceeded"


def test_max_position_zero_means_unlimited() -> None:
    """max_position=0 (default) means no limit."""
    bt = rust_backtester.Backtester(
        data={"sym": make_ticks_for_kill_switch()},
        seed=42,
        max_position=0,
    )
    strat = _BigPositionStrategy()
    bt.run(strat)
    risk_rejections = [r for r in strat.rejections if "risk" in r.lower() or "position" in r.lower()]
    assert len(risk_rejections) == 0


# ---------------------------------------------------------------------------
# max_loss (kill switch)
# ---------------------------------------------------------------------------

def test_kill_switch_stats_killed_field_exists() -> None:
    """stats() should contain a 'killed' key."""
    bt = rust_backtester.Backtester(
        data={"sym": make_ticks_for_kill_switch()},
        seed=42,
    )
    result = bt.run(_LossyStrategy())
    assert "killed" in result.stats()


def test_kill_switch_default_not_triggered() -> None:
    """Without max_loss configured, killed should be False."""
    bt = rust_backtester.Backtester(
        data={"sym": make_ticks_for_kill_switch()},
        seed=42,
    )
    result = bt.run(_LossyStrategy())
    assert result.stats()["killed"] is False


def test_kill_switch_triggered_on_max_loss() -> None:
    """Kill switch fires when cumulative loss exceeds max_loss.

    maker_fee_bps=1 ensures the buy fill produces a small net loss (fee > 0),
    which is enough to breach max_loss=-1 (the smallest negative threshold).
    """
    bt = rust_backtester.Backtester(
        data={"sym": make_ticks_for_kill_switch()},
        seed=42,
        max_loss=-1,      # threshold: -0.00000001
        maker_fee_bps=1,  # any fill creates a fee-based loss
    )
    strat = _LossyStrategy()
    result = bt.run(strat)
    assert result.stats()["killed"] is True


def test_kill_switch_rejects_after_trigger() -> None:
    """Once killed, all new orders should be rejected with a risk reason."""
    bt = rust_backtester.Backtester(
        data={"sym": make_ticks_for_kill_switch()},
        seed=42,
        max_loss=-1,
        maker_fee_bps=1,
    )
    strat = _LossyStrategy()
    bt.run(strat)
    kill_rejections = [r for r in strat.rejections if "kill" in r.lower() or "risk" in r.lower()]
    assert len(kill_rejections) > 0
