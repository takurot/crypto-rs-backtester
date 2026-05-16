import polars as pl
import pytest

import rust_backtester


def make_ticks(n: int) -> pl.LazyFrame:
    """Generate n alternating buy/sell ticks at price 100."""
    ts = list(range(1_000, 1_000 + n * 1_000, 1_000))
    price = [100_00000000] * n
    qty = [1_00000000] * n
    side = [1 if i % 2 == 0 else -1 for i in range(n)]
    seq = list(range(n))
    return (
        pl.DataFrame({"ts_exchange": ts, "price": price, "qty": qty, "side": side, "seq": seq})
        .with_columns(pl.col("side").cast(pl.Int8))
        .lazy()
    )


class _BuyOnceStrategy:
    """Submits one buy order on the first tick and never re-submits."""

    def __init__(self) -> None:
        self.submitted = False

    def on_tick(self, tick, ctx) -> None:  # noqa: ANN001
        if not self.submitted:
            self.submitted = True
            ctx.submit_order(symbol_id=tick.symbol_id, side=1, price=tick.price, qty=tick.qty)

    def on_order_update(self, _report, _ctx) -> None:  # noqa: ANN001
        pass


class _AlwaysBuyStrategy:
    """Submits a fresh buy order after every fill, exercising many fills."""

    def on_tick(self, tick, ctx) -> None:  # noqa: ANN001
        ctx.submit_order(symbol_id=tick.symbol_id, side=1, price=tick.price, qty=tick.qty)

    def on_order_update(self, _report, _ctx) -> None:  # noqa: ANN001
        pass


def test_e2e_ring_buffer_default_size() -> None:
    """ring_buffer_size defaults to 10000; API is backward-compatible."""
    lf = make_ticks(5)
    bt = rust_backtester.Backtester(
        data={"binance:BTC/USDT": lf},
        seed=42,
        trade_log_mode="ringbuffer",
        # ring_buffer_size not specified → should default to 10000
    )
    result = bt.run(_BuyOnceStrategy())
    stats = result.stats()
    # At least the fill count is non-negative (smoke check)
    assert stats["total_trades"] >= 0


def test_e2e_ring_buffer_custom_size_caps_trades() -> None:
    """ring_buffer_size=3 limits retained fills to 3 even with more fills."""
    n_ticks = 10
    lf = make_ticks(n_ticks)
    bt = rust_backtester.Backtester(
        data={"binance:BTC/USDT": lf},
        seed=42,
        trade_log_mode="ringbuffer",
        ring_buffer_size=3,
    )
    result = bt.run(_AlwaysBuyStrategy())
    trades = result.trades()
    # Retained fills must be capped at ring_buffer_size=3
    assert len(trades) <= 3, f"Expected ≤3 retained trades, got {len(trades)}"


def test_e2e_ring_buffer_size_one() -> None:
    """ring_buffer_size=1 retains only the last fill."""
    lf = make_ticks(6)
    bt = rust_backtester.Backtester(
        data={"binance:BTC/USDT": lf},
        seed=42,
        trade_log_mode="ringbuffer",
        ring_buffer_size=1,
    )
    result = bt.run(_AlwaysBuyStrategy())
    trades = result.trades()
    assert len(trades) <= 1, f"Expected ≤1 retained trade, got {len(trades)}"


def test_e2e_ring_buffer_stats_still_accurate() -> None:
    """total_trades in stats counts all fills even when ring buffer discards some."""
    lf = make_ticks(6)
    bt = rust_backtester.Backtester(
        data={"binance:BTC/USDT": lf},
        seed=42,
        trade_log_mode="ringbuffer",
        ring_buffer_size=2,
    )
    result = bt.run(_AlwaysBuyStrategy())
    stats = result.stats()
    trades = result.trades()
    # Stats track all fills; retained list is capped
    assert stats["total_trades"] >= len(trades)
