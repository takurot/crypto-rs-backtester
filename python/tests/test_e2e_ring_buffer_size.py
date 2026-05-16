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


class _AlwaysBuyStrategy:
    """Submits a fresh buy order on every tick, exercising many fills."""

    def on_tick(self, tick, ctx) -> None:  # noqa: ANN001
        ctx.submit_order(symbol_id=tick.symbol_id, side=1, price=tick.price, qty=tick.qty)

    def on_order_update(self, _report, _ctx) -> None:  # noqa: ANN001
        pass


# ---------------------------------------------------------------------------
# run() path
# ---------------------------------------------------------------------------

def test_e2e_ring_buffer_default_size_is_backward_compatible() -> None:
    """Omitting ring_buffer_size must behave like the old hardcoded 10000."""
    n = 5
    lf = make_ticks(n)
    bt_default = rust_backtester.Backtester(
        data={"binance:BTC/USDT": lf}, seed=42, trade_log_mode="ringbuffer"
    )
    bt_explicit = rust_backtester.Backtester(
        data={"binance:BTC/USDT": lf}, seed=42, trade_log_mode="ringbuffer", ring_buffer_size=10000
    )
    r1 = bt_default.run(_AlwaysBuyStrategy())
    r2 = bt_explicit.run(_AlwaysBuyStrategy())
    assert r1.stats()["total_trades"] == r2.stats()["total_trades"]
    assert len(r1.trades()) == len(r2.trades())


def test_e2e_ring_buffer_custom_size_caps_retained_fills() -> None:
    """ring_buffer_size=3 retains exactly 3 fills when more fills occurred."""
    cap = 3
    n_ticks = 10  # enough to produce > cap fills
    lf = make_ticks(n_ticks)
    bt = rust_backtester.Backtester(
        data={"binance:BTC/USDT": lf}, seed=42, trade_log_mode="ringbuffer", ring_buffer_size=cap
    )
    result = bt.run(_AlwaysBuyStrategy())
    stats = result.stats()
    trades = result.trades()

    # Stats must count every fill (not just retained ones)
    assert stats["total_trades"] > cap, (
        f"Expected total_trades > {cap}, got {stats['total_trades']}"
    )
    # Retained list must be capped at ring_buffer_size
    assert len(trades) == cap, f"Expected exactly {cap} retained trades, got {len(trades)}"


def test_e2e_ring_buffer_size_one_retains_one_fill() -> None:
    """ring_buffer_size=1 retains exactly the last fill."""
    lf = make_ticks(6)
    bt = rust_backtester.Backtester(
        data={"binance:BTC/USDT": lf}, seed=42, trade_log_mode="ringbuffer", ring_buffer_size=1
    )
    result = bt.run(_AlwaysBuyStrategy())
    trades = result.trades()
    assert len(trades) == 1, f"Expected exactly 1 retained trade, got {len(trades)}"


def test_e2e_ring_buffer_stats_count_exceeds_cap() -> None:
    """total_trades in stats counts all fills even when the ring buffer discards some."""
    cap = 2
    n_ticks = 8
    lf = make_ticks(n_ticks)
    bt = rust_backtester.Backtester(
        data={"binance:BTC/USDT": lf}, seed=42, trade_log_mode="ringbuffer", ring_buffer_size=cap
    )
    result = bt.run(_AlwaysBuyStrategy())
    stats = result.stats()
    trades = result.trades()
    assert stats["total_trades"] > len(trades), (
        "total_trades should exceed retained trade count when buffer is smaller"
    )


def test_e2e_ring_buffer_size_zero_raises() -> None:
    """ring_buffer_size=0 must raise ValueError."""
    with pytest.raises((ValueError, Exception)):
        rust_backtester.Backtester(
            data={"binance:BTC/USDT": make_ticks(2)},
            seed=42,
            trade_log_mode="ringbuffer",
            ring_buffer_size=0,
        )


# ---------------------------------------------------------------------------
# run_many() path
# ---------------------------------------------------------------------------

def test_e2e_ring_buffer_run_many_caps_retained_fills() -> None:
    """ring_buffer_size is respected in the parallel run_many() path."""
    cap = 2
    lf = make_ticks(8)
    bt = rust_backtester.Backtester(
        data={"binance:BTC/USDT": lf}, seed=42, trade_log_mode="ringbuffer", ring_buffer_size=cap
    )
    results = bt.run_many([_AlwaysBuyStrategy(), _AlwaysBuyStrategy()])
    for result in results:
        trades = result.trades()
        assert len(trades) == cap, f"run_many: expected {cap} retained trades, got {len(trades)}"


# ---------------------------------------------------------------------------
# run_arrow() path
# ---------------------------------------------------------------------------

def test_e2e_ring_buffer_run_arrow_caps_retained_fills() -> None:
    """ring_buffer_size is respected in the zero-copy run_arrow() path."""
    import pyarrow as pa  # noqa: PLC0415

    cap = 2
    n = 8
    lf = make_ticks(n)
    df = lf.collect()
    arrow_table = df.to_arrow()
    stream = pa.RecordBatchReader.from_batches(
        arrow_table.schema, arrow_table.to_batches()
    )

    bt = rust_backtester.Backtester(
        data={"binance:BTC/USDT": lf},
        seed=42,
        trade_log_mode="ringbuffer",
        ring_buffer_size=cap,
    )
    result = bt.run_arrow(stream, _AlwaysBuyStrategy())
    trades = result.trades()
    assert len(trades) == cap, (
        f"run_arrow: expected {cap} retained trades, got {len(trades)}"
    )
