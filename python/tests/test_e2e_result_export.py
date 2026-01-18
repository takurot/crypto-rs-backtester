"""
Test 5.3: Zero-Copy Result Export (trades_df, equity_curve_df).

Validates that result export methods return correct schema and values,
including support for arrow-compatible zero-copy export.
"""
import polars as pl
import pytest


def make_trade_ticks_lazyframe(with_seq: bool = True) -> pl.LazyFrame:
    """Generate minimal tick data that produces fills for testing exports."""
    # Buy at 100, counter-party sell at 100 -> fill
    # Sell at 110, counter-party buy at 110 -> fill (+10 PnL)
    ts_exchange = [1_000, 2_000, 3_000, 4_000]
    price = [100_00000000, 100_00000000, 110_00000000, 110_00000000]
    qty = [1_00000000, 1_00000000, 1_00000000, 1_00000000]
    side = [1, -1, -1, 1]  # Buy, Sell, Sell, Buy
    data = {"ts_exchange": ts_exchange, "price": price, "qty": qty, "side": side}
    if with_seq:
        data["seq"] = list(range(len(ts_exchange)))
    return pl.DataFrame(data).with_columns(pl.col("side").cast(pl.Int8)).lazy()


class SimpleStrategy:
    """Strategy that buys on first tick, sells on third tick."""

    def __init__(self):
        self.tick_count = 0

    def on_tick(self, tick, ctx):
        self.tick_count += 1
        # Buy on first tick, sell on third tick -> Realized PnL expected.
        if self.tick_count == 1:
            ctx.submit_order(
                symbol_id=1, side=1, price=tick["price"], qty=tick["qty"]
            )
        elif self.tick_count == 3:
            ctx.submit_order(
                symbol_id=1, side=-1, price=tick["price"], qty=tick["qty"]
            )

    def on_order_update(self, report, ctx):
        pass


def test_e2e_result_trades_df_values():
    """Test that trades_df() returns correct schema and values via Arrow."""
    from rust_backtester import Backtester

    lf = make_trade_ticks_lazyframe()
    data = {"BTCUSDT": lf}
    bt = Backtester(data, seed=42, python_mode="tick", trade_log_mode="all")
    strategy = SimpleStrategy()
    result = bt.run(strategy)

    # Convert to Polars DataFrame (zero copy from Arrow)
    trades_df = pl.DataFrame(result.trades_df())
    
    # Validations
    assert len(trades_df) == 2  # Buy fill and Sell fill
    # First trade: Side=Buy (1), Price=100
    assert trades_df["side"][0] == 1
    assert trades_df["price"][0] == 100_00000000
    assert trades_df["ts_exchange"][0] == 2_000
    
    # Second trade: Side=Sell (-1), Price=110
    assert trades_df["side"][1] == -1
    assert trades_df["price"][1] == 110_00000000
    assert trades_df["ts_exchange"][1] == 4_000


def test_e2e_result_equity_curve_df_values():
    """Test that equity_curve_df() returns correct PnL delta history."""
    from rust_backtester import Backtester

    lf = make_trade_ticks_lazyframe()
    data = {"BTCUSDT": lf}
    bt = Backtester(data, seed=42, python_mode="tick", trade_log_mode="all")
    strategy = SimpleStrategy()
    result = bt.run(strategy)

    eq_df = pl.DataFrame(result.equity_curve_df())
    
    # We expect 0 PnL on open (index 0) and +10 PnL on close (index 1)
    # Actually, equity_curve_from_pnl_deltas accumulates deltas.
    # Open: PnL=0. Close: PnL=+10.
    # Resulting curve: [(1000, 0), (3000, 10_000_00000)]
    
    # Check if we have rows
    assert len(eq_df) > 0
    
    # Check final equity
    final_equity = eq_df["equity"][-1]
    assert final_equity == 10_00000000  # +10.00 profit


def test_e2e_summary_only_mode():
    """Test that SummaryOnly mode returns correct stats despite empty logs."""
    from rust_backtester import Backtester

    lf = make_trade_ticks_lazyframe()
    data = {"BTCUSDT": lf}
    bt = Backtester(data, seed=42, python_mode="tick", trade_log_mode="summaryonly")
    strategy = SimpleStrategy()
    result = bt.run(strategy)

    # Trades DF should be empty (but schema present if implemented that way, or just empty list)
    # The current implementation returns empty arrays of capacity 0.
    trades_df = pl.DataFrame(result.trades_df())
    assert len(trades_df) == 0

    # Stats should be correct
    stats = result.stats()
    assert stats["total_trades"] == 2  # 2 fills
    assert stats["total_pnl"] == 10_00000000
    # Win rate calc: 1 win out of 2 fills (round trip logic vs fill logic)
    # Fill 1: Open (PnL 0). Fill 2: Close (PnL +10). 
    # Win count = 1. Total fills = 2. Win rate = 0.5.
    assert stats["win_rate"] == 0.5


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
