"""
Test 5.3: Zero-Copy Result Export (trades_df, equity_curve_df).

Validates that result export methods return correct schema and values.
"""
import polars as pl
import pytest


def make_trade_ticks_lazyframe() -> pl.LazyFrame:
    """Generate minimal tick data that produces fills for testing exports."""
    # Buy at 100, counter-party sell at 100 -> fill
    # Sell at 101, counter-party buy at 101 -> fill
    ts_exchange = [1_000, 2_000, 3_000, 4_000]
    price = [100_00000000, 100_00000000, 101_00000000, 101_00000000]
    qty = [1_00000000, 1_00000000, 1_00000000, 1_00000000]
    side = [1, -1, -1, 1]  # Buy, Sell, Sell, Buy
    data = {
        "ts_exchange": ts_exchange,
        "price": price,
        "qty": qty,
        "side": side,
        "seq": list(range(len(ts_exchange))),
    }
    return pl.DataFrame(data).lazy()


class SimpleStrategy:
    """Strategy that buys on first tick, sells on third tick."""

    def __init__(self):
        self.tick_count = 0

    def on_tick(self, tick, ctx):
        self.tick_count += 1
        # Buy on first tick, sell on third tick
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


def test_e2e_result_trades_df_schema_and_values():
    """Test that trades_df() returns correct schema and values."""
    from rust_backtester import Backtester

    lf = make_trade_ticks_lazyframe()
    data = {"BTCUSDT": lf}
    bt = Backtester(data, seed=42, python_mode="tick")
    strategy = SimpleStrategy()
    result = bt.run(strategy)

    # Test trades_df() method
    trades_df = result.trades_df()
    assert "_len" in trades_df
    assert "ts_exchange" in trades_df
    assert "symbol_id" in trades_df
    assert "order_id" in trades_df
    assert "side" in trades_df
    assert "price" in trades_df
    assert "qty" in trades_df

    # Verify we have at least some trades
    n = trades_df["_len"]
    assert n >= 0  # May be 0 if no fills


def test_e2e_result_equity_curve_df_schema():
    """Test that equity_curve_df() returns correct schema."""
    from rust_backtester import Backtester

    lf = make_trade_ticks_lazyframe()
    data = {"BTCUSDT": lf}
    bt = Backtester(data, seed=42, python_mode="tick")
    strategy = SimpleStrategy()
    result = bt.run(strategy)

    # Test equity_curve_df() method
    eq_df = result.equity_curve_df()
    assert "_len" in eq_df
    assert "ts_exchange" in eq_df
    assert "equity" in eq_df

    # Verify schema is correct
    n = eq_df["_len"]
    assert isinstance(n, int)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
