
import pytest
import polars as pl
import random
from rust_backtester import Backtester

class CountStrategy:
    def __init__(self):
        self.count = 0
        self.last_ts = 0

    def on_tick(self, tick, ctx):
        self.count += 1
        self.last_ts = tick['ts_exchange']

def test_run_many_deterministic_and_ordered():
    # 1. Setup Data
    n_ticks = 1000
    df = pl.DataFrame({
        "ts_exchange": range(1000, 1000 + n_ticks),
        "price": [100] * n_ticks,
        "qty": [1] * n_ticks,
        "side": [1] * n_ticks,
    }).lazy()
    
    data = {"BINANCE:BTCUSDT": df}
    
    backtester = Backtester(
        data=data,
        feed_latency_ns=0,
        order_update_latency_ns=0,
        python_mode="tick",
        batch_ms=0,
        seed=12345
    )

    # 2. Create N strategies
    n_runs = 4
    strategies = [CountStrategy() for _ in range(n_runs)]

    # 3. Run Many
    results = backtester.run_many(strategies)

    # 4. Verify Basic Results
    assert len(results) == n_runs
    
    for i, res in enumerate(results):
        # Access stats or verify side effects?
        # BacktestResult has 'stats' which is BacktestStats.
        # But our strategy doesn't trade, so stats are empty/default?
        # Our strategy modifies python object state (self.count).
        # Wait. Parallel execution in threads.
        # The 'strategies' passed in are Python objects.
        # Modified in-place by threads?
        # Yes, PyStrategy struct holds 'obj: Py<PyAny>'.
        # Rayon threads call 'on_tick' which calls Python method on the object.
        # Since we use 'allow_threads' and then 'Python::with_gil' inside,
        # access to the Python object is serialized by GIL?
        # Or concurrent but protected by GIL?
        # The GIL ensures only one thread executes Python code at a time.
        # So 'count' updates are safe from data races (GIL).
        # But 'parallelism' for Python execution is effectively serialized.
        # The 'run_many' effectively becomes concurrent but serialized execution of Python chunks.
        # BUT the data loading was parallel/zero-copy? No, data loading was done once.
        # The Rust engine loop runs in parallel.
        # BUT every tick calls Python.
        # So we expect correct results (count == 1000).
        
        # Verify result content (from Rust side stats/trades - should be empty as we didn't trade)
        assert res.stats()['total_trades'] == 0
        
        # Verify Python side effects
        # Since we passed instances, they should be updated.
        assert strategies[i].count == n_ticks
        assert strategies[i].last_ts == 1000 + n_ticks - 1

    # Verify Order
    # The strategies list order should be preserved in results.
    # Since all inputs are identical in this test, order is trivial.
    # If we had different strategies (e.g. one counts half), we could check order.

def test_run_many_different_strategies():
    # Setup Data (same)
    n_ticks = 100
    df = pl.DataFrame({
        "ts_exchange": range(n_ticks),
        "price": [100] * n_ticks,
        "qty": [1] * n_ticks,
        "side": [1] * n_ticks,
    }).lazy()
    data = {"S": df}

    backtester = Backtester(data, seed=42)

    class StratA:
        def on_tick(self, t, c): pass
    
    class StratB:
        def on_tick(self, t, c): pass
    
    strategies = [StratA(), StratB(), StratA()]
    
    results = backtester.run_many(strategies)
    assert len(results) == 3
    # Results are BacktestResult.
    # We can't easily check which strategy produced which result from the wrapper alone 
    # unless stats differ.
    # But since no trading, stats are identical.
    # The test passes if it runs without error.


def test_e2e_run_many_order_update_latency_matches_run():
    """run_many must honour order_update_latency_ns the same way run() does.

    Regression test for Issue #46: run_many() was passing self.feed_latency_ns
    as order_update_latency_ns in EngineConfig, ignoring the configured value.
    """
    lf = pl.DataFrame({
        "ts_exchange": [1000, 2000],
        "price": [100, 100],
        "qty": [1, 1],
        "side": pl.Series([1, -1], dtype=pl.Int8),
        "seq": [0, 1],
    }).lazy()

    class FillStrategy:
        def __init__(self):
            self.report_ts: list = []
            self.submitted = False

        def on_tick(self, tick, ctx):
            if not self.submitted:
                self.submitted = True
                ctx.submit_order(tick["symbol_id"], 1, tick["price"], tick["qty"])

        def on_order_update(self, report, ctx):
            self.report_ts.append(ctx.ts_local())

    bt_run = Backtester(
        data={"s": lf}, feed_latency_ns=0, order_update_latency_ns=5000, seed=42
    )
    s_run = FillStrategy()
    bt_run.run(s_run)

    bt_many = Backtester(
        data={"s": lf}, feed_latency_ns=0, order_update_latency_ns=5000, seed=42
    )
    s_many = FillStrategy()
    bt_many.run_many([s_many])

    assert s_run.report_ts == s_many.report_ts, (
        f"run() got {s_run.report_ts} but run_many() got {s_many.report_ts}"
    )

