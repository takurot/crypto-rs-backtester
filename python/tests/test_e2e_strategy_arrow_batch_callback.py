import pytest
import rust_backtester
import polars as pl
import pyarrow as pa

class ArrowBatchStrategy:
    def __init__(self):
        self.batches_received = 0
        self.total_rows = 0
        self.last_ts = 0

    def on_ticks(self, batch, ctx):
        self.batches_received += 1
        # Check if it is pyarrow record batch
        assert isinstance(batch, pa.RecordBatch)
        self.total_rows += batch.num_rows
        
        # Verify content access
        ts_col = batch["ts_exchange"]
        if len(ts_col) > 0:
            self.last_ts = ts_col[-1].as_py()

def test_e2e_strategy_arrow_batch_callback():
    # Generate data
    n = 1000
    data = pl.DataFrame({
        "ts_exchange": [1000 * i for i in range(n)],
        "price": [100 for _ in range(n)],
        "qty": [1 for _ in range(n)],
        "side": [1 for _ in range(n)],
    }).with_columns(pl.col("side").cast(pl.Int8)).lazy()

    strategy = ArrowBatchStrategy()
    
    backtester = rust_backtester.Backtester(
        data={"SYM": data},
        python_mode="batch",
        batch_ms=100, # Should trigger multiple batches
    )
    
    result = backtester.run(strategy)
    
    assert strategy.batches_received > 0
    assert strategy.total_rows == n
    assert strategy.last_ts == 1000 * (n - 1)
