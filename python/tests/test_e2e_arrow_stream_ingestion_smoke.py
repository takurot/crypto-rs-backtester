import pyarrow as pa
import polars as pl
import pytest
from rust_backtester import Backtester

class MinimalStrategy:
    def __init__(self):
        self.ticks_received = 0

    def on_tick(self, tick, ctx):
        self.ticks_received += 1

def make_minimal_ticks_lazyframe(*, with_seq: bool = True) -> pl.LazyFrame:
    # 4 ticks, deterministic order, trade-only.
    ts_exchange = [1_000, 2_000, 3_000, 4_000]
    price = [100_00000000, 101_00000000,  99_00000000, 100_00000000]
    qty =   [  1_00000000,   1_00000000,   1_00000000,   1_00000000]
    side =  [           1,          -1,            1,          -1]
    data = {"ts_exchange": ts_exchange, "price": price, "qty": qty, "side": side}
    if with_seq:
        data["seq"] = list(range(len(ts_exchange)))
    return pl.DataFrame(data).with_columns(pl.col("side").cast(pl.Int8)).lazy()

def test_e2e_arrow_stream_ingestion_smoke():
    # 1. Prepare data
    lf = make_minimal_ticks_lazyframe(with_seq=True)
    df = lf.collect()
    
    # Convert to Arrow Table -> RecordBatchReader
    table = df.to_arrow()
    # Ensure it is a Table (Polars to_arrow returns Table or Array)
    if not isinstance(table, pa.Table):
        table = pa.Table.from_batches([table]) # Handle if it returns a RecordBatch
        
    # Create a RecordBatchReader from the table
    # This reader implements the PyCapsule interface (__arrow_c_stream__)
    batches = table.to_batches()
    reader = pa.RecordBatchReader.from_batches(table.schema, batches)

    # 2. Run backtest via run_arrow
    backtester = Backtester(
        data={"ignored": lf}, # data arg is ignored by run_arrow but required by new for now?
        seed=42
    )
    
    # Note: Backtester.new requires data arg, but run_arrow uses the passed stream. 
    # To run run_arrow we need an instance. 
    # The current Backtester struct stores `data` in `self.data`. 
    # If we want to use run_arrow, we can pass dummy data or any dict to `new`.
    
    strategy = MinimalStrategy()
    
    # 3. Execution
    # Pass the reader directly. PyO3 + our get_arrow_stream should handle it.
    result = backtester.run_arrow(reader, strategy)
    
    # 4. Assertions
    # 4 ticks in input
    stats = result.stats()
    # We didn't trade, so total_trades is 0. 
    # But we can verify no crash.
    assert stats["total_trades"] == 0
    
    # To verify we actually processed ticks, we'd need to check strategy state.
    # But our Rust strategy wrapper creates a new Python strategy instance if we passed a class?
    # No, we pass an INSTANCE usually.
    # Let's check: in lib.rs `let strat = PyStrategy { obj: strategy };`
    # It takes Py<PyAny>. If we pass an instance, it holds that instance.
    
    assert strategy.ticks_received == 4
