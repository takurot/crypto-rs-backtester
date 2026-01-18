import polars as pl

import rust_backtester


from dataclasses import dataclass


@dataclass
class Tick:
    ts_exchange: int
    symbol_id: int
    price: int
    qty: int
    side: int


def make_minimal_ticks_lazyframe(*, with_seq: bool = True) -> pl.LazyFrame:
    ts_exchange = [1_000, 2_000, 3_000, 4_000]
    price = [100_00000000, 101_00000000, 99_00000000, 100_00000000]
    qty = [1_00000000, 1_00000000, 1_00000000, 1_00000000]
    side = [1, -1, 1, -1]
    data = {"ts_exchange": ts_exchange, "price": price, "qty": qty, "side": side}
    if with_seq:
        data["seq"] = list(range(len(ts_exchange)))
    return pl.DataFrame(data).with_columns(pl.col("side").cast(pl.Int8)).lazy()


class _TickRecorder:
    def __init__(self) -> None:
        self.ticks: list[Tick] = []

    def on_tick(self, tick, ctx) -> None:  # noqa: ANN001
        # tick is a PyTick object with attributes
        self.ticks.append(Tick(
            ts_exchange=tick.ts_exchange,
            symbol_id=tick.symbol_id,
            price=tick.price,
            qty=tick.qty,
            side=tick.side,
        ))

    def on_ticks(self, ticks, ctx) -> None:  # noqa: ANN001
        # ticks is an Arrow RecordBatch, convert each row to a Tick
        n = ticks.num_rows
        ts_idx = ticks.schema.get_field_index("ts_exchange")
        sym_idx = ticks.schema.get_field_index("symbol_id")
        price_idx = ticks.schema.get_field_index("price")
        qty_idx = ticks.schema.get_field_index("qty")
        side_idx = ticks.schema.get_field_index("side")

        ts_col = ticks.column(ts_idx)
        sym_col = ticks.column(sym_idx)
        price_col = ticks.column(price_idx)
        qty_col = ticks.column(qty_idx)
        side_col = ticks.column(side_idx)
        for i in range(n):
            self.ticks.append(Tick(
                ts_exchange=int(ts_col[i].as_py()),
                symbol_id=int(sym_col[i].as_py()),
                price=int(price_col[i].as_py()),
                qty=int(qty_col[i].as_py()),
                side=int(side_col[i].as_py()),
            ))


def test_e2e_tick_vs_batch_equivalence() -> None:
    lf = make_minimal_ticks_lazyframe(with_seq=True)

    bt_tick = rust_backtester.Backtester(
        data={"binance:BTC/USDT": lf},
        seed=42,
        python_mode="tick",
        batch_ms=100,
        feed_latency_ns=0,
    )
    s_tick = _TickRecorder()
    bt_tick.run(s_tick)

    bt_batch = rust_backtester.Backtester(
        data={"binance:BTC/USDT": lf},
        seed=42,
        python_mode="batch",
        batch_ms=1,
        feed_latency_ns=0,
    )
    s_batch = _TickRecorder()
    bt_batch.run(s_batch)

    assert s_tick.ticks == s_batch.ticks

