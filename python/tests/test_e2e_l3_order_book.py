"""E2E tests for L3 order-book support (Issue #65)."""
from __future__ import annotations

import polars as pl
import pyarrow as pa
import pytest
import rust_backtester


SCALE = 100_000_000


def make_trade_ticks() -> pl.LazyFrame:
    return pl.DataFrame(
        {
            "ts_exchange": [1_000, 2_000],
            "price": [100 * SCALE, 100 * SCALE],
            "qty": [1 * SCALE, 6],
            "side": [1, -1],
            "seq": [0, 1],
        }
    ).lazy()


def make_l3_depth() -> pl.LazyFrame:
    return pl.DataFrame(
        {
            "ts_exchange": [500],
            "price": [100 * SCALE],
            "qty": [5],
            "side": [1],
            "order_id": [20],
            "action": [1],
        }
    ).lazy()


def _reader(lf: pl.LazyFrame) -> pa.RecordBatchReader:
    table = lf.collect().to_arrow()
    return pa.RecordBatchReader.from_batches(table.schema, table.to_batches())


class BuyOnce:
    def __init__(self) -> None:
        self.submitted = False

    def on_tick(self, tick, ctx) -> None:  # noqa: ANN001
        if not self.submitted:
            self.submitted = True
            ctx.submit_order(
                symbol_id=tick.symbol_id,
                side=1,
                price=tick.price,
                qty=3,
            )

    def on_order_update(self, _report, _ctx) -> None:  # noqa: ANN001
        pass


def test_e2e_l3_exact_fill_uses_l3_queue_position() -> None:
    bt = rust_backtester.Backtester(
        data={"BTC": make_trade_ticks()},
        l3_data={"BTC": make_l3_depth()},
        depth_mode="l3",
        feed_latency_ns=0,
        order_update_latency_ns=0,
        seed=42,
        symbol_map={"BTC": 1},
    )

    result = bt.run(BuyOnce())

    trades = result.trades()
    assert len(trades) == 1
    assert trades[0]["qty"] == 1
    assert trades[0]["is_taker"] is False


def test_e2e_l3_exact_fill_works_with_run_arrow_dict_stream() -> None:
    bt = rust_backtester.Backtester(
        data={},
        l3_data={"BTC": make_l3_depth()},
        depth_mode="l3",
        feed_latency_ns=0,
        order_update_latency_ns=0,
        seed=42,
        symbol_map={"BTC": 1},
    )

    result = bt.run_arrow({"BTC": _reader(make_trade_ticks())}, BuyOnce())

    trades = result.trades()
    assert len(trades) == 1
    assert trades[0]["qty"] == 1


def test_e2e_l3_depth_mode_requires_l3_data() -> None:
    with pytest.raises(ValueError, match="l3_data"):
        rust_backtester.Backtester(
            data={"BTC": make_trade_ticks()},
            depth_mode="l3",
        )


def test_e2e_l3_missing_order_id_column_raises_clear_error() -> None:
    bad_l3 = make_l3_depth().drop("order_id")
    bt = rust_backtester.Backtester(
        data={"BTC": make_trade_ticks()},
        l3_data={"BTC": bad_l3},
        depth_mode="l3",
        feed_latency_ns=0,
        symbol_map={"BTC": 1},
    )

    with pytest.raises(ValueError, match="order_id"):
        bt.run(BuyOnce())
