"""Tests that verify _core.pyi stubs are present and mypy accepts them (Issue #62)."""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def test_core_pyi_stub_exists() -> None:
    stub = Path(__file__).parent.parent / "rust_backtester" / "_core.pyi"
    assert stub.exists(), f"_core.pyi not found at {stub}"


def test_init_pyi_stub_exists() -> None:
    stub = Path(__file__).parent.parent / "rust_backtester" / "__init__.pyi"
    assert stub.exists(), f"__init__.pyi not found at {stub}"


def test_mypy_accepts_stub_annotated_strategy(tmp_path: Path) -> None:
    """mypy must not report errors on a strategy annotated via TYPE_CHECKING."""
    strategy_src = """
from __future__ import annotations
from typing import TYPE_CHECKING
import polars as pl
from rust_backtester import Backtester, BacktestResult

if TYPE_CHECKING:
    from rust_backtester._core import Context, PyOrderReport, PyTick


class MyStrategy:
    def on_tick(self, tick: PyTick, ctx: Context) -> None:
        ctx.submit_order(symbol_id=1, side=1, price=tick.price, qty=1_00000000)

    def on_order_update(self, report: PyOrderReport, ctx: Context) -> None:
        _ = report.order_id
        _ = report.status


def run() -> BacktestResult:
    bt = Backtester(
        data={"BTC": pl.DataFrame({
            "ts_exchange": [1_000],
            "price": [100_00000000],
            "qty": [1_00000000],
            "side": [1],
            "seq": [0],
        }).lazy()},
        feed_latency_ns=0,
        seed=42,
    )
    return bt.run(MyStrategy())
"""
    src_file = tmp_path / "strategy.py"
    src_file.write_text(strategy_src)

    result = subprocess.run(
        [sys.executable, "-m", "mypy", str(src_file), "--ignore-missing-imports"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        f"mypy reported errors:\n{result.stdout}\n{result.stderr}"
    )
