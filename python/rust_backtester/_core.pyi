"""Type stubs for the _core Rust extension module."""
from __future__ import annotations

from typing import Any

__version__: str

class PyTick:
    """A single market trade/quote tick delivered to strategy callbacks."""

    ts_exchange: int
    ts_local: int
    seq: int
    symbol_id: int
    price: int
    qty: int
    side: int
    flags: int

    def __getitem__(self, key: str) -> Any: ...
    def __repr__(self) -> str: ...

class PyOrderReport:
    """Order lifecycle update (open, fill, cancel, reject) delivered to strategy callbacks."""

    order_id: int
    symbol_id: int
    status: str
    last_fill_qty: int
    last_fill_price: int
    filled_qty: int
    remaining_qty: int
    reason: str | None

    def __getitem__(self, key: str) -> Any: ...
    def __repr__(self) -> str: ...
    def get(self, key: str, default: Any = ...) -> Any: ...

class Context:
    """Strategy execution context: submit/cancel orders and read current timestamp."""

    def ts_local(self) -> int: ...
    def submit_order(
        self,
        symbol_id: int,
        side: int,
        price: int,
        qty: int,
    ) -> None: ...
    def cancel_order(self, order_id: int) -> None: ...

class BacktestResult:
    """Result of a completed backtest run."""

    def stats(self) -> dict[str, Any]: ...
    def trades(self) -> list[dict[str, Any]]: ...
    def trades_df(self) -> dict[str, Any]: ...
    def equity_curve_df(self) -> dict[str, Any]: ...

class Backtester:
    """Tick-level backtester backed by a Rust simulation engine."""

    def __init__(
        self,
        data: dict[str, Any],
        feed_latency_ns: int = 0,
        order_update_latency_ns: int | None = None,
        python_mode: str = "tick",
        batch_ms: int = 0,
        seed: int = 42,
        trade_log_mode: str = "all",
        maker_fee_bps: int = 0,
        taker_fee_bps: int = 0,
        ring_buffer_size: int = 10000,
        symbol_map: dict[str, int] | None = None,
        queue_model: str = "conservative",
        latency_model: str = "constant",
        latency_mean_ns: int = 0,
        latency_std_ns: int = 0,
    ) -> None: ...
    def run(self, strategy: Any) -> BacktestResult: ...
    def run_many(self, strategies: list[Any]) -> list[BacktestResult]: ...
    def run_arrow(self, stream: Any, strategy: Any) -> BacktestResult: ...
    def run_smoke(self) -> int: ...

def call_strategy_on_ticks(
    strategy: Any,
    batch_size: int,
    iterations: int,
) -> None: ...
