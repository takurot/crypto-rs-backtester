"""Type stubs for the rust_backtester._core compiled extension (PyO3/maturin)."""

from typing import Any, Literal

import pyarrow as pa

__version__: str

class Tick:
    """A single market tick delivered to the strategy (feed-delayed)."""

    ts_exchange: int
    ts_local: int
    seq: int
    symbol_id: int
    price: int
    qty: int
    side: int
    flags: int

    def __getitem__(self, key: str) -> int: ...
    def __repr__(self) -> str: ...

class OrderReport:
    """Order lifecycle event: open acknowledgment, partial fill, fill, cancel, or reject."""

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
    def get(self, key: str, default: Any = None) -> Any: ...

class Context:
    """Strategy execution context — submit/cancel orders, query current timestamp."""

    def ts_local(self) -> int: ...
    def submit_order(
        self,
        symbol_id: int,
        side: int,
        price: int,
        qty: int,
        order_type: Literal["limit", "market"] = "limit",
    ) -> None: ...
    def cancel_order(self, order_id: int) -> None: ...

class BacktestResult:
    """Result of a completed backtest run."""

    def stats(self) -> dict[str, Any]: ...
    def trades(self) -> list[dict[str, Any]]: ...
    def trades_df(self) -> dict[str, pa.Array]: ...
    def equity_curve_df(self) -> dict[str, pa.Array]: ...

class Backtester:
    """Main backtesting engine — configure, feed data, run strategies."""

    def __init__(
        self,
        data: dict[str, Any],
        feed_latency_ns: int = 0,
        order_update_latency_ns: int | None = None,
        python_mode: Literal["tick", "batch"] = "tick",
        batch_ms: int = 0,
        seed: int = 42,
        trade_log_mode: Literal["all", "ringbuffer", "summaryonly", "none"] = "all",
        maker_fee_bps: int = 0,
        taker_fee_bps: int = 0,
        ring_buffer_size: int = 10000,
        symbol_map: dict[str, int] | None = None,
        queue_model: Literal["conservative", "volume_clock"] = "conservative",
        latency_model: Literal["constant", "log_normal"] = "constant",
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
