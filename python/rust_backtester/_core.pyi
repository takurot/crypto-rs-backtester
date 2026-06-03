"""Type stubs for the _core Rust extension module."""
from __future__ import annotations

from typing import Any, final

__version__: str

@final
class PyTick:
    """A single market trade/quote tick delivered to strategy callbacks.

    All attributes are read-only (backed by ``#[pyo3(get)]``).
    """

    @property
    def ts_exchange(self) -> int: ...
    @property
    def ts_local(self) -> int: ...
    @property
    def seq(self) -> int: ...
    @property
    def symbol_id(self) -> int: ...
    @property
    def price(self) -> int: ...
    @property
    def qty(self) -> int: ...
    @property
    def side(self) -> int: ...
    @property
    def flags(self) -> int: ...
    def __getitem__(self, key: str) -> Any: ...
    def __repr__(self) -> str: ...

@final
class PyOrderReport:
    """Order lifecycle update (open, fill, cancel, reject) delivered to strategy callbacks.

    All attributes are read-only (backed by ``#[pyo3(get)]``).
    """

    @property
    def order_id(self) -> int: ...
    @property
    def symbol_id(self) -> int: ...
    @property
    def status(self) -> str: ...
    @property
    def last_fill_qty(self) -> int: ...
    @property
    def last_fill_price(self) -> int: ...
    @property
    def filled_qty(self) -> int: ...
    @property
    def remaining_qty(self) -> int: ...
    @property
    def reason(self) -> str | None: ...
    def __getitem__(self, key: str) -> Any: ...
    def __repr__(self) -> str: ...
    def get(self, key: str, default: Any = ...) -> Any: ...

@final
class Context:
    """Strategy execution context passed to ``on_tick`` / ``on_order_update`` callbacks.

    Note: this class is an internal engine type; it cannot be directly imported
    at runtime. Use ``from __future__ import annotations`` or
    ``TYPE_CHECKING`` guards when annotating strategy method parameters.
    """

    def ts_local(self) -> int: ...
    def submit_order(
        self,
        symbol_id: int,
        side: int,
        price: int,
        qty: int,
        order_type: str = "limit",
    ) -> None: ...
    def cancel_order(self, order_id: int) -> None: ...

@final
class BacktestResult:
    """Result of a completed backtest run."""

    def stats(self) -> dict[str, Any]: ...
    def stats_human(self) -> dict[str, Any]: ...
    def trades(self) -> list[dict[str, Any]]: ...
    def trades_df(self) -> dict[str, Any]: ...
    def equity_curve_df(self) -> dict[str, Any]: ...

@final
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
        max_open_orders: int = 0,
        max_position: int = 0,
        max_loss: int = 0,
        depth_mode: str = "l2",
        l3_data: dict[str, Any] | None = None,
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
