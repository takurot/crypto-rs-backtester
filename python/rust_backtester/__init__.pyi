"""Type stubs for rust_backtester public API."""

from ._core import (
    Backtester as Backtester,
    BacktestResult as BacktestResult,
    Context as Context,
    OrderReport as OrderReport,
    Tick as Tick,
    __version__ as __version__,
    call_strategy_on_ticks as call_strategy_on_ticks,
)

def to_fixed(val: float) -> int: ...
def from_fixed(val: int) -> float: ...

__all__ = [
    "BacktestResult",
    "Backtester",
    "__version__",
    "call_strategy_on_ticks",
    "to_fixed",
    "from_fixed",
]
