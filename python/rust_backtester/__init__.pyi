"""Type stubs for the rust_backtester public API."""
from __future__ import annotations

from ._core import (
    Backtester as Backtester,
    BacktestResult as BacktestResult,
    Context as Context,
    PyOrderReport as PyOrderReport,
    PyTick as PyTick,
    __version__ as __version__,
    call_strategy_on_ticks as call_strategy_on_ticks,
)
from ._core import __version__ as __version__

def to_fixed(val: float) -> int: ...
def from_fixed(val: int) -> float: ...

__all__ = [
    "Backtester",
    "BacktestResult",
    "Context",
    "PyOrderReport",
    "PyTick",
    "__version__",
    "call_strategy_on_ticks",
    "to_fixed",
    "from_fixed",
]
