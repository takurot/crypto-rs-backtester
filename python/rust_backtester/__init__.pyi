"""Type stubs for the rust_backtester public API.

Only symbols exported by the runtime ``__init__.py`` are re-exported here.
To annotate strategy callbacks, import internal types under ``TYPE_CHECKING``::

    from __future__ import annotations
    from typing import TYPE_CHECKING
    if TYPE_CHECKING:
        from rust_backtester._core import Context, PyOrderReport, PyTick
"""
from __future__ import annotations

from ._core import (
    Backtester as Backtester,
    BacktestResult as BacktestResult,
    __version__ as __version__,
    call_strategy_on_ticks as call_strategy_on_ticks,
)

SCALE: int

def to_fixed(val: float) -> int: ...
def from_fixed(val: int) -> float: ...
def to_float(val: int) -> float: ...

__all__ = [
    "Backtester",
    "BacktestResult",
    "SCALE",
    "__version__",
    "call_strategy_on_ticks",
    "to_fixed",
    "from_fixed",
    "to_float",
]
