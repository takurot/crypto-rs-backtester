from ._core import BacktestResult, Backtester, __version__, call_strategy_on_ticks

_SCALE: int = 100_000_000  # 1e8 fixed-point scale


def to_fixed(val: float) -> int:
    """Convert a float value to fixed-point int (scale = 1e8)."""
    return round(val * _SCALE)


def from_fixed(val: int) -> float:
    """Convert a fixed-point int (scale = 1e8) to float."""
    return val / _SCALE


__all__ = [
    "BacktestResult",
    "Backtester",
    "__version__",
    "call_strategy_on_ticks",
    "to_fixed",
    "from_fixed",
]
