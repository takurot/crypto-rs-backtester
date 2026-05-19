import math

from ._core import BacktestResult, Backtester, __version__, call_strategy_on_ticks

_SCALE: int = 100_000_000  # 1e8 fixed-point scale
_I64_MIN: int = -(1 << 63)
_I64_MAX: int = (1 << 63) - 1


def to_fixed(val: float) -> int:
    """Convert a float to a fixed-point int using 1e8 scale.

    Rounding: half-away-from-zero (matches Rust core ``f64::round()``).
    ``val`` must be finite; raises ``ValueError`` for NaN/Inf.
    Raises ``OverflowError`` if the result is outside the i64 range.
    This function is intended for I/O boundaries only — never use float
    arithmetic inside the simulation core.
    """
    if not math.isfinite(val):
        raise ValueError(f"to_fixed requires a finite value, got {val!r}")
    scaled = val * _SCALE
    result = (
        math.floor(abs(scaled) + 0.5) if scaled >= 0 else -math.floor(-scaled + 0.5)
    )
    if not (_I64_MIN <= result <= _I64_MAX):
        raise OverflowError(f"to_fixed({val!r}) = {result} overflows i64 range")
    return result


def from_fixed(val: int) -> float:
    """Convert a fixed-point int (1e8 scale) to float.

    This function is intended for I/O boundaries only — never use float
    arithmetic inside the simulation core.
    """
    return val / _SCALE


__all__ = [
    "BacktestResult",
    "Backtester",
    "__version__",
    "call_strategy_on_ticks",
    "to_fixed",
    "from_fixed",
]
