import polars as pl
import pytest

import rust_backtester


# ---------------------------------------------------------------------------
# to_fixed
# ---------------------------------------------------------------------------


def test_to_fixed_whole_number() -> None:
    assert rust_backtester.to_fixed(100.0) == 100_00000000


def test_to_fixed_decimal() -> None:
    assert rust_backtester.to_fixed(0.5) == 50_000_000


def test_to_fixed_negative() -> None:
    assert rust_backtester.to_fixed(-1.5) == -150_000_000


def test_to_fixed_zero() -> None:
    assert rust_backtester.to_fixed(0.0) == 0


def test_to_fixed_small_fraction() -> None:
    # 0.00000001 == 1 in fixed-point (minimum unit)
    assert rust_backtester.to_fixed(0.00000001) == 1


def test_to_fixed_rounding() -> None:
    # 0.000000016 is above 1.5 * 1e-8, rounds up to 2
    assert rust_backtester.to_fixed(0.000000016) == 2


def test_to_fixed_tie_positive() -> None:
    # 0.000000025 * 1e8 == 2.5 exactly → half-away-from-zero rounds to 3
    # (Python round() would give 2 via banker's rounding)
    assert rust_backtester.to_fixed(0.000000025) == 3


def test_to_fixed_tie_negative() -> None:
    # -0.000000025 * 1e8 == -2.5 → half-away-from-zero rounds to -3
    assert rust_backtester.to_fixed(-0.000000025) == -3


def test_to_fixed_nan_raises() -> None:
    with pytest.raises(ValueError, match="finite"):
        rust_backtester.to_fixed(float("nan"))


def test_to_fixed_inf_raises() -> None:
    with pytest.raises(ValueError, match="finite"):
        rust_backtester.to_fixed(float("inf"))


def test_to_fixed_neg_inf_raises() -> None:
    with pytest.raises(ValueError, match="finite"):
        rust_backtester.to_fixed(float("-inf"))


def test_to_fixed_overflow_raises() -> None:
    # 2**63 / 1e8 exceeds i64 max when scaled
    too_large = (1 << 63) / 100_000_000 + 1.0
    with pytest.raises(OverflowError):
        rust_backtester.to_fixed(too_large)


# ---------------------------------------------------------------------------
# from_fixed
# ---------------------------------------------------------------------------


def test_from_fixed_whole_number() -> None:
    assert rust_backtester.from_fixed(100_00000000) == pytest.approx(100.0)


def test_from_fixed_decimal() -> None:
    assert rust_backtester.from_fixed(50_000_000) == pytest.approx(0.5)


def test_from_fixed_negative() -> None:
    assert rust_backtester.from_fixed(-150_000_000) == pytest.approx(-1.5)


def test_from_fixed_zero() -> None:
    assert rust_backtester.from_fixed(0) == pytest.approx(0.0)


def test_from_fixed_minimum_unit() -> None:
    assert rust_backtester.from_fixed(1) == pytest.approx(1e-8)


# ---------------------------------------------------------------------------
# Roundtrip
# ---------------------------------------------------------------------------


def test_roundtrip_to_from_fixed() -> None:
    for val in [1.0, 100.0, 0.5, 0.00000001, 99999.99999999]:
        assert rust_backtester.from_fixed(
            rust_backtester.to_fixed(val)
        ) == pytest.approx(val, rel=1e-9)


def test_roundtrip_from_to_fixed() -> None:
    for val in [100_00000000, 50_000_000, 1, 0, 9999999_99999999]:
        assert rust_backtester.to_fixed(rust_backtester.from_fixed(val)) == val


# ---------------------------------------------------------------------------
# Integration: converted values flow through Backtester
# ---------------------------------------------------------------------------


class _PriceRecorder:
    def __init__(self) -> None:
        self.prices: list[int] = []

    def on_tick(self, tick, ctx) -> None:  # noqa: ANN001
        self.prices.append(tick.price)

    def on_order_update(self, _report, _ctx) -> None:  # noqa: ANN001
        pass


def test_to_fixed_price_matches_backtester_tick() -> None:
    """to_fixed output matches the fixed-point price seen inside Backtester."""
    price_float = 100.0
    expected_fixed = rust_backtester.to_fixed(price_float)

    lf = (
        pl.DataFrame(
            {
                "ts_exchange": [1_000],
                "price": [expected_fixed],
                "qty": [rust_backtester.to_fixed(1.0)],
                "side": [1],
                "seq": [0],
            }
        )
        .with_columns(pl.col("side").cast(pl.Int8))
        .lazy()
    )

    strat = _PriceRecorder()
    bt = rust_backtester.Backtester(data={"sym": lf}, seed=42)
    bt.run(strat)

    assert strat.prices == [expected_fixed], (
        f"Expected tick.price={expected_fixed}, got {strat.prices}"
    )
