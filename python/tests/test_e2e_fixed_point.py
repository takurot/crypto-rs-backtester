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
