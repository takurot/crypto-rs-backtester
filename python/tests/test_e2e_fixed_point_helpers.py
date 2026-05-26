"""Tests for Issue #64: public SCALE constant, to_float alias, and stats_human()."""
import polars as pl
import pytest

import rust_backtester


def make_minimal_ticks() -> pl.LazyFrame:
    return (
        pl.DataFrame(
            {
                "ts_exchange": [1_000, 2_000, 3_000, 4_000],
                "price": [100_00000000, 101_00000000, 99_00000000, 100_00000000],
                "qty": [1_00000000, 1_00000000, 1_00000000, 1_00000000],
                "side": [1, -1, 1, -1],
                "seq": [0, 1, 2, 3],
            }
        )
        .with_columns(pl.col("side").cast(pl.Int8))
        .lazy()
    )


# ---------------------------------------------------------------------------
# SCALE constant
# ---------------------------------------------------------------------------


def test_scale_constant_exists() -> None:
    assert hasattr(rust_backtester, "SCALE")


def test_scale_constant_value() -> None:
    assert rust_backtester.SCALE == 100_000_000


def test_scale_is_public_int() -> None:
    assert isinstance(rust_backtester.SCALE, int)


# ---------------------------------------------------------------------------
# to_float function
# ---------------------------------------------------------------------------


def test_to_float_exists() -> None:
    assert hasattr(rust_backtester, "to_float")
    assert callable(rust_backtester.to_float)


def test_to_float_whole_number() -> None:
    assert rust_backtester.to_float(100_00000000) == pytest.approx(100.0)


def test_to_float_decimal() -> None:
    assert rust_backtester.to_float(50_000_000) == pytest.approx(0.5)


def test_to_float_negative() -> None:
    assert rust_backtester.to_float(-150_000_000) == pytest.approx(-1.5)


def test_to_float_zero() -> None:
    assert rust_backtester.to_float(0) == pytest.approx(0.0)


def test_to_float_minimum_unit() -> None:
    assert rust_backtester.to_float(1) == pytest.approx(1e-8)


def test_to_float_matches_from_fixed() -> None:
    """to_float and from_fixed must be identical."""
    for val in [0, 1, 100_00000000, -1_00000000, 9999999_99999999]:
        assert rust_backtester.to_float(val) == rust_backtester.from_fixed(val)


def test_to_float_roundtrip_with_to_fixed() -> None:
    for val in [1.0, 100.0, 0.5, 0.00000001]:
        assert rust_backtester.to_float(rust_backtester.to_fixed(val)) == pytest.approx(
            val, rel=1e-9
        )


# ---------------------------------------------------------------------------
# stats_human() method
# ---------------------------------------------------------------------------


class _BuyAndHoldStrategy:
    """Submits one buy order and waits (position never closed → zero PnL)."""

    def __init__(self) -> None:
        self._submitted = False

    def on_tick(self, tick, ctx) -> None:  # noqa: ANN001
        if not self._submitted:
            ctx.submit_order(tick.symbol_id, 1, tick.price, 1_00000000)
            self._submitted = True

    def on_order_update(self, _report, _ctx) -> None:  # noqa: ANN001
        pass


class _RoundTripStrategy:
    """Buy at tick 1, sell at tick 3 to generate non-zero realized PnL."""

    def __init__(self) -> None:
        self._buys = 0
        self._sells = 0
        self._fill_count = 0

    def on_tick(self, tick, ctx) -> None:  # noqa: ANN001
        # tick prices: 100, 101, 99, 100
        if self._buys == 0 and tick.price == 100_00000000:
            ctx.submit_order(tick.symbol_id, 1, tick.price, 1_00000000)
            self._buys += 1

    def on_order_update(self, report, ctx) -> None:  # noqa: ANN001
        if report.status == "Open":
            return
        if report.status in ("PartiallyFilled", "Filled") and self._fill_count == 0:
            self._fill_count += 1
            # Immediately submit a sell at the same price to generate a round-trip.
            ctx.submit_order(report.symbol_id, -1, report.last_fill_price, 1_00000000)


def test_stats_human_exists() -> None:
    bt = rust_backtester.Backtester(data={"sym": make_minimal_ticks()}, seed=42)
    result = bt.run(_BuyAndHoldStrategy())
    assert hasattr(result, "stats_human")
    assert callable(result.stats_human)


def test_stats_human_returns_dict() -> None:
    bt = rust_backtester.Backtester(data={"sym": make_minimal_ticks()}, seed=42)
    result = bt.run(_BuyAndHoldStrategy())
    human = result.stats_human()
    assert isinstance(human, dict)


def test_stats_human_monetary_fields_are_float() -> None:
    bt = rust_backtester.Backtester(data={"sym": make_minimal_ticks()}, seed=42)
    result = bt.run(_BuyAndHoldStrategy())
    human = result.stats_human()

    for field in ("total_pnl", "avg_trade_pnl", "total_fees_paid"):
        assert field in human, f"missing field: {field}"
        assert isinstance(human[field], float), f"{field} should be float"


def test_stats_human_all_keys_present() -> None:
    """stats_human() must expose exactly the same keys as stats()."""
    bt = rust_backtester.Backtester(data={"sym": make_minimal_ticks()}, seed=42)
    result = bt.run(_BuyAndHoldStrategy())
    assert result.stats_human().keys() == result.stats().keys()


def test_stats_human_non_monetary_fields_preserved() -> None:
    bt = rust_backtester.Backtester(data={"sym": make_minimal_ticks()}, seed=42)
    result = bt.run(_BuyAndHoldStrategy())
    human = result.stats_human()
    raw = result.stats()

    # All non-monetary fields (including max_drawdown_duration, avg_holding_period)
    # should be identical between stats() and stats_human().
    monetary = {"total_pnl", "avg_trade_pnl", "total_fees_paid"}
    for field, raw_val in raw.items():
        if field not in monetary:
            assert human[field] == raw_val, f"non-monetary field {field!r} differs"


def test_stats_human_monetary_values_match_scale() -> None:
    """stats_human monetary values == stats raw / SCALE, verified with non-zero values."""
    # Use _RoundTripStrategy to ensure non-zero monetary fields in stats.
    bt = rust_backtester.Backtester(
        data={"sym": make_minimal_ticks()},
        seed=42,
        maker_fee_bps=10,  # 1 bp fee so total_fees_paid > 0
    )
    result = bt.run(_RoundTripStrategy())
    human = result.stats_human()
    raw = result.stats()

    scale = rust_backtester.SCALE
    for field in ("total_pnl", "avg_trade_pnl", "total_fees_paid"):
        expected = raw[field] / scale
        assert human[field] == pytest.approx(expected, rel=1e-9), f"{field} mismatch"
        # Verify at least one monetary field is non-zero (to catch wrong-divisor bugs).
    assert raw["total_fees_paid"] != 0, "expected non-zero fees to exercise conversion"
