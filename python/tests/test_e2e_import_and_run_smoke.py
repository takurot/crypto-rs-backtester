import polars as pl

import rust_backtester


def make_minimal_ticks_lazyframe(*, with_seq: bool = True) -> pl.LazyFrame:
    # Small deterministic stream: 4 ticks, 1 symbol, trade-only.
    ts_exchange = [1_000, 2_000, 3_000, 4_000]
    price = [100_00000000, 101_00000000, 99_00000000, 100_00000000]
    qty = [1_00000000, 1_00000000, 1_00000000, 1_00000000]
    side = [1, -1, 1, -1]
    data: dict[str, list[int]] = {
        "ts_exchange": ts_exchange,
        "price": price,
        "qty": qty,
        "side": side,
    }
    if with_seq:
        data["seq"] = list(range(len(ts_exchange)))
    return pl.DataFrame(data).lazy()


def _expected_checksum(df: pl.DataFrame) -> int:
    # Keep this aligned with Rust `checksum_from_polars_data`.
    checksum = 0
    for row in df.iter_rows(named=True):
        checksum += int(row["ts_exchange"])
        checksum += int(row["price"])
        checksum += int(row["qty"])
        checksum += int(row["side"])
        if "seq" in row:
            checksum += int(row["seq"])
    return checksum


def test_e2e_import_and_run_smoke() -> None:
    lf = make_minimal_ticks_lazyframe(with_seq=True)
    bt = rust_backtester.Backtester(data={"binance:BTC/USDT": lf}, seed=42)
    got = bt.run_smoke()

    expected = _expected_checksum(lf.collect())
    assert got == expected

