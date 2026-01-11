import time

import pytest

import rust_backtester


class _DummyStrategy:
    def on_ticks(self, ticks) -> int:  # noqa: ANN001
        # Do as little work as possible; we want to approximate callback overhead.
        return len(ticks)


@pytest.mark.bench
def test_bench_python_batch_callback_overhead() -> None:
    strategy = _DummyStrategy()
    batch_size = 256
    iterations = 1_000

    t0 = time.perf_counter()
    rust_backtester.call_strategy_on_ticks(strategy, batch_size=batch_size, iterations=iterations)
    dt = time.perf_counter() - t0

    # Not a performance gate; just a repeatable measurement harness.
    assert dt >= 0.0

