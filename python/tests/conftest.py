import importlib
import os
import subprocess
from pathlib import Path


def _ensure_rust_extension_installed() -> None:
    """
    Make `pytest` a single-command entrypoint for E2E tests.

    If the extension module isn't importable yet, build+install it into the
    current environment via `maturin develop`.
    """
    try:
        import rust_backtester  # noqa: F401

        return
    except Exception:
        # Fall through to build step.
        pass

    repo_root = Path(__file__).resolve().parents[2]

    cmd = [
        "maturin",
        "develop",
        "--extras",
        "dev",
    ]
    env = os.environ.copy()
    # Python 3.14+ may be newer than PyO3's max supported version at build time.
    # In that case, build via stable ABI (abi3) forward-compat mode.
    env.setdefault("PYO3_USE_ABI3_FORWARD_COMPATIBILITY", "1")
    subprocess.check_call(cmd, cwd=repo_root, env=env)

    importlib.invalidate_caches()
    import rust_backtester  # noqa: F401


def pytest_sessionstart(session):  # noqa: ARG001
    _ensure_rust_extension_installed()

