import importlib
import os
import subprocess
import sys
from pathlib import Path


def _ensure_rust_extension_installed() -> None:
    """
    Make `pytest` a single-command entrypoint for E2E tests.

    If the extension module isn't importable yet, build+install it into the
    current environment via `maturin develop`, targeting the *running*
    interpreter (sys.executable) so the installed .so lands in the same
    site-packages that pytest is using.
    """
    try:
        import rust_backtester  # noqa: F401

        return
    except Exception:
        # Fall through to build step.
        pass

    repo_root = Path(__file__).resolve().parents[2]

    # Use `sys.executable -m maturin` to guarantee maturin installs into the
    # same environment as the running pytest process, regardless of which
    # `maturin` binary is first on PATH.
    cmd = [
        sys.executable,
        "-m",
        "maturin",
        "develop",
        "--extras",
        "dev",
    ]
    env = os.environ.copy()
    # Python 3.14+ may be newer than PyO3's max supported version at build time.
    # In that case, build via stable ABI (abi3) forward-compat mode.
    env.setdefault("PYO3_USE_ABI3_FORWARD_COMPATIBILITY", "1")
    try:
        subprocess.check_call(cmd, cwd=repo_root, env=env)
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(
            f"maturin develop failed (exit {exc.returncode}). "
            f"Running interpreter: {sys.executable}. "
            "Ensure maturin is installed in this environment: "
            f'"{sys.executable}" -m pip install maturin'
        ) from exc

    importlib.invalidate_caches()
    try:
        import rust_backtester  # noqa: F401
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "rust_backtester still not importable after maturin develop. "
            f"Running interpreter: {sys.executable}. "
            "This usually means maturin installed the extension into a different "
            "environment. Check that maturin is installed in the same venv as pytest."
        ) from exc


def pytest_sessionstart(session):  # noqa: ARG001
    _ensure_rust_extension_installed()
