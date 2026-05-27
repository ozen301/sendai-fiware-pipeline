"""Shared pytest fixtures.

The package logger is mutated by anything that calls
:func:`sendai_pipeline.logging_setup.configure_logging` (entry-point tests,
`scripts/refresh_metadata.py`'s ``main()``, etc.). Without isolation, those
mutations leak across test modules: ``configure_logging`` sets
``propagate=False`` on the ``sendai_pipeline`` logger, which then prevents
``caplog`` from observing records emitted in later modules. We snapshot the
logger's clean state once at session start and restore it after every
function-scoped test so every test runs against the same baseline.
"""

import logging
from collections.abc import Iterator
from typing import Any

import pytest

from sendai_pipeline.logging_setup import PACKAGE_LOGGER_NAME, JsonFormatter


@pytest.fixture(scope="session")
def _initial_pkg_logger_state() -> dict[str, Any]:
    pkg = logging.getLogger(PACKAGE_LOGGER_NAME)
    return {
        "handlers": list(pkg.handlers),
        "level": pkg.level,
        "propagate": pkg.propagate,
    }


@pytest.fixture(autouse=True)
def _reset_sendai_logger_state(
    _initial_pkg_logger_state: dict[str, Any],
) -> Iterator[None]:
    JsonFormatter._warned_keys.clear()
    yield
    pkg = logging.getLogger(PACKAGE_LOGGER_NAME)
    for handler in list(pkg.handlers):
        pkg.removeHandler(handler)
        try:
            handler.close()
        except Exception:
            pass
    for handler in _initial_pkg_logger_state["handlers"]:
        pkg.addHandler(handler)
    pkg.setLevel(_initial_pkg_logger_state["level"])
    pkg.propagate = _initial_pkg_logger_state["propagate"]
    JsonFormatter._warned_keys.clear()
