"""Deterministic fixture pipeline for durable-worker and walkthrough tests.

The pipeline is ``setup_task >> noisy_task``. ``setup_task`` exists so the
resolved order has something *before* the noisy one -- the plan and
walkthrough views are about order -- and it is deliberately silent: it prints
nothing, warns about nothing, and declares no outputs, so every assertion the
worker tests make about captured output and warning attribution still names
``noisy_task`` alone.

Both tasks carry documentation metadata (``description``, ``inputs``,
``docs``) pointing at ``docs/*.md`` in this project. Metadata is a pure read
model: it does not affect scheduling or execution.

``noisy_task``'s behaviour is selected by profile:

* ``success`` -- emit output and warnings, then return.
* ``slow``    -- emit output, then block until a sentinel file appears. Tests
                 create the sentinel to release the task, so no test ever has
                 to sleep for a fixed duration.
* ``failure`` -- emit output and warnings, then raise.
* ``db_error`` -- raise ``sqlite3.Error`` from task code. kptn's own default
                  state store is SQLite, so this is the case that must not be
                  mistaken for the durable run store failing.

Two extra behaviours are gated behind environment variables so that the
default (``success``) run stays byte-for-byte predictable:

* ``KPTN_UI_FIXTURE_RUN_LEVEL_WARNING`` -- warn at import time, i.e. outside
  any task, so the captured warning has no task attribution.
* ``KPTN_UI_FIXTURE_SENTINEL`` -- path of the ``slow`` profile's sentinel file.
"""

from __future__ import annotations

import logging
import os
import sqlite3
import sys
import time
import warnings
from pathlib import Path

import kptn

SENTINEL_ENV_VAR = "KPTN_UI_FIXTURE_SENTINEL"
RUN_LEVEL_WARNING_ENV_VAR = "KPTN_UI_FIXTURE_RUN_LEVEL_WARNING"
STARTED_MARKER = "slow task waiting for sentinel"
SENTINEL_TIMEOUT_SECONDS = 60.0

if os.environ.get(RUN_LEVEL_WARNING_ENV_VAR):
    warnings.warn("run-level warning", RuntimeWarning, stacklevel=2)


def _wait_for_sentinel() -> None:
    raw_path = os.environ.get(SENTINEL_ENV_VAR)
    if not raw_path:
        raise RuntimeError(f"{SENTINEL_ENV_VAR} must be set for the 'slow' profile")
    sentinel = Path(raw_path)
    print(STARTED_MARKER, flush=True)
    deadline = time.monotonic() + SENTINEL_TIMEOUT_SECONDS
    while not sentinel.exists():
        if time.monotonic() > deadline:
            raise RuntimeError(f"sentinel {sentinel} never appeared")
        time.sleep(0.01)


@kptn.task(
    outputs=[],
    inputs=["fixture_seed"],
    description="Prepare the fixture, quietly.",
    docs="docs/setup_task.md#overview",
)
def setup_task() -> None:
    """Deliberately silent: no output, no warnings, no outputs declared."""
    return None


@kptn.task(
    outputs=[],
    inputs=["fixture_source"],
    description="Emit output and warnings.",
    docs="docs/noisy_task.md",
)
def noisy_task(mode: str = "success") -> None:
    print("ordinary output")
    print("raw stderr output", file=sys.stderr)
    warnings.warn("sample warning", UserWarning)
    logging.getLogger("fixture").warning("logged warning")

    if mode == "slow":
        _wait_for_sentinel()
    elif mode == "failure":
        raise RuntimeError("fixture failure")
    elif mode == "db_error":
        raise sqlite3.OperationalError("no such table: fixture_user_query")


pipeline = kptn.Pipeline("fixture", setup_task >> noisy_task)
