from __future__ import annotations

import sys


def emit_map(task_name: str, count: int) -> None:
    print(f"[MAP] {task_name} — expanding over {count} items", flush=True)


def emit_fail(task_name: str, reason: str) -> None:
    print(f"[FAIL] {task_name} — {reason}", file=sys.stderr, flush=True)


def emit_skip(task_name: str) -> None:
    print(f"[SKIP] {task_name} \u2014 cached", flush=True)


def emit_run(task_name: str) -> None:
    print(f"[RUN] {task_name}", flush=True)
