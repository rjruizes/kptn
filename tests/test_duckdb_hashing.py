from __future__ import annotations

import hashlib
from types import SimpleNamespace

import pytest

from kptn.caching.Hasher import DUCKDB_EMPTY_HASH, Hasher
from kptn.util.hash import hash_obj


@pytest.mark.parametrize("rows", [
    [(1, "apple"), (2, "banana")],
])
def test_duckdb_output_hash_changes_when_table_changes(tmp_path, rows):
    duckdb = pytest.importorskip("duckdb")

    db_path = tmp_path / "example.db"
    conn = duckdb.connect(str(db_path))
    conn.execute("create schema if not exists main")
    conn.execute("create or replace table main.fruit (id integer, fruit text)")
    conn.executemany("insert into main.fruit values (?, ?)", rows)

    runtime_config = SimpleNamespace(duckdb=conn)
    hasher = Hasher(
        tasks_config={"tasks": {"duck_task": {"outputs": ["duckdb://main.fruit"]}}},
        runtime_config=runtime_config,
    )

    first_hash = hasher.hash_task_outputs("duck_task")
    assert first_hash is not None

    expected_concat = conn.execute(
        "SELECT string_agg(md5(t::TEXT), '' ORDER BY md5(t::TEXT)) FROM main.fruit AS t"
    ).fetchone()[0]
    duckdb_digest = (
        DUCKDB_EMPTY_HASH
        if expected_concat is None
        else hashlib.md5(expected_concat.encode()).hexdigest()
    )
    expected_hash = hash_obj([{"duckdb://main.fruit": duckdb_digest}])
    assert first_hash == expected_hash

    conn.execute("insert into main.fruit values (3, 'cherry')")
    second_hash = hasher.hash_task_outputs("duck_task")
    assert second_hash != first_hash

    conn.execute("delete from main.fruit")
    empty_hash = hasher.hash_task_outputs("duck_task")
    assert empty_hash == hash_obj([{"duckdb://main.fruit": DUCKDB_EMPTY_HASH}])

    conn.close()
