from __future__ import annotations

import os
import shutil
from pathlib import Path
from unittest.mock import patch

import duckdb
import pytest

import kptn
from kptn.exceptions import HashError, TaskError
from kptn.sources import build
from kptn.sources.build import (
    BRONZE_DIR,
    build_table,
    fingerprint,
    fingerprints,
    is_current,
    plan_tables,
    remove_stale_tables,
    source_code_hash,
    state_value,
)
from tests.fakes import FakeStateStore


def _project(root: Path) -> Path:
    for rel, text in {
        "in/m1/orders_2024-01-24.csv": "orderid,amount\n1,200\n",
        "in/m2/orders_2024-02-01.csv": "orderid,amount\n2,300\n",
        "in/m1/refunds_2024-01-24.csv": "orderid,x\n1,a\n",
    }.items():
        p = root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(text)
    return root


def _spec(root: Path):
    return kptn.source(
        "sales",
        files=[str(root / "in" / "m1"), str(root / "in" / "m2")],
        table=lambda f: f.name.split("_")[0],
    ).__kptn__


def _plans(root: Path, spec=None):
    return plan_tables("sales", spec or _spec(root), {}, root=root)


def test_fingerprint_is_size_and_mtime(tmp_path: Path) -> None:
    p = tmp_path / "a.csv"
    p.write_text("x\n1\n")
    st = p.stat()
    assert fingerprint(p) == f"{st.st_size}:{st.st_mtime_ns}"
    with pytest.raises(HashError):
        fingerprint(tmp_path / "missing.csv")


def test_fingerprints_preserve_order(tmp_path: Path) -> None:
    paths = []
    for i in range(50):
        p = tmp_path / f"f{i}.csv"
        p.write_text("x" * i)
        paths.append(p)
    assert fingerprints(paths) == [fingerprint(p) for p in paths]


def test_plan_tables_groups_and_names(tmp_path: Path) -> None:
    plans = _plans(_project(tmp_path))
    assert [p.table for p in plans] == ["orders", "refunds"]
    assert plans[0].state_name == "sales[orders]"
    assert plans[0].parquet == tmp_path / BRONZE_DIR / "sales" / "orders.parquet"
    assert [f.rel for f in plans[0].files] == ["m1/orders_2024-01-24.csv", "m2/orders_2024-02-01.csv"]


def test_key_is_independent_of_working_directory(tmp_path: Path) -> None:
    a = _project(tmp_path / "user_a" / "branch")
    b = tmp_path / "user_b" / "other" / "deeper"
    shutil.copytree(a, b)  # copy2 keeps sizes and modification times
    assert [p.key for p in _plans(a)] == [p.key for p in _plans(b)]


def test_changing_one_file_changes_only_its_table(tmp_path: Path) -> None:
    root = _project(tmp_path)
    before = {p.table: p.key for p in _plans(root)}
    (root / "in/m1/refunds_2024-01-24.csv").write_text("orderid,x\n1,bb\n")
    after = {p.table: p.key for p in _plans(root)}
    assert after["refunds"] != before["refunds"]
    assert after["orders"] == before["orders"]


def test_touching_a_file_changes_its_key(tmp_path: Path) -> None:
    """The documented tradeoff: a new modification time means a rebuild."""
    root = _project(tmp_path)
    before = {p.table: p.key for p in _plans(root)}
    path = root / "in/m1/refunds_2024-01-24.csv"
    st = path.stat()
    os.utime(path, ns=(st.st_atime_ns, st.st_mtime_ns + 1_000_000_000))
    assert {p.table: p.key for p in _plans(root)}["refunds"] != before["refunds"]


def test_check_file_content_is_part_of_the_input_hash(tmp_path: Path) -> None:
    root = _project(tmp_path)
    before = {p.table: p.key for p in _plans(root)}
    check = root / "checks" / "sales" / "refunds.yml"
    check.parent.mkdir(parents=True)
    check.write_text("checks for refunds:\n- row_count > 0\n")
    with_check = {p.table: p.key for p in _plans(root)}
    assert with_check["refunds"] != before["refunds"]
    assert with_check["orders"] == before["orders"]
    st = check.stat()
    os.utime(check, ns=(st.st_atime_ns, st.st_mtime_ns + 1_000_000_000))
    assert {p.table: p.key for p in _plans(root)}["refunds"] == with_check["refunds"]


def test_source_code_hash_tracks_callables() -> None:
    one = kptn.source("s", files="x", table=lambda f: f.name.split("_")[0]).__kptn__
    two = kptn.source("s", files="x", table=lambda f: f.name.split("-")[0]).__kptn__
    assert source_code_hash(one) != source_code_hash(two)


def test_build_writes_parquet_and_leaves_no_temp_files(tmp_path: Path) -> None:
    root = _project(tmp_path)
    plan = _plans(root)[0]
    with pytest.warns(UserWarning):  # no check file
        assert build_table(plan, _spec(root), root=root) is False
    rows = duckdb.sql(f"SELECT orderid, _source_file FROM '{plan.parquet}' ORDER BY 2").fetchall()
    assert rows == [("1", "m1/orders_2024-01-24.csv"), ("2", "m2/orders_2024-02-01.csv")]
    assert [p.name for p in plan.parquet.parent.iterdir()] == ["orders.parquet"]


def test_failed_check_keeps_previous_parquet(tmp_path: Path) -> None:
    root = _project(tmp_path)
    plan = _plans(root)[0]
    with pytest.warns(UserWarning):
        build_table(plan, _spec(root), root=root)
    before = plan.parquet.read_bytes()
    with patch.object(build, "run_checks", side_effect=TaskError("bad data")):
        with pytest.raises(TaskError, match="bad data"):
            build_table(plan, _spec(root), root=root)
    assert plan.parquet.read_bytes() == before
    assert [p.name for p in plan.parquet.parent.iterdir()] == ["orders.parquet"]


def test_is_current_follows_stored_value_and_parquet(tmp_path: Path) -> None:
    root = _project(tmp_path)
    store = FakeStateStore()
    plan = _plans(root)[0]
    assert not is_current(plan, store, "k", "p")
    with pytest.warns(UserWarning):
        build_table(plan, _spec(root), root=root)
    store.write_hash("k", "p", plan.state_name, state_value(plan))
    assert is_current(plan, store, "k", "p")
    plan.parquet.unlink()
    assert not is_current(plan, store, "k", "p")


def test_remove_stale_tables(tmp_path: Path) -> None:
    folder = tmp_path / BRONZE_DIR / "sales"
    folder.mkdir(parents=True)
    for name in ("keep.parquet", "gone.parquet", ".x.parquet.abc.tmp"):
        (folder / name).write_bytes(b"x")
    removed = remove_stale_tables("sales", {"keep"}, root=tmp_path)
    assert [p.name for p in removed] == ["gone.parquet"]
    assert sorted(p.name for p in folder.iterdir()) == [".x.parquet.abc.tmp", "keep.parquet"]


def test_plan_carries_stamps_and_code_hash(tmp_path: Path) -> None:
    root = _project(tmp_path)
    spec = _spec(root)
    [orders, _refunds] = _plans(root, spec)
    assert orders.stamps == tuple(fingerprint(f.path) for f in orders.files)
    assert orders.code_hash == source_code_hash(spec)


def test_build_table_writes_to_dest(tmp_path: Path) -> None:
    root = _project(tmp_path)
    [plan, _] = _plans(root)
    dest = tmp_path / "elsewhere" / "out.parquet"
    with pytest.warns(UserWarning):  # no check file
        build_table(plan, _spec(root), root=root, dest=dest)
    assert dest.is_file() and not plan.parquet.exists()
    assert duckdb.sql(f"SELECT count(*) FROM '{dest}'").fetchone() == (2,)
    assert [p.name for p in dest.parent.iterdir()] == ["out.parquet"]
