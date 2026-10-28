from __future__ import annotations

from pathlib import Path

import pytest

import kptn
from kptn.exceptions import HashError
from kptn.sources.files import (
    group_by_table,
    normalize_table_name,
    resolve_files,
    table_name,
)


def _touch(root: Path, rel: str, text: str = "a\n1\n") -> Path:
    p = root / rel
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text)
    return p


def _spec(**kwargs):
    return kptn.source("s", **kwargs).__kptn__


def test_directory_means_visible_files_inside(tmp_path: Path) -> None:
    _touch(tmp_path, "Batch1/orders_2024-01-24.csv")
    _touch(tmp_path, "Batch1/.DS_Store")
    _touch(tmp_path, "Batch1/nested/deep.csv")
    files = resolve_files("s", _spec(files=str(tmp_path / "Batch1")), {})
    assert [f.rel for f in files] == ["Batch1/orders_2024-01-24.csv"]


def test_glob_base_is_the_non_glob_prefix(tmp_path: Path) -> None:
    _touch(tmp_path, "in/m2/b.csv")
    _touch(tmp_path, "in/m1/a.csv")
    files = resolve_files("s", _spec(files=str(tmp_path / "in" / "*" / "*.csv")), {})
    assert [f.rel for f in files] == ["in/m1/a.csv", "in/m2/b.csv"]


def test_single_file_base_is_its_folder(tmp_path: Path) -> None:
    p = _touch(tmp_path, "survey/survey_codes.csv")
    files = resolve_files("s", _spec(files=str(p)), {})
    assert [f.rel for f in files] == ["survey/survey_codes.csv"]


def test_callable_files_receive_config_by_name(tmp_path: Path) -> None:
    _touch(tmp_path, "Batch1/a.csv")
    spec = _spec(files=lambda config: config["sales"]["in"])
    files = resolve_files("s", spec, {"config": {"sales": {"in": [str(tmp_path / "Batch1")]}}, "engine": object()})
    assert [f.name for f in files] == ["a.csv"]


def test_callable_files_missing_config_value_is_clear(tmp_path: Path) -> None:
    spec = _spec(files=lambda config: config["x"])
    with pytest.raises(HashError, match="config"):
        resolve_files("s", spec, {})


def test_duplicates_are_removed(tmp_path: Path) -> None:
    p = _touch(tmp_path, "d/a.csv")
    files = resolve_files("s", _spec(files=[str(p), str(tmp_path / "d")]), {})
    assert len(files) == 1


def test_pattern_matching_nothing_raises(tmp_path: Path) -> None:
    with pytest.raises(HashError, match="missing"):
        resolve_files("s", _spec(files=str(tmp_path / "missing")), {})


def test_unreadable_extension_without_read_raises(tmp_path: Path) -> None:
    _touch(tmp_path, "d/a.csv")
    _touch(tmp_path, "d/README.md")
    with pytest.raises(HashError, match="README.md"):
        resolve_files("s", _spec(files=str(tmp_path / "d")), {})


def test_unreadable_extension_allowed_with_read(tmp_path: Path) -> None:
    _touch(tmp_path, "d/a.xlsx")
    spec = _spec(files=str(tmp_path / "d"), read=lambda p: None)
    assert [f.name for f in resolve_files("s", spec, {})] == ["a.xlsx"]


def test_result_is_sorted_regardless_of_pattern_order(tmp_path: Path) -> None:
    a = _touch(tmp_path, "m/a.csv")
    b = _touch(tmp_path, "m/b.csv")
    files = resolve_files("s", _spec(files=[str(b), str(a)]), {})
    assert [f.name for f in files] == ["a.csv", "b.csv"]


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("survey_codes", "survey_codes"),
        ("Batch 1 Export Report MOCK", "batch_1_export_report_mock"),
        ("DAILYTOTALS", "dailytotals"),
        ("--a--b--", "a_b"),
    ],
)
def test_normalize_table_name(raw: str, expected: str) -> None:
    assert normalize_table_name(raw) == expected


def test_normalize_table_name_rejects_empty() -> None:
    with pytest.raises(ValueError):
        normalize_table_name("---")


def test_table_defaults_to_stem_and_normalizes_callables(tmp_path: Path) -> None:
    p = _touch(tmp_path, "m/Foo Bar.csv")
    [f] = resolve_files("s", _spec(files=str(p)), {})
    assert table_name(_spec(files=str(p)), f) == "foo_bar"
    assert table_name(_spec(files=str(p), table="Subject"), f) == "subject"
    assert table_name(_spec(files=str(p), table=lambda f: f.name.upper()), f) == "foo_bar_csv"


def test_same_stem_in_different_folders_groups_together(tmp_path: Path) -> None:
    _touch(tmp_path, "m2/refunds.csv")
    _touch(tmp_path, "m1/refunds.csv")
    _touch(tmp_path, "m1/codes.csv")
    spec = _spec(files=[str(tmp_path / "m1"), str(tmp_path / "m2")])
    groups, no_table = group_by_table(spec, resolve_files("s", spec, {}))
    assert no_table == []
    assert list(groups) == ["codes", "refunds"]
    assert [f.rel for f in groups["refunds"]] == ["m1/refunds.csv", "m2/refunds.csv"]
