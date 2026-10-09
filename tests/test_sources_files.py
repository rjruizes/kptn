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
    _touch(tmp_path, "Module1/foodtotals_2024-01-24.csv")
    _touch(tmp_path, "Module1/.DS_Store")
    _touch(tmp_path, "Module1/nested/deep.csv")
    files = resolve_files("s", _spec(files=str(tmp_path / "Module1")), {})
    assert [f.rel for f in files] == ["Module1/foodtotals_2024-01-24.csv"]


def test_glob_base_is_the_non_glob_prefix(tmp_path: Path) -> None:
    _touch(tmp_path, "in/m2/b.csv")
    _touch(tmp_path, "in/m1/a.csv")
    files = resolve_files("s", _spec(files=str(tmp_path / "in" / "*" / "*.csv")), {})
    assert [f.rel for f in files] == ["in/m1/a.csv", "in/m2/b.csv"]


def test_single_file_base_is_its_folder(tmp_path: Path) -> None:
    p = _touch(tmp_path, "dhq3/dhq3_codes.csv")
    files = resolve_files("s", _spec(files=str(p)), {})
    assert [f.rel for f in files] == ["dhq3/dhq3_codes.csv"]


def test_callable_files_receive_config_by_name(tmp_path: Path) -> None:
    _touch(tmp_path, "Module1/a.csv")
    spec = _spec(files=lambda config: config["asa24"]["in"])
    files = resolve_files("s", spec, {"config": {"asa24": {"in": [str(tmp_path / "Module1")]}}, "engine": object()})
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
        ("dhq3_codes", "dhq3_codes"),
        ("Module 1 Data Delivery Subject MOCK", "module_1_data_delivery_subject_mock"),
        ("SUPPLEMENTSTOTALS", "supplementstotals"),
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
    _touch(tmp_path, "m2/ndsr.csv")
    _touch(tmp_path, "m1/ndsr.csv")
    _touch(tmp_path, "m1/codes.csv")
    spec = _spec(files=[str(tmp_path / "m1"), str(tmp_path / "m2")])
    groups = group_by_table(spec, resolve_files("s", spec, {}))
    assert list(groups) == ["codes", "ndsr"]
    assert [f.rel for f in groups["ndsr"]] == ["m1/ndsr.csv", "m2/ndsr.csv"]
