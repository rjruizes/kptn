from textwrap import dedent

import pytest

from kptn.codegen.codegen import _flatten_graphs, generate_files


def test_flatten_graphs_merges_and_overrides():
    graphs = {
        "base": {"tasks": {"a": None, "b": "a"}},
        "other": {"tasks": {"b": None, "c": "b"}},
        "child": {"extends": ["base", "other"], "tasks": {"d": ["b", "c"]}},
    }

    flattened = _flatten_graphs(graphs, ["child"])

    assert list(flattened["child"].keys()) == ["a", "b", "c", "d"]
    # First occurrence wins; 'b' stays from base graph
    assert flattened["child"]["b"] == "a"


def test_flatten_graphs_allows_extends_only():
    graphs = {
        "base": {"tasks": {"a": None, "b": "a"}},
        "child": {"extends": "base"},
    }

    flattened = _flatten_graphs(graphs, ["child"])

    assert flattened["child"] == {"a": None, "b": "a"}


def test_flatten_graphs_detects_cycles():
    graphs = {
        "one": {"extends": "two", "tasks": {"a": None}},
        "two": {"extends": "one", "tasks": {"b": None}},
    }

    with pytest.raises(ValueError, match="Cycle detected in graph inheritance"):
        _flatten_graphs(graphs, ["one"])


def test_flatten_graphs_rejects_unknown_dependencies():
    graphs = {"base": {"tasks": {"a": "missing"}}}

    with pytest.raises(ValueError, match="depends on unknown task 'missing'"):
        _flatten_graphs(graphs, ["base"])


def test_generate_files_flattens_inherited_graphs(tmp_path, monkeypatch):
    project_dir = tmp_path / "proj"
    project_dir.mkdir()
    flows_dir = project_dir / "flows"
    flows_dir.mkdir()
    (project_dir / "kptn.yaml").write_text(
        dedent(
            """
            settings:
              flows_dir: "flows"
              flow_type: vanilla

            graphs:
              basic:
                tasks:
                  a:
                  b: a
                  c: b
              other:
                tasks:
                  d:
                  e: d
                  f: e
              basic_other:
                extends: [basic, other]
                tasks:
                  g: c
                  h: f

            tasks:
              a:
                file: tasks/a.py
              b:
                file: tasks/b.py
              c:
                file: tasks/c.py
              d:
                file: tasks/d.py
              e:
                file: tasks/e.py
              f:
                file: tasks/f.py
              g:
                file: tasks/g.py
              h:
                file: tasks/h.py
            """
        ).strip()
    )

    monkeypatch.chdir(project_dir)
    generate_files(graph="basic_other")

    flow_path = flows_dir / "basic_other.py"
    output = flow_path.read_text()
    for task in ["a", "b", "c", "d", "e", "f", "g", "h"]:
        assert f'submit("{task}", opts)' in output
