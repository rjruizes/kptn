from __future__ import annotations

import pytest

import kptn
from kptn.change_detector.detector import is_stale
from kptn.graph.nodes import SourceNode
from kptn.graph.pipeline import Pipeline
from kptn.profiles.resolver import ProfileResolver
from kptn.profiles.schema import KptnConfig, ProfileSpec
from kptn.sources.spec import SourceSpec
from tests.fakes import FakeStateStore


def test_source_returns_handle_with_spec() -> None:
    survey = kptn.source("survey", files="in/survey")
    assert survey.__name__ == "survey"
    assert isinstance(survey.__kptn__, SourceSpec)
    assert survey.__kptn__.files == "in/survey"
    assert survey.__kptn__.table is None and survey.__kptn__.read is None


def test_source_records_where_it_was_declared() -> None:
    survey = kptn.source("survey", files="in/survey")
    assert survey.__kptn__.declared_at is not None
    filename, lineno = survey.__kptn__.declared_at
    assert filename.endswith("test_sources_spec.py")
    assert lineno > 0


@pytest.mark.parametrize("bad", ["Sales", "1x", "a-b", "", "a b"])
def test_source_rejects_bad_names(bad: str) -> None:
    with pytest.raises(ValueError, match="name"):
        kptn.source(bad, files="in")


def test_source_rejects_empty_file_list() -> None:
    with pytest.raises(ValueError, match="files"):
        kptn.source("a", files=[])


def test_source_doc_inputs_describe_files() -> None:
    assert kptn.source("a", files=["x/*.csv", "y"]).__kptn__.doc.inputs == ("x/*.csv", "y")
    computed = kptn.source("b", files=lambda config: config["b"]["in"])
    assert computed.__kptn__.doc.inputs == ("(computed from config)",)


def test_source_composes_into_a_source_node() -> None:
    a = kptn.source("a", files="in/a")
    graph = kptn.config(config=lambda: {}) >> a
    nodes = [n for n in graph.nodes if isinstance(n, SourceNode)]
    assert [n.name for n in nodes] == ["a"]
    assert nodes[0].spec is a.__kptn__


def test_stage_selection_prunes_sources_by_name() -> None:
    a = kptn.source("a", files="in/a")
    b = kptn.source("b", files="in/b")
    pipeline = Pipeline("p", kptn.Stage("raw_ingest", a, b))
    config = KptnConfig(profiles={"ci": ProfileSpec(stage_selections={"raw_ingest": ["a"]})})
    resolved = ProfileResolver(config).compile(pipeline, "ci")
    names = {n.name for n in resolved.graph.nodes}
    assert "a" in names and "b" not in names


def test_is_stale_treats_source_nodes_as_non_task() -> None:
    a = kptn.source("a", files="in/a")
    node = SourceNode(spec=a.__kptn__, name="a")
    assert is_stale(node, FakeStateStore(), "k", "p") == (False, "non-task node")
