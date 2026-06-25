from __future__ import annotations

import kptn
from kptn.graph.requires import coalesce_requires
from kptn.graph.topo import topo_sort


@kptn.task(outputs=[])
def load_specimen_inventory() -> None: ...


@kptn.task(outputs=[], requires=[load_specimen_inventory])
def load_mcac_reports() -> None: ...


@kptn.task(outputs=[], requires=[load_specimen_inventory])
def load_mmc_reports() -> None: ...


def _names(graph) -> list[str]:
    return [n.name for n in graph.nodes]


def test_rshift_preserves_requires_edges() -> None:
    # A requires-injected prerequisite tags an edge inside each Pipeline.
    pa = kptn.Pipeline("a", load_mcac_reports)
    pb = kptn.Pipeline("b", load_mmc_reports)
    assert pa.requires_edges  # sanity: tag exists before composition
    composed = pa >> pb
    # Composition with >> must carry the tags of both operands, not drop them.
    assert composed.requires_edges == pa.requires_edges | pb.requires_edges


def test_stage_and_parallel_preserve_requires_edges() -> None:
    pa = kptn.Pipeline("a", load_mcac_reports)
    pb = kptn.Pipeline("b", load_mmc_reports)
    expected = pa.requires_edges | pb.requires_edges
    assert kptn.Stage("s", pa, pb).requires_edges == expected
    assert kptn.parallel(pa, pb).requires_edges == expected


def test_shared_prereq_across_stage_branches_collapses_to_one_node() -> None:
    # Mirrors the real nph-curation shape: load_mcac / load_mmc are sub-Pipelines
    # used as Stage branches, each with a reports head requiring the shared prereq.
    load_mcac = kptn.Pipeline("load_mcac", load_mcac_reports)
    load_mmc = kptn.Pipeline("load_mmc", load_mmc_reports)
    main = kptn.Pipeline("main", kptn.Stage("datasets", load_mcac, load_mmc))

    assert _names(main).count("load_specimen_inventory") == 1
    order = [n.name for n in topo_sort(main)]
    assert order.index("load_specimen_inventory") < order.index("load_mcac_reports")
    assert order.index("load_specimen_inventory") < order.index("load_mmc_reports")


def test_shared_prereq_across_parallel_branches_collapses_to_one_node() -> None:
    pa = kptn.Pipeline("a", load_mcac_reports)
    pb = kptn.Pipeline("b", load_mmc_reports)
    main = kptn.Pipeline("main", kptn.parallel(pa, pb))
    assert _names(main).count("load_specimen_inventory") == 1


def test_coalesce_is_identity_without_duplicates() -> None:
    pipe = kptn.Pipeline("p", load_mcac_reports)
    # Single requirer: one prereq, nothing to coalesce.
    assert _names(pipe).count("load_specimen_inventory") == 1
    same = coalesce_requires(pipe)
    assert same is pipe  # no duplicates -> returns the input unchanged


def test_coalesce_leaves_user_placed_duplicates_untouched() -> None:
    # Two distinct user-placed nodes share a name but neither is requires-injected.
    @kptn.task(outputs=[])
    def dup() -> None: ...

    @kptn.task(outputs=[])
    def dup_again() -> None: ...

    # Force a same-name collision without any requires-edge tag.
    dup_again.__name__ = "dup"
    pa = kptn.Pipeline("a", dup)
    pb = kptn.Pipeline("b", dup_again)
    main = kptn.Pipeline("main", kptn.parallel(pa, pb))
    # Out of scope for requires-injected coalescing: both copies remain.
    assert _names(main).count("dup") == 2


def test_sequential_shared_prereq_degrades_without_cycle() -> None:
    # px >> py sequences px before py; a single shared prereq cannot be both
    # before px's consumer and inside py. Coalescing must DECLINE the merge
    # rather than emit a cyclic graph: the pipeline stays buildable and sortable.
    px = kptn.Pipeline("x", load_mcac_reports)
    py = kptn.Pipeline("y", load_mmc_reports)
    main = kptn.Pipeline("main", px >> py)
    # Duplicates remain (merge declined), but the graph is acyclic and usable.
    assert _names(main).count("load_specimen_inventory") == 2
    order = topo_sort(main)  # must not raise GraphError
    assert {"load_mcac_reports", "load_mmc_reports"} <= {n.name for n in order}


def test_shared_prereq_runs_once_across_stage_branches(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    calls: list[str] = []

    @kptn.task(outputs=[])
    def prereq() -> None:
        calls.append("prereq")

    @kptn.task(outputs=[], requires=[prereq])
    def consumer_x() -> None:
        calls.append("consumer_x")

    @kptn.task(outputs=[], requires=[prereq])
    def consumer_y() -> None:
        calls.append("consumer_y")

    px = kptn.Pipeline("x", consumer_x)
    py = kptn.Pipeline("y", consumer_y)
    main = kptn.Pipeline("main", kptn.Stage("ds", px, py))
    main.run(no_cache=True)

    assert calls.count("prereq") == 1
    assert calls.index("prereq") < calls.index("consumer_x")
    assert calls.index("prereq") < calls.index("consumer_y")
