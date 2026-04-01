"""Tests for wrapper task discovery, cache evaluation, and execution."""

from __future__ import annotations

import textwrap
from datetime import datetime
from pathlib import Path

import pytest

from kptn.caching.models import TaskState
from kptn.caching.TaskStateCache import TaskStateCache
from kptn.caching.wrapper import (
    _build_task_function_lookup,
    _iter_direct_call_targets,
    discover_wrapper_subtasks,
)
from kptn.caching.Hasher import PythonFunctionAnalyzer


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def reset_task_state_cache():
    """Ensure singleton does not leak between tests."""
    TaskStateCache._instance = None
    TaskStateCache._wrapper_subtasks_cache = {}
    yield
    TaskStateCache._instance = None
    TaskStateCache._wrapper_subtasks_cache = {}


@pytest.fixture
def wrapper_project(tmp_path):
    """Create a minimal project with a wrapper task and subtasks.

    Uses a unique package name (wt_pkg) to avoid sys.modules collisions
    when other tests import modules named 'src'.
    """
    pkg = tmp_path / "wt_pkg"
    pkg.mkdir()

    # Subtask A
    (pkg / "subtask_a.py").write_text(textwrap.dedent("""\
        def subtask_a():
            print("running subtask_a")
    """))

    # Subtask B
    (pkg / "subtask_b.py").write_text(textwrap.dedent("""\
        def subtask_b():
            print("running subtask_b")
    """))

    # Subtask C
    (pkg / "subtask_c.py").write_text(textwrap.dedent("""\
        def subtask_c():
            print("running subtask_c")
    """))

    # Wrapper function that calls all three in order
    (pkg / "wrapper.py").write_text(textwrap.dedent("""\
        from wt_pkg.subtask_a import subtask_a
        from wt_pkg.subtask_b import subtask_b
        from wt_pkg.subtask_c import subtask_c

        def load_all():
            subtask_a()
            subtask_b()
            subtask_c()
    """))

    # An __init__.py so imports work
    (pkg / "__init__.py").write_text("")

    # kptn.yaml
    kptn_yaml = tmp_path / "kptn.yaml"
    kptn_yaml.write_text(textwrap.dedent("""\
        settings:
          flows_dir: "."
          flow_type: vanilla
          py_tasks_dir: "wt_pkg"
          db: sqlite

        graphs:
          demo:
            tasks:
              load_all:

        tasks:
          load_all:
            file: wt_pkg/wrapper.py:load_all
            wrapper: true
          subtask_a:
            file: wt_pkg/subtask_a.py:subtask_a
          subtask_b:
            file: wt_pkg/subtask_b.py:subtask_b
          subtask_c:
            file: wt_pkg/subtask_c.py:subtask_c
    """))

    yield tmp_path

    # Clean up wt_pkg modules from sys.modules to prevent cross-test pollution
    import sys
    stale = [k for k in sys.modules if k == "wt_pkg" or k.startswith("wt_pkg.")]
    for k in stale:
        del sys.modules[k]


# ---------------------------------------------------------------------------
# Unit tests: _build_task_function_lookup
# ---------------------------------------------------------------------------

class TestBuildTaskFunctionLookup:
    def test_basic_lookup(self):
        tasks = {
            "my_task": {"file": "src/tasks.py:my_func"},
            "other_task": {"file": "src/other.py"},
        }
        lookup = _build_task_function_lookup(tasks)
        assert ("src.tasks", "my_func") in lookup
        assert lookup[("src.tasks", "my_func")] == "my_task"
        # When no function specified, defaults to task name
        assert ("src.other", "other_task") in lookup

    def test_skips_non_python(self):
        tasks = {
            "r_task": {"file": "tasks/run.R"},
        }
        lookup = _build_task_function_lookup(tasks)
        assert len(lookup) == 0

    def test_skips_non_mapping(self):
        tasks = {"bad": "not a dict"}
        lookup = _build_task_function_lookup(tasks)
        assert len(lookup) == 0


# ---------------------------------------------------------------------------
# Unit tests: AST subtask discovery
# ---------------------------------------------------------------------------

class TestIterDirectCallTargets:
    def test_finds_direct_calls(self, wrapper_project):
        analyzer = PythonFunctionAnalyzer([str(wrapper_project)])
        wrapper_file = wrapper_project / "wt_pkg" / "wrapper.py"
        refs = _iter_direct_call_targets(analyzer, wrapper_file, "load_all")
        ref_names = [ref.name for ref in refs]
        assert "subtask_a" in ref_names
        assert "subtask_b" in ref_names
        assert "subtask_c" in ref_names

    def test_preserves_source_order(self, wrapper_project):
        analyzer = PythonFunctionAnalyzer([str(wrapper_project)])
        wrapper_file = wrapper_project / "wt_pkg" / "wrapper.py"
        refs = _iter_direct_call_targets(analyzer, wrapper_file, "load_all")
        # Filter to only the known subtask refs
        known = {"subtask_a", "subtask_b", "subtask_c"}
        ordered = [ref.name for ref in refs if ref.name in known]
        assert ordered == ["subtask_a", "subtask_b", "subtask_c"]

    def test_raises_on_missing_file(self, tmp_path):
        analyzer = PythonFunctionAnalyzer([str(tmp_path)])
        with pytest.raises(FileNotFoundError):
            _iter_direct_call_targets(
                analyzer, tmp_path / "nonexistent.py", "func"
            )

    def test_raises_on_missing_function(self, wrapper_project):
        analyzer = PythonFunctionAnalyzer([str(wrapper_project)])
        wrapper_file = wrapper_project / "wt_pkg" / "wrapper.py"
        with pytest.raises(KeyError, match="not_a_function"):
            _iter_direct_call_targets(analyzer, wrapper_file, "not_a_function")


class TestDiscoverWrapperSubtasks:
    def test_discovers_all_subtasks(self, wrapper_project):
        import yaml
        kptn_yaml = wrapper_project / "kptn.yaml"
        config = yaml.safe_load(kptn_yaml.read_text())
        tasks_dict = config["tasks"]

        subtasks = discover_wrapper_subtasks(
            "load_all",
            tasks_dict,
            py_dirs=[str(wrapper_project / "wt_pkg"), str(wrapper_project)],
        )
        assert subtasks == ["subtask_a", "subtask_b", "subtask_c"]

    def test_raises_if_not_wrapper(self, wrapper_project):
        import yaml
        config = yaml.safe_load(
            (wrapper_project / "kptn.yaml").read_text()
        )
        tasks_dict = config["tasks"]

        with pytest.raises(ValueError, match="not marked as a wrapper"):
            discover_wrapper_subtasks(
                "subtask_a", tasks_dict,
                py_dirs=[str(wrapper_project)],
            )

    def test_raises_if_not_python(self, tmp_path):
        tasks_dict = {
            "my_r_wrapper": {"file": "tasks/run.R", "wrapper": True},
        }
        with pytest.raises(ValueError, match="must be a Python task"):
            discover_wrapper_subtasks(
                "my_r_wrapper", tasks_dict, py_dirs=[str(tmp_path)]
            )

    def test_ignores_non_task_calls(self, tmp_path):
        """Function calls that don't match any task should be silently ignored."""
        pkg = tmp_path / "wt_ign"
        pkg.mkdir()
        (pkg / "__init__.py").write_text("")
        (pkg / "helper.py").write_text("def helper(): pass\n")
        (pkg / "real_task.py").write_text("def real_task(): pass\n")
        (pkg / "wrap.py").write_text(textwrap.dedent("""\
            from wt_ign.helper import helper
            from wt_ign.real_task import real_task

            def wrap():
                helper()
                real_task()
                print("done")
        """))

        tasks_dict = {
            "wrap": {"file": "wt_ign/wrap.py:wrap", "wrapper": True},
            "real_task": {"file": "wt_ign/real_task.py:real_task"},
        }

        subtasks = discover_wrapper_subtasks(
            "wrap", tasks_dict, py_dirs=[str(tmp_path)]
        )
        assert subtasks == ["real_task"]


# ---------------------------------------------------------------------------
# Unit tests: wrapper cache evaluation
# ---------------------------------------------------------------------------

class FakeDbClient:
    """Lightweight DB client stub."""

    def __init__(self, task_states: dict[str, TaskState] | None = None):
        self._states = {
            name: {k: v for k, v in state.model_dump().items() if v is not None}
            for name, state in (task_states or {}).items()
        }

    def get_task(self, task_name, include_data=True, subset_mode=False):
        state = self._states.get(task_name)
        return dict(state) if state else None

    def create_task(self, task_name, value, data=None):
        if hasattr(value, "model_dump"):
            d = {k: v for k, v in value.model_dump().items() if v is not None}
        else:
            d = dict(value)
        self._states[task_name] = d

    def update_task(self, task_name, value):
        existing = self._states.get(task_name, {})
        if hasattr(value, "model_dump"):
            update_dict = {k: v for k, v in value.model_dump().items() if v is not None}
        else:
            update_dict = dict(value)
        existing.update(update_dict)
        self._states[task_name] = existing

    def delete_task(self, task_name):
        self._states.pop(task_name, None)

    def get_subtasks(self, task_name):
        return []

    def create_subtasks(self, task_name, value):
        return None

    def set_task_ended(self, task_name, **kwargs):
        return None

    def set_subtask_ended(self, task_name, idx, hash_value=None):
        return None

    def set_subtask_started(self, task_name, idx):
        return None

    def delete_subsetdata(self, task_name):
        return None

    def reset_subset_of_subtasks(self, task_name, value_list):
        return None


class TestWrapperCacheEvaluation:

    def _build_tscache(self, wrapper_project, db_client=None, monkeypatch=None):
        """Build a TaskStateCache for the wrapper project."""
        from kptn.util.pipeline_config import PipelineConfig

        config_path = str(wrapper_project / "kptn.yaml")
        pipeline_config = PipelineConfig(
            TASKS_CONFIG_PATH=config_path,
            PIPELINE_NAME="demo",
        )
        tscache = TaskStateCache(pipeline_config, db_client=db_client or FakeDbClient())
        return tscache

    def test_is_wrapper_task(self, wrapper_project):
        tscache = self._build_tscache(wrapper_project)
        assert tscache.is_wrapper_task("load_all") is True
        assert tscache.is_wrapper_task("subtask_a") is False

    def test_get_wrapper_subtasks(self, wrapper_project):
        tscache = self._build_tscache(wrapper_project)
        subtasks = tscache.get_wrapper_subtasks("load_all")
        assert subtasks == ["subtask_a", "subtask_b", "subtask_c"]

    def test_wrapper_subtask_cache_key(self):
        key = TaskStateCache.wrapper_subtask_cache_key("load_redcap", "create_schema")
        assert key == "load_redcap.create_schema"

    def test_evaluate_all_fresh_should_run(self, wrapper_project, monkeypatch):
        """All subtasks should run when there's no cached state."""
        tscache = self._build_tscache(wrapper_project)
        # Patch code hashing to avoid file resolution issues
        monkeypatch.setattr(
            TaskStateCache, "build_task_code_hashes",
            lambda self, name, task, **kw: ([{"function": name, "hash": "abc"}], "Python"),
        )
        should_run, reason, decisions = tscache.evaluate_wrapper_submission("load_all")
        assert should_run is True
        assert reason is not None
        assert all(run for _, run in decisions)

    def test_evaluate_all_cached_should_skip(self, wrapper_project, monkeypatch):
        """All subtasks cached and valid → skip the wrapper entirely."""
        code_hashes = [{"function": "test", "hash": "abc"}]

        def _fake_code_hashes(self, name, task, **kw):
            return code_hashes, "Python"

        monkeypatch.setattr(TaskStateCache, "build_task_code_hashes", _fake_code_hashes)

        from kptn.util.hash import hash_obj
        cached_code_version = hash_obj(code_hashes)

        states = {}
        for prefix in ["load_all.__wrapper__", "load_all.subtask_a", "load_all.subtask_b", "load_all.subtask_c"]:
            states[prefix] = TaskState(
                PK=f"task#{prefix}",
                code_hashes=code_hashes,
                status="SUCCESS",
                start_time=datetime.now().isoformat(),
                end_time=datetime.now().isoformat(),
            )

        tscache = self._build_tscache(wrapper_project, db_client=FakeDbClient(states))
        should_run, reason, decisions = tscache.evaluate_wrapper_submission("load_all")
        assert should_run is False
        assert all(not run for _, run in decisions)

    def test_evaluate_cascade_on_code_change(self, wrapper_project, monkeypatch):
        """If subtask_b code changes, subtask_b and subtask_c should re-run."""
        old_hashes = [{"function": "test", "hash": "old"}]
        new_hashes_b = [{"function": "test", "hash": "new_b"}]

        def _fake_code_hashes(self, name, task, **kw):
            if name == "subtask_b":
                return new_hashes_b, "Python"
            return old_hashes, "Python"

        monkeypatch.setattr(TaskStateCache, "build_task_code_hashes", _fake_code_hashes)

        states = {
            "load_all.__wrapper__": TaskState(
                PK="task#load_all.__wrapper__",
                code_hashes=old_hashes,
                status="SUCCESS",
                start_time=datetime.now().isoformat(),
                end_time=datetime.now().isoformat(),
            ),
        }
        for st_name in ["subtask_a", "subtask_b", "subtask_c"]:
            cache_key = f"load_all.{st_name}"
            states[cache_key] = TaskState(
                PK=f"task#{cache_key}",
                code_hashes=old_hashes,
                status="SUCCESS",
                start_time=datetime.now().isoformat(),
                end_time=datetime.now().isoformat(),
            )

        tscache = self._build_tscache(wrapper_project, db_client=FakeDbClient(states))
        should_run, reason, decisions = tscache.evaluate_wrapper_submission("load_all")

        assert should_run is True
        assert "subtask_b" in reason
        decision_map = dict(decisions)
        assert decision_map["subtask_a"] is False  # cached, before the change
        assert decision_map["subtask_b"] is True   # code changed
        assert decision_map["subtask_c"] is True   # cascade

    def test_evaluate_cascade_on_failure(self, wrapper_project, monkeypatch):
        """If subtask_a previously failed, it and all subsequent re-run."""
        code_hashes = [{"function": "test", "hash": "abc"}]

        monkeypatch.setattr(
            TaskStateCache, "build_task_code_hashes",
            lambda self, name, task, **kw: (code_hashes, "Python"),
        )

        states = {
            "load_all.__wrapper__": TaskState(
                PK="task#wrapper", code_hashes=code_hashes,
                status="SUCCESS",
                start_time=datetime.now().isoformat(),
                end_time=datetime.now().isoformat(),
            ),
            "load_all.subtask_a": TaskState(
                PK="task#a", code_hashes=code_hashes,
                status="FAILURE",
                start_time=datetime.now().isoformat(),
                end_time=datetime.now().isoformat(),
            ),
            "load_all.subtask_b": TaskState(
                PK="task#b", code_hashes=code_hashes,
                status="SUCCESS",
                start_time=datetime.now().isoformat(),
                end_time=datetime.now().isoformat(),
            ),
            "load_all.subtask_c": TaskState(
                PK="task#c", code_hashes=code_hashes,
                status="SUCCESS",
                start_time=datetime.now().isoformat(),
                end_time=datetime.now().isoformat(),
            ),
        }

        tscache = self._build_tscache(wrapper_project, db_client=FakeDbClient(states))
        should_run, reason, decisions = tscache.evaluate_wrapper_submission("load_all")
        assert should_run is True
        assert "failed" in reason.lower()
        decision_map = dict(decisions)
        assert decision_map["subtask_a"] is True  # failed → re-run
        assert decision_map["subtask_b"] is True  # cascade
        assert decision_map["subtask_c"] is True  # cascade


# ---------------------------------------------------------------------------
# Integration test: run_wrapper_task
# ---------------------------------------------------------------------------

class TestRunWrapperTask:
    def test_wrapper_executes_subtasks_and_records_state(self, wrapper_project, monkeypatch):
        """Integration test: wrapper runs, subtask proxies record cache state."""
        from kptn.caching.vanilla import run_wrapper_task
        from kptn.util.pipeline_config import PipelineConfig

        # Patch code hashing
        monkeypatch.setattr(
            TaskStateCache, "build_task_code_hashes",
            lambda self, name, task, **kw: ([{"function": name, "hash": "test"}], "Python"),
        )

        # Ensure the project directory is importable
        import sys
        monkeypatch.syspath_prepend(str(wrapper_project))

        db = FakeDbClient()
        config_path = str(wrapper_project / "kptn.yaml")
        pipeline_config = PipelineConfig(
            TASKS_CONFIG_PATH=config_path,
            PIPELINE_NAME="demo",
        )
        tscache = TaskStateCache(pipeline_config, db_client=db)

        subtask_decisions = [
            ("subtask_a", True),
            ("subtask_b", True),
            ("subtask_c", True),
        ]

        run_wrapper_task(pipeline_config, "load_all", subtask_decisions, "No cached state")

        # Verify subtask states were recorded
        for st_name in ["subtask_a", "subtask_b", "subtask_c"]:
            cache_key = f"load_all.{st_name}"
            state = db.get_task(cache_key)
            assert state is not None, f"No state recorded for {cache_key}"
            assert state.get("status") == "SUCCESS"

        # Verify wrapper state was recorded
        wrapper_key = "load_all.__wrapper__"
        wrapper_state = db.get_task(wrapper_key)
        assert wrapper_state is not None
        assert wrapper_state.get("status") == "SUCCESS"

    def test_wrapper_skips_cached_subtasks(self, wrapper_project, monkeypatch):
        """Subtasks marked as skip should not actually execute."""
        from kptn.caching.vanilla import run_wrapper_task
        from kptn.util.pipeline_config import PipelineConfig

        monkeypatch.setattr(
            TaskStateCache, "build_task_code_hashes",
            lambda self, name, task, **kw: ([{"function": name, "hash": "test"}], "Python"),
        )

        monkeypatch.syspath_prepend(str(wrapper_project))

        db = FakeDbClient()
        config_path = str(wrapper_project / "kptn.yaml")
        pipeline_config = PipelineConfig(
            TASKS_CONFIG_PATH=config_path,
            PIPELINE_NAME="demo",
        )
        tscache = TaskStateCache(pipeline_config, db_client=db)

        # Only subtask_c should run; a and b are skipped
        subtask_decisions = [
            ("subtask_a", False),
            ("subtask_b", False),
            ("subtask_c", True),
        ]

        run_wrapper_task(pipeline_config, "load_all", subtask_decisions, "Code changed")

        # Only subtask_c should have state recorded (proxy ran it)
        cache_key_c = "load_all.subtask_c"
        state_c = db.get_task(cache_key_c)
        assert state_c is not None
        assert state_c.get("status") == "SUCCESS"

        # subtask_a and _b should NOT have state recorded (proxy skipped them)
        assert db.get_task("load_all.subtask_a") is None
        assert db.get_task("load_all.subtask_b") is None
