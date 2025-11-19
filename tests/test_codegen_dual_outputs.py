from textwrap import dedent

from kptn.codegen.codegen import generate_files


def test_generate_files_emits_python_runner_for_stepfunctions(tmp_path, monkeypatch):
    project_dir = tmp_path / "proj"
    project_dir.mkdir()
    flows_dir = project_dir / "flows"
    flows_dir.mkdir()
    (project_dir / "kptn.yaml").write_text(
        dedent(
            """
            settings:
              flows_dir: "flows"
              flow_type: stepfunctions

            graphs:
              demo:
                tasks:
                  start:
                  finish: [start]

            tasks:
              start:
                file: tasks/start.py
              finish:
                file: tasks/finish.py
            """
        ).strip()
    )

    monkeypatch.chdir(project_dir)
    generate_files(graph="demo")

    json_path = flows_dir / "demo.json.tpl"
    runner_path = flows_dir / "demo.py"
    assert json_path.exists()
    assert runner_path.exists()
    runner_text = runner_path.read_text()
    assert 'KPTN_FLOW_TYPE' in runner_text
    assert 'KPTN_DB_TYPE' in runner_text
