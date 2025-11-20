from pathlib import Path

import pytest

from kptn.cli.decider_bundle import BundleDeciderError, bundle_decider_lambda


def test_bundle_decider_copies_project_code(tmp_path, monkeypatch):
    project_root = tmp_path / "project"
    tasks_dir = project_root / "tasks"
    r_dir = project_root / "r_tasks"
    tasks_dir.mkdir(parents=True)
    r_dir.mkdir(parents=True)

    (tasks_dir / "__init__.py").write_text("", encoding="utf-8")
    (tasks_dir / "example.py").write_text("print('hello')\n", encoding="utf-8")
    (r_dir / "example.R").write_text("# R script\n", encoding="utf-8")
    (project_root / "standalone.py").write_text("print('standalone')\n", encoding="utf-8")

    kptn_yaml = project_root / "kptn.yaml"
    kptn_yaml.write_text(
        "\n".join(
            [
                "settings:",
                "  flow_type: stepfunctions",
                "  py_tasks_dir: tasks",
                "  r_tasks_dir: r_tasks",
                "graphs:",
                "  main:",
                "    comment: example pipeline",
                "tasks:",
                "  example_task:",
                "    file: tasks/example.py",
                "    r_script: r_tasks/example.R",
                "  standalone_task:",
                "    file: standalone.py",
            ]
        ),
        encoding="utf-8",
    )

    installs: list[list[str]] = []

    def fake_installer(args):
        installs.append(list(args))

    bundle_dir = tmp_path / "bundle"
    result = bundle_decider_lambda(
        project_root=project_root,
        output_dir=bundle_dir,
        installer=fake_installer,
    )

    assert result.bundle_dir == bundle_dir
    assert result.pipeline_name == "main"
    assert (bundle_dir / "kptn.yaml").is_file()
    assert (bundle_dir / "lambda_function.py").is_file()
    assert (bundle_dir / "tasks" / "example.py").read_text(encoding="utf-8") == "print('hello')\n"
    assert (bundle_dir / "r_tasks" / "example.R").is_file()
    assert (bundle_dir / "standalone.py").read_text(encoding="utf-8") == "print('standalone')\n"

    assert installs, "Expected uv installer to be invoked"
    first_target = installs[0][-1]
    assert first_target.endswith("kptn"), first_target

    installs.clear()
    bundle_dir_install = tmp_path / "bundle_with_install"
    bundle_decider_lambda(
        project_root=project_root,
        output_dir=bundle_dir_install,
        installer=fake_installer,
        install_project=True,
    )
    assert installs[0][-1].endswith("kptn")
    assert installs[1][-1] == str(project_root)

    installs.clear()
    bundle_decider_lambda(
        project_root=project_root,
        output_dir=tmp_path / "bundle_pypi",
        installer=fake_installer,
        prefer_local_kptn=False,
    )
    assert installs[0][-1] == "kptn"


def test_bundle_decider_rejects_external_paths(tmp_path):
    project_root = tmp_path / "project"
    project_root.mkdir()
    (project_root / "kptn.yaml").write_text(
        "\n".join(
            [
                "settings:",
                "  flow_type: stepfunctions",
                "graphs:",
                "  main:",
                "    comment: example",
                "tasks:",
                "  bad_task:",
                f"    file: {Path('/tmp/outside.py')}",
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(BundleDeciderError):
        bundle_decider_lambda(
            project_root=project_root,
            output_dir=tmp_path / "bundle",
            installer=lambda args: None,
        )


def test_bundle_decider_handles_placeholder_paths(tmp_path):
    project_root = tmp_path / "project"
    base_dir = project_root / "srs" / "generate_estimates" / "Tables_Core"
    (base_dir / "actual_table").mkdir(parents=True)
    (base_dir / "actual_table" / "script.R").write_text("# script\n", encoding="utf-8")

    (project_root / "kptn.yaml").write_text(
        "\n".join(
            [
                "settings:",
                "  flow_type: stepfunctions",
                "graphs:",
                "  main:",
                "    comment: example",
                "tasks:",
                "  placeholder_task:",
                "    file: srs/generate_estimates/Tables_Core/${TABLE_NAME}/script.R",
            ]
        ),
        encoding="utf-8",
    )

    bundle_dir = tmp_path / "bundle"
    bundle_decider_lambda(
        project_root=project_root,
        output_dir=bundle_dir,
        installer=lambda args: None,
    )

    copied_dir = bundle_dir / "srs" / "generate_estimates" / "Tables_Core"
    assert copied_dir.is_dir()
    assert not (bundle_dir / "infra").exists()
