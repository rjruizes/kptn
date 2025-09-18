"""Basic smoke tests to validate the repository setup."""

from pathlib import Path


def test_readme_present() -> None:
    """Ensure the top-level README exists as a quick health check."""
    project_root = Path(__file__).resolve().parents[1]
    assert (project_root / "README.md").is_file()
