from kptn.util.filepaths import project_root
from pathlib import Path


def get_active_branch_name():
    """Get the active branch name from the git HEAD file"""
    head_dir = Path(project_root) / ".git" / "HEAD"
    with head_dir.open("r") as f:
        content = f.read().splitlines()

    for line in content:
        if line[0:4] == "ref:":
            return line.partition("refs/heads/")[2]