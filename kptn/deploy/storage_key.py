from kptn.deploy.get_active_branch_name import get_active_branch_name
from kptn.util.filepaths import project_root
from pathlib import Path
import yaml

def read_branch_storage_key(branch: str) -> str:
    """Read the storage key for a given branch"""
    storage_key_path = Path(project_root) / "branch_conf" / f"{branch}.yaml"
    if not storage_key_path.exists():
        return ""
    with storage_key_path.open("r") as f:
        conf = yaml.load(f, Loader=yaml.FullLoader)
    return conf["storage_key"]

# Test the function
if __name__ == "__main__":
    branch = get_active_branch_name()
    key = read_branch_storage_key(branch)
    print(key)