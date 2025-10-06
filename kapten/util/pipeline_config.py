import os
from enum import Enum
from pathlib import Path
from pydantic import BaseModel, computed_field, model_validator
from kapten.deploy.ecr_image import get_full_image_and_branch
from kapten.util.filepaths import project_root
import yaml


class StoreType(str, Enum):
    fs = "fs"
    s3 = "s3"


class FileType(str, Enum):
    csv = "csv"
    parquet = "parquet"


def _module_path_from_dir(py_tasks_dir: str) -> str:
    """Convert a directory path to a Python module path (e.g., 'src' -> 'src', 'py_tasks/foo' -> 'py_tasks.foo')"""
    parts = [part for part in Path(py_tasks_dir).parts if part and part != "."]
    module_path = ".".join(parts)
    if not module_path:
        raise ValueError("Unable to derive module path from py-tasks-dir setting")
    return module_path


def _read_py_tasks_dir_from_config(tasks_config_path: str) -> str | None:
    """Read the py-tasks-dir setting from the kapten.yaml config file"""
    try:
        config_path = Path(tasks_config_path)
        if not config_path.exists():
            return None
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        return config.get('settings', {}).get('py-tasks-dir')
    except Exception:
        return None


def get_scratch_dir(pipeline_config) -> Path:
    output_pipeline_dir = os.getenv("SCRATCH_DIR")  # Should be /data/$branch in AWS container
    storage_key = get_storage_key(pipeline_config)
    if output_pipeline_dir:
        return Path(output_pipeline_dir) / storage_key
    else:
        return Path(project_root) / "scratch" / storage_key


class PipelineConfig(BaseModel):
    IMAGE: str = ""
    BRANCH: str = ""
    DATA_YEAR: str = "2022"
    STORAGE_KEY: str = ""
    SUBSET_MODE: bool = False
    OUTPUT_STORETYPE: StoreType = "fs"
    OUTPUT_FILETYPE: FileType = "csv"
    PIPELINE_NAME: str
    PY_MODULE_PATH: str = ""
    TASKS_CONFIG_PATH: str = "/code/tests/mock_pipeline/tasks.yaml"
    R_TASKS_DIR_PATH: str = "/code/tests/mock_pipeline/r_tasks"

    @model_validator(mode='after')
    def _derive_py_module_path(self):
        """Auto-derive PY_MODULE_PATH from py-tasks-dir in kapten.yaml if not explicitly set"""
        if not self.PY_MODULE_PATH and self.TASKS_CONFIG_PATH:
            py_tasks_dir = _read_py_tasks_dir_from_config(self.TASKS_CONFIG_PATH)
            if py_tasks_dir:
                self.PY_MODULE_PATH = _module_path_from_dir(py_tasks_dir)
        return self

    @computed_field
    def scratch_dir(self) -> str:
        """Scratch directory: parent directory for input, output, and retrieved external files"""
        if self.OUTPUT_STORETYPE == "s3":
            # CDK sets ARTIFACT_STORE in the container
            storage_key = get_storage_key(self)
            return os.path.join(f"s3://{os.getenv('ARTIFACT_STORE')}", storage_key)
        else:
            return get_scratch_dir(self)

    @computed_field
    def externals_dir(self) -> str:
        """External files directory: where external files are retrieved to"""
        return os.path.join(self.scratch_dir, "externals")


def generateConfig(pipeline_name: str, r_tasks_dir_path: str, py_module_path: str, tasks_config_path: str = "", authproxy_endpoint=None, storage_key="") -> PipelineConfig:
    """Called locally to generate a PipelineConfig for a Prefect Deployment"""
    image, branch = get_full_image_and_branch(authproxy_endpoint)
    return PipelineConfig(
        IMAGE=image,
        BRANCH=branch,
        DATA_YEAR="2022",
        STORAGE_KEY=storage_key,
        OUTPUT_STORETYPE="fs",
        OUTPUT_FILETYPE="csv",
        PIPELINE_NAME=pipeline_name,
        PY_MODULE_PATH=py_module_path,
        TASKS_CONFIG_PATH=tasks_config_path,
        R_TASKS_DIR_PATH=r_tasks_dir_path,
    )

def get_storage_key(pipeline_config: PipelineConfig) -> str:
    return pipeline_config.STORAGE_KEY or pipeline_config.BRANCH

#     return {
#         "EXTERNAL_FILE_PATH": os.getenv(
#             "EXTERNAL_FILE_PATH"
#         ),  # Used by R code which expects external files to exist there
#         # V2: get_ext_file retrieves external files from S3 if necessary and puts them in EXTERNAL_FILE_PATH or scratch/externals
#         "EXTERNALS_BUCKET": os.getenv(
#             "EXTERNALS_BUCKET"
#         ),  # used by R code to retrieve external files
#         # V2: get_ext_file retrieves external files from S3 using this bucket
#     }


"""
Like DATA_YEAR, the defaults for the following variables are set in deployments, but can be overridden task by task via tasks.yaml

- OUTPUT_STORETYPE: fs|s3
- OUTPUT_FILETYPE: csv|parquet
- OUTPUT_PIPELINE_DIR: path to output files

OUTPUT_PIPELINE_DIR = s3://$EXTERNALS_BUCKET/$branch/$task
OR
OUTPUT_PIPELINE_DIR = /data/$branch/$task

Example:
    Default: csv
    A: none (csv)
    B: parquet
    C: none (Build can determine that input is 'parquet' for B; output is csv)

Problem: get_artifact currently is based on ARTIFACT_TYPE, but a task could have multiple dependencies with different artifact types.
Solution: 

Ignore below
EXTERNAL_FILE_PATH: does NOT need to be set via environment variable OR flow. If unset, it will default to scratch/externals
EXTERNALS_BUCKET: DOES need to be set by environment variable; set by CDK in cloud; must be set locally; does NOT need to be passed
SCRATCH_DIR: computed based on branch; does NOT need to be set by environment variable; does NOT need to be passed
"""
