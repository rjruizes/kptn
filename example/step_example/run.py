import os
from pathlib import Path
from kptn.caching.TaskStateCache import run_task
from kptn.util.pipeline_config import PipelineConfig

if __name__ == "__main__":
    tasks_config_path = Path(__file__).parent / "kptn.yaml"
    pipeline_config = PipelineConfig(
        TASKS_CONFIG_PATH=str(tasks_config_path),
        PIPELINE_NAME=os.getenv("KAPTEN_PIPELINE"),
    )
    run_task(pipeline_config, os.getenv("KAPTEN_TASK"))