from pathlib import Path
from kptn.util.pipeline_config import PipelineConfig


def combo_process(pipeline_config: PipelineConfig, item1: str, item2: str) -> list[str]:
    """
    - On first run of T1, fail to simulate a partial mapped task failure
    - Succeed on the second run
    """
    output_dir = Path(pipeline_config.scratch_dir) / "combo_process"
    output_dir.mkdir(parents=True, exist_ok=True)
    if item1 == "T1" and item2 == "1":
        output_file = output_dir / "T1.txt"
        if output_file.exists():
            pass
        else:
            output_file.write_text("T1")
            raise ValueError("Purposefully failing for testing")
