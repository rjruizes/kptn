from pathlib import Path
from kptn.util.pipeline_config import PipelineConfig


def B(pipeline_config: PipelineConfig):
    input_file = Path(pipeline_config.scratch_dir) / "A" / "A_2024.csv"
    output_file = Path(pipeline_config.scratch_dir) / "B" / "B_2024.csv"
    input_data = input_file.read_text()
    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file.write_text(input_data + "B")
    print("Wrote", output_file)
