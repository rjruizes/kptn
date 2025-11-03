from pathlib import Path
from kptn.util.pipeline_config import PipelineConfig


def E(pipeline_config: PipelineConfig):
    input_file_B = Path(pipeline_config.scratch_dir) / "B" / "B_2024.csv"
    input_file_D = Path(pipeline_config.scratch_dir) / "D" / "D_2024.csv"
    output_file = Path(pipeline_config.scratch_dir) / "E" / "E_2024.csv"
    input_data_B = input_file_B.read_text()
    input_data_D = input_file_D.read_text()
    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file.write_text(input_data_B + input_data_D + "E")
