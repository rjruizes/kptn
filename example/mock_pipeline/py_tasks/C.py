from kptn.util.pipeline_config import PipelineConfig


def C(pipeline_config: PipelineConfig, s: str = "C") -> list[str]:  # type: ignore
    return [s + "1", s + "2"]
