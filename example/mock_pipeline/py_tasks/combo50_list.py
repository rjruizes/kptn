from kptn.util.pipeline_config import PipelineConfig


def combo50_list(pipeline_config: PipelineConfig) -> list[tuple[str, str]]:
    """
    Return 50 tuples, e.g. [("T1", "1"), ("T2", "2"), ..., ("T50", "50")]
    """
    return [(f"T{i}", str(i)) for i in range(1, 51)]
