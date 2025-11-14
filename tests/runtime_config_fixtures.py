def build_value() -> int:
    return 99


def build_engine() -> dict[str, str]:
    return {"url": "sqlite://example"}


def build_value_with_task_info(task_info: dict) -> dict:
    return {
        "task_name": task_info.get("task_name"),
        "task_lang": task_info.get("task_lang"),
    }


TEST_CONFIG_PATH: str | None = None


def get_config(path="config.json", task_info=None):
    target_path = TEST_CONFIG_PATH or path
    if task_info and task_info.get("task_language") == "duckdb_sql":
        return target_path
    return None
