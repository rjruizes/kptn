def build_value() -> int:
    return 99


def build_engine() -> dict[str, str]:
    return {"url": "sqlite://example"}


def build_value_with_task_info(task_info: dict) -> dict:
    return {
        "task_name": task_info.get("task_name"),
        "task_lang": task_info.get("task_lang"),
    }
