from types import SimpleNamespace

from kptn.util.logger import get_logger


def test_get_logger_writes_runtime_logs_to_configured_file(tmp_path):
    log_path = tmp_path / "logs" / "kptn.log"
    pipeline_config = SimpleNamespace(runtime_log_file=str(log_path))
    logger = get_logger(pipeline_config, logger_name="tests.runtime_logger")

    logger.info("runtime log test")
    for handler in logger.handlers:
        handler.flush()

    assert log_path.exists()
    assert "runtime log test" in log_path.read_text(encoding="utf-8")


def test_get_logger_does_not_duplicate_file_handlers(tmp_path):
    log_path = tmp_path / "logs" / "kptn.log"
    pipeline_config = SimpleNamespace(runtime_log_file=str(log_path))
    logger_name = "tests.runtime_logger_no_duplicates"

    first = get_logger(pipeline_config, logger_name=logger_name)
    second = get_logger(pipeline_config, logger_name=logger_name)

    file_handlers = [
        handler
        for handler in second.handlers
        if getattr(handler, "_kptn_file_handler", False)
    ]

    assert first is second
    assert len(file_handlers) == 1
