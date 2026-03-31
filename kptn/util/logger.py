import logging
import os
from pathlib import Path

class CustomFormatter(logging.Formatter):
    # Define color codes
    COLORS = {
        'WARN': '\x1b[33m',  # Yellow
        'CRIT': '\x1b[31m',  # Red
        'ERR': '\x1b[31m',   # Red
        'DEFAULT': '\x1b[37m', # White
    }
    RESET = '\x1b[0m'

    def format(self, record):
        # Set the color based on the levelname
        original_levelname = record.levelname
        color = self.COLORS.get(record.levelname, self.COLORS['DEFAULT'])
        record.levelname = f"{color}{record.levelname}{self.RESET}"
        try:
            return super().format(record)
        finally:
            record.levelname = original_levelname

def _has_stream_handler(logger: logging.Logger) -> bool:
    return any(getattr(handler, "_kptn_stream_handler", False) for handler in logger.handlers)


def _has_file_handler(logger: logging.Logger, log_file: str) -> bool:
    target = str(Path(log_file).resolve())
    return any(
        getattr(handler, "_kptn_file_handler", False)
        and getattr(handler, "_kptn_log_file", None) == target
        for handler in logger.handlers
    )


def setup_logger(logger_name, log_file: str | None = None):

    logger = logging.getLogger(logger_name)
    logging.addLevelName(logging.WARNING, 'WARN')
    logging.addLevelName(logging.CRITICAL, 'CRIT')
    logging.addLevelName(logging.ERROR, 'ERR')
    format = '{asctime} {filename}:{lineno} {levelname}: {message}'
    datefmt = '%m-%d %H:%M:%S'

    if not _has_stream_handler(logger):
        handler = logging.StreamHandler()
        is_prod = os.getenv("IS_PROD") == "1"
        # If in production, don't use color
        if is_prod:
            handler.setFormatter(logging.Formatter(fmt=format, datefmt=datefmt, style='{'))
        else:
            handler.setFormatter(CustomFormatter(fmt=format, datefmt=datefmt, style='{'))
        handler._kptn_stream_handler = True
        logger.addHandler(handler)

    if log_file and not _has_file_handler(logger, log_file):
        resolved_log_file = Path(log_file).resolve()
        resolved_log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(resolved_log_file, encoding="utf-8")
        file_handler.setFormatter(logging.Formatter(fmt=format, datefmt=datefmt, style='{'))
        file_handler._kptn_file_handler = True
        file_handler._kptn_log_file = str(resolved_log_file)
        logger.addHandler(file_handler)

    logger.propagate = False
    logger.setLevel(logging.INFO)
    return logger

def get_logger(pipeline_config=None, logger_name=__name__):

    # If running in Prefect, use Prefect's logger
    prefect_env_vars = [
        "PREFECT__FLOW_ID",
        "PREFECT__FLOW_RUN_ID",
        "PREFECT__TASK_RUN_ID",
    ]
    if any(env_var in os.environ for env_var in prefect_env_vars):
        from prefect.logging import get_run_logger
        return get_run_logger()
    else:
        log_file = getattr(pipeline_config, "runtime_log_file", None) if pipeline_config is not None else None
        logger = setup_logger(logger_name, log_file=log_file)

    return logger
