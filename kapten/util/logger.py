import logging
import os

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
        color = self.COLORS.get(record.levelname, self.COLORS['DEFAULT'])
        record.levelname = f"{color}{record.levelname}{self.RESET}"
        return super().format(record)

logger_initialized = False

def setup_logger(logger_name):
    global logger_initialized
    if logger_initialized:
        return logging.getLogger(logger_name)

    logger = logging.getLogger(logger_name)
    logging.addLevelName(logging.WARNING, 'WARN')
    logging.addLevelName(logging.CRITICAL, 'CRIT')
    logging.addLevelName(logging.ERROR, 'ERR')
    format = '{asctime} {filename}:{lineno} {levelname}: {message}'
    datefmt = '%m-%d %H:%M:%S'

    handler = logging.StreamHandler()
    is_prod = os.getenv("IS_PROD") == "1"
    # If in production, don't use color
    if is_prod:
        handler.setFormatter(logging.Formatter(fmt=format, datefmt=datefmt, style='{'))
    else:
        handler.setFormatter(CustomFormatter(fmt=format, datefmt=datefmt, style='{'))

    logger.addHandler(handler)
    logger.propagate = False
    logger.setLevel(logging.INFO)
    logger_initialized = True
    return logger

def get_logger():

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
        logger = setup_logger(__name__)

    return logger