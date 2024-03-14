import logging
import logging.config
import os

import iotvm_extensions.config as config

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "verbose": {
            "format": "{levelname} {asctime} {module} {process:d} {thread:d} {message}",
            "style": "{",
        },
        "simple": {
            "format": "{levelname} {asctime} {module} {message}",
            "style": "{",
        },
    },
    "handlers": {
        "console": {"class": "logging.StreamHandler", "formatter": "verbose"},
        "timed_rotating_file": {
            "class": "logging.handlers.TimedRotatingFileHandler",
            "filename": os.path.join(config.SYSTEM_ROOT_DIR, "logs", "app.log"),
            "formatter": "verbose",
            "when": "midnight",
            "interval": 1,
        },
    },
    "loggers": {
        # DEBUG, INFO, WARNING, ERROR
        # "": {
        #     "level": "INFO",
        #     "handlers": ["console", "timed_rotating_file"],
        # },
        "root": {
            "level": "INFO",
            "handlers": ["console", "timed_rotating_file"],
        },
        "apscheduler": {
            "level": "ERROR",
        },
        "iotvm_extensions": {
            "level": "DEBUG",
        },
    },
}


def set_up_logging() -> None:
    logging.config.dictConfig(LOGGING)
