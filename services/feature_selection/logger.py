"""
Enterprise Logger for IR-IDS Feature Selection Service
"""

import logging
from pathlib import Path

from config import (
    LOG_FOLDER,
    LOG_FILE,
    LOG_LEVEL,
    LOG_FORMAT,
    DATE_FORMAT,
)


class IDSLogger:

    _logger = None

    def __init__(self):

        if IDSLogger._logger is not None:
            return

        LOG_FOLDER.mkdir(parents=True, exist_ok=True)

        logger = logging.getLogger("FeatureSelection")

        logger.setLevel(getattr(logging, LOG_LEVEL))

        logger.propagate = False

        formatter = logging.Formatter(
            LOG_FORMAT,
            datefmt=DATE_FORMAT
        )

        # ==========================================================
        # Console Logger
        # ==========================================================

        console_handler = logging.StreamHandler()

        console_handler.setFormatter(formatter)

        logger.addHandler(console_handler)

        # ==========================================================
        # File Logger
        # ==========================================================

        file_handler = logging.FileHandler(
            LOG_FILE,
            encoding="utf-8"
        )

        file_handler.setFormatter(formatter)

        logger.addHandler(file_handler)

        IDSLogger._logger = logger

    # ==========================================================
    # Get Logger
    # ==========================================================

    def get_logger(self):

        return IDSLogger._logger