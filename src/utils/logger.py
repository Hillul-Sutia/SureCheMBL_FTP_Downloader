import logging
import os
from pathlib import Path

# def get_logger(name):

#     logging.basicConfig(
#         level=logging.INFO,
#         format="%(asctime)s - %(levelname)s - %(message)s"
#     )

#     return logging.getLogger(name)

def get_logger(name):
    logger = logging.getLogger(name)

    # Avoid adding handlers multiple times
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s"
    )

    # Create a log file based on the logger name
    file_handler = logging.FileHandler(f"{name}.log")
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)

    return logger