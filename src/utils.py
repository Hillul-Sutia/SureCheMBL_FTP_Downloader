import logging
import yaml
import os


def setup_logging():
    os.makedirs("logs", exist_ok=True)

    logging.basicConfig(
        filename="logs/pipeline.log",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )


def load_config(path="config.yml"):
    with open(path, "r") as f:
        return yaml.safe_load(f)