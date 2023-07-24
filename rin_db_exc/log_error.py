import logging.config

import yaml

log_map = {
    "critical": logging.CRITICAL,
    "error": logging.ERROR,
    "warning": logging.WARNING,
    "info": logging.INFO,
    "debug": logging.DEBUG,
    "": logging.NOTSET
}


def get_yaml(path: str) -> dict:
    with open(path, "r") as file:
        config = yaml.safe_load(file.read())
    return config
