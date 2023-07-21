from typing import Dict
import os
import yaml
import logging.config

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


def log_vals() -> Dict[str, str]:
    with open(os.path.dirname(__file__) + "/config.yaml", "r") as f:
        conf_dict = yaml.safe_load(f.read())
    return conf_dict
