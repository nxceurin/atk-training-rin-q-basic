import logging
import os
import random
import signal
import sqlite3
import subprocess
import sys
from time import sleep

from rin_db_exc.classes.Consumer import Consumer
from rin_db_exc.log_error import get_yaml


class TimeoutException(Exception):
    """
    Error class for when Consumer takes too long to process job
    """
    pass


def timeout_handler(signum, frame):
    raise TimeoutException("Function execution timed out.")


def consume():
    logger: logging.Logger = logging.getLogger(__name__)
    signal.signal(signal.SIGALRM, timeout_handler)

    cpath: list = sys.argv[1:]  # get args from terminal/ used with pm2
    conf_dict = get_yaml(cpath[0])  # get the first arg- path to config.yaml

    # get values from config file
    fpath: str = conf_dict.get('general', {"primary_path": os.getcwd()}).get('primary_path', os.getcwd())
    time_out = conf_dict.get('consumer', {'n_time': 3600}).get('n_time', 3600)
    num_tries = conf_dict.get('consumer', {'n_tries': 3}).get('n_tries', 3)

    while True:
        cons = Consumer(cpath=fpath, name=cpath[1], tries=num_tries)
        for _ in range(3):  # if queue is empty for 60seconds, stop itself
            try:
                job_name, job_id = cons.get_job_name()  # redundant ??
                break
            except TypeError:
                print("Queue empty. Waiting 20s...")
                sleep(20)
                continue
            except sqlite3.OperationalError:
                print("Waiting for lock to be released...")
                sleep(5)
        else:  # executes if for loop hasn't been broken -> queue is empty over 60s
            subprocess.run(f"pm2 stop {cpath[1]}", shell=True, check=True)

        try:
            signal.alarm(time_out)
            cons()
            signal.alarm(0)
        except TimeoutException:
            logger.error(f"Job took too long. Skipping {job_name}...")
            try:
                cons.set_invalid(job_id)
                break
            except Exception:
                pass

        while True:  # keep trying till processed job is marked completed
            try:
                cons.set_completed(job_id)
                break
            except sqlite3.OperationalError:
                print("Lock detected. Trying till lock is removed...")
                sleep(5)
            except Exception:
                sleep(5)
        sleep(random.randint(7, 15))


if __name__ == "__main__":
    consume()
