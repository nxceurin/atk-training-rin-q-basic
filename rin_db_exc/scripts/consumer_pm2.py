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


finished_processes = []
discarded_processes = []


def get_processes():
    return finished_processes, discarded_processes


if __name__ == "__main__":
    logger: logging.Logger = logging.getLogger(__name__)
    signal.signal(signal.SIGALRM, timeout_handler)

    cpath: list = sys.argv[1:]  # get args from terminal/ used with pm2
    conf_dict = get_yaml(cpath[0])  # get the first arg- path to config.yaml

    # get values from config file
    fpath: str = conf_dict.get('general', {"primary_path": os.getcwd()}).get('primary_path', os.getcwd())
    time_out = conf_dict.get('consumer', {'n_time': 3600}).get('n_time', 3600)
    num_tries = conf_dict.get('consumer', {'n_tries': 3}).get('n_tries', 3)

    while True:
        cons = Consumer(cpath=fpath, name=cpath[1])
        for _ in range(3):  # if queue is empty for 60seconds, stop itself
            try:
                job_name, job_id = cons.get_job_name()
                break
            except TypeError:
                print("Queue empty. Waiting 20s...")
                job_name = ""
                sleep(20)
                continue
        else:  # executes if for loop hasn't been broken -> queue is empty over 60s
            subprocess.run(f"pm2 stop {cpath[1]}", shell=True, check=True)

        for _ in range(num_tries):  # no. tries job can fail before discarding
            try:
                signal.alarm(time_out)
                cons()
                signal.alarm(0)
                finished_processes.append({"id": job_id, "filename": job_name})
                break
            except TimeoutException or FileNotFoundError:
                logger.error(f"Job took too long or was unable to be found. Skipping {job_name}...")
                try:
                    os.rename(job_name, job_name + ".failed")  # add .failed to skipped jobs so cleaner can handle it
                    discarded_processes.append({"id": job_id, "filename": job_name})
                except Exception:
                    pass
                break
            except Exception as e:
                logger.error(e)
                logger.warning(f"Encountered error while processing job. Trying again...\n")

        else:  # triggers when all tries are exhausted
            logger.error(f"Job unable to be finished after {num_tries}. Skipping {job_name}...")
            try:
                os.rename(job_name, job_name + ".failed")
            except Exception:
                pass
            discarded_processes.append({"id": id, "filename": job_name})
        while True:  # keep trying till processed job is deleted from queue
            try:
                cons.delete_entry()
                break
            except sqlite3.OperationalError:
                print("Lock detected. Trying till lock is removed...")
                sleep(5)
            except TypeError:  # if queue is empty, leave
                break
            except Exception:
                sleep(5)
        sleep(random.randint(7, 15))
