import logging
import os
import random
import signal
import sys
from time import sleep

from rin_db_exc.PIQ import Consumer
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
        cons = Consumer(cpath=fpath, name="consumer")
        try:
            job_name, job_id = cons.get_job_name()
        except TypeError:
            print("Queue empty. Waiting 20s...")
            job_name = ""
            sleep(20)
            break
        for _ in range(num_tries):
            try:
                signal.alarm(time_out)
                cons()
                signal.alarm(0)
                finished_processes.append({"id": job_id, "filename": job_name})
                break
            except TimeoutException:
                logger.error(f"Job took more than {time_out} seconds. Skipping {job_name}...")
                discarded_processes.append({"id": job_id, "filename": job_name})
                break
            except Exception as e:
                logger.error(e)
                logger.warning(f"Encountered error while processing job. Trying again...\n")
                try:
                    os.rename(job_name, job_name + ".failed")
                except Exception:
                    pass
        else:
            logger.error(f"Job unable to be finished. Skipping {job_name}...")
            discarded_processes.append({"id": id, "filename": job_name})
        try:
            cons.delete_entry()
        except TypeError:
            logger.info("Empty queue")
        sleep(random.randint(7, 15))
