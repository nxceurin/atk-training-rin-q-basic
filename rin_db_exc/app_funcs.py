import asyncio
import logging
import os
import random

from rin_db_exc.PIQ import Producer, Consumer, Cleaner
from rin_db_exc.log_error import get_yaml

logger: logging.Logger = logging.getLogger(__name__)
finished_processes = []
discarded_processes = []


async def producer(cpath: str = os.path.dirname(__file__) + "/config.yaml") -> None:
    fpath: str = get_yaml(cpath).get('general', {"primary_path": os.getcwd()}).get('primary_path', os.getcwd())
    while True:
        prod = Producer(fpath)
        prod()
        await asyncio.sleep(5)


async def consumer(cpath: str = os.path.dirname(__file__)+"/config.yaml", name: str = "Consumer") -> None:
    conf_dict = get_yaml(cpath)
    fpath: str = conf_dict.get('general', {"primary_path": os.getcwd()}).get('primary_path', os.getcwd())
    time_out = conf_dict.get('consumer', {'n_time': 3600}).get('n_time', 3600)
    num_tries = conf_dict.get('consumer', {'n_tries': 3}).get('n_tries', 3)

    while True:
        cons = Consumer(name, fpath)
        try:
            job_name, job_id = cons.get_job_name()
        except TypeError:
            job_name = ""
        for _ in range(num_tries):
            try:
                await asyncio.wait_for(cons(), timeout=time_out)
                finished_processes.append({"id": job_id, "filename": job_name})
                break
            except asyncio.TimeoutError:
                logger.error(f"Job took more than {time_out} seconds. Skipping {job_name}...")
                discarded_processes.append({"id": id, "filename": job_name})
                break
            except Exception as e:
                logger.error(e)
                logger.warning(f"Encountered error while processing job. Trying again...\n")
                try:
                    os.rename(job_name, job_name+".failed")
                except Exception:
                    pass
        else:
            logger.error(f"Job unable to be finished. Skipping {job_name}...")
            discarded_processes.append({"id": id, "filename": job_name})
        try:
            cons.delete_entry()
        except TypeError:
            logger.info("Empty queue")
        await asyncio.sleep(random.randint(7, 15))


async def cleaner(cpath: str) -> None:
    fpath: str = get_yaml(cpath).get('general', {"primary_path": os.getcwd()}).get('primary_path', os.getcwd())
    while True:
        cln = Cleaner(fpath)
        cln()
        await asyncio.sleep(30)
