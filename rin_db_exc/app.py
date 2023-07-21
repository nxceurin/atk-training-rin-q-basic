import asyncio
import os

import uvicorn
from fastapi import FastAPI

from rin_db_exc.PIQ import PersistentQSQLite as psql
from rin_db_exc.app_funcs import producer, consumer, cleaner, finished_processes, discarded_processes
from rin_db_exc.log_error import get_yaml

app = FastAPI()
config_path: str = os.path.dirname(__file__) + "/config.yaml"


@app.get("/")
async def execute_functions():
    conf = get_yaml(config_path)
    for _ in range(conf.get('general', {'n_prod': 1}).get('n_prod', 1)):
        asyncio.create_task(producer(config_path))
    for i in range(conf.get('general', {'n_cons': 1}).get('n_cons', 1)):
        asyncio.create_task(consumer(config_path, f"Consumer{i + 1}"))
    asyncio.create_task(cleaner(config_path))


@app.get("/tables")
async def show_table():
    queue = psql().get_jobs()
    return {"pending jobs": queue,
            "completed jobs": finished_processes,
            "discarded jobs": discarded_processes
            }


def run_web_app(path: str):
    global config_path
    config_path = path
    uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
