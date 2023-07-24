import os

import uvicorn
from fastapi import FastAPI

from rin_db_exc.PIQ import PersistentQSQLite as psql
# from rin_db_exc.consumer_pm2 import get_processes
from rin_db_exc.log_error import get_yaml

app = FastAPI()
config_path: str = os.path.dirname(__file__) + "/config.yaml"


# @app.get("/")
# async def execute_functions():
#     conf = get_yaml(config_path)
#     for _ in range(conf.get('general', {'n_prod': 1}).get('n_prod', 1)):
#         asyncio.create_task(producer(config_path))
#     for i in range(conf.get('general', {'n_cons': 1}).get('n_cons', 1)):
#         asyncio.create_task(consumer(config_path, f"Consumer{i + 1}"))
#     asyncio.create_task(cleaner(config_path))


@app.get("/")
async def show_table():
    global config_path
    path = get_yaml(config_path).get('general', {"primary_path": os.getcwd()}).get('primary_path', os.getcwd())
    queue = psql(path).get_jobs()
    print("all good so far")
    return {"pending jobs": queue,
            # "completed jobs": get_processes()[0],
            # "discarded jobs": get_processes()[1]
            }


def run_web_app(path: str):
    """
    hi?
    :param path:
    :return:
    """
    global config_path
    config_path = path
    uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
