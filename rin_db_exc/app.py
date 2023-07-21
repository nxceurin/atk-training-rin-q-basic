import asyncio
import uvicorn
import os
from fastapi import FastAPI
from rin_db_exc.app_funcs import producer, consumer, cleaner
from rin_db_exc.log_error import get_yaml
app = FastAPI()


@app.get("/")
async def execute_functions(config_path: str = os.path.dirname(__file__)+"/config.yaml"):
    # Create tasks for each function
    conf = get_yaml(config_path)
    for _ in range(conf.get('general', {'n_prod': 1}).get('n_prod', 1)):
        asyncio.create_task(producer(config_path))
    for i in range(conf.get('general', {'n_cons': 1}).get('n_cons', 1)):
        asyncio.create_task(consumer(config_path, f"Consumer{i+1}"))
    asyncio.create_task(cleaner(config_path))


def run_web_app():
    uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
