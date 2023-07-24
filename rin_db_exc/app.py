import os
import subprocess

import fastapi
import uvicorn
from fastapi import FastAPI
from fastapi.responses import HTMLResponse

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

@app.get("/", response_class=HTMLResponse)
async def get_form():
    return """
        <form method="post">
            <label for="config_path">Config Path:</label><br>
            <input type="text" id="conf_path" name="conf_path" required><br><br>
            
            <label for="p_name">Process name</label><br>
            <input type="text" id="p_name" name="p_name">
            <button type="submit" name="action" value="add_prod">Create Producer</button>
            <button type="submit" name="action" value="del_prod">Stop Producer</button>
            <br><br>
            
            <label for="c_name">Consumer name</label><br>
            <input type="text" id="c_name" name="c_name">
            <button type="submit" name="action" value="add_cons">Start Consumer</button>
            <button type="submit" name="action" value="del_cons">Create Consumer</button>
        </form>
        """


@app.post("/")
async def execute_command(conf_path: str = fastapi.Form(...), action: str = fastapi.Form(...),
                          p_name: str = fastapi.Form(...), c_name: str = fastapi.Form(...)):
    if action == "add_prod":
        command = f"pm2 start producer_pm2.py --name {p_name} -- {conf_path}"
    elif action == "add_cons":
        command = f"pm2 start consumer_pm2.py --name {c_name} -- {conf_path}"
    elif action == "del_prod":
        command = f"pm2 stop {p_name}"
    elif action == "del_cons":
        command = f"pm2 stop {c_name}"
    else:
        return "Invalid action!"

    try:
        subprocess.run(command, shell=True, check=True)
        return f"Command executed successfully: {command}"
    except subprocess.CalledProcessError as e:
        return f"Command execution failed: {command}, Error: {e}"


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
