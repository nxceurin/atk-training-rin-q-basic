import os
import subprocess

import fastapi
import uvicorn
from fastapi import FastAPI
from fastapi.responses import HTMLResponse

from rin_db_exc.PIQ import PersistentQSQLite as psql
from rin_db_exc.log_error import get_yaml

app = FastAPI()
config_path: str = os.path.dirname(__file__) + "/config.yaml"


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
            <br><br>

            <button type="submit" name="action" value="manager">Start Manager</button>
            <button type="submit" name="action" value="cleaner">Start Cleaner</button>
        </form>
        """


@app.post("/")
async def execute_command(conf_path: str = fastapi.Form(...), action: str = fastapi.Form(...),
                          p_name: str = fastapi.Form(...), c_name: str = fastapi.Form(...)):
    map_button = {
        "add_prod": f"pm2 start producer_pm2.py --name {p_name} -- {conf_path}",
        "add_cons": f"pm2 start consumer_pm2.py --name {c_name} -- {conf_path}",
        "del_prod": f"pm2 stop {p_name}",
        "del_cons": f"pm2 stop {c_name}",
        "manager": f"pm2 start manager_pm2.py -- {conf_path}",
        "cleaner": f"pm2 stop cleaner_pm2.py -- {conf_path}"
    }
    command = map_button.get(action, "invalid")
    if command == "invalid":
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
    return {"pending jobs": queue
            }


def run_web_app():
    uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
