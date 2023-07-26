import os
import subprocess

import fastapi
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from rin_db_exc.classes.PersistentQSQLite import PersistentQSQLite as psql
from rin_db_exc.log_error import get_yaml

config_path: str = os.path.dirname(__file__) + "/config.yaml"
script_path = os.path.dirname(os.path.realpath(__file__))
app = FastAPI()
templates = Jinja2Templates(directory=script_path + "/templates")


@app.get("/", response_class=HTMLResponse)
async def get_form():
    """
    Generates the HTML-basedd form where user can enter configuration file path, producer and consumer names, and have
    the option to start/end producer, consumer, manager and cleaner.
    """
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
            <button type="submit" name="action" value="add_cons">Create Consumer</button>
            <button type="submit" name="action" value="del_cons">Stop Consumer</button>
            <br><br>

            <button type="submit" name="action" value="yes_man">Start Manager</button>
            <button type="submit" name="action" value="no_man">Stop Manager</button><br>
            <button type="submit" name="action" value="yes_cln">Start Cleaner</button>
            <button type="submit" name="action" value="no_cln">Stop Cleaner</button>
            <br><br>
            <button type="submit" name="action" value="tables">View jobs</button>
        </form>
        """


@app.post("/")
async def execute_command(conf_path: str = fastapi.Form(...), action: str = fastapi.Form(...),
                          p_name: str = fastapi.Form(...), c_name: str = fastapi.Form(...)) -> str:
    """
        Function responsible for executing pm2 commands.
        Input is received from the submitted form and returns a string.
        """
    global config_path
    global script_path
    config_path = conf_path

    map_button = {
        "add_prod": f"pm2 start {script_path}/scripts/producer_pm2.py --name {p_name} -- {conf_path} {p_name}",
        "add_cons": f"pm2 start {script_path}/scripts/consumer_pm2.py --name {c_name} -- {conf_path} {c_name}",
        "del_prod": f"pm2 stop {p_name}",
        "del_cons": f"pm2 stop {c_name}",
        "yes_man": f"pm2 start {script_path}/scripts/manager_pm2.py -- {conf_path}",
        "yes_cln": f"pm2 start {script_path}/scripts/cleaner_pm2.py -- {conf_path}",
        "no_man": f"pm2 stop {script_path}/scripts/manager_pm2.py",
        "no_cln": f"pm2 stop {script_path}/scripts/cleaner_pm2.py",
        "tables": "redir tables"
    }
    command = map_button.get(action, "invalid")
    if command == "redir tables":
        return RedirectResponse("/tables", status_code=303)

    elif command == "invalid":
        return "Invalid action!"

    try:
        subprocess.run(command, shell=True, check=True)
        return f"Command executed successfully: {command}"
    except subprocess.CalledProcessError as e:
        return f"Command execution failed: {command}, Error: {e}"


@app.get("/tables", response_class=HTMLResponse)
async def show_tables(request: Request):
    global config_path
    path = get_yaml(config_path).get('general', {"primary_path": os.getcwd()}).get('primary_path', os.getcwd())
    queue = psql(path)
    unprocessed = queue.get_state("unprocessed")
    invalid = queue.get_state("invalid")
    finished = queue.get_state("processed")

    return templates.TemplateResponse("tables.html", {
        "request": request,
        "unprocessed_jobs": unprocessed,
        "finished_jobs": finished,
        "invalid_jobs": invalid,
    })


def run_web_app():
    uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
