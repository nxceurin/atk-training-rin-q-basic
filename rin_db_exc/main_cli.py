from typer import Typer
from rin_db_exc.app import run_web_app

app = Typer()


@app.command()
def web_app():
    run_web_app()
