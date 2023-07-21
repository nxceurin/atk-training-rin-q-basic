from typer import Typer
import rin_db_exc.app_funcs as afunc
from rin_db_exc.app import run_web_app
import asyncio

app = Typer()


@app.command()
def producer():
    asyncio.run(afunc.producer())


@app.command()
def consumer():
    asyncio.run(afunc.consumer())


@app.command()
def web_app(path: str):
    run_web_app(path)