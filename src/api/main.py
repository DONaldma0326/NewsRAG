from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from jinja2 import Environment, FileSystemLoader
from starlette.responses import RedirectResponse
from starlette.templating import Jinja2Templates

from api.routes import router
from api.sources import start_metrics_consumer

app = FastAPI(title="NewsRAG")

UI_DIR = Path(__file__).parents[1] / "UI"
jinja_env = Environment(
    loader=FileSystemLoader(str(UI_DIR / "templates")),
    auto_reload=True,
    cache_size=0,
)
templates = Jinja2Templates(env=jinja_env)
app.state.templates = templates

app.mount("/static", StaticFiles(directory=str(UI_DIR / "static")), name="static")
app.include_router(router)


@app.on_event("startup")
async def startup():
    start_metrics_consumer()


@app.get("/")
def root():
    return RedirectResponse(url="/chat")
