from fastapi import FastAPI, Request, Form
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles

from recommender import get_recommendation


app = FastAPI()

templates = Jinja2Templates(directory="templates")

app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
def home(request: Request):

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "results": [],
            "user_id": None,
            "location": None
        }
    )

@app.post("/search")
def search(
    request: Request,
    user_id: int = Form(...),
    location: str = Form(...)
):

    try:
        results = get_recommendation(user_id, location, 10)
    except Exception:
        results = []

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "results": results,
            "user_id": user_id,
            "location": location
        }
    )