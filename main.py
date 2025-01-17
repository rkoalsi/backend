from fastapi import FastAPI
from fastapi.responses import RedirectResponse
import uvicorn
from .routes.api import router
from .config.root import connect_to_mongo, disconnect_on_exit
from fastapi.middleware.cors import CORSMiddleware

origins = [
    "http://localhost:3000",
    "https://localhost:3000",
    "http://localhost:8080",
    "http://127.0.0.1:8080",
]


# Set up logging
# logging.basicConfig(level=logging.DEBUG)

app = FastAPI()
client, db = connect_to_mongo()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(router, prefix="/api")


@app.get("/")
def hello_world():
    return "Application is Running"


@app.options("/{path:path}")
async def handle_options():
    return {"message": "CORS preflight passed"}


@app.exception_handler(404)
async def custom_404_handler(_, __):
    return RedirectResponse("/")


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
    app.add_event_handler("shutdown", disconnect_on_exit(client))
