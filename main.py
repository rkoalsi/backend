import logging

logging.basicConfig(format="%(levelname)s: %(message)s", level=logging.INFO)

from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
from .routes.api import router
from .config.root import connect_to_mongo, disconnect_on_exit
from .config.scheduler import scheduler_startup, scheduler_shutdown
import uvicorn

origins = [
    "http://localhost:3000",
    "https://localhost:3000",
    "http://localhost:8080",
    "http://127.0.0.1:8080",
]

# Initialize the app
client, db = connect_to_mongo()


app = FastAPI()

# Add CORS Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API router
app.include_router(router, prefix="/api")

# Add shutdown handler for MongoDB
app.add_event_handler("startup", scheduler_startup)
app.add_event_handler("shutdown", disconnect_on_exit(client))
app.add_event_handler("shutdown", scheduler_shutdown)


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
