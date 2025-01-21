from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.routing import APIRoute
from contextlib import asynccontextmanager
from .routes.api import router
from .config.root import connect_to_mongo, disconnect_on_exit
import uvicorn

origins = [
    "http://localhost:3000",
    "https://localhost:3000",
    "http://localhost:8080",
    "http://127.0.0.1:8080",
]

# Initialize the app
client, db = connect_to_mongo()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Log available routes
    for route in app.routes:
        if isinstance(route, APIRoute):
            print(f"Path: {route.path}, Methods: {route.methods}")
    yield
    print("Application shutdown")


app = FastAPI(lifespan=lifespan)

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
app.add_event_handler("shutdown", disconnect_on_exit(client))


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
