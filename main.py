import logging

logging.basicConfig(format="%(levelname)s: %(message)s", level=logging.INFO)

from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
from .routes.api import router
from .config.root import get_database, disconnect_on_exit
from .config.crons import cron_shutdown, cron_startup
from .config.scheduler import notification_scheduler_startup, notification_scheduler_shutdown, scheduler
import uvicorn

origins = [
    "http://localhost:3000",
    "https://localhost:3000",
    "http://localhost:8080",
    "http://127.0.0.1:8080",
    "https://test.orderform.pupscribe.in",  # Frontend (home route)
    "http://test.orderform.pupscribe.in",   # HTTP fallback (will redirect to HTTPS)
]

# Initialize the app
app = FastAPI()

# Add CORS Middleware with security improvements
# Note: If you have production domains, add them to the origins list above
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  # Use specific origins instead of ["*"] for better security
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],  # Specify allowed methods
    allow_headers=["*"],
)

# Include API router
app.include_router(router, prefix="/api")


# Startup handler to initialize database connection
@app.on_event("startup")
async def startup_db():
    """Initialize the shared database connection on startup."""
    get_database()


# Shutdown handler for MongoDB and other resources
@app.on_event("shutdown")
async def shutdown_db():
    """Close the shared database connection on shutdown."""
    disconnect_on_exit()


# Add shutdown handler for MongoDB
# app.add_event_handler("startup", notification_scheduler_startup)
# app.add_event_handler("startup", cron_startup)
# app.add_event_handler("shutdown", cron_shutdown)
# app.add_event_handler("shutdown", notification_scheduler_shutdown)


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
