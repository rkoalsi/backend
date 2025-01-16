from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
from routes.api import router  # Use absolute imports
from config.root import connect_to_mongo, disconnect_on_exit

# Define allowed origins for CORS
origins = [
    "http://localhost:3000",
    "https://localhost:3000",
    "http://localhost:8080",
    "http://127.0.0.1:8080",
    # Add production frontend domains here
]

# Initialize FastAPI app
app = FastAPI()

# MongoDB connection
client, db = connect_to_mongo()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  # Use the defined origins list
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routes
app.include_router(router)


# Handle OPTIONS preflight requests
@app.options("/{path:path}")
async def handle_options():
    return {"message": "CORS preflight passed"}


# Custom 404 handler
@app.exception_handler(404)
async def custom_404_handler(_, __):
    return RedirectResponse("/")


# Event handlers for app lifecycle
@app.on_event("startup")
async def startup_event():
    print("Application startup: MongoDB connected.")


@app.on_event("shutdown")
async def shutdown_event():
    disconnect_on_exit(client)
    print("Application shutdown: MongoDB disconnected.")


# Main entry point
if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=800, reload=True)
