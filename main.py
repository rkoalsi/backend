from fastapi import FastAPI
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
app.include_router(router)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=1000)
    app.add_event_handler("shutdown", disconnect_on_exit(client))
