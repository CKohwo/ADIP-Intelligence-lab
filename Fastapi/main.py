from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from Fastapi.routes import router
from Fastapi.config import (
    API_TITLE,
    API_VERSION,
    API_DESCRIPTION
)

app = FastAPI(
    title = API_TITLE,
    version = API_VERSION,
    description = API_DESCRIPTION,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)

@app.get("/")
def root() -> dict:
    """
    ROOT ENDPOINT
    """
    return {
        "application": API_TITLE,
        "version": API_VERSION,
        "documentation": "/docs",
        "health": "/health"
    }


