from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from functools import lru_cache

import lancedb
from fastapi import FastAPI

from api.config import Settings
from api.routers.rest import router

from sentence_transformers import SentenceTransformer

model_type = "sbert"


@lru_cache()
def get_settings():
    # Use lru_cache to avoid loading .env file for every request
    return Settings()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Async context manager for lancedb connection."""
    settings = get_settings()
    model_checkpoint = settings.embedding_model_checkpoint
    app.model = SentenceTransformer(model_checkpoint)
    app.model_type = "sbert"
    # Define LanceDB client
    db = lancedb.connect("./lancedb")
    app.table = db.open_table("wines")
    print("Successfully connected to LanceDB")
    yield
    print("Successfully closed LanceDB connection and released resources")


app = FastAPI(
    title="REST API for wine reviews on LanceDB",
    description=(
        "Query from a LanceDB database of 130k wine reviews from the Wine Enthusiast magazine"
    ),
    version=get_settings().tag,
    lifespan=lifespan,
)


@app.get("/", include_in_schema=False)
async def root():
    return {
        "message": "REST API for querying LanceDB database of 130k wine reviews from the Wine Enthusiast magazine"
    }


# Attach routes
app.include_router(router, prefix="/wine", tags=["wine"])
