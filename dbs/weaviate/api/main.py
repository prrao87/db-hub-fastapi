from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from functools import lru_cache

import weaviate
from fastapi import FastAPI
from sentence_transformers import SentenceTransformer

from api.config import Settings
from api.routers import rest

model_type = "sbert"


@lru_cache()
def get_settings():
    # Use lru_cache to avoid loading .env file for every request
    return Settings()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Async context manager for Weaviate database connection."""
    settings = get_settings()
    model_checkpoint = settings.embedding_model_checkpoint
    app.model = SentenceTransformer(model_checkpoint)
    app.model_type = "sbert"
    # Create Weaviate client
    HOST = settings.weaviate_service
    PORT = settings.weaviate_port
    app.client = weaviate.Client(f"http://{HOST}:{PORT}")
    print("Successfully connected to Weaviate")
    yield
    print("Successfully closed Weaviate connection and released resources")


app = FastAPI(
    title="REST API for wine reviews on Weaviate",
    description=(
        "Query from a Weaviate database of 130k wine reviews from the Wine Enthusiast magazine"
    ),
    version=get_settings().tag,
    lifespan=lifespan,
)


@app.get("/", include_in_schema=False)
async def root():
    return {
        "message": "REST API for querying Weaviate database of 130k wine reviews from the Wine Enthusiast magazine"
    }


# Attach routes
app.include_router(rest.router, prefix="/wine", tags=["wine"])
