from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from functools import lru_cache

from fastapi import FastAPI
from qdrant_client import QdrantClient

from api.config import Settings
from api.routers import rest


from sentence_transformers import SentenceTransformer

model_type = "sbert"


@lru_cache()
def get_settings():
    # Use lru_cache to avoid loading .env file for every request
    return Settings()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Async context manager for Qdrant database connection."""
    settings = get_settings()
    model_checkpoint = settings.embedding_model_checkpoint
    app.model = SentenceTransformer(model_checkpoint)
    app.model_type = "sbert"
    # Define Qdrant client
    app.client = QdrantClient(host=settings.qdrant_service, port=settings.qdrant_port)
    print("Successfully connected to Qdrant")
    yield
    print("Successfully closed Qdrant connection and released resources")


app = FastAPI(
    title="REST API for wine reviews on Qdrant",
    description=(
        "Query from a Qdrant database of 130k wine reviews from the Wine Enthusiast magazine"
    ),
    version=get_settings().tag,
    lifespan=lifespan,
)


@app.get("/", include_in_schema=False)
async def root():
    return {
        "message": "REST API for querying Qdrant database of 130k wine reviews from the Wine Enthusiast magazine"
    }


# Attach routes
app.include_router(rest.router, prefix="/wine", tags=["wine"])
