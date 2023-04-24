from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from functools import lru_cache

from fastapi import FastAPI
from qdrant_client import QdrantClient

from api.config import Settings
from api.routers.wine import wine_router

try:
    from optimum import ORTModelForCustomTasks, pipeline
    from transformers import AutoTokenizer

    model_type = "onnx"
except ModuleNotFoundError:
    from sentence_transformers import SentenceTransformer

    model_type = "sbert"


@lru_cache()
def get_settings():
    # Use lru_cache to avoid loading .env file for every request
    return Settings()


def get_embedding_pipeline(onnx_path, model_filename: str):
    """
    Create a sentence embedding pipeline using the optimized ONNX model, if available in the environment
    """
    # Reload tokenizer
    tokenizer = AutoTokenizer.from_pretrained(onnx_path)
    optimized_model = ORTModelForCustomTasks.from_pretrained(onnx_path, file_name=model_filename)
    embedding_pipeline = pipeline("feature-extraction", model=optimized_model, tokenizer=tokenizer)
    return embedding_pipeline


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Async context manager for Qdrant database connection."""
    settings = get_settings()
    model_checkpoint = settings.embedding_model_checkpoint
    if model_type == "sbert":
        app.model = SentenceTransformer(model_checkpoint)
        app.model_type = "sbert"
    elif model_type == "onnx":
        app.model = get_embedding_pipeline(
            "onnx_models", model_filename=settings.onnx_model_filename
        )
        app.model_type = "onnx"
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
app.include_router(wine_router, prefix="/wine", tags=["wine"])
