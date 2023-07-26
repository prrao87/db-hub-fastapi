import warnings
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from functools import lru_cache

from elasticsearch import AsyncElasticsearch
from fastapi import FastAPI

from api.config import Settings
from api.routers import rest


@lru_cache()
def get_settings():
    # Use lru_cache to avoid loading .env file for every request
    return Settings()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Async context manager for Elasticsearch connection."""
    settings = get_settings()
    username = settings.elastic_user
    password = settings.elastic_password
    port = settings.elastic_port
    service = settings.elastic_service
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        elastic_client = AsyncElasticsearch(
            f"http://{service}:{port}",
            basic_auth=(username, password),
            request_timeout=60,
            max_retries=3,
            retry_on_timeout=True,
            verify_certs=False,
        )
        """Async context manager for Elasticsearch connection."""
        app.client = elastic_client
        print("Successfully connected to Elasticsearch")
        yield
        await elastic_client.close()
        print("Successfully closed Elasticsearch connection")


app = FastAPI(
    title="REST API for wine reviews on Elasticsearch",
    description=(
        "Query from an Elasticsearch database of 130k wine reviews from the Wine Enthusiast magazine"
    ),
    version=get_settings().tag,
    lifespan=lifespan,
)


@app.get("/", include_in_schema=False)
async def root():
    return {
        "message": "REST API for querying Elasticsearch database of 130k wine reviews from the Wine Enthusiast magazine"
    }


# Attach routes
app.include_router(rest.router, prefix="/wine", tags=["wine"])
