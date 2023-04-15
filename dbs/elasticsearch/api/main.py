from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from functools import lru_cache
import warnings

from dotenv import load_dotenv
from fastapi import FastAPI
from elasticsearch import AsyncElasticsearch

from api.config import Settings
from api.routers.wine import wine_router

load_dotenv()


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
    version="0.1.0",
    lifespan=lifespan,
)


@app.get("/", include_in_schema=False)
async def root():
    return {
        "message": "REST API for querying Elasticsearch database of 130k wine reviews from the Wine Enthusiast magazine"
    }


# Attach routes
app.include_router(wine_router, prefix="/wine", tags=["wine"])


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
