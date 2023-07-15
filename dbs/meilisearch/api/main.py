from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from functools import lru_cache

from fastapi import FastAPI
from meilisearch_python_async import Client

from api.config import Settings
from api.routers import rest


@lru_cache()
def get_settings():
    # Use lru_cache to avoid loading .env file for every request
    return Settings()


async def get_search_api_key(settings) -> str:
    URI = f"http://{settings.meili_service}:{settings.meili_port}"
    MASTER_KEY = settings.meili_master_key
    async with Client(URI, MASTER_KEY) as client:
        response = await client.get_keys()
        # Search key is always the first result obtained (followed by admin key)
        search_key = response.results[0].key
        return search_key


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    # Search for wines by keyword phrase using Meilisearch
    settings = get_settings()
    print(settings)
    search_key = await get_search_api_key(settings)
    URI = f"http://{settings.meili_service}:{settings.meili_port}"
    async with Client(URI, search_key) as client:
        app.client = client
        print("Successfully connected to Meilisearch")
        yield
        print("Successfully closed Meilisearch connection")


app = FastAPI(
    title="REST API for wine reviews on Meilisearch",
    description=(
        "Query from a Meilisearch database of 130k wine reviews from the Wine Enthusiast magazine"
    ),
    version=get_settings().tag,
    lifespan=lifespan,
)


@app.get("/", include_in_schema=False)
async def root():
    return {
        "message": "REST API for querying Meilisearch database of 130k wine reviews from the Wine Enthusiast magazine"
    }


# Attach routes
app.include_router(rest.router, prefix="/wine", tags=["wine"])
