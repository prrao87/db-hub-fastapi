from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from functools import lru_cache

from dotenv import load_dotenv
from fastapi import FastAPI
from neo4j import AsyncGraphDatabase

from api.config import Settings
from api.routers.wine import wine_router

load_dotenv()


@lru_cache()
def get_settings():
    # Use lru_cache to avoid loading .env file for every request
    return Settings()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Async context manager for MongoDB connection."""
    settings = get_settings()
    service = settings.db_service
    URI = f"bolt://{service}:7687"
    AUTH = ("neo4j", settings.neo4j_password)
    async with AsyncGraphDatabase.driver(URI, auth=AUTH) as driver:
        async with driver.session(database="neo4j") as db_session:
            app.state.db_session = db_session
            print("Successfully connected to wine reviews Neo4j DB")
            yield
    print("Successfully closed wine reviews Neo4j connection")


app = FastAPI(
    title="REST API for wine reviews on Neo4j",
    description=(
        "Query from a Neo4j database of 130k wine reviews from the Wine Enthusiast magazine"
    ),
    version="0.1.0",
    lifespan=lifespan,
)


@app.get("/", include_in_schema=False)
async def root():
    return {"message": "REST API for querying wine reviews Neo4j data"}


# Attach routes
app.include_router(wine_router, prefix="/wine", tags=["wine"])


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
