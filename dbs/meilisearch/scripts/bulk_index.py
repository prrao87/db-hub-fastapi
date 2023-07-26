from __future__ import annotations

import argparse
import asyncio
import os
import sys
from concurrent.futures import ProcessPoolExecutor
from functools import lru_cache, partial
from pathlib import Path
from typing import Any, Iterator

import srsly
from dotenv import load_dotenv
from meilisearch_python_async import Client
from meilisearch_python_async.index import Index
from meilisearch_python_async.models.settings import MeilisearchSettings
from pydantic.main import ModelMetaclass

sys.path.insert(1, os.path.realpath(Path(__file__).resolve().parents[1]))
from api.config import Settings
from schemas.wine import Wine

load_dotenv()
# Custom types
JsonBlob = dict[str, Any]


class FileNotFoundError(Exception):
    pass


# --- Blocking functions ---


@lru_cache()
def get_settings():
    # Use lru_cache to avoid loading .env file for every request
    return Settings()


def chunk_iterable(item_list: list[JsonBlob], chunksize: int) -> Iterator[tuple[JsonBlob, ...]]:
    """
    Break a large iterable into an iterable of smaller iterables of size `chunksize`
    """
    for i in range(0, len(item_list), chunksize):
        yield tuple(item_list[i : i + chunksize])


def get_json_data(data_dir: Path, filename: str) -> list[JsonBlob]:
    """Get all line-delimited json files (.jsonl) from a directory with a given prefix"""
    file_path = data_dir / filename
    if not file_path.is_file():
        # File may not have been uncompressed yet so try to do that first
        data = srsly.read_gzip_jsonl(file_path)
        # This time if it isn't there it really doesn't exist
        if not file_path.is_file():
            raise FileNotFoundError(f"No valid .jsonl file found in `{data_dir}`")
    else:
        data = srsly.read_gzip_jsonl(file_path)
    return data


def validate(
    data: list[JsonBlob],
    model: ModelMetaclass,
    exclude_none: bool = False,
) -> list[JsonBlob]:
    validated_data = [model(**item).dict(exclude_none=exclude_none) for item in data]
    return validated_data


def process_chunks(data: list[JsonBlob]) -> tuple[list[JsonBlob], str]:
    validated_data = validate(data, Wine, exclude_none=True)
    return validated_data


def get_meili_settings(filename: str) -> MeilisearchSettings:
    settings = dict(srsly.read_json(filename))
    # Convert to MeilisearchSettings pydantic model object
    settings = MeilisearchSettings(**settings)
    return settings


# --- Async functions ---


async def update_documents_to_index(index: Index, primary_key: str, data: list[JsonBlob]) -> None:
    ids = [item[primary_key] for item in data]
    await index.update_documents(data, primary_key)
    print(f"Processed ids in range {min(ids)}-{max(ids)}")


async def main(data: list[JsonBlob], meili_settings: MeilisearchSettings) -> None:
    settings = Settings()
    URI = f"http://{settings.meili_url}:{settings.meili_port}"
    MASTER_KEY = settings.meili_master_key
    index_name = "wines"
    primary_key = "id"
    async with Client(URI, MASTER_KEY) as client:
        # Create index
        index = client.index(index_name)
        # Update settings
        await client.index(index_name).update_settings(meili_settings)
        print("Finished updating database index settings")

        # Process multiple chunks of data in a process pool to avoid blocking the event loop
        print("Processing chunks")
        chunked_data = chunk_iterable(data, CHUNKSIZE)

        with ProcessPoolExecutor() as pool:
            loop = asyncio.get_running_loop()
            executor_tasks = [partial(process_chunks, chunk) for chunk in chunked_data]
            awaitables = [loop.run_in_executor(pool, call) for call in executor_tasks]
            # Attach process pool to running event loop so that we can process multiple chunks in parallel
            validated_data = await asyncio.gather(*awaitables)
            tasks = [update_documents_to_index(index, primary_key, data) for data in validated_data]
        try:
            await asyncio.gather(*tasks)
            print("Finished execution!")
        except Exception as e:
            print(f"{e}: Error while indexing to db")


if __name__ == "__main__":
    # fmt: off
    parser = argparse.ArgumentParser("Bulk index database from the wine reviews JSONL data")
    parser.add_argument("--limit", type=int, default=0, help="Limit the size of the dataset to load for testing purposes")
    parser.add_argument("--chunksize", type=int, default=10_000, help="Size of each chunk to break the dataset into before processing")
    parser.add_argument("--filename", type=str, default="winemag-data-130k-v2.jsonl.gz", help="Name of the JSONL zip file to use")
    args = vars(parser.parse_args())
    # fmt: on

    LIMIT = args["limit"]
    DATA_DIR = Path(__file__).parents[3] / "data"
    FILENAME = args["filename"]
    CHUNKSIZE = args["chunksize"]

    data = list(get_json_data(DATA_DIR, FILENAME))
    if LIMIT > 0:
        data = data[:LIMIT]

    meili_settings = get_meili_settings(filename="settings/settings.json")

    # Run main async event loop
    if data:
        asyncio.run(main(data, meili_settings))
