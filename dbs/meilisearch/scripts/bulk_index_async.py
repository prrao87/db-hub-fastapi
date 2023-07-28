from __future__ import annotations

import argparse
import asyncio
import os
import sys
from functools import lru_cache
from pathlib import Path
from typing import Any

import srsly
from codetiming import Timer
from dotenv import load_dotenv
from meilisearch_python_async import Client
from meilisearch_python_async.index import Index
from meilisearch_python_async.models.settings import MeilisearchSettings
from schemas.wine import Wine
from tqdm.asyncio import tqdm_asyncio

sys.path.insert(1, os.path.realpath(Path(__file__).resolve().parents[1]))
from api.config import Settings

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
    exclude_none: bool = True,
) -> list[JsonBlob]:
    validated_data = [Wine(**item).model_dump(exclude_none=exclude_none) for item in data]
    return validated_data


def get_meili_settings(filename: str) -> MeilisearchSettings:
    settings = dict(srsly.read_json(filename))
    # Convert to MeilisearchSettings pydantic model object
    settings = MeilisearchSettings(**settings)
    return settings


# --- Async functions ---


async def main() -> None:
    meili_settings = get_meili_settings(filename="settings/settings.json")
    config = Settings()
    URI = f"http://{config.meili_url}:{config.meili_port}"
    MASTER_KEY = config.meili_master_key
    index_name = "wines"
    primary_key = "id"
    async with Client(URI, MASTER_KEY) as client:
        with Timer(name="Bulk Index", text="Bulk index took {:.4f} seconds"):
            # Create index
            index = client.index(index_name)
            # Update settings
            await client.index(index_name).update_settings(meili_settings)
            print("Finished updating database index settings")
            # Process data
            validated_data = validate(data)
            try:
                tasks = [
                    # Update index
                    index.update_documents_in_batches(
                        validated_data, batch_size=CHUNKSIZE, primary_key=primary_key
                    )
                    for _ in range(BENCHMARK_NUM)
                ]
                await tqdm_asyncio.gather(*tasks)
                print(f"Finished running benchmarks")
            except Exception as e:
                print(f"{e}: Error while indexing to db")


if __name__ == "__main__":
    # fmt: off
    parser = argparse.ArgumentParser("Bulk index database from the wine reviews JSONL data")
    parser.add_argument("--limit", type=int, default=0, help="Limit the size of the dataset to load for testing purposes")
    parser.add_argument("--chunksize", type=int, default=5_000, help="Size of each chunk to break the dataset into before processing")
    parser.add_argument("--filename", type=str, default="winemag-data-130k-v2.jsonl.gz", help="Name of the JSONL zip file to use")
    parser.add_argument("--benchmark", "-b", type=int, default=1, help="Run a benchmark of the script N times")
    args = vars(parser.parse_args())
    # fmt: on

    LIMIT = args["limit"]
    DATA_DIR = Path(__file__).parents[3] / "data"
    FILENAME = args["filename"]
    CHUNKSIZE = args["chunksize"]
    BENCHMARK_NUM = args["benchmark"]

    data = list(get_json_data(DATA_DIR, FILENAME))
    if LIMIT > 0:
        data = data[:LIMIT]

    meili_settings = get_meili_settings(filename="settings/settings.json")

    # Run main async event loop
    asyncio.run(main())
