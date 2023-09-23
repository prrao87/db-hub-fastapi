import argparse
import asyncio
import os
import sys
import warnings
from concurrent.futures import ProcessPoolExecutor
from functools import lru_cache, partial
from pathlib import Path
from typing import Any, Iterator

import srsly
from dotenv import load_dotenv
from elasticsearch import AsyncElasticsearch, helpers
from schemas.wine import Wine

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
    data: tuple[JsonBlob],
    exclude_none: bool = False,
) -> list[JsonBlob]:
    validated_data = [Wine(**item).dict(exclude_none=exclude_none) for item in data]
    return validated_data


def process_chunks(data: list[JsonBlob]) -> tuple[list[JsonBlob], str]:
    validated_data = validate(data, exclude_none=True)
    return validated_data


# --- Async functions ---


async def get_elastic_client(settings) -> AsyncElasticsearch:
    # Get environment variables
    USERNAME = settings.elastic_user
    PASSWORD = settings.elastic_password
    PORT = settings.elastic_port
    ELASTIC_URL = settings.elastic_url
    # Connect to ElasticSearch
    elastic_client = AsyncElasticsearch(
        f"http://{ELASTIC_URL}:{PORT}",
        basic_auth=(USERNAME, PASSWORD),
        request_timeout=300,
        max_retries=3,
        retry_on_timeout=True,
        verify_certs=False,
    )
    return elastic_client


async def create_index(client: AsyncElasticsearch, index: str, mappings_path: Path) -> None:
    """Create an index associated with an alias in ElasticSearch"""
    elastic_config = dict(srsly.read_json(mappings_path))
    assert elastic_config is not None

    if not client.indices.exists_alias(name=index):
        print(f"Did not find index {index} in db, creating index...\n")
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            #  Get settings and mappings from the mappings.json file
            mappings = elastic_config.get("mappings")
            settings = elastic_config.get("settings")
            index_name = f"{index}-1"
            try:
                await client.indices.create(index=index_name, mappings=mappings, settings=settings)
                await client.indices.put_alias(index=index_name, name=INDEX_ALIAS)
                # Verify that the new index has been created
                assert await client.indices.exists(index=index_name)
                index_and_alias = await client.indices.get_alias(index=index_name)
                print(index_and_alias)
            except Exception as e:
                print(f"Warning: Did not create index {index_name} due to exception {e}\n")
    else:
        print(f"Found index {index} in db, skipping index creation...\n")


async def update_documents_to_index(
    client: AsyncElasticsearch, index: str, data: list[Wine]
) -> None:
    await helpers.async_bulk(
        client,
        data,
        index=index,
        chunk_size=CHUNKSIZE,
    )
    ids = [item["id"] for item in data]
    print(f"Processed ids in range {min(ids)}-{max(ids)}")


async def main(data: list[JsonBlob], index: str) -> None:
    settings = get_settings()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        elastic_client = await get_elastic_client(settings)
        await create_index(elastic_client, index, Path("mapping/mapping.json"))

        # Process multiple chunks of data in a process pool to avoid blocking the event loop
        print("Processing chunks")
        chunked_data = chunk_iterable(data, CHUNKSIZE)

        with ProcessPoolExecutor() as pool:
            loop = asyncio.get_running_loop()
            executor_tasks = [partial(process_chunks, chunk) for chunk in chunked_data]
            awaitables = [loop.run_in_executor(pool, call) for call in executor_tasks]
            # Attach process pool to running event loop so that we can process multiple chunks in parallel
            validated_data = await asyncio.gather(*awaitables)
            tasks = [
                update_documents_to_index(elastic_client, index, data) for data in validated_data
            ]
        try:
            await asyncio.gather(*tasks)
            print("Finished execution!")
        except Exception as e:
            print(f"{e}: Error while indexing to db")
        # Close AsyncElasticsearch client
        await elastic_client.close()


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

    # Specify an alias to index the data under
    INDEX_ALIAS = get_settings().elastic_index_alias
    assert INDEX_ALIAS

    data = list(get_json_data(DATA_DIR, FILENAME))
    if LIMIT > 0:
        data = data[:LIMIT]

    # Run main async event loop
    if data:
        asyncio.run(main(data, INDEX_ALIAS))
