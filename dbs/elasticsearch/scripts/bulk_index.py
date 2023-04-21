import argparse
import asyncio
import json
import os
import sys
import warnings
from pathlib import Path
from typing import Any, Iterator

import srsly
from dotenv import load_dotenv
from elasticsearch import AsyncElasticsearch, helpers
from pydantic.main import ModelMetaclass

sys.path.insert(1, os.path.realpath(Path(__file__).resolve().parents[1]))
from schemas.wine import Wine

load_dotenv()
# Custom types
JsonBlob = dict[str, Any]


class FileNotFoundError(Exception):
    pass


# --- Blocking functions ---


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
    model: ModelMetaclass,
    exclude_none: bool = False,
) -> list[JsonBlob]:
    validated_data = [model(**item).dict(exclude_none=exclude_none) for item in data]
    return validated_data


# --- Async functions ---


async def get_elastic_client() -> AsyncElasticsearch:
    # Get environment variables
    USERNAME = os.environ.get("ELASTIC_USER")
    PASSWORD = os.environ.get("ELASTIC_PASSWORD")
    PORT = os.environ.get("ELASTIC_PORT")
    ELASTIC_URL = os.environ.get("ELASTIC_URL")
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


async def create_index(client: AsyncElasticsearch, mappings_path: Path) -> None:
    """Create an index associated with an alias in ElasticSearch"""
    with open(mappings_path, "rb") as f:
        config = json.load(f)

    INDEX_ALIAS = os.environ.get("ELASTIC_INDEX_ALIAS")
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        #  Get settings and mappings from the mappings.json file
        mappings = config.get("mappings")
        settings = config.get("settings")
        index_name = f"{INDEX_ALIAS}-1"
        try:
            await client.indices.create(index=index_name, mappings=mappings, settings=settings)
            await client.indices.put_alias(index=index_name, name=INDEX_ALIAS)
            # Verify that the new index has been created
            assert await client.indices.exists(index=index_name)
            index_and_alias = await client.indices.get_alias(index=index_name)
            print(index_and_alias)
        except Exception as e:
            print(f"Warning: Did not create index {index_name} due to exception {e}\n")


async def bulk_index_wines_to_elastic(
    client: AsyncElasticsearch, index: str, wines: list[Wine]
) -> None:
    """Bulk index a wine JsonBlob to ElasticSearch"""
    async for success, response in helpers.async_streaming_bulk(
        client,
        wines,
        index=index,
        chunk_size=5000,
        max_retries=3,
        initial_backoff=3,
        max_backoff=10,
    ):
        if not success:
            print(f"A document failed to index: {response}")


async def main(chunked_data: Iterator[tuple[JsonBlob, ...]]) -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        elastic_client = await get_elastic_client()
        INDEX_ALIAS = os.environ.get("ELASTIC_INDEX_ALIAS")
        if not elastic_client.indices.exists_alias(name=INDEX_ALIAS):
            print(f"Did not find index {INDEX_ALIAS} in db, creating index...\n")
            await create_index(elastic_client, Path("mapping/mapping.json"))
        counter = 0
        for chunk in chunked_data:
            validated_data = validate(chunk, Wine)
            counter += len(validated_data)
            ids = [item["id"] for item in validated_data]
            try:
                await bulk_index_wines_to_elastic(elastic_client, INDEX_ALIAS, validated_data)
                print(f"Indexed {counter} items in the ID range {min(ids)}-{max(ids)} to db")
            except Exception as e:
                print(f"{e}: Failed to index items in the ID range {min(ids)}-{max(ids)} to db")
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

    data = list(get_json_data(DATA_DIR, FILENAME))
    if LIMIT > 0:
        data = data[:LIMIT]

    chunked_data = chunk_iterable(data, CHUNKSIZE)

    # Run main async event loop
    asyncio.run(main(chunked_data))
    print("Finished execution")
