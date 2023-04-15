import argparse
import asyncio
import glob
import json
import os
import sys
import warnings
import zipfile
from pathlib import Path
from typing import Any

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


def get_json_files(file_prefix: str, file_path: Path) -> list[str]:
    """Get all line-delimited json files (.jsonl) from a directory with a given prefix"""
    files = sorted(glob.glob(f"{file_path}/{file_prefix}*.jsonl"))
    if not files:
        raise FileNotFoundError(
            f"No .jsonl files with prefix `{file_prefix}` found in `{file_path}`"
        )
    return files


def clean_directory(dirname: Path) -> None:
    """Clean up existing files to avoid conflicts"""
    if Path(dirname).exists():
        for f in Path(dirname).glob("*"):
            if f.is_file():
                f.unlink()


def extract_json_from_zip(data_path: Path, file_path: Path) -> None:
    """
    Extract .jsonl files from zip file and save them in `file_path`
    """
    clean_directory(file_path)
    zipfiles = sorted(glob.glob(f"{str(data_path)}/*.zip"))
    for file in zipfiles:
        with zipfile.ZipFile(file, "r") as zipf:
            for fn in zipf.infolist():
                fn.filename = Path(fn.filename).name
                zipf.extract(fn, file_path)


def read_jsonl_from_file(filename: str) -> list[JsonBlob]:
    with open(filename, "r") as f:
        data = [json.loads(line.strip()) for line in f.readlines()]
    return data


def validate(
    data: list[JsonBlob],
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


async def create_index(mappings_path: Path, client: AsyncElasticsearch) -> None:
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
            await client.indices.create(
                index=index_name, mappings=mappings, settings=settings
            )
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


async def main(files: list[str]) -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        elastic_client = await get_elastic_client()
        INDEX_ALIAS = os.environ.get("ELASTIC_INDEX_ALIAS")
        if not elastic_client.indices.exists_alias(name=INDEX_ALIAS):
            print(f"Did not find index {INDEX_ALIAS} in db, creating index...\n")
            await create_index(Path("mapping/mapping.json"), elastic_client)
        for file in files:
            data = read_jsonl_from_file(file)
            validated_data = validate(data, Wine)
            try:
                await bulk_index_wines_to_elastic(
                    elastic_client, INDEX_ALIAS, validated_data
                )
                print(f"Indexed {Path(file).name} to db")
            except Exception as e:
                print(f"{e}: Failed to index {Path(file).name} to db")
        # Close AsyncElasticsearch client
        await elastic_client.close()
        print("Finished execution")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        "Bulk index database from the wine reviews JSONL data"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Limit the size of the dataset to load for testing purposes",
    )
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Refresh zip file data by clearing existing directory & extracting them again",
    )
    parser.add_argument(
        "--filename",
        type=str,
        default="winemag-data-130k-v2-jsonl.zip",
        help="Name of the JSONL zip file to use",
    )
    args = vars(parser.parse_args())

    LIMIT = args["limit"]
    DATA_DIR = Path(__file__).parents[3] / "data"
    ZIPFILE = DATA_DIR / args["filename"]
    # Get file path for unzipped files
    filename = Path(args["filename"]).stem
    FILE_PATH = DATA_DIR / filename

    # Extract JSONL files from zip files if `--refresh` flag is set
    if args["refresh"]:
        # Extract all json files from zip files from their parent directory and save them in `parent_dir/data`.
        extract_json_from_zip(DATA_DIR, FILE_PATH)

    files = get_json_files("winemag-data", FILE_PATH)
    assert files, f"No files found in {FILE_PATH}"

    if LIMIT > 0:
        files = files[:LIMIT]

    # Run main async event loop
    if files:
        asyncio.run(main(files))
