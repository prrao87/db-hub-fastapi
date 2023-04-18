from __future__ import annotations

import argparse
import asyncio
import glob
import json
from functools import lru_cache
import os
import sys
import zipfile
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from meilisearch_python_async import Client
from meilisearch_python_async.index import Index
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


def get_json_files(file_prefix: str, file_path: Path, data_dir: Path) -> list[str]:
    """Get all line-delimited json files (.jsonl) from a directory with a given prefix"""
    files = sorted(glob.glob(f"{file_path}/{file_prefix}*.jsonl"))

    # Files may not have been unzipped yet so try to do that first
    if not files:
        extract_json_from_zip(data_dir, file_path)

        # No try to get the files again after unzipping
        files = sorted(glob.glob(f"{file_path}/{file_prefix}*.jsonl"))

        # This time if they aren't there they really don't exist
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


async def _update_searchable_attributes(
    client: Client,
    index_name: str,
) -> None:
    """Set searchable attributes as a subset of fields (only those that we need) to speed up indexing"""
    fields = [
        "title",
        "description",
        "country",
        "province",
        "variety",
        "region_1",
        "region_2",
        "taster_name",
    ]
    index = client.index(index_name)
    await index.update_searchable_attributes(fields)


async def _update_filterable_attributes(
    client: Client,
    index_name: str,
) -> None:
    """Set faceted filters so that we can more effectively narrow down search results"""
    fields = [
        "price",
        "points",
        "country",
        "province",
        "variety",
    ]
    index = client.index(index_name)
    await index.update_filterable_attributes(fields)


async def _update_sortable_attributes(
    client: Client,
    index_name: str,
) -> None:
    """Set sortable order in results to obtain more relevant search results"""
    fields = [
        "points",
        "price",
    ]
    index = client.index(index_name)
    await index.update_sortable_attributes(fields)


async def do_indexing(index: Index, data: list[JsonBlob], file_name: str) -> None:
    await index.update_documents(data, "id")
    print(f"Indexed {Path(file_name).name} to db")


async def main(files: list[str]) -> None:
    settings = Settings()
    URI = f"http://{settings.meili_url}:{settings.meili_port}"
    MASTER_KEY = settings.meili_master_key
    async with Client(URI, MASTER_KEY) as client:
        await asyncio.gather(
            _update_searchable_attributes(client, "wines"),
            _update_filterable_attributes(client, "wines"),
            _update_sortable_attributes(client, "wines"),
        )
        index = client.index("wines")
        tasks = []
        print("Processing files")
        for file in files:
            data = read_jsonl_from_file(file)
            data = validate(data, Wine, exclude_none=True)
            tasks.append(do_indexing(index, data, file))
        try:
            # Set id as primary key prior to indexing
            await asyncio.gather(*tasks)
        except Exception as e:
            print(f"{e}: Error while indexing to db")
    print(f"Finished indexing {len(files)} JSONL files to db")


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

    files = get_json_files("winemag-data", FILE_PATH, DATA_DIR)

    assert files, f"No files found in {FILE_PATH}"

    if LIMIT > 0:
        files = files[:LIMIT]

    # Run main async event loop
    asyncio.run(main(files))
