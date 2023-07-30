from __future__ import annotations

import argparse
import os
import sys
from functools import lru_cache
from pathlib import Path
from typing import Any

import srsly
from codetiming import Timer
from dotenv import load_dotenv
from meilisearch import Client
from meilisearch.index import Index
from schemas.wine import Wine
from tqdm import tqdm

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


def get_json_data(file_path: Path) -> list[JsonBlob]:
    """Get all line-delimited json files (.jsonl) from a directory with a given prefix"""
    if not file_path.is_file():
        # File may not have been uncompressed yet so try to do that first
        data = srsly.read_gzip_jsonl(file_path)
        # This time if it isn't there it really doesn't exist
        if not file_path.is_file():
            raise FileNotFoundError(
                f"`{file_path}` doesn't contain a valid `.jsonl.gz` file - check and try again."
            )
    else:
        data = srsly.read_gzip_jsonl(file_path)
    return data


def validate(
    data: list[JsonBlob],
    exclude_none: bool = True,
) -> list[JsonBlob]:
    validated_data = [Wine(**item).model_dump(exclude_none=exclude_none) for item in data]
    return validated_data


def get_meili_settings(filename: str) -> dict[str, Any]:
    settings = dict(srsly.read_json(filename))
    return settings


def update_documents(filepath: Path, index: Index, primary_key: str, batch_size: int):
    data = list(get_json_data(filepath))
    if LIMIT > 0:
        data = data[:LIMIT]
    validated_data = validate(data)
    index.update_documents_in_batches(
        validated_data,
        batch_size=batch_size,
        primary_key=primary_key,
    )


def main(data_files: list[Path]) -> None:
    meili_settings = get_meili_settings(filename="settings/settings.json")
    config = Settings()
    URI = f"http://{config.meili_url}:{config.meili_port}"
    MASTER_KEY = config.meili_master_key
    index_name = "wines"
    primary_key = "id"

    client = Client(URI, MASTER_KEY)
    with Timer(name="Bulk Index", text="Bulk index took {:.4f} seconds"):
        # Create index
        index = client.index(index_name)
        # Update settings
        client.index(index_name).update_settings(meili_settings)
        print("Finished updating database index settings")
        try:
            # In a real case we'd be iterating through a list of files
            # For this example, it's just looping through the same file N times
            for filepath in tqdm(data_files):
                # Update index
                update_documents(filepath, index, primary_key=primary_key, batch_size=BATCHSIZE)
        except Exception as e:
            print(f"{e}: Error while indexing to db")


if __name__ == "__main__":
    # fmt: off
    parser = argparse.ArgumentParser("Bulk index database from the wine reviews JSONL data")
    parser.add_argument("--limit", type=int, default=0, help="Limit the size of the dataset to load for testing purposes")
    parser.add_argument("--batchsize", "-b", type=int, default=10_000, help="Size of each chunk to break the dataset into before processing")
    parser.add_argument("--filename", type=str, default="winemag-data-130k-v2.jsonl.gz", help="Name of the JSONL zip file to use")
    parser.add_argument("--benchmark_num", "-n", type=int, default=1, help="Run a benchmark of the script N times")
    args = vars(parser.parse_args())
    # fmt: on

    LIMIT = args["limit"]
    DATA_DIR = Path(__file__).parents[3] / "data"
    FILENAME = args["filename"]
    BATCHSIZE = args["batchsize"]
    BENCHMARK_NUM = args["benchmark_num"]

    # Get a list of all files in the data directory
    data_files = [f for f in DATA_DIR.glob("*.jsonl.gz") if f.is_file()]
    # For benchmarking, we want to run on the same data multiple times (in the real world this would be many different files)
    benchmark_data_files = data_files * BENCHMARK_NUM

    meili_settings = get_meili_settings(filename="settings/settings.json")

    # Run main function
    main(benchmark_data_files)
