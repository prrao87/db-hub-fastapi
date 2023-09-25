import argparse
import os
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterator

import lancedb
import pandas as pd
import srsly
from codetiming import Timer
from dotenv import load_dotenv
from lancedb.pydantic import pydantic_to_schema
from tqdm import tqdm

sys.path.insert(1, os.path.realpath(Path(__file__).resolve().parents[1]))
from api.config import Settings
from schemas.wine import LanceModelWine, Wine
from sentence_transformers import SentenceTransformer

load_dotenv()
# Custom types
JsonBlob = dict[str, Any]


class FileNotFoundError(Exception):
    pass


@lru_cache()
def get_settings():
    # Use lru_cache to avoid loading .env file for every request
    return Settings()


def chunk_iterable(item_list: list[JsonBlob], chunksize: int) -> Iterator[list[JsonBlob]]:
    """
    Break a large iterable into an iterable of smaller iterables of size `chunksize`
    """
    for i in range(0, len(item_list), chunksize):
        yield item_list[i : i + chunksize]


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
    exclude_none: bool = False,
) -> list[JsonBlob]:
    validated_data = [Wine(**item).model_dump(exclude_none=exclude_none) for item in data]
    return validated_data


def embed_func(batch: list[str], model) -> list[list[float]]:
    return [model.encode(sentence.lower()) for sentence in batch]


def vectorize_text(data: list[JsonBlob]) -> list[LanceModelWine] | None:
    # Load a sentence transformer model for semantic similarity from a specified checkpoint
    model_id = get_settings().embedding_model_checkpoint
    assert model_id, "Invalid embedding model checkpoint specified in .env file"
    MODEL = SentenceTransformer(model_id)

    ids = [item["id"] for item in data]
    to_vectorize = [text.get("to_vectorize") for text in data]
    vectors = embed_func(to_vectorize, MODEL)
    try:
        data_batch = [{**d, "vector": vector} for d, vector in zip(data, vectors)]
    except Exception as e:
        print(f"{e}: Failed to add ID range {min(ids)}-{max(ids)}")
        return None
    return data_batch


def embed_batches(tbl: str, validated_data: list[JsonBlob]) -> pd.DataFrame:
    with ProcessPoolExecutor(max_workers=WORKERS) as executor:
        chunked_data = chunk_iterable(validated_data, CHUNKSIZE)
        embed_data = []
        for chunk in tqdm(chunked_data, total=len(validated_data) // CHUNKSIZE):
            futures = [executor.submit(vectorize_text, chunk)]
            embed_data = [f.result() for f in as_completed(futures) if f.result()][0]
            df = pd.DataFrame.from_dict(embed_data)
            tbl.add(df, mode="overwrite")


def main(data: list[JsonBlob]) -> None:
    DB_NAME = "../lancedb"
    TABLE = "wines"
    db = lancedb.connect(DB_NAME)

    tbl = db.create_table(TABLE, schema=pydantic_to_schema(LanceModelWine), mode="overwrite")
    print(f"Created table `{TABLE}`, with length {len(tbl)}")

    with Timer(name="Bulk Index", text="Validated data using Pydantic in {:.4f} sec"):
        validated_data = validate(data, exclude_none=False)

    with Timer(name="Embed batches", text="Created sentence embeddings in {:.4f} sec"):
        embed_batches(tbl, validated_data)

    print(f"Finished inserting {len(tbl)} items into LanceDB table")

    with Timer(name="Create index", text="Created IVF-PQ index in {:.4f} sec"):
        # Creating index (choose num partitions as a power of 2 that's closest to len(dataset) // 5000)
        # In this case, we have 130k datapoints, so the nearest power of 2 is 130000//5000 ~ 32)
        tbl.create_index(metric="cosine", num_partitions=32, num_sub_vectors=96)


if __name__ == "__main__":
    # fmt: off
    parser = argparse.ArgumentParser("Bulk index database from the wine reviews JSONL data")
    parser.add_argument("--limit", type=int, default=0, help="Limit the size of the dataset to load for testing purposes")
    parser.add_argument("--chunksize", type=int, default=1000, help="Size of each chunk to break the dataset into before processing")
    parser.add_argument("--filename", type=str, default="winemag-data-130k-v2.jsonl.gz", help="Name of the JSONL zip file to use")
    parser.add_argument("--workers", type=int, default=4, help="Number of workers to use for vectorization")
    args = vars(parser.parse_args())
    # fmt: on

    LIMIT = args["limit"]
    DATA_DIR = Path(__file__).parents[3] / "data"
    FILENAME = args["filename"]
    CHUNKSIZE = args["chunksize"]
    WORKERS = args["workers"]

    data = list(get_json_data(DATA_DIR, FILENAME))
    assert data, "No data found in the specified file"
    data = data[:LIMIT] if LIMIT > 0 else data
    main(data)
    print("Finished execution!")
