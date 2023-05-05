import argparse
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterator

import srsly
from dotenv import load_dotenv
from pydantic.main import ModelMetaclass
import lancedb
from tqdm import tqdm


sys.path.insert(1, os.path.realpath(Path(__file__).resolve().parents[1]))
from api.config import Settings
from schemas.wine import Wine
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
    model: ModelMetaclass,
    exclude_none: bool = False,
) -> list[JsonBlob]:
    validated_data = [model(**item).dict(exclude_none=exclude_none) for item in data]
    return validated_data


def vectorize_text(data_chunk: list[JsonBlob]) -> list[JsonBlob]:
    data = validate(data_chunk, Wine, exclude_none=True)

    # Load a sentence transformer model for semantic similarity from a specified checkpoint
    model_id = get_settings().embedding_model_checkpoint
    MODEL = SentenceTransformer(model_id)

    ids = [item["id"] for item in data]
    to_vectorize = [text.pop("to_vectorize") for text in data]
    vectors = [MODEL.encode(text.lower(), batch_size=64).tolist() for text in to_vectorize]
    try:
        # Generate sentence embeddings on batch
        data_chunk = [{"vector": vector, **d} for d, vector in zip(data, vectors)]
    except Exception as e:
        print(f"{e}: Failed to index ID range {min(ids)}-{max(ids)}")
        return None
    return data_chunk


def get_concat_vectors_and_data(validated_data: list[JsonBlob]) -> list[JsonBlob]:
    vector_start_time = time.time()
    print("Vectorizing data on multiple cores:")
    with ProcessPoolExecutor(max_workers=WORKERS) as executor:
        chunked_data = chunk_iterable(validated_data, CHUNKSIZE)
        final_data = []
        for chunk in tqdm(chunked_data, total=len(validated_data) // CHUNKSIZE):
            futures = [executor.submit(vectorize_text, chunk)]
            final_data.extend(f.result() for f in as_completed(futures) if f.result())
    print(
        f"Finished vectorizing {len(data)} texts in {time.time() - vector_start_time:.4f} seconds"
    )
    return final_data


def main(data: list[JsonBlob]) -> None:
    settings = get_settings()
    TABLE = "wines"
    # Because LanceDB is serverless, we can simply specify a disk path
    db_name = "lancedb"
    db = lancedb.connect(f"{db_name}")
    validated_data = validate(data, Wine, exclude_none=True)

    try:
        tbl = db.open_table(TABLE).head(1)
    except:
        # Create a LanceDB table if it doesn't already exist
        print(f"No table named {TABLE} found, creating new table with data and vectors")
        final_data = get_concat_vectors_and_data(validated_data)

        tbl = db.create_table(TABLE, data=final_data[0], mode="overwrite")
        print(f"Created table `{tbl.name}`, with length {len(tbl)}")

    # Work with an existing table
    tbl = db.open_table(TABLE)
    print(f"Opened table `{tbl.name}`, with length {len(tbl)}")

    # Creating index (choose num partitions as a power of 2 that's closest to len(dataset) // 5000)
    # In this case, we have 130k datapoints, so the nearest power of 2 is 130000//5000 ~ 32)
    print("Creating index")
    tbl.create_index(metric="cosine", num_partitions=32, num_sub_vectors=96)


if __name__ == "__main__":
    # fmt: off
    parser = argparse.ArgumentParser("Bulk index database from the wine reviews JSONL data")
    parser.add_argument("--limit", type=int, default=0, help="Limit the size of the dataset to load for testing purposes")
    parser.add_argument("--chunksize", type=int, default=512, help="Size of each chunk to break the dataset into before processing")
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

    if data:
        data = data[:LIMIT] if LIMIT > 0 else data
        main(data)
        print("Finished execution!")
