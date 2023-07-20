import argparse
import json
import os
import sys
from concurrent.futures import ProcessPoolExecutor
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterator

import srsly
import weaviate
from dotenv import load_dotenv
from optimum.onnxruntime import ORTModelForCustomTasks
from optimum.pipelines import pipeline
from tqdm import tqdm
from transformers import AutoTokenizer
from weaviate.client import Client

sys.path.insert(1, os.path.realpath(Path(__file__).resolve().parents[1]))
from api.config import Settings
from schemas.wine import Wine

load_dotenv()
# Custom types
JsonBlob = dict[str, Any]


class FileNotFoundError(Exception):
    pass


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
    exclude_none: bool = False,
) -> list[JsonBlob]:
    validated_data = [Wine(**item).model_dump(exclude_none=exclude_none) for item in data]
    return validated_data


def get_embedding_pipeline(onnx_path, model_filename: str) -> pipeline:
    """
    Create a sentence embedding pipeline using the optimized ONNX model
    """
    # Reload tokenizer
    tokenizer = AutoTokenizer.from_pretrained(onnx_path)
    optimized_model = ORTModelForCustomTasks.from_pretrained(onnx_path, file_name=model_filename)
    embedding_pipeline = pipeline("feature-extraction", model=optimized_model, tokenizer=tokenizer)
    return embedding_pipeline


def create_or_update_schema(client: Client) -> None:
    # Create a schema with no vectorizer (we will be adding our own vectors)
    with open("settings/schema.json", "r") as f:
        schema = json.load(f)
    class_names = [class_["class"] for class_ in schema["classes"]]
    assert class_names, "No classes found in schema, please check schema definition and try again"
    if not client.schema.get()["classes"]:
        print(f"Creating schema with classes: {', '.join(class_names)}")
        client.schema.create(schema)
    else:
        print(f"Existing schema found, deleting it & creating it again...")
        client.schema.delete_all()
        client.schema.create(schema)


def add_vectors_to_index(data_chunk: tuple[JsonBlob, ...]) -> None:
    settings = get_settings()
    CLASS_NAME = "Wine"
    HOST = settings.weaviate_host
    PORT = settings.weaviate_port
    client = weaviate.Client(f"http://{HOST}:{PORT}")
    data = validate(data_chunk, exclude_none=True)

    # Preload optimized, quantized ONNX sentence transformers model
    # NOTE: This requires that the script ../onnx_model/onnx_optimizer.py has been run beforehand
    pipeline = get_embedding_pipeline(ONNX_PATH, model_filename="model_optimized_quantized.onnx")

    ids = [item.pop("id") for item in data]
    # Rename "id" (Weaviate reserves the "id" key for its own uuid assignment, so we can't use it)
    data = [{"wineID": id, **fields} for id, fields in zip(ids, data)]
    to_vectorize = [text.pop("to_vectorize") for text in data]
    sentence_embeddings = [pipeline(text.lower(), truncate=True)[0][0] for text in to_vectorize]
    print(f"Finished vectorizing data in the ID range {min(ids)}-{max(ids)}")
    try:
        # Use a context manager to manage batch flushing
        with client.batch as batch:
            batch.batch_size = 64
            batch.dynamic = True
            for i, item in enumerate(data):
                batch.add_data_object(
                    item,
                    CLASS_NAME,
                    vector=sentence_embeddings[i],
                )
        print(f"Indexed ID range {min(ids)}-{max(ids)} to db")
    except Exception as e:
        print(f"{e}: Failed to index items in the ID range {min(ids)}-{max(ids)} to db")


def main(chunked_data: Iterator[tuple[JsonBlob, ...]]) -> None:
    settings = get_settings()
    CLASS_NAME = "Wine"
    HOST = settings.weaviate_host
    PORT = settings.weaviate_port
    client = weaviate.Client(f"http://{HOST}:{PORT}")
    # Add schema
    create_or_update_schema(client)

    print("Processing chunks")
    with ProcessPoolExecutor(max_workers=WORKERS) as executor:
        chunked_data = chunk_iterable(data, CHUNKSIZE)
        for _ in executor.map(add_vectors_to_index, chunked_data):
            pass


if __name__ == "__main__":
    # fmt: off
    parser = argparse.ArgumentParser("Bulk index database from the wine reviews JSONL data")
    parser.add_argument("--limit", type=int, default=0, help="Limit the size of the dataset to load for testing purposes")
    parser.add_argument("--chunksize", type=int, default=512, help="Size of each chunk to break the dataset into before processing")
    parser.add_argument("--filename", type=str, default="winemag-data-130k-v2.jsonl.gz", help="Name of the JSONL zip file to use")
    parser.add_argument("--workers", type=int, default=3, help="Number of workers to use for vectorization")
    args = vars(parser.parse_args())
    # fmt: on

    LIMIT = args["limit"]
    DATA_DIR = Path(__file__).parents[3] / "data"
    FILENAME = args["filename"]
    CHUNKSIZE = args["chunksize"]
    WORKERS = args["workers"]
    ONNX_PATH = Path(__file__).parents[1] / "onnx_model" / "onnx"

    data = list(get_json_data(DATA_DIR, FILENAME))

    if data:
        data = data[:LIMIT] if LIMIT > 0 else data
        main(data)
        print("Finished execution!")
