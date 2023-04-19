import argparse
import glob
import json
from functools import lru_cache
import os
import sys
import zipfile
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from qdrant_client import QdrantClient
from qdrant_client.http import models
from pydantic.main import ModelMetaclass
from tqdm import tqdm

import torch
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
    "Read jsonl file and add a new field `to_vectorize` to use for sentence embeddings"
    with open(filename, "r") as f:
        data = [json.loads(line) for line in f.readlines()]
    return data


def validate(
    data: list[JsonBlob],
    model: ModelMetaclass,
    exclude_none: bool = False,
) -> list[JsonBlob]:
    validated_data = [model(**item).dict(exclude_none=exclude_none) for item in data]
    return validated_data


def create_payload_index_on_text_field(
    client: QdrantClient,
    collection_name: str,
    field_name: str,
) -> None:
    field_schema = models.TextIndexParams(
        type="text",
        tokenizer=models.TokenizerType.WORD,
        min_token_len=3,
        max_token_len=15,
        lowercase=True,
    )
    client.create_payload_index(
        collection_name=collection_name,
        field_name=field_name,
        field_schema=field_schema,
)


def main(files: list[str]) -> None:
    settings = get_settings()
    COLLECTION = "wines"
    client = QdrantClient(host=settings.qdrant_host, port=settings.qdrant_port, timeout=None)
    # Create or recreate collection
    client.recreate_collection(
        collection_name=COLLECTION,
        vectors_config=models.VectorParams(size=384, distance=models.Distance.COSINE),
    )
    # Create payload with text field whose sentence embeddings will be added to the index
    create_payload_index_on_text_field(client, "wines", "to_vectorize")

    # # Initialize sentence embedding pipeline
    model_checkpoint = os.environ.get("EMBEDDING_MODEL_CHECKPOINT", "multi-qa-MiniLM-L6-cos-v1")
    # pipeline = SentenceEmbeddingPipeline(model_checkpoint)
    device = torch.device('cuda:0' if torch.cuda.is_available() else 'cpu')
    model = SentenceTransformer(model_checkpoint, device=device)

    for file in tqdm(files):
        data = read_jsonl_from_file(file)
        data = validate(data, Wine, exclude_none=True)[:50]
        ids = [item.get("id") for item in data]
        to_vectorize = [item.pop("to_vectorize") for item in data]
        sentence_embeddings = [model.encode(item).tolist() for item in to_vectorize]
        # Upsert payload
        client.upsert(
            collection_name=COLLECTION,
            points=models.Batch(
                ids=ids,
                payloads=data,
                vectors=sentence_embeddings,
            )
        )
        print(f"Indexed {Path(file).name} to db")
    print(f"Finished indexing {len(files)} JSONL files to db")


if __name__ == "__main__":
    parser = argparse.ArgumentParser("Bulk index database from the wine reviews JSONL data")
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

    main(files)


"""
import aiohttp

## TODO: Try to make async PUT requests via aiohttp to upsert data
url = 'http://localhost/collections/wines/points'
data = {"points": [{
            "id": str(uuid.uuid1()),
            "payload": dict[str, Any],
            "vector": vector: list[float],
        }]}
async def main():
    async with aiohttp.ClientSession() as client:
        async with client.put(url, json=data) as resp:
            result = await resp.json()
            print(result)

asyncio.run(main())
"""
