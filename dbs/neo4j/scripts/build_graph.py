import argparse
import asyncio
import glob
import json
import os
import sys
import zipfile
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from neo4j import AsyncGraphDatabase, AsyncManagedTransaction, AsyncSession
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


async def create_indexes_and_constraints(session: AsyncSession) -> None:
    queries = [
        # constraints
        "CREATE CONSTRAINT countryName IF NOT EXISTS FOR (c:Country) REQUIRE c.countryName IS UNIQUE ",
        "CREATE CONSTRAINT wineID IF NOT EXISTS FOR (w:Wine) REQUIRE w.wineID IS UNIQUE ",
        # indexes
        "CREATE INDEX provinceName IF NOT EXISTS FOR (p:Province) ON (p.provinceName) ",
        "CREATE INDEX tasterName IF NOT EXISTS FOR (p:Person) ON (p.tasterName) ",
        "CREATE FULLTEXT INDEX searchText IF NOT EXISTS FOR (w:Wine) ON EACH [w.title, w.description, w.variety] ",
    ]
    for query in queries:
        await session.run(query)


async def build_query(tx: AsyncManagedTransaction, data: list[JsonBlob]) -> None:
    query = """
        UNWIND $data AS record
        MERGE (wine:Wine {wineID: record.id})
            SET wine += {
                points: record.points,
                title: record.title,
                description: record.description,
                price: record.price,
                variety: record.variety,
                winery: record.winery,
                vineyard: record.vineyard,
                region_1: record.region_1,
                region_2: record.region_2
            }
        WITH record, wine
            WHERE record.taster_name IS NOT NULL
            MERGE (taster:Person {tasterName: record.taster_name})
                SET taster += {tasterTwitterHandle: record.taster_twitter_handle}
            MERGE (wine)-[:TASTED_BY]->(taster)
        WITH record, wine
            MERGE (country:Country {countryName: record.country})
            MERGE (wine)-[:IS_FROM_COUNTRY]->(country)
        WITH record, wine, country
        WHERE record.province IS NOT NULL
            MERGE (province:Province {provinceName: record.province})
            MERGE (wine)-[:IS_FROM_PROVINCE]->(province)
        WITH record, wine, country, province
            WHERE record.province IS NOT NULL AND record.country IS NOT NULL
            MERGE (province)-[:IS_LOCATED_IN]->(country)
        """
    await tx.run(query, data=data)


async def main(files: list[str]) -> None:
    async with AsyncGraphDatabase.driver(URI, auth=(NEO4J_USER, NEO4J_PASSWORD)) as driver:
        async with driver.session(database="neo4j") as session:
            # Create indexes and constraints
            await create_indexes_and_constraints(session)
            # Build subgraph of wines and location edges using async transactions
            for file in files:
                data = read_jsonl_from_file(file)
                validated_data = validate(data, Wine)
                await session.execute_write(build_query, validated_data)
                print(f"Ingested {Path(file).name} to db")


if __name__ == "__main__":
    parser = argparse.ArgumentParser("Build a graph from the wine reviews JSONL data")
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

    # # Neo4j
    URI = "bolt://localhost:7687"
    NEO4J_USER = "neo4j"
    NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD")

    # Extract JSONL files from zip files if `--refresh` flag is set
    if args["refresh"]:
        # Extract all json files from zip files from their parent directory and save them in `parent_dir/data`.
        extract_json_from_zip(DATA_DIR, FILE_PATH)

    files = get_json_files("winemag-data", FILE_PATH)
    assert files, f"No files found in {FILE_PATH}"

    if LIMIT > 0:
        files = files[:LIMIT]
    # Run async graph loader
    import uvloop

    uvloop.install()
    asyncio.run(main(files))
