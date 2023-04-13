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
        "CREATE FULLTEXT INDEX titlesAndDescriptions IF NOT EXISTS FOR (w:Wine) ON EACH [w.title, w.description] ", 
    ]
    for query in queries:
        await session.run(query)


async def wine_nodes(tx: AsyncManagedTransaction, data: list[JsonBlob]) -> None:
    query = """
        UNWIND $data AS d
        MERGE (wine:Wine {wineID: d.id})
            SET wine += {
                points: toInteger(d.points),
                title: d.title,
                description: d.description,
                price: toFloat(d.price),
                variety: d.variety,
                winery: d.winery
            }
        """
    await tx.run(query, data=data)


async def wine_country_rels(tx: AsyncManagedTransaction, data: list[JsonBlob]) -> None:
    query = """
        UNWIND $data AS d
        MATCH (wine:Wine {wineID: d.id})
        UNWIND d.location as loc
        WITH wine, loc
            WHERE loc.country IS NOT NULL
            MERGE (country:Country:Location {countryName: loc.country})
            MERGE (wine)-[:IS_FROM_COUNTRY]->(country)
    """
    await tx.run(query, data=data)


async def wine_province_rels(tx: AsyncManagedTransaction, data: list[JsonBlob]) -> None:
    query = """
        UNWIND $data AS d
        MATCH (wine:Wine {wineID: d.id})
        UNWIND d.location as loc
        WITH wine, loc
            WHERE loc.province IS NOT NULL
            MERGE (province:Province:Location {provinceName: loc.province})
            MERGE (wine)-[:IS_FROM_PROVINCE]->(province)
    """
    await tx.run(query, data=data)


async def country_province_rels(
    tx: AsyncManagedTransaction, data: list[JsonBlob]
) -> None:
    query = """
        UNWIND $data AS d
        UNWIND d.location as loc
        WITH loc
            WHERE loc.province IS NOT NULL AND loc.country IS NOT NULL
            MATCH (country:Country {countryName: loc.country})
            MATCH (province:Province {provinceName: loc.province})
            MERGE (province)-[:IS_LOCATED_IN]->(country)
    """
    await tx.run(query, data=data)


async def wine_taster_rels(tx: AsyncManagedTransaction, data: list[JsonBlob]) -> None:
    query = """
        UNWIND $data AS d
        MATCH (wine:Wine {wineID: d.id})
        UNWIND d.taster as t
        WITH wine, t
            WHERE t.taster_name IS NOT NULL
            MERGE (taster:Person {tasterName: t.taster_name})
                SET taster += {tasterTwitterHandle: t.taster_twitter_handle}
            MERGE (wine)-[:TASTED_BY]->(taster)
    """
    await tx.run(query, data=data)


async def main(files: list[str]) -> None:
    async with AsyncGraphDatabase.driver(
        URI, auth=(NEO4J_USER, NEO4J_PASSWORD)
    ) as driver:
        async with driver.session(database="neo4j") as session:
            # Create indexes and constraints
            await create_indexes_and_constraints(session)
            # Build subgraph of wines and location edges using async transactions
            for file in files:
                data = read_jsonl_from_file(file)
                validated_data = validate(data, Wine)
                await session.execute_write(wine_nodes, validated_data)
                await session.execute_write(wine_country_rels, validated_data)
                await session.execute_write(wine_province_rels, validated_data)
                await session.execute_write(country_province_rels, validated_data)
                await session.execute_write(wine_taster_rels, validated_data)
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
    asyncio.run(main(files))
