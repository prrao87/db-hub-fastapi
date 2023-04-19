import json
import zipfile
from typing import Any, Iterator

JsonBlob = dict[str, Any]


def read_data(filename: str) -> list[JsonBlob]:
    with open(filename) as f:
        data = json.load(f)
        for idx, item in enumerate(data, 1):
            item["id"] = idx
    return data


def chunk_iterable(item_list: list[JsonBlob], chunksize: int) -> Iterator[tuple[JsonBlob, ...]]:
    """
    Break a large iterable into an iterable of smaller iterables of size `chunksize`
    """
    for i in range(0, len(item_list), chunksize):
        yield tuple(item_list[i : i + chunksize])


def write_chunked_data(item_list: list[JsonBlob], output_name: str, chunksize: int = 5000) -> None:
    """
    Write data to a zip file in chunks so that we don't dump all data into a single huge JSON file
    """
    zipfilename = f"{output_name}-jsonl.zip"
    with zipfile.ZipFile(
        zipfilename,
        "w",
        compression=zipfile.ZIP_DEFLATED,
        compresslevel=5,
    ) as zipf:
        chunked_data = chunk_iterable(item_list, chunksize)
        for num, chunk in enumerate(chunked_data, 1):
            filename = f"{output_name}-{num}.jsonl"
            chunk_json = "\n".join(json.dumps(item) for item in chunk)
            # Write the JSONL data into the specified filename *inside* the ZIP file
            zipf.writestr(filename, data=chunk_json)


if __name__ == "__main__":
    # Download the JSON data file from https://www.kaggle.com/datasets/zynicide/wine-reviews
    data = read_data("winemag-data-130k-v2.json")
    write_chunked_data(data, "winemag-data-130k-v2")
