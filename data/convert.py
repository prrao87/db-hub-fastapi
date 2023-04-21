"""
Run `pip install srsly` to use this script

This script converts the JSON data file from https://www.kaggle.com/datasets/zynicide/wine-reviews
to a .gzip line-delimited (.jsonl) file for use downstream with the databases in question.

Full credit to the original author, @zynicide, on Kaggle, for the data.
"""
from pathlib import Path
from typing import Any

import srsly

JsonBlob = dict[str, Any]


def convert_to_jsonl(filename: str) -> None:
    data = srsly.read_json(filename)
    # Add an `id` field to the start of each dict item so we have a primary key for indexing
    new_data = [{"id": idx, **item} for idx, item in enumerate(data, 1)]
    srsly.write_gzip_jsonl(f"{Path(filename).stem}.jsonl.gz", new_data)


if __name__ == "__main__":
    # Download the JSON data file from https://www.kaggle.com/datasets/zynicide/wine-reviews'
    convert_to_jsonl("winemag-data-130k-v2.json")
