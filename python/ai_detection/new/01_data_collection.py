import gzip
from pathlib import Path
from typing import List, Tuple

import polars as pl
from tqdm import tqdm

from lib import download, download_tmp, STORAGE_DIR
from lib import FAST_DIR

# Parameters
CC_BASE_URL = "https://data.commoncrawl.org"
ARCHIVE_VERSION = "CC-MAIN-2026-08"
# 1 segment = 0.9 GiB
SEGMENTS_COUNT = 500
SEGMENT_DIR = STORAGE_DIR / "segments"

if __name__ == '__main__':
    # Read segments
    print("? Loading segments")
    paths_file: Path = download_tmp(
        f"https://data.commoncrawl.org/crawl-data/{ARCHIVE_VERSION}/warc.paths.gz",
        "warc.paths.gz",
    )

    segments: List[str] = []

    with gzip.open(paths_file, "r") as reader:
        for line in reader:
            segments.append(line.decode().strip())

    print(f"! Loaded {len(segments)} segments")

    segments = pl.DataFrame({ "segments": segments }).sample(SEGMENTS_COUNT)["segments"].to_list()

    # Download segments
    print("? Downloading segments")
    SEGMENT_DIR.mkdir(parents=True, exist_ok=True)
    for url in tqdm(segments, position=0):
        name = url.split("/")[-1]

        if (FAST_DIR / "processed" / f"{name}.parquet").exists():
            continue

        download(f"{CC_BASE_URL}/{url}", SEGMENT_DIR / f"{name}.warc.gz", position=1)
    print(f"! Downloaded {len(segments)} segments")
