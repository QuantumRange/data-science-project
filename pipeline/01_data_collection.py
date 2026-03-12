import gzip
import os
from pathlib import Path
from typing import List, Tuple

import polars as pl
import requests
from tqdm import tqdm

CC_BASE_URL = "https://data.commoncrawl.org"
ARCHIVE_VERSION = "CC-MAIN-2026-08"
SEGMENTS_COUNT = 500  # 1 segment = 0.9 GiB

OUTPUT_DIR = Path("/mnt/Fast2T/data/stage_1")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# https://gist.github.com/yanqd0/c13ed29e29432e3cf3e7c38467f42f51
def download(url: str, file_path: Path, chunk_size=1024 * 1024, position=0) -> None:
    if file_path.is_file(): return

    temp_file = file_path.with_suffix(".tmp")

    resp = requests.get(url, stream=True)
    total = int(resp.headers.get('content-length', 0))
    with open(temp_file, 'wb') as file, tqdm(
            desc=file_path.name,
            total=total,
            unit='iB',
            position=position,
            unit_scale=True,
            unit_divisor=1024,
            leave=False,
    ) as bar:
        for data in resp.iter_content(chunk_size=chunk_size):
            size = file.write(data)
            bar.update(size)

    os.rename(temp_file, file_path)


def download_tmp(url: str, name: str, chunk_size=1024) -> Path:
    file_path = Path("/tmp") / name

    download(url, file_path, chunk_size)

    return file_path


if __name__ == '__main__':
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

    print("? Downloading segments")
    for url in tqdm(segments, position=0):
        name = url.split("/")[-1]

        if (Path("/mnt/Fast2T/data/new/processed/") / f"{name}.parquet").exists(): continue

        download(f"{CC_BASE_URL}/{url}", OUTPUT_DIR / f"{name}.warc.gz", position=1)
    print(f"! Downloaded {len(segments)} segments")
