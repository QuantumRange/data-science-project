import os
from pathlib import Path

import requests
from tqdm import tqdm

STORAGE_DIR = Path("/mnt/data-dump/new")
CACHE_DIR = STORAGE_DIR / "cache"
FAST_DIR = Path("/mnt/Fast2T/data/new")

for directory in [STORAGE_DIR, CACHE_DIR, FAST_DIR]:
    directory.mkdir(parents=True, exist_ok=True)


# https://gist.github.com/yanqd0/c13ed29e29432e3cf3e7c38467f42f51
def download(url: str, file_path: Path, chunk_size=1024 * 1024, position=0) -> None:
    if file_path.is_file():
        return

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
            leave=False
    ) as bar:
        for data in resp.iter_content(chunk_size=chunk_size):
            size = file.write(data)
            bar.update(size)

    os.rename(temp_file, file_path)


def download_tmp(url: str, name: str, chunk_size=1024) -> Path:
    file_path = CACHE_DIR / name

    download(url, file_path, chunk_size)

    return file_path
