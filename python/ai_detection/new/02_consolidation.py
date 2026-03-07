import os
import trafilatura
import polars as pl

from collections import Counter
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin, urlsplit
from fast_langdetect import detect
from fastwarc import ArchiveIterator
from fastwarc.stream_io import GZipStream
from fastwarc.warc import is_http
from selectolax.lexbor import LexborHTMLParser
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from tqdm import tqdm

from lib import FAST_DIR, STORAGE_DIR

PROCESSED_WARC_DIR = FAST_DIR / "processed"
SEGMENT_DIR = STORAGE_DIR / "segments"

_detector = None


def _init_worker():
    global _detector
    detect("warmup", model="full", k=1)


def parse_html(url: str, body: str) -> Optional[str]:
    return trafilatura.extract(
        body,
        url=url,
        output_format="txt",
        fast=True,
        include_comments=False,
        target_language="en",
        include_tables=False,
        include_images=False,
        include_formatting=False,
        include_links=False,
        deduplicate=True,
        with_metadata=False,
    )


def parse_file(file: Path):
    parquet_file = PROCESSED_WARC_DIR / f"{file.name.split(".")[0]}.parquet"

    rows = []
    try:
        with open(file, "rb") as f:
            for record in ArchiveIterator(
                    GZipStream(f),
                    func_filter=is_http,
            ):
                # Quality Filters
                if record.http_content_type != "text/html":
                    continue
                if "WARC-Target-URI" not in record.headers:
                    raise Exception("Should not happen")
                if record.content_length < 1024:
                    continue

                url: str = record.headers.get("WARC-Target-URI", "")
                host: str | None = urlsplit(url).hostname

                body = record.reader.read().decode("utf-8", errors="replace")

                # Parse text
                text = parse_html(url, body)

                if text is None:
                    continue

                lang_result = detect(text, model="full", k=2)
                langs = { r["lang"] for r in lang_result }
                if "en" not in langs:
                    continue

                # Parse links
                tree = LexborHTMLParser(body)
                outflow = Counter()

                for node in tree.css("a"):
                    href = node.attrs.sget("href")

                    if not href:
                        continue

                    try:
                        href: str = urljoin(url, href)
                        host_name: str | None = urlsplit(href).hostname

                        if host_name:
                            outflow[host_name.lower()] += 1
                    except Exception:
                        pass

                rows.append(
                    {
                        "host": host,
                        "url": url,
                        "lang": lang_result,
                        "outflow": outflow,
                        "text": text,
                    },
                )
    except Exception as e:
        pass

    if len(rows) != 0:
        (
            pl
            .DataFrame(rows)
            .write_parquet(
                parquet_file,
                compression="zstd",
                compression_level=6,
            )
        )
    else:
        parquet_file.touch()

    os.remove(file)


if __name__ == '__main__':
    print("? Loading warc files")
    files = list(sorted(SEGMENT_DIR.glob("*.warc.gz")))

    print(f"! Found {len(files)} warc files")

    print("? Processing warc files")
    PROCESSED_WARC_DIR.mkdir(parents=True, exist_ok=True)
    with ProcessPoolExecutor(max_workers=32, initializer=_init_worker) as pool:
        futures = { pool.submit(parse_file, f): f for f in files }
        for future in tqdm(as_completed(futures), total=len(files)):
            future.result()
    print(f"! Processed warc files!")
