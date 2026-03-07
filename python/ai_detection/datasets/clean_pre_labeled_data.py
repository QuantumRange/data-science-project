from pathlib import Path
from pqdm.processes import pqdm

import fasttext
import polars as pl

def process(file: Path):
    model = fasttext.load_model("lid.176.bin")

    def detect_lang(text: str, threshold: float = 0.6) -> bool:
        try:
            text = text.replace("\n", " ").strip()
            # skip boilerplate at edges
            n = len(text)
            chunk = text[n // 4: 3 * n // 4]
            if len(chunk) < 200:
                chunk = text
            labels, scores = model.predict(text, k=2)
            if labels[0] != "__label__en":
                return False
            if len(scores) > 1 and scores[1] > 0.15:
                return False  # significant second language detected
            return bool(scores[0] >= threshold)
        except:
            return False

    try:
        (
            pl
            .scan_parquet(file)
            .filter(pl.col("text").str.len_bytes() > 200)
            .with_columns(
                pl.col("text")
                .map_elements(detect_lang, return_dtype=pl.Boolean)
                .alias("is_english")
            )
            .filter(pl.col("is_english") == True)
            .select("id", "url", "text")
            .sink_parquet(
                Path("/home/qr/data/for_scanning/") / file.name,
                compression="zstd",
                compression_level=12
            )
        )
    except:
        print(f"Skipping {file.name}")


if __name__ == '__main__':
    files = sorted(list(Path("/mnt/data-dump/for_enriching/").glob("*.parquet")))
    pqdm(files, process, n_jobs=32)
