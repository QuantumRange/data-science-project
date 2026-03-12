import os
import re
import nltk
import polars as pl

from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed
from functools import lru_cache
from pathlib import Path
from typing import Dict, List
from nltk.corpus import english_wordnet, stopwords, wordnet
from simplemma import text_lemmatizer
from tqdm import tqdm

SOURCE_DIR = Path("/mnt/Fast2T/data/stage_4/")
TARGET_DIR = Path("/mnt/Fast2T/data/stage_5/")

stopwords = frozenset(stopwords.words('english'))
whitelist = frozenset(english_wordnet.words())

ai_type = pl.Enum(["AI", "UNKNOWN", "HUMAN"])

blacklist = [
    "Please wait while your request is being verified...",
    "AI scrapers break the web, to use this page you'll need JavaScript enabled.",
    "Javascript is required for this site. Consider enabling Javascript or upgrading to a modern browser.",
    "This domain name registration has expired and renewal or deletion are pending. If you are the registrant and "
    "want to renew the domain name, please contact your registration service provider.",
    "We're sorry but doesn't work properly without JavaScript enabled. Please enable it to continue."
]


@lru_cache(maxsize=256_000)
def has_synsets(word: str) -> bool:
    if word in whitelist:
        return True
    return bool(wordnet.synsets(word))


def lemmatize(text: str) -> List[Dict[str, int]]:
    ai_count = sum(
        Counter(
            re.findall(r"(?im)[.,\-—!()\s]ai(?=[.,\-—!()\s])|^ai(?=[.,\-—!()\s])|[.,\-—!()\s]ai$|^ai$", text),
        ).values(),
    )
    tokens = text_lemmatizer(text.lower(), lang="en")

    arr = [{ "stem": "ai", "occurrences": ai_count }] if ai_count > 0 else []

    return [
        { "stem": k, "occurrences": v }
        for k, v in Counter(tokens).items()
    ] + arr


def process(file: Path) -> None:
    target = TARGET_DIR / file.name
    target_tmp = target.with_suffix(".tmp")

    (
        pl
        .scan_parquet(file)
        .filter(pl.col("lang_en") > 0.3)
        .filter(pl.col("text").str.len_bytes() > 100)
        .filter(~pl.col("text").str.contains_any(blacklist))
        .with_columns(
            pl
            .when(pl.col('ai') < 0.005).then(pl.lit("HUMAN"))
            .when(pl.col('ai') > 0.9).then(pl.lit("AI"))
            .otherwise(pl.lit("UNKNOWN"))
            .cast(ai_type).alias("is_ai"),
        )
        .with_columns(
            pl
            .col("outflow")
            .map_elements(
                lambda s: [{ "url": k, "occurrences": v } for k, v in s.items() if v is not None],
                return_dtype=pl.List(pl.Struct({ "url": pl.String, "occurrences": pl.Int64 })),
            ),
        )
        .collect()
        .with_columns(
            pl
            .col("text")
            .map_elements(
                lemmatize,
                return_dtype=pl.List(pl.Struct({ "stem": pl.String, "occurrences": pl.Int64 })),
            )
            .alias("stems"),
        )
        .select("host", "url", "text", "stems", "outflow", "ai", "is_ai")
        .write_parquet(target_tmp, compression="zstd")
    )

    os.rename(target_tmp, target)


if __name__ == '__main__':
    nltk.download('punkt', quiet=True)
    nltk.download('punkt_tab', quiet=True)
    nltk.download('stopwords', quiet=True)
    nltk.download('english_wordnet', quiet=True)
    nltk.download('averaged_perceptron_tagger_eng', quiet=True)

    files = list(sorted(filter(lambda f: not (TARGET_DIR / f.name).exists(), SOURCE_DIR.glob("*.parquet"))))

    with ProcessPoolExecutor(max_workers=32) as executor:
        futures = [executor.submit(process, file) for file in files]

        for future in tqdm(as_completed(futures), total=len(futures)):
            future.result()
