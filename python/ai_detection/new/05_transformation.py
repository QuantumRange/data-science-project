import os
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from collections import Counter
from pathlib import Path
from typing import Dict, List, Tuple

import nltk
from Stemmer import Stemmer
from tqdm import tqdm
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize

import polars as pl
import Stemmer

nltk.download('punkt')
nltk.download('punkt_tab')
nltk.download('stopwords')

SOURCE_DIR = Path("/mnt/Fast2T/data/new/clean_data/")
TARGET_DIR = Path("/mnt/Fast2T/data/new/stage_5/")

stopwords = set(stopwords.words('english'))

ai_type = pl.Enum(["AI", "UNKNOWN", "HUMAN"])
blacklist = [
    "Please wait while your request is being verified...",
    "AI scrapers break the web, to use this page you'll need JavaScript enabled.",
    "Javascript is required for this site. Consider enabling Javascript or upgrading to a modern browser.",
    "This domain name registration has expired and renewal or deletion are pending. If you are the registrant and "
    "want to renew the domain name, please contact your registration service provider.",
    "We're sorry but doesn't work properly without JavaScript enabled. Please enable it to continue."
]


def stem(stemmer: Stemmer, text: str) -> List[Dict[str, int]]:
    text = text.lower()
    tokens = word_tokenize(text)
    filtered_tokens = filter(lambda w: w not in stopwords, tokens)
    stems = stemmer.stemWords(list(filtered_tokens))
    return [{ "stem": k, "occurrence": v } for k, v in sorted(Counter(stems).items(), key=lambda x: x[1], reverse=True)]


def process(file: Path) -> None:
    stemmer: Stemmer = Stemmer.Stemmer('english')

    target = TARGET_DIR / file.name
    target_tmp = target.with_suffix(".tmp")

    (
        pl
        .read_parquet(file)
        .filter(pl.col("lang_en") > 0.3)
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
        .with_columns(
            pl
            .col("text")
            .map_elements(
                lambda t: stem(stemmer, t),
                return_dtype=pl.List(pl.Struct({ "stem": pl.String, "occurrence": pl.Int64 })),
            )
            .alias("stems"),
        )
        .select("host", "url", "text", "stems", "outflow", "is_ai")
        .write_parquet(target_tmp, compression="zstd", compression_level=12)
    )

    os.rename(target_tmp, target)


if __name__ == '__main__':
    files = list(sorted(filter(lambda f: not (TARGET_DIR / f.name).exists(), SOURCE_DIR.glob("*.parquet"))))
    with ProcessPoolExecutor(max_workers=28) as executor:
        futures = [executor.submit(process, file) for file in files]

        for future in tqdm(as_completed(futures), total=len(futures)):
            future.result()
