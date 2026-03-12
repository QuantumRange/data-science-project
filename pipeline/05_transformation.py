from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Tuple
from nltk import pos_tag_sents
from nltk.corpus import english_wordnet, stopwords, wordnet
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.tokenize import sent_tokenize, word_tokenize
from tqdm import tqdm

import os
import nltk
import polars as pl

SOURCE_DIR = Path("/mnt/Fast2T/data/stage_4/")
TARGET_DIR = Path("/mnt/Fast2T/data/stage_5/")

stopwords = set(stopwords.words('english'))
whitelist = set(english_wordnet.words())

ai_type = pl.Enum(["AI", "UNKNOWN", "HUMAN"])

blacklist = [
    "Please wait while your request is being verified...",
    "AI scrapers break the web, to use this page you'll need JavaScript enabled.",
    "Javascript is required for this site. Consider enabling Javascript or upgrading to a modern browser.",
    "This domain name registration has expired and renewal or deletion are pending. If you are the registrant and "
    "want to renew the domain name, please contact your registration service provider.",
    "We're sorry but doesn't work properly without JavaScript enabled. Please enable it to continue."
]


def get_wordnet_pos(tag):
    if tag.startswith('J'):
        return wordnet.ADJ
    elif tag.startswith('V'):
        return wordnet.VERB
    elif tag.startswith('N'):
        return wordnet.NOUN
    elif tag.startswith('R'):
        return wordnet.ADV
    else:
        return wordnet.NOUN


def stem(lemma: WordNetLemmatizer, text: str) -> List[Dict[str, int]]:
    positions: List[List[Tuple[str, str]]] = pos_tag_sents(
        [
            word_tokenize(sent)
            for sent in sent_tokenize(text)
        ],
    )

    tokens: List[str] = [
        lemma.lemmatize(word, get_wordnet_pos(pos)).lower()
        for sent in positions
        for word, pos in sent
        if wordnet.synsets(word)
    ]

    return [
        { "stem": k, "occurrence": v }
        for k, v in Counter(tokens).items()
    ]


def process(file: Path) -> None:
    lemma = WordNetLemmatizer()

    target = TARGET_DIR / file.name
    target_tmp = target.with_suffix(".tmp")

    (
        pl
        .read_parquet(file)
        .filter(pl.col("lang_en") > 0.6)
        .filter(pl.col("text").str.len_bytes() > 200)
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
                lambda t: stem(lemma, t),
                return_dtype=pl.List(pl.Struct({ "stem": pl.String, "occurrence": pl.Int64 })),
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
