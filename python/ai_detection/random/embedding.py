import glob
from pathlib import Path
from typing import List, Any

from tqdm import tqdm

import polars as pl

def chunk_array(array: List[Any], size: int) -> List[List[Any]]:
    chunks = []

    for i in range(0, len(array), size):
        chunks.append(array[i:i + size])

    return chunks


# 0 = maybe ai
# 1 = no ai
# 2 = ai

def import_webdata():
    TARGET_RAW = Path("/mnt/Fast2T/data/ai-training-set/raw/")
    WEBSITE_FILES = sorted(glob.glob("/mnt/Fast2T/data/link/*.parquet"))
    i = 0

    for f in tqdm(chunk_array(WEBSITE_FILES, 4)):
        (
            pl.scan_parquet(f)
            .filter(pl.col("type") == 2)
            .filter(pl.col("lang") == "ENGLISH")
            .select(pl.col("id"), pl.col("text").str.split(' ').alias("term"))
            .with_columns
                (
                pl
                .col("term")
                .list.eval(
                    pl.element().filter(~pl.element().str.contains(r'[\d\."]'))
                )
                .alias("term")
            )
            .explode("term")
            .group_by("id", "term")
            .len()
            .with_columns(classification=0)
            .sink_parquet(TARGET_RAW / (str(i) + ".parquet"), compression="zstd", maintain_order=False)
        )
        i = i + 1


# import_webdata()

def preprocess_ai(source_folder: str, target_folder: Path):
    FILES = sorted(glob.glob(source_folder))

    i = 0

    for f in tqdm(FILES):
        (
            pl.scan_parquet(f)
            .filter(pl.col("language") == "ENGLISH")
            .unique('text')
            .select(pl.col("id"), pl.col("text").str.split(' ').alias("term"))
            .with_columns
                (
                pl
                .col("term")
                .list.eval(
                    pl.element().filter(~pl.element().str.contains(r'[\d\."]'))
                )
                .alias("term")
            )
            .explode("term")
            .group_by("id", "term")
            .len()
            .with_columns(classification=2)
            .sink_parquet(target_folder / (str(i) + ".parquet"), compression="zstd", maintain_order=False)
        )
        i = i + 1


# preprocess_ai(
#     "/mnt/Fast2T/data/ai-training-set/raw-ai/*.parquet",
#     Path("/mnt/Fast2T/data/ai-training-set/exploded-ai")
# )
# preprocess_ai(
#     "/mnt/Fast2T/data/ai-training-set/raw-human/*.parquet",
#     Path("/mnt/Fast2T/data/ai-training-set/exploded-wiki")
# )

# class
# 0 = wiki
# 1 = ai response
# 2 = website

def find_top_words():
    AI_FILES = sorted(glob.glob("/mnt/Fast2T/data/ai-training-set/exploded-ai/*.parquet"))
    WIKI_FILES = sorted(glob.glob("/mnt/Fast2T/data/ai-training-set/exploded-wiki/*.parquet"))
    WEBSITE_FILES = sorted(glob.glob("/mnt/Fast2T/data/ai-training-set/exploded-website/*.parquet"))

    (
        pl.scan_parquet(WIKI_FILES)
        .select(pl.col("term"), pl.col("len"))
        .group_by(pl.col("term"))
        .agg(pl.col("len").sum())
        .with_columns(type=0)
        .sink_parquet("/mnt/Fast2T/data/ai-training-set/total-words/wiki.parquet", compression="zstd",
                      maintain_order=False)
    )

    (
        pl.scan_parquet(AI_FILES)
        .select(pl.col("term"), pl.col("len"))
        .group_by(pl.col("term"))
        .agg(pl.col("len").sum())
        .with_columns(type=1)
        .sink_parquet("/mnt/Fast2T/data/ai-training-set/total-words/ai.parquet", compression="zstd",
                      maintain_order=False)
    )

    i = 0
    for files in tqdm(chunk_array(WEBSITE_FILES, 64)):
        (
            pl.scan_parquet(files)
            .select(pl.col("term"), pl.col("len"))
            .group_by(pl.col("term"))
            .agg(pl.col("len").sum())
            .sink_parquet("/mnt/Fast2T/data/ai-training-set/total-words/tmp.website-" + str(i) + ".parquet",
                          compression="zstd",
                          maintain_order=False)
        )
        i += 1

    (
        pl.scan_parquet(sorted(glob.glob("/mnt/Fast2T/data/ai-training-set/total-words/tmp.website-*.parquet")))
        .select(pl.col("term"), pl.col("len"))
        .group_by(pl.col("term"))
        .agg(pl.col("len").sum())
        .with_columns(type=2)
        .sink_parquet("/mnt/Fast2T/data/ai-training-set/total-words/website.parquet",
                      compression="zstd",
                      maintain_order=False)
    )

find_top_words()

def word_analysis():
    categories = ["ai", "website", "wiki"]

    TOP_K = 1_000_000

    df = (pl.read_parquet("/mnt/Fast2T/data/ai-training-set/total-words/" + categories[0] + ".parquet")
          .filter(pl.col("term").str.contains(r'\d|\.|"|\[|\]|\||=|#|\n|\'\'|\)|\(|–').not_())
          .top_k(k=TOP_K, by='len')
          .sort('len', descending=True))

    for cat in categories[1:]:
        new_df = (pl.read_parquet("/mnt/Fast2T/data/ai-training-set/total-words/" + cat + ".parquet")
                  .filter(pl.col("term").str.contains(r'\d|\.|"|\[|\]|\||=|#|\n|\'\'|\)|\(|–').not_())
                  .top_k(k=TOP_K, by='len')
                  .sort('len', descending=True))

        df = pl.concat(
            [df, new_df],
            how="vertical",
            rechunk=True
        )

    df.with_columns(
        pl.col("len").sum().over("type").cast(pl.Int64).alias("total")
    ).write_parquet("/mnt/Fast2T/data/ai-training-set/total-words/all.parquet")


# word_analysis()

# df = pl.read_parquet("/mnt/Fast2T/data/ai-training-set/total-words/all.parquet")
#
# print(
#     df
#     .group_by("term").agg(pl.col("len").sum())
#     .top_k(k=20_000, by='len')
# )
#
# type_amount = df.group_by("type").len().count()["type"][0]
#
# f = df.group_by("term").agg(pl.col("len").sum())
# tf = (df
#       .join(f, left_on="term", right_on="term")
#       .with_columns(tf=pl.col("len") / pl.col("len_right"))
#       .drop("len_right", "total"))
# idf = (df.group_by("term")
#        .agg(pl.col("type"))
#        .with_columns(idf=(type_amount / pl.col("type").list.len()).log())
#        .drop("type"))
#
# tf_idf = (tf
#           .join(idf, left_on="term", right_on="term")
#           .with_columns(tf_idf=pl.col("tf") * pl.col("idf"))
#           .drop("tf", "idf")
#           .sort("tf_idf", descending=True)
#           .filter(pl.col("tf_idf") != 0))
#
# print(tf_idf.filter(pl.col("type") == 0))
# print(tf_idf.filter(pl.col("type") == 1))
# print(tf_idf.filter(pl.col("type") == 2))
