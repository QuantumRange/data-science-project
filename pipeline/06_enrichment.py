import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import polars as pl
from tqdm import tqdm

SOURCE_DIR = Path("/mnt/Fast2T/data/stage_5/")
TARGET_DIR = Path("/mnt/Fast2T/data/stage_6/")
TARGET_DIR.mkdir(parents=True, exist_ok=True)
THRESHOLD = 1.1

with open("../data/newsapi-sites.txt", "r") as f:
    d = [l.strip() for l in f.readlines()]
    news_domains = [f"/{l}" for l in d] + [f".{l}" for l in d]

# weights = {
#     'ai': 1.0, 'artificial': 0.2, 'intelligence': 0.1, 'llm': 1.0, 'language': 0.1, 'model': 0.3, 'gpt': 1.0,
#     'chatgpt': 1.0, 'openai': 0.9, 'ollama': 1.0, 'gemini': 0.8, 'copilot': 1.0, 'perplexity': 0.3,
#     'claude': 1.0, 'integrate': 0.3, 'powerful': 0.1, 'token': 0.2, 'productivity': 0.1, 'deploy': 0.1,
#     'performance': 0.1, 'boost': 0.1, 'agent': 0.75
# }
weights = {
    'ai': 1.0, 'artificial': 0.2, 'intelligence': 0.1, 'llm': 1.0, 'language': 0.1, 'model': 0.3, 'gpt': 1.0,
    'chatgpt': 1.0, 'openai': 0.9, 'ollama': 1.0, 'geminus': 0.8, 'copilot': 1.0, 'perplexity': 0.3,
    'claude': 1.0, 'integrate': 0.3, 'powerful': 0.1, 'token': 0.2, 'productivity': 0.1, 'deploy': 0.1,
    'performance': 0.1, 'boost': 0.1, 'agent': 0.75
}

weight_df = pl.DataFrame([{ 'stem': key, 'weight': weight } for key, weight in weights.items()])


def process(file: Path) -> None:
    target = TARGET_DIR / file.name
    target_tmp = target.with_suffix(".tmp")

    df = pl.read_parquet(file)

    (
        df
        .with_columns(
            pl
            .when(pl.col("url").str.contains("/blog/|blog\\.")).then(pl.lit("BLOG"))
            .when(pl.col("url").str.contains("/wiki|\\.wiki|wiki\\.")).then(pl.lit("WIKI"))
            .when(pl.col("url").str.contains("/news/|/articles/|articles\\.|news\\.")).then(pl.lit("NEWS"))
            .when(pl.col("url").str.contains_any(news_domains)).then(pl.lit("NEWSAPI"))
            .when(pl.col("url").str.contains("/shop/|shop\\.")).then(pl.lit("SHOP"))
            .otherwise(pl.lit("UNKNOWN"))
            .cast(pl.Enum(["UNKNOWN", "BLOG", "WIKI", "NEWS", "NEWSAPI", "SHOP"])).alias("topic"),
        )
        .join(
            df
            .explode("stems")
            .unnest("stems")
            .join(weight_df, left_on="stem", right_on="stem", how="inner")
            .rename({ "weight": "ai_topic_score" })
            .group_by("url")
            .agg(pl.col("ai_topic_score").sum()),
            on="url",
            how="left",
        )
        .fill_null(0)
        .with_columns(
            ai_topic=pl.when(pl.col("ai_topic_score") > THRESHOLD).then(pl.lit("AI"))
            .otherwise(pl.lit("NOT_AI"))
            .cast(pl.Enum(["AI", "NOT_AI"])),
        )
        .write_parquet(target_tmp, compression="zstd")
    )

    os.rename(target_tmp, target)


if __name__ == '__main__':
    files = list(sorted(filter(lambda file: not (TARGET_DIR / file.name).exists(), SOURCE_DIR.glob("*.parquet"))))

    with ProcessPoolExecutor(max_workers=32) as executor:
        futures = [executor.submit(process, file) for file in files]

        for future in tqdm(as_completed(futures), total=len(futures)):
            future.result()
