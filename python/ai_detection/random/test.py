from pathlib import Path

from gptzero import GPT2PPL
import polars as pl

VALIDATION_DIR = Path("/mnt/Fast2T/data/ai-training-set/playground/validation_set/")


def generate(original: str, raw_pattern: str, output: str):
    DATA_SET = (pl
                .read_parquet(original)
                .select("id", "text")).join(
        (pl
         .read_parquet(raw_pattern)
         .select("id", "text")),
        left_on="id", right_on="id"
    )

    DATA_SET = (DATA_SET
                .rename({"text": "normal", "text_right": "stem"})
                .filter(pl.col("stem").str.len_chars() > 1000)
                .sample(1000))

    DATA_SET.write_parquet(output, compression="zstd")


def load_or_generate(original: str, raw_pattern: str, output: str) -> pl.DataFrame:
    if not Path(output).exists():
        generate(original, raw_pattern, output)
    return pl.read_parquet(output).sample(30)


# ai_df = load_or_generate(
#     "/mnt/Fast2T/data/ai/lmsys-chat-1m-synth/output/gpt-oss-lmsys-chat-1m-synth-ja+en_chunk0021.parquet",
#     "/mnt/Fast2T/data/ai-training-set/raw-ai/*.parquet",
#     "/mnt/Fast2T/data/ai-training-set/playground/validation_set/ai.parquet"
# )
#
# wiki_df = load_or_generate(
#     "/mnt/Fast2T/data/ai-training-set/human/wiki_parquet_output/wiki_00000.parquet",
#     "/mnt/Fast2T/data/ai-training-set/raw-human/*.parquet",
#     "/mnt/Fast2T/data/ai-training-set/playground/validation_set/wiki.parquet"
# )
#
# model = GPT2PPL(device="cuda:0")
#
# def analyse(text: str) -> float:
#     try:
#         result = model(text)[0]["Perplexity per line"]
#         return float(result)
#     except Exception:
#         return -1.0
#
#
# def process(frame: pl.DataFrame) -> pl.DataFrame:
#     return (frame
#             .with_columns(pl.col("normal").map_elements(analyse, return_dtype=pl.Float64).alias("normal"))
#             .filter(pl.col("normal") >= 0)
#             .with_columns(pl.col("stem").map_elements(analyse, return_dtype=pl.Float64).alias("stem"))
#             .filter(pl.col("stem") >= 0)
#             )
#
# process(wiki_df).write_parquet(VALIDATION_DIR / "analysed_wiki.parquet")
# process(ai_df).write_parquet(VALIDATION_DIR / "analysed_ai.parquet")

wiki_df = pl.read_parquet(VALIDATION_DIR / "analysed_wiki.parquet")
ai_df = pl.read_parquet(VALIDATION_DIR / "analysed_ai.parquet")

print(ai_df.select("normal", "stem").describe())
print(wiki_df.select("normal", "stem").describe())
