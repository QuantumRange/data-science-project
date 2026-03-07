from concurrent.futures import ProcessPoolExecutor
import mwparserfromhell
import re
import polars as pl
from concurrent.futures import ProcessPoolExecutor

def clean_wikitext(raw: str) -> str:
    if raw.strip().upper().startswith("#REDIRECT"):
        return ""

    parsed = mwparserfromhell.parse(raw)
    for tag in parsed.filter_tags():
        if tag.tag in ("ref", "gallery"):
            try:
                parsed.remove(tag)
            except ValueError:
                pass
    for tpl in parsed.filter_templates():
        try:
            parsed.remove(tpl)
        except ValueError:
            pass
    for comment in parsed.filter_comments():
        try:
            parsed.remove(comment)
        except ValueError:
            pass

    text = parsed.strip_code()
    text = re.sub(r"\[\[(Category|File|Image):.*?\]\]", "", text)
    text = re.sub(r"\{\|.*?\|\}", "", text, flags=re.DOTALL)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()

def clean_batch(texts: list[str]) -> list[str]:
    return [clean_wikitext(t) for t in texts]


def main():
    df = pl.read_parquet(
        "/mnt/Fast2T/data/ai-training-set/human/wiki_parquet_output/wiki_00000.parquet",
        columns=["text"],
    )

    texts = df["text"].to_list()
    n_workers = 128
    chunk_size = len(texts) // n_workers + 1
    chunks = [texts[i : i + chunk_size] for i in range(0, len(texts), chunk_size)]

    with ProcessPoolExecutor(max_workers=n_workers) as pool:
        results = list(pool.map(clean_batch, chunks))

    clean = [t for batch in results for t in batch]

    df = df.with_columns(pl.Series("clean_text", clean)).filter(
        pl.col("clean_text").str.len_chars() > 200
    )
    df.write_parquet("/mnt/Fast2T/data/heh/wiki.parquet")
    print(df)

if __name__ == "__main__":
    main()