from pathlib import Path
import json
import os
import polars as pl
import sys
import progressbar as pb

if len(sys.argv) > 1:
    datadir = Path(sys.argv[1])
else:
    print("No data directory given. Please provide the data directory as the first command line argument", file=sys.stderr)

# Create working directory.
workdir = datadir / "questions/word-distribution/"
workdir.mkdir(parents=True, exist_ok=True)

# Create global word count dictionaries.
words_ai      = dict()
words_unknown = dict()
words_human   = dict()

def add_words(d, w, n = 1):
    if w in d:
        d[w] += n
    else:
        d[w] = n

# Count word occurrences through all data files.
for datafile in pb.progressbar(os.listdir(datadir / "stage_5")):
    try:
        df = pl.read_parquet(datadir / "stage_5" / datafile)
    except:
        print(f"WARNING: {datafile} could not be read as a parquet file.", file=sys.stderr)
        continue

    for stems in df.filter(pl.col('is_ai').eq('AI'))['stems']:
        for stem in stems:
            add_words(words_ai, stem['stem'], stem['occurrence'])
            add_words(words_unknown, stem['stem'], 0)
            add_words(words_human, stem['stem'], 0)
            
    for stems in df.filter(pl.col('is_ai').eq('UNKNOWN'))['stems']:
        for stem in stems:
            add_words(words_ai, stem['stem'], 0)
            add_words(words_unknown, stem['stem'], stem['occurrence'])
            add_words(words_human, stem['stem'], 0)
    for stems in df.filter(pl.col('is_ai').eq('HUMAN'))['stems']:
        for stem in stems:
            add_words(words_ai, stem['stem'], 0)
            add_words(words_unknown, stem['stem'], 0)
            add_words(words_human, stem['stem'], stem['occurrence'])

# Write data to json files.
with open(workdir / "ai-word-distribution.json", "w") as f:
    json.dump(words_ai, f)
with open(workdir / "unknown-word-distribution.json", "w") as f:
    json.dump(words_unknown, f)
with open(workdir/ "human-word-distribution.json", "w") as f:
    json.dump(words_human, f)
