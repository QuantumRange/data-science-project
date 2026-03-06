# @author Claude
from pathlib import Path

import numpy as np
import polars as pl
from tqdm import tqdm

DATASET_DIR = Path("/mnt/Fast2T/data/ai-training-set/dataset/complete/")
OUTPUT_DIR = Path("/home/qr/data/")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

NUM_SHARDS = 10
TRAIN_RATIO = 0.6
TEST_CHUNK = 1_000_000


class TestBuffer:
    """Accumulates test samples and flushes to disk in fixed-size chunks."""

    def __init__(self, chunk_size: int, output_dir: Path):
        self.chunk_size = chunk_size
        self.output_dir = output_dir
        self.emb_buf: list[np.ndarray] = []
        self.lbl_buf: list[np.ndarray] = []
        self.buf_len = 0
        self.shard_idx = 0

    def add(self, emb: np.ndarray, lbl: np.ndarray):
        self.emb_buf.append(emb)
        self.lbl_buf.append(lbl)
        self.buf_len += len(lbl)

        while self.buf_len >= self.chunk_size:
            self._flush_one()

    def finalize(self):
        """Flush any remaining samples as the last (possibly smaller) shard."""
        if self.buf_len > 0:
            self._flush_one(take_all=True)
        print(f"  Total test shards written: {self.shard_idx}")

    def _flush_one(self, take_all: bool = False):
        all_emb = np.concatenate(self.emb_buf, axis=0)
        all_lbl = np.concatenate(self.lbl_buf, axis=0)

        if take_all:
            out_emb, out_lbl = all_emb, all_lbl
            self.emb_buf = []
            self.lbl_buf = []
            self.buf_len = 0
        else:
            out_emb, out_lbl = all_emb[: self.chunk_size], all_lbl[: self.chunk_size]
            remain_emb, remain_lbl = all_emb[self.chunk_size :], all_lbl[self.chunk_size :]
            self.emb_buf = [remain_emb]
            self.lbl_buf = [remain_lbl]
            self.buf_len = len(remain_lbl)

        # Shuffle within chunk
        rng = np.random.default_rng(seed=99 + self.shard_idx)
        perm = rng.permutation(len(out_lbl))
        out_emb = out_emb[perm]
        out_lbl = out_lbl[perm]

        print(f"  Saving test-{self.shard_idx} ({len(out_lbl):,} samples, {out_emb.nbytes / 1e9:.2f} GB)...")
        np.save(self.output_dir / f"test-{self.shard_idx}.emb.npy", out_emb)
        np.save(self.output_dir / f"test-{self.shard_idx}.lbl.npy", out_lbl)
        self.shard_idx += 1


def load_parquet(path: Path) -> tuple[np.ndarray, np.ndarray]:
    """Load a parquet file and return (embeddings, labels) as numpy arrays."""
    df = pl.read_parquet(path)
    emb = np.stack(df["embedding"].to_numpy()).astype(np.float32)
    lbl = df["flag"].to_numpy().astype(np.int64)
    return emb, lbl


def process_shard(idx: int, test_buffer: TestBuffer):
    """Load, merge, shuffle one ai+wiki pair. Save train portion, buffer test portion."""
    print(f"\n--- Shard {idx}: loading ai-{idx} + wiki-{idx} ---")

    ai_emb, ai_lbl = load_parquet(DATASET_DIR / f"ai-{idx}.parquet")
    print(f"  ai-{idx}:   {len(ai_lbl):>10,} samples")

    wiki_emb, wiki_lbl = load_parquet(DATASET_DIR / f"wiki-{idx}.parquet")
    print(f"  wiki-{idx}: {len(wiki_lbl):>10,} samples")

    # Merge
    emb = np.concatenate([ai_emb, wiki_emb], axis=0)
    lbl = np.concatenate([ai_lbl, wiki_lbl], axis=0)
    del ai_emb, ai_lbl, wiki_emb, wiki_lbl

    # Shuffle
    total = len(lbl)
    print(f"  Shuffling {total:,} samples...")
    rng = np.random.default_rng(seed=42 + idx)
    perm = rng.permutation(total)
    emb = emb[perm]
    lbl = lbl[perm]

    # Split
    split = int(total * TRAIN_RATIO)
    train_emb, test_emb = emb[:split], emb[split:]
    train_lbl, test_lbl = lbl[:split], lbl[split:]
    del emb, lbl

    # Save train shard
    print(f"  Saving train-{idx} ({len(train_lbl):,} samples, {train_emb.nbytes / 1e9:.2f} GB)...")
    np.save(OUTPUT_DIR / f"train-{idx}.emb.npy", train_emb)
    np.save(OUTPUT_DIR / f"train-{idx}.lbl.npy", train_lbl)
    del train_emb, train_lbl

    # Buffer test portion (flushes to disk automatically when full)
    test_buffer.add(test_emb, test_lbl)
    del test_emb, test_lbl


if __name__ == "__main__":
    test_buffer = TestBuffer(TEST_CHUNK, OUTPUT_DIR)

    for i in tqdm(range(NUM_SHARDS), desc="Processing shards"):
        process_shard(i, test_buffer)

    print("\n--- Flushing remaining test data ---")
    test_buffer.finalize()

    print(f"\nDone. Output at {OUTPUT_DIR}")
    print(f"  {NUM_SHARDS} train shards + {test_buffer.shard_idx} test shards")
    print(f"  Train: {TRAIN_RATIO:.0%}, Test: {1 - TRAIN_RATIO:.0%} (per source pair)")
    print(f"  Test chunk size: {TEST_CHUNK:,}")