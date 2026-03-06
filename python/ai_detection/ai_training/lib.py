from pathlib import Path

DATA_DIR = Path("/mnt/Fast2T/data/ai-training-set/")
DATASET_DIR = Path(DATA_DIR, "dataset/")
SHARD_DIR = Path("/mnt/Fast2T/data/ai-training-set/shards/")
VISUAL_DIR = Path("/mnt/Fast2T/kotlin/2026/data-science-project/python/training_visuals")

SHARD_COUNT = 3
TEST_SHARD_COUNT = 1
EMBEDDING_SIZE = 128
CLASS_SIZE = 2

EPOCH_COUNT = 200