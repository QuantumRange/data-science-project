from pathlib import Path

DATA_DIR = Path("/mnt/Fast2T/data/ai-training-set/")
DATASET_DIR = Path(DATA_DIR, "dataset/")
SHARD_DIR = Path("/home/qr/data/")
VISUAL_DIR = Path("/mnt/Fast2T/kotlin/2026/data-science-project/python/training_visuals")

SHARD_COUNT = 10
TEST_SHARD_COUNT = 8
EMBEDDING_SIZE = 2048
CLASS_SIZE = 2

EPOCH_COUNT = 50