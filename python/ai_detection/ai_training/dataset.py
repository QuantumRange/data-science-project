import numpy as np
import torch
from torch.utils.data import Dataset

from python.ai_training.lib import SHARD_DIR


class ShardDataset(Dataset):
    def __init__(self, embeddings: np.ndarray, labels: np.ndarray):
        self.embeddings = embeddings
        self.labels = labels

def load_shard(name: str) -> tuple[torch.Tensor, torch.Tensor]:
    embeddings = np.load(SHARD_DIR / f"{name}.emb.npy")
    label = np.load(SHARD_DIR / f"{name}.lbl.npy")

    return torch.tensor(embeddings).pin_memory(), torch.tensor(label, dtype=torch.long).pin_memory()