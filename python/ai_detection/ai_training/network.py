import torch.nn as nn

from python.ai_training.lib import EMBEDDING_SIZE, CLASS_SIZE


class SentenceClassificationNetwork(nn.Module):
    def __init__(self):
        super().__init__()

        self.linear_relu_stack = nn.Sequential(
            nn.Linear(EMBEDDING_SIZE, 1),
            nn.ReLU(),
            nn.Linear(1, CLASS_SIZE),
        )

    def forward(self, embedding):
        return self.linear_relu_stack(embedding)
