# @author Claude
import matplotlib
import matplotlib.gridspec as gridspec
import numpy as np
import polars as pl
import seaborn as sns

from typing import List
from matplotlib import pyplot as plt
from sklearn.metrics import confusion_matrix
from python.ai_training.lib import DATASET_DIR
from python.ai_training.lib import VISUAL_DIR
from python.ai_training.network import SentenceClassificationNetwork

matplotlib.use("Agg")

token_embedding = pl.read_parquet(DATASET_DIR / "embedding.parquet")
tokens = token_embedding["token"].to_list()
del token_embedding


def render_epoch(
        model: SentenceClassificationNetwork,
        train_losses: List[float],
        test_losses: List[float],
        epoch: int,
        all_predictions: np.ndarray,
        all_labels: np.ndarray
):
    w1 = model.linear_relu_stack[0].weight.detach().cpu().numpy()
    w2 = model.linear_relu_stack[2].weight.detach().cpu().numpy()
    combined = w2 @ w1
    importance = np.abs(combined).mean(axis=0)

    TOP_N = 30
    topN_idx = np.argsort(importance)[-TOP_N:][::-1]
    topN_labels = [(i, tokens[i], importance[i]) for i in topN_idx]

    cm = confusion_matrix(all_labels, all_predictions)

    fig = plt.figure(figsize=(16, 10))
    gs = gridspec.GridSpec(2, 2, height_ratios=[1, 1], hspace=0.35, wspace=0.3)

    ax_w = fig.add_subplot(gs[0, :])
    im = ax_w.imshow(np.abs(combined), aspect="auto", cmap="viridis")
    fig.colorbar(im, ax=ax_w, label="|weight|")
    ax_w.set_xlabel("Embedding dimension")
    ax_w.set_ylabel("Hidden neuron")
    ax_w.set_title("Weight Importance")

    legend_lines = []
    for rank, (idx, label, val) in enumerate(topN_labels):
        # ax_w.axvline(x=idx, color="red", alpha=0.5, linewidth=1, linestyle="--")
        ax_w.text(
            idx, 0, f"#{rank + 1}",
            ha="center", va="center", fontsize=7,
            color="white", fontweight="bold",
            bbox=dict(boxstyle="round,pad=0.2", facecolor="red", alpha=0.8),
        )
        legend_lines.append(f"#{rank + 1}  [{idx}] {label}")

    ax_w.text(
        0.01, 0.98, "\n".join(legend_lines),
        transform=ax_w.transAxes, fontsize=7,
        verticalalignment="top", fontfamily="monospace",
        bbox=dict(boxstyle="round,pad=0.4", facecolor="black", alpha=0.7, edgecolor="red"),
        color="white",
    )

    ax_cm = fig.add_subplot(gs[1, 0])
    sns.heatmap(
        cm, annot=True, fmt="d", cmap="Blues", ax=ax_cm,
        xticklabels=["human", "ai"], yticklabels=["human", "ai"],
    )
    ax_cm.set_xlabel("Predicted")
    ax_cm.set_ylabel("Actual")
    ax_cm.set_title("Confusion Matrix")

    ax_loss = fig.add_subplot(gs[1, 1])
    epochs_so_far = range(len(train_losses))

    if train_losses:
        ax_loss.plot(range(len(train_losses)), train_losses, label="Train loss", marker="o", markersize=3)
    if test_losses:
        ax_loss.plot(range(len(test_losses)), test_losses, label="Test loss", marker="s", markersize=3)

    ax_loss.set_xlabel("Epoch")
    ax_loss.set_ylabel("Loss")
    ax_loss.set_title("Loss Curve")
    ax_loss.legend()
    ax_loss.set_xlim(0, max(len(train_losses) - 1, 1))

    fig.suptitle(f"Epoch {epoch}", fontsize=16, fontweight="bold")
    fig.subplots_adjust(top=0.92, hspace=0.35, wspace=0.3)
    fig.savefig(VISUAL_DIR / f"epoch_{epoch:04d}.png", dpi=200)
    plt.close(fig)
