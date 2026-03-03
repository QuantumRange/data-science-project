from concurrent.futures import ThreadPoolExecutor

import numpy as np
import torch
import torch.nn as nn
from tqdm import tqdm

from python.ai_training.dataset import load_shard
from python.ai_training.lib import DATASET_DIR, SHARD_COUNT, EPOCH_COUNT, TEST_SHARD_COUNT
from python.ai_training.network import SentenceClassificationNetwork
from python.ai_training.visualizer import render_epoch

# I use del a lot because we need all the memory
device = torch.accelerator.current_accelerator().type if torch.accelerator.is_available() else "cpu"

if __name__ == '__main__':
    torch.set_float32_matmul_precision('high')

    model = SentenceClassificationNetwork()
    model = torch.compile(model)
    model = model.to(device)
    print(model)

    loss_fn = nn.CrossEntropyLoss()
    optimizer = torch.optim.AdamW(model.parameters(), lr=1e-3, weight_decay=1e-4)

    train_losses = []
    test_losses = []


    def train(epoch: int):
        model.train()
        running_loss = 0.0
        n_batches = 0

        rng = np.random.default_rng(seed=epoch)
        shard_order = rng.permutation(SHARD_COUNT)

        executor = ThreadPoolExecutor(max_workers=1)
        future = executor.submit(load_shard, f"train-{shard_order[0]}")

        for i, shard_idx in enumerate(shard_order):
            embeddings_cpu, labels_cpu = future.result()

            if i + 1 < len(shard_order):
                future = executor.submit(load_shard, f"train-{shard_order[i + 1]}")

            all_embeddings = embeddings_cpu.to(device)
            all_labels = labels_cpu.to(device)
            del embeddings_cpu, labels_cpu

            total = len(all_labels)
            chunk_size = 4096
            num_chunks = (total + chunk_size - 1) // chunk_size

            perm = torch.randperm(total, device=device)

            for j in tqdm(range(num_chunks), desc=f"Epoch {epoch} shard {shard_idx}", leave=True):
                start = j * chunk_size
                end = min(start + chunk_size, total)
                idx = perm[start:end]

                X, y = all_embeddings[idx], all_labels[idx]
                pred = model(X)
                loss = loss_fn(pred, y)
                loss.backward()
                optimizer.step()
                optimizer.zero_grad(set_to_none=True)
                running_loss += loss.item()
                n_batches += 1

            del all_embeddings, all_labels

        train_losses.append(running_loss / n_batches)
        executor.shutdown()


    def test(epoch: int) -> tuple[np.ndarray, np.ndarray]:
        model.eval()
        test_loss, correct = 0, 0
        total_samples = 0
        all_predictions = []
        all_labels = []

        executor = ThreadPoolExecutor(max_workers=1)
        future = executor.submit(load_shard, f"test-{0}")

        with torch.no_grad():
            for i in tqdm(range(TEST_SHARD_COUNT), desc=f"Epoch {epoch} test", leave=True):
                embeddings_cpu, labels_cpu = future.result()

                if i + 1 < TEST_SHARD_COUNT:
                    future = executor.submit(load_shard, f"test-{i + 1}")

                embeddings = embeddings_cpu.to(device)
                labels = labels_cpu.to(device)
                del embeddings_cpu, labels_cpu

                pred = model(embeddings)
                test_loss += loss_fn(pred, labels).item()
                prediction_batch = pred.argmax(1)
                correct += (prediction_batch == labels).sum().item()
                total_samples += len(labels)
                all_predictions.append(prediction_batch.cpu())
                all_labels.append(labels.cpu())

                del embeddings, labels

        executor.shutdown()

        test_loss /= TEST_SHARD_COUNT
        correct /= total_samples
        test_losses.append(test_loss)
        print(f"Epoch {epoch}: accuracy={100 * correct:>0.1f}%, loss={test_loss:>8f}")

        return torch.cat(all_predictions).numpy(), torch.cat(all_labels).numpy()


    predictions, labels = test(0)
    train_losses.append(test_losses[0])
    render_epoch(model, train_losses, test_losses, 0, predictions, labels)

    for epoch in range(1, EPOCH_COUNT + 1):
        train(epoch)
        predictions, labels = test(epoch)
        render_epoch(model, train_losses, test_losses, epoch, predictions, labels)
        torch.save(model.state_dict(), DATASET_DIR / f"model-{epoch}.pth")
