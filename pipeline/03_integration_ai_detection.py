import queue
import threading
from pathlib import Path
from typing import Optional

import polars as pl
import torch
from tqdm import tqdm
from transformers import AutoModelForSequenceClassification, AutoTokenizer, BatchEncoding, PreTrainedTokenizerBase

MODEL_ID: str = "fakespot-ai/roberta-base-ai-text-detection-v1"
NUM_GPU = 4
BATCH_SIZE = 512
NUM_TOKENIZERS = 18

INPUT_DIR: Path = Path("/workspace/input/")
OUTPUT_DIR: Path = Path("/workspace/output/")

type FileQueueItem = Optional[tuple[Path, pl.DataFrame]]
type TokenQueueItem = Optional[tuple[Path, pl.DataFrame, BatchEncoding]]
type ResultQueueItem = Optional[tuple[Path, pl.DataFrame]]


def scanner(
        out_queue: queue.Queue[FileQueueItem],
        error_event: threading.Event,
        bar: tqdm,
) -> None:
    try:
        files = list(
            filter(
                lambda f: not (OUTPUT_DIR / f.name).exists(),
                INPUT_DIR.glob("*.parquet"),
            ),
        )

        for file in files:
            if error_event.is_set():
                break

            try:
                df = pl.read_parquet(file)
                out_queue.put((file, df))
            except:
                pass
            bar.update(1)

    except Exception as e:
        error_event.set()
        raise


def tokenizer(
        in_queue: queue.Queue[FileQueueItem],
        out_queue: queue.Queue[TokenQueueItem],
        error_event: threading.Event,
        bar: tqdm,
) -> None:
    try:
        # noinspection PyNoneFunctionAssignment,PyTypeChecker
        text_tokenizer: PreTrainedTokenizerBase = AutoTokenizer.from_pretrained(MODEL_ID)

        if text_tokenizer is None:
            raise RuntimeError("Invalid model id?")

        while True:
            item: FileQueueItem = in_queue.get()
            if item is None: break

            if error_event.is_set():
                in_queue.task_done()
                break

            file, df = item

            # noinspection PyCallingNonCallable
            tokens: BatchEncoding = text_tokenizer(
                df["text"].to_list(),
                return_tensors="pt",
                truncation=True,
                max_length=512,
                padding=True,
            )

            bar.update(tokens["input_ids"].shape[0])
            out_queue.put((file, df, tokens))
            in_queue.task_done()
    except Exception as e:
        error_event.set()
        raise


def evaluator(
        gpu: int,
        in_queue: queue.Queue[TokenQueueItem],
        out_queue: queue.Queue[ResultQueueItem],
        error_event: threading.Event,
        bar: tqdm,
) -> None:
    try:
        device = f"cuda:{gpu}"

        model = AutoModelForSequenceClassification.from_pretrained(MODEL_ID)
        model = model.to(device)
        model.eval()

        while True:
            item: TokenQueueItem = in_queue.get()
            if item is None: break

            if error_event.is_set():
                in_queue.task_done()
                break

            file, df, tokens = item
            n = tokens["input_ids"].shape[0]
            labels = []
            scores = []

            for start in range(0, n, BATCH_SIZE):
                batch = { k: v[start:start + BATCH_SIZE].to(device) for k, v in tokens.items() }

                with torch.no_grad():
                    outputs = model(**batch)

                probs = torch.softmax(outputs.logits, dim=-1)
                id2label = model.config.id2label

                labels.extend([id2label[i] for i in probs.argmax(dim=-1).tolist()])
                scores.extend([{ id2label[i]: p for i, p in enumerate(row.tolist()) } for row in probs])

                batch_size = next(iter(batch.values())).shape[0]
                bar.update(batch_size)

            df = df.with_columns(
                [
                    pl.Series("predicted_label", labels),
                    pl.Series("label_scores", scores),
                ],
            )

            out_queue.put((file, df))
            in_queue.task_done()
    except Exception as e:
        error_event.set()
        raise


def storer(
        in_queue: queue.Queue[ResultQueueItem],
        error_event: threading.Event,
        bar: tqdm,
) -> None:
    try:
        while True:
            item = in_queue.get()
            if item is None: break

            if error_event.is_set():
                in_queue.task_done()
                break

            file, df = item

            df.write_parquet(
                OUTPUT_DIR / file.name,
                compression="zstd",
                compression_level=8,
            )
            bar.update(1)
            in_queue.task_done()
    except Exception as e:
        error_event.set()
        raise


def main():
    error_event = threading.Event()

    file_queue = queue.Queue(maxsize=4)
    token_queue = queue.Queue(maxsize=4)
    result_queue = queue.Queue(maxsize=4)

    total_files = len(list(filter(lambda f: not (OUTPUT_DIR / f.name).exists(), INPUT_DIR.glob("*.parquet"))))

    scan_bar = tqdm(desc="scan     ", position=0, total=total_files, unit="file", dynamic_ncols=True)
    token_bar = tqdm(desc="tokenize ", position=1, unit="row", dynamic_ncols=True)
    eval_bar = tqdm(desc="evaluate ", position=2, unit="row", dynamic_ncols=True)
    store_bar = tqdm(desc="store    ", position=3, total=total_files, unit="file", dynamic_ncols=True)

    scanners = [
        threading.Thread(target=scanner, args=(file_queue, error_event, scan_bar))
    ]

    tokenizers = [
        threading.Thread(target=tokenizer, args=(file_queue, token_queue, error_event, token_bar))
        for _ in range(NUM_TOKENIZERS)
    ]

    evaluators = [
        threading.Thread(target=evaluator, args=(i, token_queue, result_queue, error_event, eval_bar))
        for i in range(NUM_GPU)
    ]

    storers = [
        threading.Thread(target=storer, args=(result_queue, error_event, store_bar))
    ]

    for t in scanners + tokenizers + evaluators + storers:
        t.daemon = True
        t.start()

    for t in scanners: t.join()

    for _ in tokenizers: file_queue.put(None)
    for t in tokenizers: t.join()

    for _ in evaluators: token_queue.put(None)
    for t in evaluators: t.join()

    for _ in storers: result_queue.put(None)
    for t in storers: t.join()

    for bar in (scan_bar, token_bar, eval_bar, store_bar):
        bar.close()


if __name__ == '__main__':
    main()
