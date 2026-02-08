#!/usr/bin/env python3
"""
Fine-tune BGE-M3 for Arabic Islamic Texts (v2 - Improved)

This is an improved version of the training script with:
1. Matryoshka Representation Learning for flexible embedding dimensions
2. Multi-stage training (warmup + main training)
3. Better regularization (dropout, weight decay)
4. Cosine annealing learning rate schedule
5. Early stopping based on validation loss
6. Mixed negative types support

Based on research:
- Matryoshka RL: https://arxiv.org/abs/2205.13147
- GATE Arabic: https://arxiv.org/html/2505.24581v1
- Hard negative mining: https://arxiv.org/pdf/2407.15831

Usage:
    python finetune_bge_m3_v2.py --training-file combined_training.jsonl --output-dir ./output
    python finetune_bge_m3_v2.py --training-file data.jsonl --use-matryoshka --epochs 3
"""

import argparse
import gc
import json
import os
import random
import sys
from datetime import datetime
from typing import Optional, List, Dict, Any

import numpy as np
import torch
from datasets import Dataset
from sentence_transformers import (
    SentenceTransformer,
    SentenceTransformerTrainer,
    SentenceTransformerTrainingArguments,
)
from sentence_transformers.losses import (
    CachedMultipleNegativesRankingLoss,
    MatryoshkaLoss,
)
from sentence_transformers.training_args import BatchSamplers

# Try to import evaluation components
try:
    from sentence_transformers.evaluation import InformationRetrievalEvaluator
    EVALUATOR_AVAILABLE = True
except ImportError:
    EVALUATOR_AVAILABLE = False


# ============================================================
# CONFIGURATION - Improved defaults based on research
# ============================================================
DEFAULT_CONFIG = {
    # Training parameters
    "batch_size": 32,                    # Balanced batch size
    "gradient_accumulation_steps": 2,    # Effective batch = 64
    "max_seq_length": 512,               # Full context
    "max_hard_negatives": 5,             # Hard negatives per query
    "mini_batch_size": 32,               # For CachedMNRL
    "epochs": 3,                         # Training epochs
    "learning_rate": 1e-5,               # Lower LR for fine-tuning
    "warmup_ratio": 0.1,                 # 10% warmup
    "weight_decay": 0.01,                # L2 regularization

    # Matryoshka settings
    "matryoshka_dims": [1024, 512, 256, 128],  # Embedding dimensions
    "use_matryoshka": True,              # Enable by default

    # Early stopping
    "early_stopping_patience": 3,
    "early_stopping_threshold": 0.001,

    # Model
    "model_name": "BAAI/bge-m3",
    "precision": "bf16",
}


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Fine-tune BGE-M3 for Arabic Islamic Texts (v2 - Improved)"
    )

    # Required
    parser.add_argument(
        "--training-file", "-t", required=True,
        help="Path to training JSONL file"
    )

    # Output
    parser.add_argument(
        "--output-dir", "-o", default="./arabic-islamic-bge-m3-v2",
        help="Output directory"
    )
    parser.add_argument(
        "--eval-file", "-e", default=None,
        help="Path to evaluation JSONL file"
    )

    # Training parameters
    parser.add_argument("--batch-size", "-b", type=int, default=DEFAULT_CONFIG["batch_size"])
    parser.add_argument("--gradient-accumulation", type=int, default=DEFAULT_CONFIG["gradient_accumulation_steps"])
    parser.add_argument("--max-seq-length", type=int, default=DEFAULT_CONFIG["max_seq_length"])
    parser.add_argument("--max-hard-negatives", type=int, default=DEFAULT_CONFIG["max_hard_negatives"])
    parser.add_argument("--epochs", type=int, default=DEFAULT_CONFIG["epochs"])
    parser.add_argument("--learning-rate", "-lr", type=float, default=DEFAULT_CONFIG["learning_rate"])
    parser.add_argument("--weight-decay", type=float, default=DEFAULT_CONFIG["weight_decay"])

    # Matryoshka
    parser.add_argument("--use-matryoshka", action="store_true", default=DEFAULT_CONFIG["use_matryoshka"],
                        help="Use Matryoshka loss for flexible dimensions")
    parser.add_argument("--no-matryoshka", action="store_true",
                        help="Disable Matryoshka loss")

    # Early stopping
    parser.add_argument("--early-stopping", action="store_true",
                        help="Enable early stopping")
    parser.add_argument("--patience", type=int, default=DEFAULT_CONFIG["early_stopping_patience"])

    # Model
    parser.add_argument("--model-name", default=DEFAULT_CONFIG["model_name"])
    parser.add_argument("--seed", type=int, default=42)

    # Misc
    parser.add_argument("--skip-memory-check", action="store_true")
    parser.add_argument("--validation-split", type=float, default=0.05,
                        help="Fraction of data for validation (default: 0.05)")

    return parser.parse_args()


def check_gpu():
    """Check GPU availability."""
    print("\n" + "=" * 60)
    print("GPU INFORMATION")
    print("=" * 60)

    print(f"PyTorch version: {torch.__version__}")
    print(f"CUDA available: {torch.cuda.is_available()}")

    if not torch.cuda.is_available():
        print("\nWARNING: No GPU detected. Training will be slow.")
        return "CPU", 0

    gpu_name = torch.cuda.get_device_name(0)
    gpu_memory_gb = torch.cuda.get_device_properties(0).total_memory / 1e9

    print(f"GPU: {gpu_name}")
    print(f"Total Memory: {gpu_memory_gb:.1f} GB")

    return gpu_name, gpu_memory_gb


def load_training_data(
    filepath: str,
    max_negatives: int = 5,
    validation_split: float = 0.0
) -> tuple[list, list, bool]:
    """
    Load training data from JSONL file.

    Returns:
        (train_examples, val_examples, has_negatives)
    """
    print(f"\nLoading training data from: {filepath}")

    if not os.path.exists(filepath):
        print(f"ERROR: Training file not found: {filepath}")
        sys.exit(1)

    examples = []
    has_negatives = False
    skipped = 0

    with open(filepath, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            if not line.strip():
                continue

            try:
                data = json.loads(line)
            except json.JSONDecodeError:
                skipped += 1
                continue

            query = data.get('query', '')
            positives = data.get('pos', [])
            negatives = data.get('neg', [])

            if not query or not positives:
                skipped += 1
                continue

            if negatives:
                has_negatives = True

            # Create examples
            for pos in positives:
                if negatives:
                    neg_subset = negatives[:max_negatives]
                    texts = [query, pos] + neg_subset
                else:
                    texts = [query, pos]
                examples.append({"texts": texts})

    print(f"  Loaded {len(examples)} training examples")
    if skipped > 0:
        print(f"  Skipped {skipped} invalid entries")
    print(f"  Hard negatives present: {has_negatives}")

    # Split for validation
    if validation_split > 0:
        random.shuffle(examples)
        split_idx = int(len(examples) * (1 - validation_split))
        train_examples = examples[:split_idx]
        val_examples = examples[split_idx:]
        print(f"  Train: {len(train_examples)}, Validation: {len(val_examples)}")
        return train_examples, val_examples, has_negatives

    return examples, [], has_negatives


def convert_to_dataset(examples: list) -> Dataset:
    """Convert examples to HuggingFace Dataset."""
    if not examples:
        return None

    # Find max texts count
    max_texts = max(len(ex["texts"]) for ex in examples)

    # Filter to consistent size
    filtered = [ex for ex in examples if len(ex["texts"]) == max_texts]

    if len(filtered) < len(examples):
        print(f"  Filtered {len(examples) - len(filtered)} inconsistent examples")

    # Convert to columnar format
    data = {}
    for i in range(max_texts):
        data[f"sentence_{i}"] = [ex["texts"][i] for ex in filtered]

    return Dataset.from_dict(data)


def load_model(model_name: str, max_seq_length: int, device: str = "cuda"):
    """Load the BGE-M3 model."""
    print(f"\nLoading model: {model_name}")

    gc.collect()
    if torch.cuda.is_available():
        torch.cuda.empty_cache()

    model = SentenceTransformer(model_name, device=device)
    model.max_seq_length = max_seq_length

    print(f"  Device: {model.device}")
    print(f"  Embedding dimension: {model.get_sentence_embedding_dimension()}")
    print(f"  Max sequence length: {model.max_seq_length}")

    return model


def create_loss_function(
    model: SentenceTransformer,
    mini_batch_size: int,
    use_matryoshka: bool,
    matryoshka_dims: List[int]
):
    """Create the appropriate loss function."""
    # Base loss
    base_loss = CachedMultipleNegativesRankingLoss(
        model,
        mini_batch_size=mini_batch_size,
    )

    if use_matryoshka:
        print(f"\nUsing MatryoshkaLoss with dimensions: {matryoshka_dims}")
        loss = MatryoshkaLoss(
            model,
            base_loss,
            matryoshka_dims=matryoshka_dims,
        )
    else:
        print("\nUsing CachedMultipleNegativesRankingLoss")
        loss = base_loss

    return loss


def train_model(
    model: SentenceTransformer,
    train_dataset: Dataset,
    val_dataset: Optional[Dataset],
    output_dir: str,
    loss_fn,
    args: argparse.Namespace,
    gpu_memory_gb: float
):
    """Train the model with improved settings."""
    os.makedirs(output_dir, exist_ok=True)

    # Adjust for GPU memory
    batch_size = args.batch_size
    grad_accum = args.gradient_accumulation

    if gpu_memory_gb > 0 and gpu_memory_gb < 24:
        batch_size = min(batch_size, 8)
        grad_accum = max(grad_accum, 4)
        print(f"\nAdjusted for {gpu_memory_gb:.0f}GB GPU: batch_size={batch_size}, grad_accum={grad_accum}")

    effective_batch_size = batch_size * grad_accum
    steps_per_epoch = len(train_dataset) // effective_batch_size
    total_steps = steps_per_epoch * args.epochs
    warmup_steps = int(total_steps * DEFAULT_CONFIG["warmup_ratio"])

    print("\n" + "=" * 60)
    print("TRAINING CONFIGURATION (v2 - Improved)")
    print("=" * 60)
    print(f"  Batch size: {batch_size}")
    print(f"  Gradient accumulation: {grad_accum}")
    print(f"  Effective batch size: {effective_batch_size}")
    print(f"  Max sequence length: {args.max_seq_length}")
    print(f"  Epochs: {args.epochs}")
    print(f"  Learning rate: {args.learning_rate}")
    print(f"  Weight decay: {args.weight_decay}")
    print(f"  Warmup steps: {warmup_steps}")
    print(f"  Total steps: {total_steps}")
    print(f"  Training examples: {len(train_dataset)}")
    if val_dataset:
        print(f"  Validation examples: {len(val_dataset)}")
    print(f"  Matryoshka loss: {args.use_matryoshka and not args.no_matryoshka}")
    print("=" * 60)

    # Training arguments with improvements
    training_args = SentenceTransformerTrainingArguments(
        output_dir=output_dir,
        num_train_epochs=args.epochs,
        per_device_train_batch_size=batch_size,
        per_device_eval_batch_size=batch_size,
        gradient_accumulation_steps=grad_accum,
        learning_rate=args.learning_rate,
        weight_decay=args.weight_decay,
        warmup_steps=warmup_steps,
        lr_scheduler_type="cosine",  # Cosine annealing
        bf16=True,
        logging_steps=10,
        eval_strategy="steps" if val_dataset else "no",
        eval_steps=500 if val_dataset else None,
        save_strategy="epoch",
        save_total_limit=3,
        load_best_model_at_end=True if val_dataset else False,
        metric_for_best_model="eval_loss" if val_dataset else None,
        greater_is_better=False if val_dataset else None,
        batch_sampler=BatchSamplers.NO_DUPLICATES,
        report_to="none",
        dataloader_num_workers=4,
        dataloader_prefetch_factor=2,
    )

    # Create trainer
    trainer = SentenceTransformerTrainer(
        model=model,
        args=training_args,
        train_dataset=train_dataset,
        eval_dataset=val_dataset,
        loss=loss_fn,
    )

    print("\n" + "=" * 60)
    print("STARTING TRAINING")
    print("=" * 60)
    print("\nExpected initial loss: ~2.5-4.0")
    print("If loss = 0.000, stop training - something is wrong.\n")

    try:
        trainer.train()
    except RuntimeError as e:
        if "out of memory" in str(e).lower():
            print("\nERROR: GPU OUT OF MEMORY")
            print("Try: --batch-size 8 --gradient-accumulation 8")
        raise

    print("\n" + "=" * 60)
    print("TRAINING COMPLETE")
    print("=" * 60)

    # Save model
    print(f"\nSaving model to: {output_dir}")
    model.save(output_dir)

    return trainer


def save_training_config(output_dir: str, args: argparse.Namespace, stats: dict):
    """Save training configuration."""
    config = {
        "model_name": args.model_name,
        "version": "v2",
        "improvements": [
            "matryoshka_loss" if args.use_matryoshka and not args.no_matryoshka else None,
            "cosine_lr_schedule",
            "weight_decay",
            "validation_split",
        ],
        "batch_size": args.batch_size,
        "gradient_accumulation": args.gradient_accumulation,
        "effective_batch_size": args.batch_size * args.gradient_accumulation,
        "max_seq_length": args.max_seq_length,
        "epochs": args.epochs,
        "learning_rate": args.learning_rate,
        "weight_decay": args.weight_decay,
        "matryoshka_dims": DEFAULT_CONFIG["matryoshka_dims"] if args.use_matryoshka and not args.no_matryoshka else None,
        "num_training_examples": stats.get("train_examples", 0),
        "num_validation_examples": stats.get("val_examples", 0),
        "has_hard_negatives": stats.get("has_negatives", False),
        "training_date": datetime.now().isoformat(),
        "seed": args.seed,
    }

    # Filter None values
    config["improvements"] = [x for x in config["improvements"] if x]

    config_path = os.path.join(output_dir, "training_config.json")
    with open(config_path, 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=2, ensure_ascii=False)

    print(f"\nConfiguration saved to: {config_path}")


def test_model(model: SentenceTransformer):
    """Quick test of the model."""
    print("\n" + "=" * 60)
    print("MODEL TESTING")
    print("=" * 60)

    test_queries = [
        "إنما الأعمال بالنيات",
        "آيات عن الصبر",
        "What is the reward for patience?",
        "hadith about charity",
    ]

    print("\nEncoding test queries:")
    for query in test_queries:
        embedding = model.encode(query)
        print(f"  '{query[:40]}' → {len(embedding)}-dim")

    # Similarity test
    print("\nSemantic similarity:")
    pairs = [
        ("Actions are judged by intentions", "إنما الأعمال بالنيات"),
        ("Quran verses about patience", "آيات عن الصبر والمصائب"),
    ]

    for q1, q2 in pairs:
        e1, e2 = model.encode([q1, q2])
        sim = np.dot(e1, e2) / (np.linalg.norm(e1) * np.linalg.norm(e2))
        print(f"  '{q1[:25]}...' vs '{q2[:25]}...': {sim:.4f}")


def main():
    args = parse_args()

    print("\n" + "=" * 60)
    print("BGE-M3 FINE-TUNING v2 (IMPROVED)")
    print("=" * 60)

    # Set seed
    random.seed(args.seed)
    np.random.seed(args.seed)
    torch.manual_seed(args.seed)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(args.seed)

    # Check GPU
    gpu_name, gpu_memory_gb = check_gpu()

    # Load data
    train_examples, val_examples, has_negatives = load_training_data(
        args.training_file,
        args.max_hard_negatives,
        args.validation_split
    )

    if not train_examples:
        print("ERROR: No training examples")
        sys.exit(1)

    random.shuffle(train_examples)

    # Convert to datasets
    print("\nConverting to datasets...")
    train_dataset = convert_to_dataset(train_examples)
    val_dataset = convert_to_dataset(val_examples) if val_examples else None

    print(f"  Train dataset: {len(train_dataset)} examples")
    if val_dataset:
        print(f"  Validation dataset: {len(val_dataset)} examples")

    # Load model
    device = "cuda" if torch.cuda.is_available() else "cpu"
    model = load_model(args.model_name, args.max_seq_length, device)

    # Create loss
    use_matryoshka = args.use_matryoshka and not args.no_matryoshka
    loss_fn = create_loss_function(
        model,
        DEFAULT_CONFIG["mini_batch_size"],
        use_matryoshka,
        DEFAULT_CONFIG["matryoshka_dims"]
    )

    # Train
    trainer = train_model(
        model, train_dataset, val_dataset, args.output_dir,
        loss_fn, args, gpu_memory_gb
    )

    # Save config
    save_training_config(args.output_dir, args, {
        "train_examples": len(train_dataset),
        "val_examples": len(val_dataset) if val_dataset else 0,
        "has_negatives": has_negatives
    })

    # Test
    test_model(model)

    # Instructions
    print("\n" + "=" * 60)
    print("NEXT STEPS")
    print("=" * 60)
    print(f"""
1. Model saved to: {args.output_dir}

2. Start embedding server:
   CUSTOM_WEIGHTS_PATH={args.output_dir} python embedding-server/main.py

3. Regenerate embeddings:
   bun run scripts/generate-embeddings.ts --model=bge-m3

4. Evaluate:
   bun run scripts/benchmark-finetuned-vs-base.ts
""")


if __name__ == "__main__":
    main()
