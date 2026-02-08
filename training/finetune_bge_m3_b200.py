#!/usr/bin/env python3
"""
Fine-tune BGE-M3 for Arabic Islamic Texts
Optimized for NVIDIA B200 (192GB VRAM)

This script is designed for RunPod or similar cloud GPU providers with
high-memory GPUs. It maximizes training quality by using larger batches
and longer sequences while staying under 160GB memory usage.

Usage:
    python finetune_bge_m3_b200.py --training-file combined_training.jsonl --output-dir ./output

    # With evaluation file
    python finetune_bge_m3_b200.py \
        --training-file combined_training.jsonl \
        --eval-file gold_standard_evaluation.jsonl \
        --output-dir ./arabic-islamic-bge-m3 \
        --epochs 3

    # Custom configuration
    python finetune_bge_m3_b200.py \
        --training-file data.jsonl \
        --batch-size 32 \
        --max-seq-length 384 \
        --epochs 2

Requirements:
    pip install sentence-transformers>=3.4.0 datasets accelerate torch

Memory Usage (estimated for B200 192GB):
    - Model weights (bf16): ~1.1GB
    - Optimizer states: ~4.5GB
    - Gradients: ~1.1GB
    - Activations (batch=48, seq=512): ~30-45GB
    - Loss computation cache: ~15GB
    - Total: ~60-70GB (well under 160GB target)
"""

import argparse
import gc
import json
import os
import random
import sys
from datetime import datetime
from typing import Optional

import numpy as np
import torch
from datasets import Dataset
from sentence_transformers import (
    SentenceTransformer,
    SentenceTransformerTrainer,
    SentenceTransformerTrainingArguments,
)
from sentence_transformers.losses import CachedMultipleNegativesRankingLoss
from sentence_transformers.training_args import BatchSamplers


# ============================================================
# CONFIGURATION - Optimized for B200 192GB
# ============================================================
DEFAULT_CONFIG = {
    "batch_size": 48,                    # Large batch for more in-batch negatives
    "gradient_accumulation_steps": 1,    # Not needed with B200's memory
    "max_seq_length": 512,               # Full context (vs 256 on H100)
    "max_hard_negatives": 5,             # More hard negatives (vs 3 on H100)
    "mini_batch_size": 48,               # For CachedMNRL loss computation
    "epochs": 3,                         # More epochs for better convergence
    "learning_rate": 2e-5,               # Slightly higher with larger batch
    "warmup_ratio": 0.1,                 # 10% warmup
    "precision": "bf16",                 # Better gradient precision than fp16
    "model_name": "BAAI/bge-m3",
    "memory_limit_gb": 160,              # Safety limit (leaves 32GB headroom)
}


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Fine-tune BGE-M3 for Arabic Islamic Texts (B200 Optimized)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    # Required arguments
    parser.add_argument(
        "--training-file", "-t",
        required=True,
        help="Path to training JSONL file (e.g., combined_training.jsonl)"
    )

    # Optional arguments
    parser.add_argument(
        "--output-dir", "-o",
        default="./arabic-islamic-bge-m3",
        help="Output directory for fine-tuned model (default: ./arabic-islamic-bge-m3)"
    )
    parser.add_argument(
        "--eval-file", "-e",
        default=None,
        help="Path to evaluation JSONL file (optional)"
    )
    parser.add_argument(
        "--batch-size", "-b",
        type=int,
        default=DEFAULT_CONFIG["batch_size"],
        help=f"Training batch size (default: {DEFAULT_CONFIG['batch_size']})"
    )
    parser.add_argument(
        "--max-seq-length",
        type=int,
        default=DEFAULT_CONFIG["max_seq_length"],
        help=f"Maximum sequence length (default: {DEFAULT_CONFIG['max_seq_length']})"
    )
    parser.add_argument(
        "--max-hard-negatives",
        type=int,
        default=DEFAULT_CONFIG["max_hard_negatives"],
        help=f"Max hard negatives per query (default: {DEFAULT_CONFIG['max_hard_negatives']})"
    )
    parser.add_argument(
        "--epochs",
        type=int,
        default=DEFAULT_CONFIG["epochs"],
        help=f"Number of training epochs (default: {DEFAULT_CONFIG['epochs']})"
    )
    parser.add_argument(
        "--learning-rate", "-lr",
        type=float,
        default=DEFAULT_CONFIG["learning_rate"],
        help=f"Learning rate (default: {DEFAULT_CONFIG['learning_rate']})"
    )
    parser.add_argument(
        "--model-name",
        default=DEFAULT_CONFIG["model_name"],
        help=f"Base model name (default: {DEFAULT_CONFIG['model_name']})"
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed (default: 42)"
    )
    parser.add_argument(
        "--skip-memory-check",
        action="store_true",
        help="Skip memory safety check (not recommended)"
    )

    return parser.parse_args()


def check_gpu():
    """Check GPU availability and print info."""
    print("\n" + "=" * 60)
    print("GPU INFORMATION")
    print("=" * 60)

    print(f"PyTorch version: {torch.__version__}")
    print(f"CUDA available: {torch.cuda.is_available()}")

    if not torch.cuda.is_available():
        print("\nERROR: No GPU detected. This script requires a GPU.")
        print("If running on RunPod, ensure you selected a GPU instance.")
        sys.exit(1)

    gpu_name = torch.cuda.get_device_name(0)
    gpu_memory_gb = torch.cuda.get_device_properties(0).total_memory / 1e9

    print(f"GPU: {gpu_name}")
    print(f"Total Memory: {gpu_memory_gb:.1f} GB")

    # Check if this is a high-memory GPU
    if gpu_memory_gb < 40:
        print(f"\nWARNING: GPU has only {gpu_memory_gb:.1f}GB memory.")
        print("This script is optimized for B200 (192GB) or similar.")
        print("Consider using the Colab notebook for smaller GPUs.")
        print("Continuing with reduced settings...")

    return gpu_name, gpu_memory_gb


def get_memory_usage():
    """Get current GPU memory usage in GB."""
    if torch.cuda.is_available():
        return torch.cuda.memory_allocated() / 1e9
    return 0


def print_memory_status(label: str = ""):
    """Print current GPU memory status."""
    if torch.cuda.is_available():
        allocated = torch.cuda.memory_allocated() / 1e9
        reserved = torch.cuda.memory_reserved() / 1e9
        total = torch.cuda.get_device_properties(0).total_memory / 1e9
        print(f"GPU Memory {label}: {allocated:.1f}GB allocated, {reserved:.1f}GB reserved, {total:.1f}GB total")


def load_training_data(filepath: str, max_negatives: int = 5):
    """
    Load training data from JSONL file.

    Expected format:
    {"query": "...", "pos": ["..."], "neg": ["...", "...", ...]}

    Returns:
        List of example dicts with 'texts' key containing [query, pos, neg1, neg2, ...]
        Boolean indicating if hard negatives are present
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
            except json.JSONDecodeError as e:
                print(f"  Warning: Invalid JSON on line {line_num}: {e}")
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

            # Create one example per positive
            for pos in positives:
                if negatives:
                    # With hard negatives: [query, positive, neg1, neg2, ...]
                    neg_subset = negatives[:max_negatives]
                    texts = [query, pos] + neg_subset
                else:
                    # Without negatives: [query, positive]
                    texts = [query, pos]

                examples.append({"texts": texts})

    print(f"  Loaded {len(examples)} training examples")
    if skipped > 0:
        print(f"  Skipped {skipped} invalid entries")
    print(f"  Hard negatives present: {has_negatives}")

    if has_negatives:
        # Get actual negative count from first example with negatives
        sample_neg_count = len(examples[0]["texts"]) - 2 if examples else 0
        print(f"  Texts per example: {len(examples[0]['texts'])} (query + pos + {sample_neg_count} neg)")

    return examples, has_negatives


def convert_to_dataset(examples: list) -> Dataset:
    """
    Convert list of example dicts to HuggingFace Dataset.

    SentenceTransformerTrainer expects columns: sentence_0, sentence_1, ...
    """
    # Find the expected number of texts (max across all examples)
    expected_num_texts = max(len(ex["texts"]) for ex in examples)

    # Filter to only include examples with the expected number of texts
    # (CachedMultipleNegativesRankingLoss needs consistent format)
    filtered_examples = [ex for ex in examples if len(ex["texts"]) == expected_num_texts]

    if len(filtered_examples) < len(examples):
        print(f"  Filtered {len(examples) - len(filtered_examples)} examples with inconsistent text count")
        print(f"  Using {len(filtered_examples)} examples with {expected_num_texts} texts each")

    data = {}
    for i in range(expected_num_texts):
        data[f"sentence_{i}"] = [ex["texts"][i] for ex in filtered_examples]

    return Dataset.from_dict(data)


def load_model(model_name: str, max_seq_length: int, device: str = "cuda"):
    """Load the BGE-M3 model with specified configuration."""
    print(f"\nLoading model: {model_name}")

    # Clear memory before loading
    gc.collect()
    torch.cuda.empty_cache()

    model = SentenceTransformer(model_name, device=device)
    model.max_seq_length = max_seq_length

    print(f"  Device: {model.device}")
    print(f"  Embedding dimension: {model.get_sentence_embedding_dimension()}")
    print(f"  Max sequence length: {model.max_seq_length}")
    print_memory_status("(after model load)")

    return model


def train_model(
    model: SentenceTransformer,
    train_dataset: Dataset,
    output_dir: str,
    batch_size: int,
    epochs: int,
    learning_rate: float,
    warmup_ratio: float,
    mini_batch_size: int,
    max_hard_negatives: int,
    use_bf16: bool = True,
    memory_limit_gb: float = 160,
    skip_memory_check: bool = False,
):
    """
    Train the model with optimized settings for B200.
    """
    os.makedirs(output_dir, exist_ok=True)

    # Calculate training parameters
    steps_per_epoch = len(train_dataset) // batch_size
    total_steps = steps_per_epoch * epochs
    warmup_steps = int(total_steps * warmup_ratio)
    in_batch_negatives = batch_size - 1
    total_negatives = in_batch_negatives + max_hard_negatives

    print("\n" + "=" * 60)
    print("TRAINING CONFIGURATION (B200 Optimized)")
    print("=" * 60)
    print(f"  GPU: {torch.cuda.get_device_name(0)}")
    print(f"  GPU Memory: {torch.cuda.get_device_properties(0).total_memory / 1e9:.0f} GB")
    print(f"  Batch size: {batch_size}")
    print(f"  Gradient accumulation: 1 (not needed with B200)")
    print(f"  Effective batch size: {batch_size}")
    print(f"  Max sequence length: {model.max_seq_length}")
    print(f"  Loss function: CachedMultipleNegativesRankingLoss")
    print(f"  Mini-batch size (for loss): {mini_batch_size}")
    print(f"  In-batch negatives: {in_batch_negatives}")
    print(f"  Hard negatives: {max_hard_negatives}")
    print(f"  Total negatives per sample: {total_negatives}")
    print(f"  Epochs: {epochs}")
    print(f"  Learning rate: {learning_rate}")
    print(f"  Warmup steps: {warmup_steps}")
    print(f"  Steps per epoch: {steps_per_epoch}")
    print(f"  Total steps: {total_steps}")
    print(f"  Training examples: {len(train_dataset)}")
    print(f"  Precision: {'bf16' if use_bf16 else 'fp16'}")
    print("=" * 60)

    # Create loss function
    train_loss = CachedMultipleNegativesRankingLoss(
        model,
        mini_batch_size=mini_batch_size,
    )

    # Training arguments
    training_args = SentenceTransformerTrainingArguments(
        output_dir=output_dir,
        num_train_epochs=epochs,
        per_device_train_batch_size=batch_size,
        gradient_accumulation_steps=1,  # Not needed with B200
        learning_rate=learning_rate,
        warmup_steps=warmup_steps,
        bf16=use_bf16,
        fp16=not use_bf16,
        logging_steps=10,
        save_strategy="epoch",
        save_total_limit=2,  # Keep 2 checkpoints
        batch_sampler=BatchSamplers.NO_DUPLICATES,
        report_to="none",  # Disable wandb/tensorboard
        dataloader_num_workers=4,  # Speed up data loading
    )

    # Create trainer
    trainer = SentenceTransformerTrainer(
        model=model,
        args=training_args,
        train_dataset=train_dataset,
        loss=train_loss,
    )

    print("\n" + "=" * 60)
    print("STARTING TRAINING")
    print("=" * 60)
    print(f"\nIMPORTANT: Initial loss should be ~2.5-4.0 (NOT 0.000000)")
    print("If loss = 0.000, stop training - something is wrong.\n")

    # Train with memory monitoring
    try:
        trainer.train()
    except RuntimeError as e:
        if "out of memory" in str(e).lower():
            print("\n" + "=" * 60)
            print("ERROR: GPU OUT OF MEMORY")
            print("=" * 60)
            print(f"Current memory usage: {get_memory_usage():.1f}GB")
            print("\nTry reducing these parameters:")
            print("  --batch-size 32")
            print("  --max-seq-length 384")
            print("=" * 60)
        raise

    # Check memory after training
    print("\n" + "=" * 60)
    print("TRAINING COMPLETE")
    print("=" * 60)
    print_memory_status("(final)")

    # Memory safety check
    final_memory = get_memory_usage()
    if final_memory > memory_limit_gb and not skip_memory_check:
        print(f"\nWARNING: Memory usage ({final_memory:.1f}GB) exceeded limit ({memory_limit_gb}GB)")
        print("Consider reducing batch size or sequence length for production.")

    # Save the final model
    print(f"\nSaving model to: {output_dir}")
    model.save(output_dir)

    return trainer


def save_training_config(
    output_dir: str,
    args: argparse.Namespace,
    num_examples: int,
    has_hard_negatives: bool,
    gpu_name: str,
):
    """Save training configuration to JSON file."""
    config = {
        "model_name": args.model_name,
        "base_model": args.model_name,
        "fine_tuned_for": "Arabic Islamic Text Search",
        "batch_size": args.batch_size,
        "gradient_accumulation_steps": 1,
        "effective_batch_size": args.batch_size,
        "max_seq_length": args.max_seq_length,
        "epochs": args.epochs,
        "learning_rate": args.learning_rate,
        "warmup_ratio": DEFAULT_CONFIG["warmup_ratio"],
        "loss_function": "CachedMultipleNegativesRankingLoss",
        "mini_batch_size": DEFAULT_CONFIG["mini_batch_size"],
        "hard_negatives_per_query": args.max_hard_negatives,
        "in_batch_negatives": args.batch_size - 1,
        "total_negatives_per_sample": args.batch_size - 1 + args.max_hard_negatives,
        "num_training_examples": num_examples,
        "has_hard_negatives": has_hard_negatives,
        "precision": "bf16",
        "gpu": gpu_name,
        "optimization_target": "B200-192GB",
        "training_date": datetime.now().isoformat(),
        "seed": args.seed,
        "target_metrics": {
            "precision_at_5": "> 0.85",
            "mrr": "> 0.80",
            "false_positive_rate": "< 15%"
        }
    }

    config_path = os.path.join(output_dir, "training_config.json")
    with open(config_path, 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=2, ensure_ascii=False)

    print(f"\nTraining configuration saved to: {config_path}")
    return config


def test_model(model: SentenceTransformer):
    """Quick test of the fine-tuned model."""
    print("\n" + "=" * 60)
    print("MODEL TESTING")
    print("=" * 60)

    test_queries = [
        # Arabic queries
        "إنما الأعمال بالنيات",  # Actions are by intentions
        "الصلاة في وقتها",  # Prayer on time
        "آية الكرسي",  # Ayat al-Kursi
        "ما حكم الصيام في رمضان؟",  # What is the ruling on fasting in Ramadan?
        # English queries
        "What is the reward for patience?",
        "hadith about charity",
        "fasting in Ramadan",
        "importance of good intentions",
    ]

    print("\nEncoding test queries:")
    for query in test_queries:
        embedding = model.encode(query)
        print(f"  '{query[:40]}...' → {len(embedding)}-dim vector")

    # Test semantic similarity
    print("\nSemantic similarity test:")

    def cosine_similarity(a, b):
        return np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b))

    test_pairs = [
        # Should be similar
        ("Actions are judged by intentions", "إنما الأعمال بالنيات"),
        ("What does Islam say about patience?", "الصبر في الإسلام"),
        # Should be less similar
        ("Actions are judged by intentions", "الصلاة في وقتها"),
    ]

    for q1, q2 in test_pairs:
        e1 = model.encode(q1)
        e2 = model.encode(q2)
        sim = cosine_similarity(e1, e2)
        q1_short = q1[:25] + "..." if len(q1) > 25 else q1
        q2_short = q2[:25] + "..." if len(q2) > 25 else q2
        print(f"  '{q1_short}' vs '{q2_short}': {sim:.4f}")


def evaluate_model(model: SentenceTransformer, eval_file: str, training_file: str):
    """
    Evaluate model on gold standard queries.

    Note: This is a simplified evaluation using similarity scores.
    For full Precision@K and MRR, use the TypeScript evaluation script
    with your Qdrant index.
    """
    if not os.path.exists(eval_file):
        print(f"\nEvaluation file not found: {eval_file}")
        return None

    print("\n" + "=" * 60)
    print("GOLD STANDARD EVALUATION")
    print("=" * 60)

    # Load evaluation queries
    eval_queries = []
    with open(eval_file, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip():
                eval_queries.append(json.loads(line))

    print(f"\nLoaded {len(eval_queries)} evaluation queries")

    # Load corpus from training data
    corpus = []
    with open(training_file, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip():
                data = json.loads(line)
                for pos in data.get('pos', []):
                    if pos not in corpus:
                        corpus.append(pos)

    print(f"Corpus size: {len(corpus)} passages")
    print("Encoding corpus (this may take a few minutes)...")

    # Encode corpus
    corpus_embeddings = model.encode(corpus, show_progress_bar=True, batch_size=64)

    # Evaluate each query
    from collections import defaultdict
    results = {
        'top1_similarity': [],
        'top5_similarity': [],
        'top10_similarity': [],
        'by_category': defaultdict(list),
        'by_difficulty': defaultdict(list),
    }

    print("\nEvaluating queries...")
    for eq in eval_queries:
        query = eq['query']
        category = eq.get('category', 'unknown')
        difficulty = eq.get('difficulty', 'medium')

        # Encode query
        query_embedding = model.encode(query)

        # Compute similarities
        similarities = np.dot(corpus_embeddings, query_embedding)
        top_indices = np.argsort(similarities)[::-1][:10]
        top_sims = [similarities[i] for i in top_indices]

        # Record metrics
        results['top1_similarity'].append(top_sims[0] if top_sims else 0)
        results['top5_similarity'].append(np.mean(top_sims[:5]))
        results['top10_similarity'].append(np.mean(top_sims[:10]))
        results['by_category'][category].append(top_sims[0] if top_sims else 0)
        results['by_difficulty'][difficulty].append(top_sims[0] if top_sims else 0)

    # Print results
    print("\n" + "-" * 40)
    print("EVALUATION RESULTS (similarity-based)")
    print("-" * 40)
    print(f"\nOverall Metrics:")
    print(f"  Avg Top-1 Similarity:  {np.mean(results['top1_similarity']):.4f}")
    print(f"  Avg Top-5 Similarity:  {np.mean(results['top5_similarity']):.4f}")
    print(f"  Avg Top-10 Similarity: {np.mean(results['top10_similarity']):.4f}")

    if results['by_category']:
        print(f"\nBy Category:")
        for cat, scores in sorted(results['by_category'].items()):
            print(f"  {cat}: {np.mean(scores):.4f} (n={len(scores)})")

    if results['by_difficulty']:
        print(f"\nBy Difficulty:")
        for diff, scores in sorted(results['by_difficulty'].items()):
            print(f"  {diff}: {np.mean(scores):.4f} (n={len(scores)})")

    print("\n" + "-" * 40)
    print("Note: For full Precision@K and MRR evaluation, run:")
    print("  bun run training/scripts/evaluate-precision.ts --model=bge-m3")
    print("-" * 40)

    return results


def main():
    """Main entry point."""
    args = parse_args()

    print("\n" + "=" * 60)
    print("BGE-M3 FINE-TUNING FOR ARABIC ISLAMIC TEXTS")
    print("Optimized for NVIDIA B200 (192GB VRAM)")
    print("=" * 60)

    # Set random seed
    random.seed(args.seed)
    np.random.seed(args.seed)
    torch.manual_seed(args.seed)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(args.seed)

    # Check GPU
    gpu_name, gpu_memory_gb = check_gpu()

    # Adjust settings for smaller GPUs
    if gpu_memory_gb < 80:
        print(f"\nAdjusting settings for {gpu_memory_gb:.0f}GB GPU...")
        if args.batch_size == DEFAULT_CONFIG["batch_size"]:
            args.batch_size = 8
            print(f"  Reduced batch size to: {args.batch_size}")
        if args.max_seq_length == DEFAULT_CONFIG["max_seq_length"]:
            args.max_seq_length = 256
            print(f"  Reduced max sequence length to: {args.max_seq_length}")

    # Load training data
    train_examples, has_hard_negatives = load_training_data(
        args.training_file,
        max_negatives=args.max_hard_negatives
    )

    if not train_examples:
        print("ERROR: No training examples loaded")
        sys.exit(1)

    # Shuffle training data
    random.shuffle(train_examples)

    # Convert to dataset
    print("\nConverting to HuggingFace Dataset...")
    train_dataset = convert_to_dataset(train_examples)
    print(f"  Columns: {train_dataset.column_names}")
    print(f"  Size: {len(train_dataset)} examples")

    # Load model
    model = load_model(
        args.model_name,
        args.max_seq_length,
        device="cuda"
    )

    # Train
    trainer = train_model(
        model=model,
        train_dataset=train_dataset,
        output_dir=args.output_dir,
        batch_size=args.batch_size,
        epochs=args.epochs,
        learning_rate=args.learning_rate,
        warmup_ratio=DEFAULT_CONFIG["warmup_ratio"],
        mini_batch_size=min(args.batch_size, DEFAULT_CONFIG["mini_batch_size"]),
        max_hard_negatives=args.max_hard_negatives,
        use_bf16=True,
        memory_limit_gb=DEFAULT_CONFIG["memory_limit_gb"],
        skip_memory_check=args.skip_memory_check,
    )

    # Save configuration
    save_training_config(
        args.output_dir,
        args,
        len(train_dataset),
        has_hard_negatives,
        gpu_name
    )

    # Test the model
    test_model(model)

    # Evaluate if eval file provided
    if args.eval_file:
        evaluate_model(model, args.eval_file, args.training_file)

    # Final instructions
    print("\n" + "=" * 60)
    print("NEXT STEPS")
    print("=" * 60)
    print(f"""
1. Download the model from: {args.output_dir}
   - Use RunPod's file browser, or
   - zip -r model.zip {args.output_dir} && download model.zip

2. On your local machine:
   - Extract to: training/outputs/arabic-islamic-bge-m3/

3. Start embedding server with custom weights:
   CUSTOM_WEIGHTS_PATH=./training/outputs/arabic-islamic-bge-m3 python embedding-server/main.py

4. Regenerate embeddings:
   bun run scripts/generate-embeddings.ts --model=bge-m3

5. Evaluate results:
   bun run training/scripts/evaluate-precision.ts --model=bge-m3
""")
    print("=" * 60)


if __name__ == "__main__":
    main()
