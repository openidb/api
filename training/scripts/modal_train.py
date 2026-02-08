"""
Modal Cloud Training Script for BGE-M3

Run training on Modal's cloud GPUs directly from your local machine.
No SSH required - just run and results are synced automatically.

Setup:
    pip install modal
    modal setup  # Login via browser

Usage:
    # Prepare data locally first
    bun run training/scripts/combine-training-data.ts

    # Run training on cloud GPU
    modal run training/scripts/modal_train.py

    # Run with custom settings
    modal run training/scripts/modal_train.py --batch-size 32 --epochs 3

Cost: ~$2-4 for full training on A100
"""

import modal
import os
from pathlib import Path

# Modal app setup
app = modal.App("arabic-islamic-bge-m3")

# Define the container image with all dependencies
training_image = modal.Image.debian_slim(python_version="3.11").pip_install(
    "torch>=2.0.0",
    "sentence-transformers>=3.0.0",
    "accelerate>=0.21.0",
    "datasets>=2.14.0",
    "transformers>=4.36.0",
    "tqdm",
)

# Get the training data directory
TRAINING_DIR = Path(__file__).parent.parent
DATA_DIR = TRAINING_DIR / "data"

# Create volume for persisting model outputs
model_volume = modal.Volume.from_name("bge-m3-outputs", create_if_missing=True)


@app.function(
    image=training_image,
    gpu="A100",  # Options: "T4", "A10G", "A100", "H100"
    timeout=14400,  # 4 hours max
    volumes={"/outputs": model_volume},
)
def train_bge_m3(
    training_data: bytes,
    eval_data: bytes = None,
    batch_size: int = 16,
    epochs: int = 2,
    learning_rate: float = 1e-5,
    hard_negatives: int = 3,
    warmup_ratio: float = 0.1,
):
    """Train BGE-M3 on Modal cloud GPU."""
    import json
    import random
    from sentence_transformers import SentenceTransformer, InputExample, losses
    from torch.utils.data import DataLoader

    print("=" * 60)
    print("BGE-M3 Fine-tuning on Modal Cloud GPU")
    print("=" * 60)

    # Decode training data
    training_lines = training_data.decode("utf-8").strip().split("\n")
    print(f"Received {len(training_lines)} training examples")

    # Parse training data
    examples = []
    has_negatives = False

    for line in training_lines:
        if not line.strip():
            continue
        try:
            data = json.loads(line)
            query = data.get("query", "")
            positives = data.get("pos", [])
            negatives = data.get("neg", [])

            if not query or not positives:
                continue

            if negatives:
                has_negatives = True

            for pos in positives:
                if negatives:
                    texts = [query, pos] + negatives[:hard_negatives]
                else:
                    texts = [query, pos]
                examples.append(InputExample(texts=texts))
        except json.JSONDecodeError:
            continue

    print(f"Parsed {len(examples)} training examples")
    print(f"Hard negatives present: {has_negatives}")

    # Shuffle
    random.shuffle(examples)

    # Load model
    print("\nLoading BAAI/bge-m3 model...")
    model = SentenceTransformer("BAAI/bge-m3")
    print(f"Model embedding dimension: {model.get_sentence_embedding_dimension()}")

    # Create dataloader
    train_dataloader = DataLoader(examples, shuffle=True, batch_size=batch_size)

    # Loss function
    train_loss = losses.MultipleNegativesRankingLoss(model)

    # Calculate warmup
    total_steps = len(train_dataloader) * epochs
    warmup_steps = int(total_steps * warmup_ratio)

    # Print config
    print(f"\nTraining configuration:")
    print(f"  Batch size: {batch_size}")
    print(f"  Epochs: {epochs}")
    print(f"  Learning rate: {learning_rate}")
    print(f"  Warmup steps: {warmup_steps}")
    print(f"  Total steps: {total_steps}")
    print(f"  Hard negatives: {hard_negatives if has_negatives else 'None'}")

    # Train
    print("\nStarting training...")
    output_path = "/outputs/arabic-islamic-bge-m3"

    model.fit(
        train_objectives=[(train_dataloader, train_loss)],
        epochs=epochs,
        warmup_steps=warmup_steps,
        output_path=output_path,
        show_progress_bar=True,
        checkpoint_save_steps=500,
        checkpoint_path=f"{output_path}/checkpoints",
        optimizer_params={"lr": learning_rate},
    )

    print(f"\nTraining complete!")
    print(f"Model saved to volume at: {output_path}")

    # Test the model
    print("\nTesting fine-tuned model...")
    test_queries = [
        "إنما الأعمال بالنيات",
        "What is the reward for patience?",
        "آية الكرسي",
        "pillars of Islam",
    ]

    for query in test_queries:
        embedding = model.encode(query)
        print(f"  '{query[:30]}...' -> {len(embedding)}-dim")

    # Save config
    config = {
        "batch_size": batch_size,
        "epochs": epochs,
        "learning_rate": learning_rate,
        "hard_negatives": hard_negatives,
        "warmup_ratio": warmup_ratio,
        "num_examples": len(examples),
        "has_negatives": has_negatives,
    }
    with open(f"{output_path}/training_config.json", "w") as f:
        json.dump(config, f, indent=2)

    return {
        "status": "success",
        "examples_trained": len(examples),
        "output_path": output_path,
    }


@app.function(
    image=training_image,
    volumes={"/outputs": model_volume},
)
def download_model():
    """Download the trained model from Modal volume."""
    import shutil
    import os

    model_path = "/outputs/arabic-islamic-bge-m3"
    if not os.path.exists(model_path):
        return {"error": "Model not found. Run training first."}

    # Create tarball
    shutil.make_archive("/tmp/model", "zip", model_path)

    with open("/tmp/model.zip", "rb") as f:
        return f.read()


@app.local_entrypoint()
def main(
    batch_size: int = 16,
    epochs: int = 2,
    lr: float = 1e-5,
    hard_negatives: int = 3,
):
    """Main entry point - runs from local machine."""
    import sys

    # Check for training data
    combined_file = DATA_DIR / "combined_training.jsonl"
    eval_file = DATA_DIR / "gold_standard_evaluation.jsonl"

    if not combined_file.exists():
        print(f"Error: Training data not found at {combined_file}")
        print("Run: bun run training/scripts/combine-training-data.ts")
        sys.exit(1)

    print(f"Loading training data from {combined_file}...")
    with open(combined_file, "rb") as f:
        training_data = f.read()

    eval_data = None
    if eval_file.exists():
        print(f"Loading evaluation data from {eval_file}...")
        with open(eval_file, "rb") as f:
            eval_data = f.read()

    print(f"\nStarting cloud training with:")
    print(f"  Batch size: {batch_size}")
    print(f"  Epochs: {epochs}")
    print(f"  Learning rate: {lr}")
    print(f"  Hard negatives: {hard_negatives}")
    print()

    # Run training on cloud
    result = train_bge_m3.remote(
        training_data=training_data,
        eval_data=eval_data,
        batch_size=batch_size,
        epochs=epochs,
        learning_rate=lr,
        hard_negatives=hard_negatives,
    )

    print("\n" + "=" * 60)
    print("TRAINING COMPLETE")
    print("=" * 60)
    print(f"Status: {result['status']}")
    print(f"Examples trained: {result['examples_trained']}")
    print(f"Output path (on Modal): {result['output_path']}")
    print()
    print("To download the model, run:")
    print("  modal run training/scripts/modal_train.py::download_and_save")


@app.local_entrypoint()
def download_and_save():
    """Download trained model from Modal to local machine."""
    print("Downloading model from Modal...")

    model_bytes = download_model.remote()

    if isinstance(model_bytes, dict) and "error" in model_bytes:
        print(f"Error: {model_bytes['error']}")
        return

    output_path = TRAINING_DIR / "outputs" / "arabic-islamic-bge-m3.zip"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "wb") as f:
        f.write(model_bytes)

    print(f"Model downloaded to: {output_path}")
    print(f"Unzip with: unzip {output_path} -d {output_path.parent}/arabic-islamic-bge-m3")
