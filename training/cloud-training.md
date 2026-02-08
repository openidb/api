# Cloud GPU Training Guide

## Quick Comparison

| Provider | GPU | VRAM | Cost/hr | Best For |
|----------|-----|------|---------|----------|
| Google Colab | T4 | 16GB | Free/$10/mo | Quick tests |
| RunPod | RTX 4090 | 24GB | $0.44 | Best value |
| RunPod | A100 40GB | 40GB | $1.09 | Faster training |
| Lambda Labs | A100 40GB | 40GB | $1.10 | Reliable |
| Vast.ai | RTX 4090 | 24GB | $0.30-0.50 | Cheapest |
| Modal | A100 40GB | 40GB | $1.10 | Python-native |

**Recommended**: RunPod RTX 4090 (~$0.44/hr) - training takes ~2-4 hours = **~$1-2 total**

---

## Option 1: RunPod (Recommended)

### Step 1: Setup Account
1. Go to https://runpod.io
2. Create account and add credits ($10 minimum)
3. Go to "Pods" → "Deploy"

### Step 2: Deploy Pod
```
Template: RunPod Pytorch 2.1
GPU: RTX 4090 (24GB) - $0.44/hr
     or A100 40GB - $1.09/hr for larger batches
Container Disk: 20GB
Volume Disk: 50GB (for model weights)
```

### Step 3: Connect & Setup
```bash
# SSH into pod (connection string shown in RunPod dashboard)
ssh root@<pod-ip> -p <port>

# Install dependencies
pip install sentence-transformers>=3.0.0 accelerate datasets transformers

# Create training directory
mkdir -p /workspace/training && cd /workspace/training
```

### Step 4: Upload Training Data (from your local machine)
```bash
# Get connection details from RunPod dashboard
POD_IP="xxx.xxx.xxx.xxx"
POD_PORT="22"

# Upload files
scp -P $POD_PORT training/data/combined_training.jsonl root@$POD_IP:/workspace/training/
scp -P $POD_PORT training/data/gold_standard_evaluation.jsonl root@$POD_IP:/workspace/training/
scp -P $POD_PORT training/scripts/train_bge_m3.py root@$POD_IP:/workspace/training/
```

### Step 5: Run Training (on the pod)
```bash
cd /workspace/training

# Basic training
python train_bge_m3.py \
    --data-file=combined_training.jsonl \
    --output-dir=outputs/arabic-islamic-bge-m3 \
    --batch-size=16 \
    --epochs=2 \
    --lr=1e-5

# With evaluation and early stopping
python train_bge_m3.py \
    --data-file=combined_training.jsonl \
    --eval-file=gold_standard_evaluation.jsonl \
    --early-stop \
    --eval-steps=500
```

### Step 6: Download Results (to your local machine)
```bash
scp -P $POD_PORT -r root@$POD_IP:/workspace/training/outputs ./training/
```

### Step 7: Stop Pod
**Important**: Stop or terminate the pod when done to avoid charges!

---

## Option 2: Google Colab Pro

### Upload Notebook
Use the existing `BGE_M3_Finetune_Colab.ipynb` or create a new one:

```python
# Cell 1: Setup
!pip install sentence-transformers>=3.0.0 accelerate datasets

# Cell 2: Upload files
from google.colab import files
uploaded = files.upload()  # Upload combined_training.jsonl

# Cell 3: Training
import json
from sentence_transformers import SentenceTransformer, InputExample, losses
from torch.utils.data import DataLoader

# Load data
examples = []
with open('combined_training.jsonl') as f:
    for line in f:
        data = json.loads(line)
        if data.get('neg'):
            texts = [data['query'], data['pos'][0]] + data['neg'][:3]
        else:
            texts = [data['query'], data['pos'][0]]
        examples.append(InputExample(texts=texts))

print(f"Loaded {len(examples)} examples")

# Load model
model = SentenceTransformer('BAAI/bge-m3')

# Train
train_dataloader = DataLoader(examples, shuffle=True, batch_size=16)
train_loss = losses.MultipleNegativesRankingLoss(model)

model.fit(
    train_objectives=[(train_dataloader, train_loss)],
    epochs=2,
    warmup_steps=100,
    output_path='./arabic-islamic-bge-m3',
    show_progress_bar=True,
    checkpoint_save_steps=500,
)

# Cell 4: Download model
!zip -r model.zip arabic-islamic-bge-m3
files.download('model.zip')
```

---

## Option 3: Modal (Python-Native)

Great if you prefer running everything from your local machine.

### Install Modal
```bash
pip install modal
modal setup  # Login with browser
```

### Create Training Script
```python
# modal_train.py
import modal

app = modal.App("bge-m3-training")

# Define image with dependencies
image = modal.Image.debian_slim().pip_install(
    "sentence-transformers>=3.0.0",
    "accelerate",
    "datasets",
    "transformers",
    "torch>=2.0.0",
)

# Mount local data
training_data = modal.Mount.from_local_dir(
    "training/data",
    remote_path="/data"
)

@app.function(
    image=image,
    gpu="A100",  # or "T4" for cheaper
    timeout=14400,  # 4 hours
    mounts=[training_data],
)
def train_model():
    import json
    from sentence_transformers import SentenceTransformer, InputExample, losses
    from torch.utils.data import DataLoader

    # Load data
    examples = []
    with open('/data/combined_training.jsonl') as f:
        for line in f:
            data = json.loads(line)
            if data.get('neg'):
                texts = [data['query'], data['pos'][0]] + data['neg'][:3]
            else:
                texts = [data['query'], data['pos'][0]]
            examples.append(InputExample(texts=texts))

    print(f"Loaded {len(examples)} examples")

    # Train
    model = SentenceTransformer('BAAI/bge-m3')
    train_dataloader = DataLoader(examples, shuffle=True, batch_size=16)
    train_loss = losses.MultipleNegativesRankingLoss(model)

    model.fit(
        train_objectives=[(train_dataloader, train_loss)],
        epochs=2,
        warmup_steps=100,
        output_path='/data/outputs/arabic-islamic-bge-m3',
        show_progress_bar=True,
    )

    return "Training complete!"

@app.local_entrypoint()
def main():
    result = train_model.remote()
    print(result)
```

### Run
```bash
modal run modal_train.py
```

---

## Option 4: Vast.ai (Cheapest)

### Step 1: Setup
1. Go to https://vast.ai
2. Create account, add credits
3. Search for RTX 4090 instances (~$0.30-0.50/hr)

### Step 2: Deploy
- Select a machine with PyTorch image
- SSH connection provided in dashboard

### Step 3: Same as RunPod
```bash
# SSH in and run same commands as RunPod
pip install sentence-transformers accelerate datasets
# ... upload data and train
```

---

## Batch Size Recommendations by GPU

| GPU | VRAM | Max Batch Size | Recommended |
|-----|------|----------------|-------------|
| T4 | 16GB | 8-16 | 8 |
| RTX 4090 | 24GB | 16-24 | 16 |
| A100 40GB | 40GB | 32-48 | 32 |
| A100 80GB | 80GB | 64-96 | 64 |
| H100 80GB | 80GB | 64-128 | 64 |

**Note**: Smaller batch sizes = harder in-batch negatives = better precision (but slower)

---

## Training Time Estimates

For ~60K training examples:

| GPU | Batch Size | Time (2 epochs) |
|-----|------------|-----------------|
| T4 | 8 | ~6-8 hours |
| RTX 4090 | 16 | ~2-3 hours |
| A100 40GB | 32 | ~1-2 hours |

---

## Cost Estimates

| Provider | GPU | Time | Total Cost |
|----------|-----|------|------------|
| Colab Free | T4 | 6-8h | Free |
| Colab Pro | T4 | 6-8h | $10/mo |
| RunPod | RTX 4090 | 2-3h | ~$1.50 |
| RunPod | A100 | 1-2h | ~$2 |
| Vast.ai | RTX 4090 | 2-3h | ~$1 |

---

## After Training: Deploy the Model

### 1. Download the model
```bash
# From cloud to local
scp -r root@<pod>:/workspace/training/outputs/arabic-islamic-bge-m3 ./
```

### 2. Update embedding server
```bash
# In embedding-server/.env or environment
export CUSTOM_WEIGHTS_PATH="/path/to/arabic-islamic-bge-m3"
```

### 3. Restart embedding server
```bash
cd embedding-server
python main.py
```

### 4. Regenerate embeddings
```bash
bun run pipelines/embed/generate-embeddings.ts --model=bge-m3 --collection=quran
bun run pipelines/embed/generate-embeddings.ts --model=bge-m3 --collection=hadith
```

### 5. Evaluate
```bash
bun run training/scripts/evaluate-precision.ts --model=bge-m3
```
