#!/bin/bash
# RunPod Setup Script for BGE-M3 Training
#
# Usage:
#   1. Create a RunPod account at https://runpod.io
#   2. Deploy a pod with PyTorch template (RTX 4090 or A100 recommended)
#   3. SSH into the pod and run this script
#
# Estimated costs:
#   - RTX 4090 (24GB): ~$0.44/hr - good for batch_size=16
#   - A100 40GB: ~$1.09/hr - good for batch_size=32+
#   - H100 80GB: ~$2.39/hr - fastest, batch_size=64+

set -e

echo "=========================================="
echo "BGE-M3 Training Setup for RunPod"
echo "=========================================="

# Update system
apt-get update && apt-get install -y git wget curl

# Install Python dependencies
pip install --upgrade pip
pip install \
    torch>=2.0.0 \
    sentence-transformers>=3.0.0 \
    accelerate>=0.21.0 \
    datasets>=2.14.0 \
    transformers>=4.36.0 \
    wandb \
    tqdm

# Create workspace
mkdir -p /workspace/training
cd /workspace/training

echo ""
echo "Setup complete! Next steps:"
echo ""
echo "1. Upload your training data:"
echo "   scp combined_training.jsonl root@<pod-ip>:/workspace/training/"
echo "   scp gold_standard_evaluation.jsonl root@<pod-ip>:/workspace/training/"
echo ""
echo "2. Upload training script:"
echo "   scp train_bge_m3.py root@<pod-ip>:/workspace/training/"
echo ""
echo "3. Run training:"
echo "   python train_bge_m3.py --data-file=combined_training.jsonl"
echo ""
echo "4. Download results:"
echo "   scp -r root@<pod-ip>:/workspace/training/outputs ."
echo ""
