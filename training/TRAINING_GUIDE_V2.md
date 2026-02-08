# BGE-M3 Fine-Tuning Guide v2

This guide documents the improved training pipeline for fine-tuning BGE-M3 on Arabic Islamic texts.

## Problem Summary

The original fine-tuned model **underperformed** both base BGE-M3 and Gemini due to:
- 99% English queries (target: 50% Arabic)
- 0.4% conceptual queries (target: 20%)
- ~70% false negatives in hard negatives
- 87% Hadith vs 11% Quran imbalance

## Improvements Implemented

### Phase 1: Training Data Improvements

1. **Arabic Query Generation** (`scripts/generate-arabic-queries.py`)
   - Generates native Arabic queries using LLM
   - Multiple query types: questions, keywords, conceptual
   - Target: 50% Arabic queries

2. **Conceptual Query Generation** (`scripts/generate-conceptual-queries.py`)
   - Generates thematic queries like "آيات عن الصبر"
   - Covers key Islamic themes
   - Target: 20% conceptual queries

3. **Balanced Data Combination** (`scripts/combine-balanced-training-data.py`)
   - Combines all data sources
   - Balances by language, query type, and source
   - Creates `combined_training_v2.jsonl`

### Phase 2: Hard Negative Improvements

1. **False Negative Filtering** (`scripts/filter-false-negatives.py`)
   - Positive-aware filtering (threshold: 0.95 × positive_score)
   - Jaccard similarity filtering
   - Removes ~70% of false negatives

2. **Improved Mining** (`scripts/mine-hard-negatives-v2.py`)
   - Mines from ranks 10-200 (avoids false negatives in top 10)
   - Mixes hard + semi-hard + random negatives
   - Positive-aware filtering at mining time

### Phase 3: Training Improvements

1. **Matryoshka Loss** - Flexible embedding dimensions (1024, 512, 256, 128)
2. **Cosine LR Schedule** - Better convergence
3. **Weight Decay** - L2 regularization (0.01)
4. **Validation Split** - Early stopping capability
5. **Gradient Accumulation** - Effective batch size of 64

### Phase 5: Evaluation Framework

1. **Gold Standard v2** (`data/gold_standard_v2.jsonl`)
   - 25 exact Arabic queries
   - 15 thematic Arabic queries
   - 15 English cross-lingual queries
   - Covers Quran and Hadith

2. **Automated Evaluation** (`scripts/evaluate-model.ts`)
   - Recall@K metrics
   - MRR (Mean Reciprocal Rank)
   - Breakdown by category, language, difficulty

## Usage Guide

### Step 1: Generate Training Data

```bash
cd training

# 1. Generate Arabic queries (requires OPENROUTER_API_KEY)
python scripts/generate-arabic-queries.py --pilot  # Test with 500 passages
python scripts/generate-arabic-queries.py          # Full run

# 2. Generate conceptual queries
python scripts/generate-conceptual-queries.py --dry-run  # Preview
python scripts/generate-conceptual-queries.py            # Full run

# 3. Analyze current data
python scripts/analyze-training-data.py --all

# Expected output:
#   Arabic queries: 50% ✅
#   Conceptual queries: 20% ✅
#   Quran pairs: 25% ✅
```

### Step 2: Filter and Mine Hard Negatives

```bash
# 1. Filter false negatives from existing data
python scripts/filter-false-negatives.py \
  --input data/combined_training.jsonl \
  --output data/combined_training_filtered.jsonl \
  --threshold 0.95

# 2. Re-mine with improved strategy (optional, requires Qdrant)
python scripts/mine-hard-negatives-v2.py \
  --source quran \
  --start-rank 10 \
  --end-rank 200 \
  --dry-run
```

### Step 3: Combine and Balance Data

```bash
# Combine all data sources with target ratios
python scripts/combine-balanced-training-data.py \
  --arabic-target 0.50 \
  --conceptual-target 0.20 \
  --quran-target 0.25 \
  --output combined_training_v2.jsonl

# Verify balance
python scripts/analyze-training-data.py --input data/combined_training_v2.jsonl
```

### Step 4: Train the Model

```bash
# On RunPod or similar GPU instance
python finetune_bge_m3_v2.py \
  --training-file data/combined_training_v2.jsonl \
  --output-dir ./arabic-islamic-bge-m3-v2 \
  --use-matryoshka \
  --epochs 3 \
  --batch-size 32 \
  --gradient-accumulation 2 \
  --learning-rate 1e-5 \
  --validation-split 0.05
```

### Step 5: Evaluate

```bash
# Single model evaluation
bun run training/scripts/evaluate-model.ts --model=bge-m3 --test-set=gold_standard_v2.jsonl

# Compare all models
bun run training/scripts/evaluate-model.ts --compare-all --output-json=results.json

# Expected metrics:
#   Recall@10: 65%+ (up from 35.7%)
#   MRR: 0.35+ (up from 0.241)
```

## File Structure

```
training/
├── data/
│   ├── combined_training_v2.jsonl     # Balanced training data
│   ├── combined_training_v2_stats.json
│   ├── gold_standard_v2.jsonl         # Improved test set
│   ├── arabic_queries.jsonl           # Generated Arabic queries
│   └── quran_conceptual_queries.jsonl # Thematic queries
├── scripts/
│   ├── generate-arabic-queries.py     # Phase 1.1
│   ├── generate-conceptual-queries.py # Phase 1.2
│   ├── combine-balanced-training-data.py # Phase 1.3
│   ├── filter-false-negatives.py      # Phase 2.1
│   ├── mine-hard-negatives-v2.py      # Phase 2.2
│   ├── analyze-training-data.py       # Analysis
│   └── evaluate-model.ts              # Evaluation
├── finetune_bge_m3_v2.py              # Improved training script
└── TRAINING_GUIDE_V2.md               # This guide
```

## Expected Improvements

| Metric | Before | Target | Method |
|--------|--------|--------|--------|
| Recall@10 | 35.7% | 65%+ | Balanced training data |
| MRR | 0.241 | 0.35+ | Better negatives |
| Arabic queries | 0.7% | 50% | Arabic query generation |
| Conceptual queries | 0.4% | 20% | Thematic generation |
| False negatives | ~70% | <20% | Positive-aware filtering |

## Research References

- [Matryoshka Representation Learning](https://arxiv.org/abs/2205.13147)
- [Contrastive Learning Penalty](https://arxiv.org/html/2412.17364v1)
- [NV-Retriever Hard Negative Mining](https://arxiv.org/pdf/2407.15831)
- [GATE Arabic Embedding](https://arxiv.org/html/2505.24581v1)
- [RocketQA](https://arxiv.org/abs/2010.08191)
