# BGE-M3 Training for Arabic Islamic Texts

This directory contains scripts to generate training data and fine-tune BGE-M3 on Arabic Islamic texts with a **precision-focused** strategy.

## Overview

**Strategy**: Quality over quantity - 60K high-quality pairs with verified hard negatives

**Target Metrics**:
- Precision@5 > 0.85
- MRR > 0.80
- False Positive Rate < 15%

## Quick Start (Precision-Optimized Pipeline)

```bash
# 1. FIRST: Establish baseline with gold standard evaluation
bun run training/scripts/evaluate-precision.ts --model=gemini

# 2. Mine hard negatives with similarity thresholds (~$5)
bun run training/scripts/mine-hard-negatives.ts --export-review

# 3. Review borderline cases (manual, ~3 hours)
# Edit training/data/borderline_review.json

# 4. Generate stratified synthetic queries (~$5)
bun run training/scripts/generate-synthetic-queries.ts --pilot
bun run training/scripts/generate-synthetic-queries.ts  # stratified by default

# 5. Combine and train
bun run training/scripts/combine-training-data.ts
python training/scripts/train_bge_m3.py --early-stop

# 6. Evaluate fine-tuned model
bun run training/scripts/evaluate-precision.ts --model=bge-m3

# 7. Analyze errors and iterate
bun run training/scripts/analyze-errors.ts
```

## Prerequisites

### Environment Variables

Create `.env` in `web/`:

```bash
DATABASE_URL="postgresql://postgres:password@localhost:5432/arabic_texts_library"
OPENROUTER_API_KEY="sk-or-v1-..."
QDRANT_URL="http://localhost:6333"
```

### Running Services

- **PostgreSQL**: Quran and Hadith data
- **Qdrant**: Vector database with Gemini embeddings

## Precision-Focused Training Data Pipeline

### Phase 1: Evaluation First (CRITICAL)

Always establish baseline metrics before training:

```bash
bun run training/scripts/evaluate-precision.ts --model=gemini
```

This creates `evaluation_report.json` with baseline Precision@K, MRR, and failure analysis.

### Phase 2: Strategic Hard Negative Mining

**Similarity Thresholds**:
```
0.90 - 1.00: EXCLUDE (likely duplicate/relevant)
0.85 - 0.90: MANUAL REVIEW REQUIRED
0.65 - 0.85: IDEAL HARD NEGATIVES ← Target zone
0.50 - 0.65: SEMI-HARD (useful for diversity)
< 0.50:      TOO EASY (in-batch sufficient)
```

```bash
bun run training/scripts/mine-hard-negatives.ts [options]

Options:
  --source=quran|hadith|all   Source type (default: all)
  --negatives=N               Hard negatives per query (default: 3)
  --min-sim=0.50              Minimum similarity threshold
  --max-sim=0.85              Maximum similarity threshold
  --review-threshold=0.80     Flag borderline cases above this
  --export-review             Export borderline cases for manual review
  --dry-run                   Show what would be done
```

### Phase 3: Stratified Synthetic Query Generation

Generates 3 high-quality queries (not 5 generic ones) for strategically sampled passages:

**Quran (1,000 of 5,625)**:
- 200 short ayahs (need most help)
- 200 famous/commonly searched
- 200 with key Islamic terms
- 200 narrative passages
- 200 random

**Hadith (4,000 of 46,674)**:
- 500 from 40 Nawawi
- 500 fiqh-related
- 500 aqeedah-related
- 500 ethical teachings
- 1000 long hadiths
- 1000 random

```bash
bun run training/scripts/generate-synthetic-queries.ts [options]

Options:
  --pilot              200 passages (~$0.20)
  --stratified         5,000 selected passages (~$5) [default]
  --full               All passages (~$45)
  --dry-run            Preview without API calls
```

### Phase 4: Training

```bash
python training/scripts/train_bge_m3.py [options]

Precision-Optimized Defaults:
  --batch-size=16      Smaller = harder in-batch negatives
  --epochs=2           More epochs with quality data
  --lr=1e-5            Conservative learning rate
  --warmup=0.1         10% warmup ratio
  --hard-negatives=3   Use all mined negatives
  --eval-steps=500     Evaluate checkpoints frequently
  --early-stop         Stop if no improvement

Options:
  --eval-file=<path>   Gold standard for checkpoint evaluation
  --use-matryoshka     Multi-dimension embeddings
  --resume=<path>      Resume from checkpoint
```

### Phase 5: Error Analysis and Iteration

```bash
bun run training/scripts/analyze-errors.ts

Error Categories:
  - short_query: Very short queries lacking context
  - transliteration: English transliteration not matching Arabic
  - source_confusion: Quran queries returning hadith or vice versa
  - adjacent_passage: Retrieving nearby ayahs instead of target
  - msa_classical: MSA queries missing classical Arabic text
  - similar_vocab_wrong_topic: Same vocabulary, different meaning
```

## Scripts Reference

| Script | Purpose | Cost |
|--------|---------|------|
| `evaluate-precision.ts` | Compute P@K, MRR, NDCG on gold standard | Free |
| `analyze-errors.ts` | Categorize failures, generate targeted training | Free |
| `mine-hard-negatives.ts` | Find similar-but-wrong passages | ~$5 |
| `generate-synthetic-queries.ts` | LLM-generated diverse queries | ~$5 (stratified) |
| `generate-arabic-paraphrases.ts` | MSA paraphrases | ~$3 |
| `combine-training-data.ts` | Merge and deduplicate | Free |
| `validate-training-data.ts` | Quality assurance | Free |
| `train_bge_m3.py` | Fine-tune model | GPU time |

## Expected Training Data

| Data Type | Count | Purpose |
|-----------|-------|---------|
| Original translation pairs | 52,299 | Cross-lingual baseline |
| + Hard negatives | 52,299 × 3 | Discrimination |
| Synthetic queries (stratified) | ~15,000 | Query diversity |
| Arabic paraphrases | ~2,500 | MSA↔Classical bridging |
| **Total pairs** | **~60,000** | Quality-focused |

## Cost Summary

| Task | Method | Cost |
|------|--------|------|
| Gold standard creation | Manual | ~8 hours |
| Hard negative mining | Gemini embeddings | ~$5 |
| Borderline review | Manual | ~3 hours |
| Synthetic queries (stratified) | Gemini Flash | ~$5 |
| Arabic paraphrases | Gemini Flash | ~$3 |
| **Total API** | | **~$13** |

## File Structure

```
training/
├── data/
│   ├── gold_standard_evaluation.jsonl  # 200 curated test queries
│   ├── quran_pairs.jsonl               # Original Quran pairs
│   ├── hadith_pairs.jsonl              # Original Hadith pairs
│   ├── quran_pairs_negatives.jsonl     # With hard negatives + scores
│   ├── hadith_pairs_negatives.jsonl    # With hard negatives + scores
│   ├── borderline_review.json          # Cases needing manual review
│   ├── synthetic_queries.jsonl         # Stratified LLM queries
│   ├── arabic_paraphrases.jsonl        # MSA paraphrases
│   ├── targeted_training.jsonl         # Error-driven additions
│   ├── combined_training.jsonl         # Final merged data
│   ├── evaluation_report.json          # Precision metrics
│   └── error_analysis.json             # Failure categorization
├── scripts/
│   ├── evaluate-precision.ts           # NEW: Compute P@K, MRR, NDCG
│   ├── analyze-errors.ts               # NEW: Categorize failures
│   ├── mine-hard-negatives.ts          # UPDATED: Similarity thresholds
│   ├── generate-synthetic-queries.ts   # UPDATED: Stratified sampling
│   ├── train_bge_m3.py                 # UPDATED: Precision hyperparams
│   └── ...
├── outputs/
│   └── arabic-islamic-bge-m3/
│       └── checkpoints/                # Checkpoint models
├── config/                             # Training configurations
└── README.md
```

## Gold Standard Evaluation Set

The file `gold_standard_evaluation.jsonl` contains 200 manually curated queries:

| Category | Count | Examples |
|----------|-------|----------|
| Quran - Famous Verses | 30 | Ayat al-Kursi, Al-Fatiha |
| Quran - Topical | 30 | "patience in Quran", "charity verses" |
| Hadith - Famous | 30 | 40 Nawawi hadiths |
| Hadith - Topical | 30 | "ruling on backbiting" |
| Cross-collection | 20 | Same hadith in Bukhari vs Muslim |
| Edge Cases | 40 | Short queries, transliteration |
| Arabic queries | 20 | فصحى queries |

## Troubleshooting

### "OPENROUTER_API_KEY not set"
```bash
export OPENROUTER_API_KEY="sk-or-v1-..."
```

### "Database connection failed"
```bash
brew services start postgresql
```

### "Qdrant not available"
```bash
docker run -p 6333:6333 qdrant/qdrant
```

### Low precision after training
1. Run `analyze-errors.ts` to identify failure patterns
2. Create targeted training pairs for specific error categories
3. Add to training data and retrain
4. Iterate until metrics improve

## Iteration Schedule

| Week | Focus | Additions |
|------|-------|-----------|
| 1 | Baseline + hard negatives | 52K + 157K neg |
| 2 | Error analysis + fixes | +2K targeted |
| 3 | Synthetic queries (pilot) | +5K synthetic |
| 4 | Arabic paraphrases | +2.5K paraphrases |
| 5 | Final error analysis | +1K targeted |
