#!/usr/bin/env python3
"""
Filter False Negatives from Training Data

Research shows ~70% of naive hard negatives are actually positives (RocketQA finding).
This script implements positive-aware filtering to remove likely false negatives.

Filtering Strategies:
1. Positive-aware threshold: Remove negatives with score > 0.95 × positive_score
2. Cross-encoder reranking: Use a more accurate model to verify negatives
3. Semantic similarity: Remove negatives that are paraphrases of positives

Based on:
- RocketQA: https://arxiv.org/abs/2010.08191
- Contrastive Learning Penalty: https://arxiv.org/html/2412.17364v1
- NV-Retriever: https://arxiv.org/pdf/2407.15831

Usage:
    python filter-false-negatives.py --input combined_training.jsonl --output filtered_training.jsonl
    python filter-false-negatives.py --threshold 0.95  # Adjust filtering threshold
    python filter-false-negatives.py --analyze-only  # Just show statistics
"""

import argparse
import json
import os
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Optional

# Optional: sentence-transformers for cross-encoder verification
try:
    from sentence_transformers import CrossEncoder
    CROSS_ENCODER_AVAILABLE = True
except ImportError:
    CROSS_ENCODER_AVAILABLE = False


def parse_args():
    parser = argparse.ArgumentParser(description="Filter false negatives from training data")
    parser.add_argument("--input", "-i", type=str, required=True, help="Input JSONL file")
    parser.add_argument("--output", "-o", type=str, help="Output file (default: input_filtered.jsonl)")
    parser.add_argument("--threshold", "-t", type=float, default=0.95,
                        help="Filter negatives with score > threshold × positive_score (default: 0.95)")
    parser.add_argument("--min-similarity", type=float, default=0.50,
                        help="Minimum similarity for valid negatives (default: 0.50)")
    parser.add_argument("--max-similarity", type=float, default=0.85,
                        help="Maximum similarity - above this is too risky (default: 0.85)")
    parser.add_argument("--use-cross-encoder", action="store_true",
                        help="Use cross-encoder for verification (slower but more accurate)")
    parser.add_argument("--cross-encoder-model", type=str, default="cross-encoder/ms-marco-MiniLM-L-6-v2",
                        help="Cross-encoder model to use")
    parser.add_argument("--analyze-only", action="store_true",
                        help="Only analyze without filtering")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show detailed output")
    return parser.parse_args()


def normalize_arabic(text: str) -> str:
    """Normalize Arabic text for comparison."""
    # Remove diacritics
    diacritics = '\u064B\u064C\u064D\u064E\u064F\u0650\u0651\u0652'
    for d in diacritics:
        text = text.replace(d, '')
    # Normalize alef variants
    text = text.replace('أ', 'ا').replace('إ', 'ا').replace('آ', 'ا')
    # Normalize taa marbuta
    text = text.replace('ة', 'ه')
    # Normalize yaa
    text = text.replace('ى', 'ي')
    return text.strip().lower()


def jaccard_similarity(text1: str, text2: str) -> float:
    """Calculate Jaccard similarity between two texts."""
    words1 = set(normalize_arabic(text1).split())
    words2 = set(normalize_arabic(text2).split())

    if not words1 or not words2:
        return 0.0

    intersection = len(words1 & words2)
    union = len(words1 | words2)

    return intersection / union if union > 0 else 0.0


def is_likely_positive(
    negative_text: str,
    positive_texts: list[str],
    negative_score: float,
    positive_score: float = 1.0,
    threshold: float = 0.95,
    jaccard_threshold: float = 0.6
) -> tuple[bool, str]:
    """
    Determine if a negative is likely a false negative (actually positive).

    Returns:
        (is_false_negative, reason)
    """
    # Strategy 1: Score-based filtering
    # If negative score is very close to positive score, it's likely a false negative
    if positive_score > 0:
        relative_score = negative_score / positive_score
        if relative_score > threshold:
            return True, f"score_threshold ({relative_score:.3f} > {threshold})"

    # Strategy 2: High absolute similarity
    if negative_score > 0.90:
        return True, f"high_similarity ({negative_score:.3f})"

    # Strategy 3: Jaccard text similarity
    for pos in positive_texts:
        jaccard = jaccard_similarity(negative_text, pos)
        if jaccard > jaccard_threshold:
            return True, f"jaccard_overlap ({jaccard:.3f})"

    # Strategy 4: Substring match (same text different length)
    neg_norm = normalize_arabic(negative_text)
    for pos in positive_texts:
        pos_norm = normalize_arabic(pos)
        # Check if one contains most of the other
        if len(neg_norm) > 30 and len(pos_norm) > 30:
            shorter = min(neg_norm, pos_norm, key=len)
            longer = max(neg_norm, pos_norm, key=len)
            if shorter in longer:
                return True, "substring_match"

    return False, "valid_negative"


def filter_training_pair(
    pair: dict,
    threshold: float = 0.95,
    min_sim: float = 0.50,
    max_sim: float = 0.85,
    cross_encoder: Optional[object] = None
) -> tuple[dict, dict]:
    """
    Filter false negatives from a single training pair.

    Returns:
        (filtered_pair, stats)
    """
    stats = {
        "original_negatives": 0,
        "filtered_negatives": 0,
        "removed_score_threshold": 0,
        "removed_high_similarity": 0,
        "removed_jaccard": 0,
        "removed_substring": 0,
        "removed_too_easy": 0,
        "removed_cross_encoder": 0,
    }

    negatives = pair.get("neg", [])
    neg_scores = pair.get("neg_scores", [])
    positives = pair.get("pos", [])

    if not negatives:
        return pair, stats

    stats["original_negatives"] = len(negatives)

    # Estimate positive score (usually 1.0 for exact match, or highest neg score + margin)
    positive_score = 1.0
    if neg_scores:
        positive_score = max(max(neg_scores) + 0.1, 1.0)

    filtered_negs = []
    filtered_scores = []

    for i, neg in enumerate(negatives):
        score = neg_scores[i] if i < len(neg_scores) else 0.7  # Default if no score

        # Check if too easy
        if score < min_sim:
            stats["removed_too_easy"] += 1
            continue

        # Check if likely false negative
        is_false_neg, reason = is_likely_positive(
            neg, positives, score, positive_score, threshold
        )

        if is_false_neg:
            if "score_threshold" in reason:
                stats["removed_score_threshold"] += 1
            elif "high_similarity" in reason:
                stats["removed_high_similarity"] += 1
            elif "jaccard" in reason:
                stats["removed_jaccard"] += 1
            elif "substring" in reason:
                stats["removed_substring"] += 1
            continue

        # Optional: Cross-encoder verification
        if cross_encoder is not None and score > max_sim:
            # Use cross-encoder to verify
            query = pair.get("query", "")
            ce_score = cross_encoder.predict([(query, neg)])[0]
            # Cross-encoder scores are typically 0-1, high = relevant
            if ce_score > 0.7:  # Likely relevant
                stats["removed_cross_encoder"] += 1
                continue

        # Keep this negative
        filtered_negs.append(neg)
        filtered_scores.append(score)

    stats["filtered_negatives"] = len(filtered_negs)

    # Create filtered pair
    filtered_pair = {
        **pair,
        "neg": filtered_negs,
        "neg_scores": filtered_scores
    }

    # Remove review flags if no borderline cases remain
    if "needs_review" in filtered_pair:
        if not any(s > 0.80 for s in filtered_scores):
            del filtered_pair["needs_review"]
        if "review_candidates" in filtered_pair:
            del filtered_pair["review_candidates"]

    return filtered_pair, stats


def analyze_negatives(filepath: str) -> dict:
    """Analyze negative quality in training data."""
    analysis = {
        "total_pairs": 0,
        "pairs_with_negatives": 0,
        "total_negatives": 0,
        "score_distribution": defaultdict(int),
        "potentially_false": 0,
        "high_jaccard_overlap": 0,
        "by_source": defaultdict(lambda: {"total": 0, "suspicious": 0}),
    }

    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            if not line.strip():
                continue
            try:
                pair = json.loads(line)
            except json.JSONDecodeError:
                continue

            analysis["total_pairs"] += 1
            source = pair.get("source", "unknown")

            negatives = pair.get("neg", [])
            neg_scores = pair.get("neg_scores", [])
            positives = pair.get("pos", [])

            if negatives:
                analysis["pairs_with_negatives"] += 1
                analysis["total_negatives"] += len(negatives)
                analysis["by_source"][source]["total"] += len(negatives)

            for i, neg in enumerate(negatives):
                score = neg_scores[i] if i < len(neg_scores) else 0.7

                # Score distribution
                if score >= 0.90:
                    analysis["score_distribution"]["0.90-1.00"] += 1
                elif score >= 0.85:
                    analysis["score_distribution"]["0.85-0.90"] += 1
                elif score >= 0.80:
                    analysis["score_distribution"]["0.80-0.85"] += 1
                elif score >= 0.70:
                    analysis["score_distribution"]["0.70-0.80"] += 1
                elif score >= 0.60:
                    analysis["score_distribution"]["0.60-0.70"] += 1
                else:
                    analysis["score_distribution"]["< 0.60"] += 1

                # Check for likely false negatives
                is_false, reason = is_likely_positive(neg, positives, score, 1.0, 0.95)
                if is_false:
                    analysis["potentially_false"] += 1
                    analysis["by_source"][source]["suspicious"] += 1

                # Check jaccard
                for pos in positives:
                    if jaccard_similarity(neg, pos) > 0.6:
                        analysis["high_jaccard_overlap"] += 1
                        break

    return analysis


def print_analysis(analysis: dict):
    """Print negative analysis results."""
    print("\n" + "=" * 60)
    print("NEGATIVE QUALITY ANALYSIS")
    print("=" * 60)

    print(f"\n📊 Overview:")
    print(f"  Total pairs: {analysis['total_pairs']:,}")
    print(f"  Pairs with negatives: {analysis['pairs_with_negatives']:,}")
    print(f"  Total negatives: {analysis['total_negatives']:,}")

    if analysis['total_negatives'] > 0:
        false_pct = 100 * analysis['potentially_false'] / analysis['total_negatives']
        print(f"\n⚠️  Potentially false negatives: {analysis['potentially_false']:,} ({false_pct:.1f}%)")
        print(f"  High Jaccard overlap: {analysis['high_jaccard_overlap']:,}")

        print(f"\n📊 Score Distribution:")
        for range_label in ["0.90-1.00", "0.85-0.90", "0.80-0.85", "0.70-0.80", "0.60-0.70", "< 0.60"]:
            count = analysis['score_distribution'].get(range_label, 0)
            pct = 100 * count / analysis['total_negatives']
            bar = "█" * int(pct / 2)
            risk = "⚠️ HIGH RISK" if range_label in ["0.90-1.00", "0.85-0.90"] else ""
            print(f"    {range_label:12} {count:>8,} ({pct:>5.1f}%) {bar} {risk}")

        print(f"\n📚 By Source:")
        for source, data in analysis['by_source'].items():
            if data['total'] > 0:
                suspicious_pct = 100 * data['suspicious'] / data['total']
                print(f"    {source:15} {data['total']:>6,} negs, {data['suspicious']:>5,} suspicious ({suspicious_pct:.1f}%)")


def main():
    args = parse_args()

    print("=" * 60)
    print("False Negative Filtering for Training Data")
    print("=" * 60)

    if not os.path.exists(args.input):
        print(f"ERROR: Input file not found: {args.input}")
        sys.exit(1)

    # Analyze first
    print(f"\nAnalyzing: {args.input}")
    analysis = analyze_negatives(args.input)
    print_analysis(analysis)

    if args.analyze_only:
        return

    # Filter negatives
    print(f"\n{'='*60}")
    print("FILTERING")
    print(f"{'='*60}")
    print(f"\nConfiguration:")
    print(f"  Threshold: {args.threshold} (remove if neg_score > {args.threshold} × pos_score)")
    print(f"  Min similarity: {args.min_similarity}")
    print(f"  Max similarity: {args.max_similarity}")
    print(f"  Use cross-encoder: {args.use_cross_encoder}")

    # Load cross-encoder if requested
    cross_encoder = None
    if args.use_cross_encoder:
        if not CROSS_ENCODER_AVAILABLE:
            print("\nWARNING: sentence-transformers not installed. Skipping cross-encoder.")
        else:
            print(f"\nLoading cross-encoder: {args.cross_encoder_model}")
            cross_encoder = CrossEncoder(args.cross_encoder_model)

    # Process file
    output_path = args.output or args.input.replace(".jsonl", "_filtered.jsonl")

    total_stats = defaultdict(int)
    filtered_pairs = []

    print(f"\nProcessing...")
    with open(args.input, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            if not line.strip():
                continue
            try:
                pair = json.loads(line)
            except json.JSONDecodeError:
                continue

            filtered_pair, stats = filter_training_pair(
                pair, args.threshold, args.min_similarity, args.max_similarity, cross_encoder
            )

            filtered_pairs.append(filtered_pair)

            for key, value in stats.items():
                total_stats[key] += value

            if line_num % 10000 == 0:
                print(f"  Processed {line_num:,} pairs...")

    # Write output
    with open(output_path, 'w', encoding='utf-8') as f:
        for pair in filtered_pairs:
            f.write(json.dumps(pair, ensure_ascii=False) + "\n")

    # Print summary
    print(f"\n{'='*60}")
    print("FILTERING COMPLETE")
    print(f"{'='*60}")
    print(f"\n📊 Results:")
    print(f"  Total pairs processed: {len(filtered_pairs):,}")
    print(f"  Original negatives: {total_stats['original_negatives']:,}")
    print(f"  Filtered negatives: {total_stats['filtered_negatives']:,}")

    removed = total_stats['original_negatives'] - total_stats['filtered_negatives']
    if total_stats['original_negatives'] > 0:
        removed_pct = 100 * removed / total_stats['original_negatives']
        print(f"  Removed: {removed:,} ({removed_pct:.1f}%)")

    print(f"\n📋 Removal Breakdown:")
    print(f"  Score threshold: {total_stats['removed_score_threshold']:,}")
    print(f"  High similarity (>0.90): {total_stats['removed_high_similarity']:,}")
    print(f"  Jaccard overlap: {total_stats['removed_jaccard']:,}")
    print(f"  Substring match: {total_stats['removed_substring']:,}")
    print(f"  Too easy (<{args.min_similarity}): {total_stats['removed_too_easy']:,}")
    if args.use_cross_encoder:
        print(f"  Cross-encoder: {total_stats['removed_cross_encoder']:,}")

    print(f"\n📁 Output: {output_path}")


if __name__ == "__main__":
    main()
