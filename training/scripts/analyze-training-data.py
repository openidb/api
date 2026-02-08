#!/usr/bin/env python3
"""
Analyze Training Data Balance and Quality

This script analyzes the training data to verify:
1. Language distribution (target: 50% Arabic queries)
2. Query type distribution (target: 20% conceptual)
3. Source balance (Quran vs Hadith)
4. Hard negative quality

Usage:
    python analyze-training-data.py --input combined_training.jsonl
    python analyze-training-data.py --all  # Analyze all data files
"""

import argparse
import json
import os
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any


def parse_args():
    parser = argparse.ArgumentParser(description="Analyze training data balance")
    parser.add_argument("--input", "-i", type=str, help="Input JSONL file to analyze")
    parser.add_argument("--all", "-a", action="store_true", help="Analyze all data files")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show detailed breakdown")
    parser.add_argument("--check-negatives", action="store_true", help="Analyze negative quality")
    return parser.parse_args()


def detect_language(text: str) -> str:
    """Detect if text is primarily Arabic, English, or mixed."""
    if not text:
        return "unknown"

    arabic_chars = sum(1 for c in text if '\u0600' <= c <= '\u06FF')
    latin_chars = sum(1 for c in text if 'a' <= c.lower() <= 'z')
    total_chars = len(text.replace(" ", ""))

    if total_chars == 0:
        return "unknown"

    arabic_ratio = arabic_chars / total_chars
    latin_ratio = latin_chars / total_chars

    if arabic_ratio > 0.7:
        return "ar"
    elif latin_ratio > 0.7:
        return "en"
    elif arabic_ratio > 0.3 and latin_ratio > 0.3:
        return "mixed"
    elif arabic_ratio > latin_ratio:
        return "ar"
    else:
        return "en"


def classify_query_type(query: str, explicit_type: str = None) -> str:
    """Classify query type based on content and explicit label."""
    if explicit_type and explicit_type not in ["translation", "unknown"]:
        return explicit_type

    query_lower = query.lower()

    # Conceptual/thematic patterns
    conceptual_patterns = [
        r"آيات عن", r"أحاديث عن", r"أحاديث في", r"ما ورد في",
        r"مفهوم", r"معنى", r"حكم", r"فضل",
        r"verses about", r"hadith about", r"what does .* say about",
        r"islamic .* on", r"quran on", r"teaching on"
    ]

    question_patterns = [
        r"^ما ", r"^كيف ", r"^هل ", r"^لماذا ", r"^متى ",
        r"^what ", r"^how ", r"^why ", r"^when ", r"^is ",
        r"\?$"
    ]

    keyword_patterns = [
        r"^[\w\s]{5,30}$",  # Short keyword-style
    ]

    for pattern in conceptual_patterns:
        if re.search(pattern, query_lower):
            return "conceptual"

    for pattern in question_patterns:
        if re.search(pattern, query_lower):
            return "natural_question"

    # Check if it looks like a translation (long English text)
    if detect_language(query) == "en" and len(query) > 100:
        return "translation"

    # Check if it looks like Arabic text excerpt
    if detect_language(query) == "ar" and len(query) > 50:
        return "text_excerpt"

    return "keywords"


def analyze_file(filepath: str, verbose: bool = False) -> dict:
    """Analyze a single JSONL file."""
    stats = {
        "total_pairs": 0,
        "unique_queries": set(),
        "unique_positives": set(),
        "with_negatives": 0,
        "negative_counts": [],
        "negative_scores": [],
        "by_source": defaultdict(int),
        "by_query_type": defaultdict(int),
        "by_language": defaultdict(int),
        "query_lengths": [],
        "positive_lengths": [],
        "needs_review_count": 0,
    }

    with open(filepath, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            if not line.strip():
                continue
            try:
                data = json.loads(line)
            except json.JSONDecodeError:
                continue

            stats["total_pairs"] += 1

            # Query analysis
            query = data.get("query", "")
            stats["unique_queries"].add(query)
            stats["query_lengths"].append(len(query))

            # Language detection
            query_lang = data.get("language", detect_language(query))
            stats["by_language"][query_lang] += 1

            # Query type classification
            explicit_type = data.get("query_type", "")
            query_type = classify_query_type(query, explicit_type)
            stats["by_query_type"][query_type] += 1

            # Source
            source = data.get("source", "unknown")
            stats["by_source"][source] += 1

            # Positives
            positives = data.get("pos", [])
            for pos in positives:
                stats["unique_positives"].add(pos)
                stats["positive_lengths"].append(len(pos))

            # Negatives
            negatives = data.get("neg", [])
            if negatives:
                stats["with_negatives"] += 1
                stats["negative_counts"].append(len(negatives))

            neg_scores = data.get("neg_scores", [])
            if neg_scores:
                stats["negative_scores"].extend(neg_scores)

            # Review flags
            if data.get("needs_review"):
                stats["needs_review_count"] += 1

    # Convert sets to counts
    stats["unique_queries"] = len(stats["unique_queries"])
    stats["unique_positives"] = len(stats["unique_positives"])

    return stats


def print_analysis(filepath: str, stats: dict, verbose: bool = False):
    """Print analysis results."""
    print(f"\n{'='*60}")
    print(f"Analysis: {Path(filepath).name}")
    print(f"{'='*60}")

    # Basic counts
    print(f"\n📊 BASIC STATISTICS")
    print(f"  Total pairs: {stats['total_pairs']:,}")
    print(f"  Unique queries: {stats['unique_queries']:,}")
    print(f"  Unique positives: {stats['unique_positives']:,}")
    print(f"  With negatives: {stats['with_negatives']:,} ({100*stats['with_negatives']/stats['total_pairs']:.1f}%)")
    if stats['needs_review_count']:
        print(f"  Needs review: {stats['needs_review_count']:,}")

    # Source distribution
    print(f"\n📚 SOURCE DISTRIBUTION")
    total = stats['total_pairs']
    for source, count in sorted(stats['by_source'].items(), key=lambda x: -x[1]):
        pct = 100 * count / total
        bar = "█" * int(pct / 5)
        print(f"  {source:20} {count:>8,} ({pct:>5.1f}%) {bar}")

    # Language distribution
    print(f"\n🌍 LANGUAGE DISTRIBUTION")
    for lang, count in sorted(stats['by_language'].items(), key=lambda x: -x[1]):
        pct = 100 * count / total
        bar = "█" * int(pct / 5)
        status = "✅" if (lang == "ar" and pct >= 40) else ("⚠️" if lang == "ar" else "")
        print(f"  {lang:20} {count:>8,} ({pct:>5.1f}%) {bar} {status}")

    # Query type distribution
    print(f"\n🔍 QUERY TYPE DISTRIBUTION")
    for qtype, count in sorted(stats['by_query_type'].items(), key=lambda x: -x[1]):
        pct = 100 * count / total
        bar = "█" * int(pct / 5)
        status = "✅" if (qtype == "conceptual" and pct >= 15) else ""
        print(f"  {qtype:20} {count:>8,} ({pct:>5.1f}%) {bar} {status}")

    # Length statistics
    if stats['query_lengths']:
        avg_query_len = sum(stats['query_lengths']) / len(stats['query_lengths'])
        avg_pos_len = sum(stats['positive_lengths']) / len(stats['positive_lengths']) if stats['positive_lengths'] else 0
        print(f"\n📏 LENGTH STATISTICS")
        print(f"  Avg query length: {avg_query_len:.1f} chars")
        print(f"  Avg positive length: {avg_pos_len:.1f} chars")

    # Negative statistics
    if stats['negative_counts']:
        avg_negs = sum(stats['negative_counts']) / len(stats['negative_counts'])
        print(f"\n❌ NEGATIVE STATISTICS")
        print(f"  Avg negatives per pair: {avg_negs:.2f}")

    if stats['negative_scores']:
        scores = stats['negative_scores']
        avg_score = sum(scores) / len(scores)
        print(f"  Avg negative score: {avg_score:.3f}")

        # Score distribution
        ranges = [
            (0.80, 1.00, "0.80-1.00 (borderline)"),
            (0.70, 0.80, "0.70-0.80 (ideal)"),
            (0.60, 0.70, "0.60-0.70 (good)"),
            (0.50, 0.60, "0.50-0.60 (semi-hard)"),
            (0.00, 0.50, "< 0.50 (easy)"),
        ]

        print(f"\n  Score distribution:")
        for min_s, max_s, label in ranges:
            count = sum(1 for s in scores if min_s <= s < max_s)
            pct = 100 * count / len(scores)
            bar = "█" * int(pct / 5)
            print(f"    {label:25} {count:>6,} ({pct:>5.1f}%) {bar}")


def print_target_comparison(stats: dict):
    """Compare current stats against targets."""
    print(f"\n{'='*60}")
    print("🎯 TARGET COMPARISON")
    print(f"{'='*60}")

    total = stats['total_pairs']

    # Language target: 50% Arabic
    ar_pct = 100 * stats['by_language'].get('ar', 0) / total
    ar_target = 50
    ar_status = "✅" if ar_pct >= ar_target * 0.8 else "❌"
    print(f"\n  Arabic queries: {ar_pct:.1f}% (target: {ar_target}%) {ar_status}")

    # Conceptual target: 20%
    conceptual_types = ["conceptual", "thematic_ar", "thematic_en", "question_ar", "question_en"]
    conceptual_count = sum(stats['by_query_type'].get(t, 0) for t in conceptual_types)
    conceptual_pct = 100 * conceptual_count / total
    conceptual_target = 20
    conceptual_status = "✅" if conceptual_pct >= conceptual_target * 0.8 else "❌"
    print(f"  Conceptual queries: {conceptual_pct:.1f}% (target: {conceptual_target}%) {conceptual_status}")

    # Quran balance
    quran_count = stats['by_source'].get('quran', 0)
    quran_pct = 100 * quran_count / total
    quran_target = 25  # At least 25%
    quran_status = "✅" if quran_pct >= quran_target else "❌"
    print(f"  Quran pairs: {quran_pct:.1f}% (target: >={quran_target}%) {quran_status}")

    # Hard negative quality
    if stats['negative_scores']:
        scores = stats['negative_scores']
        # Target: < 5% in borderline range (0.85+)
        borderline = sum(1 for s in scores if s >= 0.85)
        borderline_pct = 100 * borderline / len(scores)
        borderline_target = 5
        borderline_status = "✅" if borderline_pct <= borderline_target else "❌"
        print(f"  Borderline negatives (>0.85): {borderline_pct:.1f}% (target: <{borderline_target}%) {borderline_status}")


def main():
    args = parse_args()

    print("=" * 60)
    print("Training Data Analysis")
    print("=" * 60)

    script_dir = Path(__file__).parent
    data_dir = script_dir.parent / "data"

    files_to_analyze = []

    if args.input:
        files_to_analyze.append(args.input)
    elif args.all:
        # Analyze all relevant files
        patterns = [
            "combined_training.jsonl",
            "quran_pairs.jsonl",
            "quran_pairs_negatives.jsonl",
            "hadith_pairs.jsonl",
            "hadith_pairs_negatives.jsonl",
            "synthetic_queries.jsonl",
            "arabic_queries.jsonl",
            "quran_conceptual_queries.jsonl",
        ]
        for pattern in patterns:
            filepath = data_dir / pattern
            if filepath.exists():
                files_to_analyze.append(str(filepath))
    else:
        # Default: analyze combined training
        default_file = data_dir / "combined_training.jsonl"
        if default_file.exists():
            files_to_analyze.append(str(default_file))
        else:
            print(f"ERROR: Default file not found: {default_file}")
            print("Use --input to specify a file or --all to analyze all files")
            sys.exit(1)

    if not files_to_analyze:
        print("No files found to analyze.")
        sys.exit(1)

    # Analyze each file
    all_stats = {}
    for filepath in files_to_analyze:
        if not os.path.exists(filepath):
            print(f"Warning: File not found: {filepath}")
            continue

        stats = analyze_file(filepath, args.verbose)
        all_stats[filepath] = stats
        print_analysis(filepath, stats, args.verbose)

    # If analyzing combined training, show target comparison
    for filepath, stats in all_stats.items():
        if "combined" in filepath:
            print_target_comparison(stats)
            break

    # Summary if multiple files
    if len(all_stats) > 1:
        print(f"\n{'='*60}")
        print("📋 SUMMARY")
        print(f"{'='*60}")
        total_pairs = sum(s['total_pairs'] for s in all_stats.values())
        print(f"  Files analyzed: {len(all_stats)}")
        print(f"  Total pairs across all files: {total_pairs:,}")


if __name__ == "__main__":
    main()
