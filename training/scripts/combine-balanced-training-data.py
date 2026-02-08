#!/usr/bin/env python3
"""
Combine and Balance Training Data

This script combines all training data sources and rebalances them according to targets:
- 50% Arabic queries (up from 0.7%)
- 20% conceptual queries (up from 0.4%)
- 25%+ Quran content (up from 11%)
- Mix of hard, semi-hard, and random negatives

Data Sources:
1. quran_pairs_negatives.jsonl - Original Quran translation pairs
2. hadith_pairs_negatives.jsonl - Original Hadith translation pairs
3. arabic_queries.jsonl - Generated Arabic queries (Phase 1.1)
4. quran_conceptual_queries.jsonl - Conceptual queries (Phase 1.2)
5. synthetic_queries.jsonl - Synthetic queries with diverse types

Output:
- combined_training_v2.jsonl - Balanced training data
- combined_training_v2_stats.json - Statistics

Usage:
    python combine-balanced-training-data.py
    python combine-balanced-training-data.py --output combined_training_v2.jsonl
    python combine-balanced-training-data.py --dry-run
"""

import argparse
import json
import os
import random
from collections import defaultdict
from pathlib import Path
from typing import Any


def parse_args():
    parser = argparse.ArgumentParser(description="Combine and balance training data")
    parser.add_argument("--output", "-o", type=str, default="combined_training_v2.jsonl")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--max-total", type=int, default=100000, help="Maximum total pairs")
    parser.add_argument("--arabic-target", type=float, default=0.50, help="Target Arabic query ratio")
    parser.add_argument("--conceptual-target", type=float, default=0.20, help="Target conceptual query ratio")
    parser.add_argument("--quran-target", type=float, default=0.25, help="Target Quran content ratio")
    return parser.parse_args()


def detect_language(text: str) -> str:
    """Detect if text is primarily Arabic or English."""
    if not text:
        return "unknown"
    arabic_chars = sum(1 for c in text if '\u0600' <= c <= '\u06FF')
    latin_chars = sum(1 for c in text if 'a' <= c.lower() <= 'z')
    total = len(text.replace(" ", ""))
    if total == 0:
        return "unknown"
    if arabic_chars / total > 0.5:
        return "ar"
    return "en"


def classify_query_type(query: str, explicit_type: str = "") -> str:
    """Classify query as conceptual or literal."""
    conceptual_patterns = [
        "آيات عن", "أحاديث عن", "أحاديث في", "ما ورد",
        "مفهوم", "معنى", "حكم", "فضل", "ما أجر", "كيف",
        "verses about", "hadith about", "what does", "how to",
        "islamic guidance", "quran on", "teaching on"
    ]

    query_lower = query.lower()
    for pattern in conceptual_patterns:
        if pattern in query_lower:
            return "conceptual"

    if explicit_type in ["conceptual", "thematic_ar", "thematic_en", "question_ar", "question_en"]:
        return "conceptual"

    return "literal"


def load_jsonl(filepath: str) -> list[dict]:
    """Load JSONL file."""
    if not os.path.exists(filepath):
        print(f"  Warning: File not found: {filepath}")
        return []

    pairs = []
    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip():
                try:
                    pairs.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    return pairs


def analyze_data(pairs: list[dict]) -> dict:
    """Analyze training data distribution."""
    stats = {
        "total": len(pairs),
        "by_language": defaultdict(int),
        "by_source": defaultdict(int),
        "by_query_type": defaultdict(int),
        "with_negatives": 0,
    }

    for p in pairs:
        query = p.get("query", "")
        source = p.get("source", "unknown")
        explicit_type = p.get("query_type", "")

        lang = p.get("language", detect_language(query))
        query_type = classify_query_type(query, explicit_type)

        stats["by_language"][lang] += 1
        stats["by_source"][source] += 1
        stats["by_query_type"][query_type] += 1

        if p.get("neg"):
            stats["with_negatives"] += 1

    return stats


def print_stats(name: str, stats: dict):
    """Print statistics."""
    print(f"\n📊 {name}")
    print(f"  Total: {stats['total']}")

    if stats["total"] > 0:
        print("  By language:")
        for lang, count in sorted(stats["by_language"].items(), key=lambda x: -x[1]):
            pct = 100 * count / stats["total"]
            print(f"    {lang}: {count} ({pct:.1f}%)")

        print("  By source:")
        for source, count in sorted(stats["by_source"].items(), key=lambda x: -x[1]):
            pct = 100 * count / stats["total"]
            print(f"    {source}: {count} ({pct:.1f}%)")

        print("  By query type:")
        for qtype, count in sorted(stats["by_query_type"].items(), key=lambda x: -x[1]):
            pct = 100 * count / stats["total"]
            print(f"    {qtype}: {count} ({pct:.1f}%)")


def balance_data(
    all_pairs: list[dict],
    arabic_target: float,
    conceptual_target: float,
    quran_target: float,
    max_total: int
) -> list[dict]:
    """Balance data to meet targets."""
    # Categorize all pairs
    arabic_pairs = []
    english_pairs = []
    conceptual_pairs = []
    literal_pairs = []
    quran_pairs = []
    hadith_pairs = []

    for p in all_pairs:
        query = p.get("query", "")
        source = p.get("source", "")
        explicit_type = p.get("query_type", "")

        lang = p.get("language", detect_language(query))
        query_type = classify_query_type(query, explicit_type)
        is_quran = source == "quran" or source.startswith("quran")

        if lang == "ar":
            arabic_pairs.append(p)
        else:
            english_pairs.append(p)

        if query_type == "conceptual":
            conceptual_pairs.append(p)
        else:
            literal_pairs.append(p)

        if is_quran:
            quran_pairs.append(p)
        else:
            hadith_pairs.append(p)

    print(f"\n📊 Available Data:")
    print(f"  Arabic queries: {len(arabic_pairs)}")
    print(f"  English queries: {len(english_pairs)}")
    print(f"  Conceptual queries: {len(conceptual_pairs)}")
    print(f"  Literal queries: {len(literal_pairs)}")
    print(f"  Quran pairs: {len(quran_pairs)}")
    print(f"  Hadith pairs: {len(hadith_pairs)}")

    # Calculate target counts
    target_arabic = int(max_total * arabic_target)
    target_conceptual = int(max_total * conceptual_target)
    target_quran = int(max_total * quran_target)

    print(f"\n🎯 Targets:")
    print(f"  Arabic queries: {target_arabic} ({arabic_target*100:.0f}%)")
    print(f"  Conceptual queries: {target_conceptual} ({conceptual_target*100:.0f}%)")
    print(f"  Quran content: {target_quran} ({quran_target*100:.0f}%)")

    # Build balanced dataset
    balanced = []
    used_queries = set()

    def add_pairs(source_pairs: list[dict], count: int, description: str):
        added = 0
        random.shuffle(source_pairs)
        for p in source_pairs:
            if added >= count:
                break
            query = p.get("query", "")
            if query not in used_queries:
                balanced.append(p)
                used_queries.add(query)
                added += 1
        print(f"  Added {added} {description}")
        return added

    # 1. Prioritize Arabic conceptual queries (rare and valuable)
    arabic_conceptual = [p for p in conceptual_pairs if detect_language(p.get("query", "")) == "ar"]
    add_pairs(arabic_conceptual, min(len(arabic_conceptual), target_arabic // 2), "Arabic conceptual")

    # 2. Add remaining Arabic queries
    remaining_arabic = [p for p in arabic_pairs if p.get("query", "") not in used_queries]
    add_pairs(remaining_arabic, target_arabic - len([p for p in balanced if detect_language(p.get("query", "")) == "ar"]), "Arabic literal")

    # 3. Add English conceptual queries
    english_conceptual = [p for p in conceptual_pairs if detect_language(p.get("query", "")) != "ar"]
    add_pairs(english_conceptual, target_conceptual - len([p for p in balanced if classify_query_type(p.get("query", ""), p.get("query_type", "")) == "conceptual"]), "English conceptual")

    # 4. Add Quran pairs to meet target
    remaining_quran = [p for p in quran_pairs if p.get("query", "") not in used_queries]
    current_quran = len([p for p in balanced if p.get("source", "").startswith("quran")])
    add_pairs(remaining_quran, target_quran - current_quran, "Quran pairs")

    # 5. Fill remaining with hadith pairs
    remaining_hadith = [p for p in hadith_pairs if p.get("query", "") not in used_queries]
    remaining_slots = max_total - len(balanced)
    add_pairs(remaining_hadith, remaining_slots, "Hadith pairs")

    print(f"\n✅ Final dataset: {len(balanced)} pairs")

    return balanced


def main():
    args = parse_args()

    print("=" * 60)
    print("Combine and Balance Training Data")
    print("=" * 60)

    script_dir = Path(__file__).parent
    data_dir = script_dir.parent / "data"

    # Load all data sources
    print("\nLoading data sources...")
    data_sources = {
        "quran_pairs_negatives": load_jsonl(str(data_dir / "quran_pairs_negatives.jsonl")),
        "hadith_pairs_negatives": load_jsonl(str(data_dir / "hadith_pairs_negatives.jsonl")),
        "arabic_queries": load_jsonl(str(data_dir / "arabic_queries.jsonl")),
        "conceptual_queries": load_jsonl(str(data_dir / "quran_conceptual_queries.jsonl")),
        "synthetic_queries": load_jsonl(str(data_dir / "synthetic_queries.jsonl")),
    }

    # Print stats for each source
    for name, pairs in data_sources.items():
        if pairs:
            stats = analyze_data(pairs)
            print_stats(name, stats)

    # Combine all pairs
    all_pairs = []
    for name, pairs in data_sources.items():
        for p in pairs:
            # Ensure source is set
            if "source" not in p or not p["source"]:
                if "quran" in name:
                    p["source"] = "quran"
                elif "hadith" in name:
                    p["source"] = "hadith"
                else:
                    p["source"] = "synthetic"
            all_pairs.append(p)

    print(f"\n📊 Combined total: {len(all_pairs)} pairs")

    # Analyze combined data before balancing
    combined_stats = analyze_data(all_pairs)
    print_stats("Combined (before balancing)", combined_stats)

    # Balance the data
    print("\n" + "=" * 60)
    print("BALANCING DATA")
    print("=" * 60)

    balanced_pairs = balance_data(
        all_pairs,
        args.arabic_target,
        args.conceptual_target,
        args.quran_target,
        args.max_total
    )

    # Analyze after balancing
    balanced_stats = analyze_data(balanced_pairs)
    print_stats("Balanced", balanced_stats)

    if args.dry_run:
        print("\nDRY RUN - No files written")
        return

    # Shuffle
    random.shuffle(balanced_pairs)

    # Write output
    output_path = data_dir / args.output
    with open(output_path, 'w', encoding='utf-8') as f:
        for p in balanced_pairs:
            f.write(json.dumps(p, ensure_ascii=False) + "\n")

    # Write stats
    stats_path = data_dir / args.output.replace(".jsonl", "_stats.json")
    with open(stats_path, 'w', encoding='utf-8') as f:
        json.dump({
            "total": len(balanced_pairs),
            "by_language": dict(balanced_stats["by_language"]),
            "by_source": dict(balanced_stats["by_source"]),
            "by_query_type": dict(balanced_stats["by_query_type"]),
            "with_negatives": balanced_stats["with_negatives"],
            "targets": {
                "arabic": args.arabic_target,
                "conceptual": args.conceptual_target,
                "quran": args.quran_target,
            }
        }, f, indent=2, ensure_ascii=False)

    print(f"\n✅ Output written to: {output_path}")
    print(f"✅ Stats written to: {stats_path}")

    # Verify targets
    print("\n" + "=" * 60)
    print("TARGET VERIFICATION")
    print("=" * 60)

    total = len(balanced_pairs)
    ar_count = balanced_stats["by_language"].get("ar", 0)
    conceptual_count = balanced_stats["by_query_type"].get("conceptual", 0)
    quran_count = balanced_stats["by_source"].get("quran", 0)

    ar_actual = ar_count / total if total > 0 else 0
    conceptual_actual = conceptual_count / total if total > 0 else 0
    quran_actual = quran_count / total if total > 0 else 0

    def check_target(name, actual, target, tolerance=0.1):
        status = "✅" if abs(actual - target) <= tolerance else "⚠️"
        return f"{status} {name}: {actual*100:.1f}% (target: {target*100:.0f}%)"

    print(check_target("Arabic queries", ar_actual, args.arabic_target))
    print(check_target("Conceptual queries", conceptual_actual, args.conceptual_target))
    print(check_target("Quran content", quran_actual, args.quran_target))


if __name__ == "__main__":
    main()
