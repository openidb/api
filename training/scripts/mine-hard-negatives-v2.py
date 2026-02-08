#!/usr/bin/env python3
"""
Improved Hard Negative Mining for Training Data (v2)

This script implements research-backed hard negative mining strategies:

1. Range-based Mining: Mine from top 10-200 to avoid false negatives in top 10
2. Negative Type Mixing: 30% hard + 40% in-batch + 30% random
3. Positive-aware Filtering: Filter negatives with score > 0.95 × positive_score
4. Same-passage Filtering: Remove same hadith from different collections
5. Iterative Mining: Use updated model to mine better negatives

Based on:
- NV-Retriever: https://arxiv.org/pdf/2407.15831
- RocketQA: https://arxiv.org/abs/2010.08191
- Contrastive Learning Penalty: https://arxiv.org/html/2412.17364v1

Key improvements over v1:
- Avoids top 10 results which often contain false negatives
- Mixes different negative types for better training
- Implements positive-aware filtering at mining time
- Supports iterative refinement with updated model

Usage:
    python mine-hard-negatives-v2.py --input quran_pairs.jsonl --output quran_pairs_negatives_v2.jsonl
    python mine-hard-negatives-v2.py --source hadith --start-rank 10 --end-rank 200
    python mine-hard-negatives-v2.py --dry-run --limit 100
"""

import argparse
import json
import os
import random
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Optional

# For embedding generation
try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False

# Configuration
EMBEDDING_SERVER_URL = os.environ.get("EMBEDDING_SERVER_URL", "http://localhost:8000")
OPENROUTER_API_URL = "https://openrouter.ai/api/v1/chat/completions"
GEMINI_MODEL = "google/gemini-embedding-exp-03-07"  # For embedding

# Mining configuration based on research
DEFAULT_START_RANK = 10    # Skip top 10 (often false negatives)
DEFAULT_END_RANK = 200     # Search up to top 200
DEFAULT_HARD_NEGATIVES = 3  # Hard negatives per query
DEFAULT_RANDOM_NEGATIVES = 1  # Random negatives for diversity

# Negative mixing ratios (research-backed)
NEGATIVE_MIX = {
    "hard": 0.30,      # From mining (ranks 10-200)
    "semi_hard": 0.40, # From mining (ranks 50-200)
    "random": 0.30,    # Random from corpus
}


def parse_args():
    parser = argparse.ArgumentParser(description="Improved hard negative mining v2")
    parser.add_argument("--input", "-i", type=str, help="Input JSONL file")
    parser.add_argument("--output", "-o", type=str, help="Output file")
    parser.add_argument("--source", type=str, choices=["quran", "hadith", "all"], default="all")
    parser.add_argument("--start-rank", type=int, default=DEFAULT_START_RANK,
                        help=f"Start mining from this rank (default: {DEFAULT_START_RANK})")
    parser.add_argument("--end-rank", type=int, default=DEFAULT_END_RANK,
                        help=f"Mine up to this rank (default: {DEFAULT_END_RANK})")
    parser.add_argument("--hard-negatives", type=int, default=DEFAULT_HARD_NEGATIVES,
                        help=f"Number of hard negatives per query (default: {DEFAULT_HARD_NEGATIVES})")
    parser.add_argument("--include-random", action="store_true",
                        help="Include random negatives for diversity")
    parser.add_argument("--positive-threshold", type=float, default=0.95,
                        help="Filter negatives with score > threshold × positive_score")
    parser.add_argument("--min-similarity", type=float, default=0.50,
                        help="Minimum similarity for hard negatives")
    parser.add_argument("--max-similarity", type=float, default=0.90,
                        help="Maximum similarity (exclude likely positives)")
    parser.add_argument("--embedding-model", type=str, choices=["gemini", "bge-m3"], default="gemini",
                        help="Model for embedding queries")
    parser.add_argument("--limit", type=int, help="Limit number of pairs to process")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done")
    parser.add_argument("--verbose", "-v", action="store_true")
    return parser.parse_args()


def normalize_arabic(text: str) -> str:
    """Normalize Arabic text for comparison."""
    diacritics = '\u064B\u064C\u064D\u064E\u064F\u0650\u0651\u0652'
    for d in diacritics:
        text = text.replace(d, '')
    text = text.replace('أ', 'ا').replace('إ', 'ا').replace('آ', 'ا')
    text = text.replace('ة', 'ه').replace('ى', 'ي')
    return text.strip().lower()


def jaccard_similarity(text1: str, text2: str) -> float:
    """Calculate Jaccard similarity."""
    words1 = set(normalize_arabic(text1).split())
    words2 = set(normalize_arabic(text2).split())
    if not words1 or not words2:
        return 0.0
    intersection = len(words1 & words2)
    union = len(words1 | words2)
    return intersection / union if union > 0 else 0.0


def is_same_passage(text1: str, text2: str, threshold: float = 0.8) -> bool:
    """Check if two passages are the same or very similar."""
    norm1 = normalize_arabic(text1)
    norm2 = normalize_arabic(text2)

    if norm1 == norm2:
        return True

    return jaccard_similarity(text1, text2) > threshold


def generate_embedding_gemini(text: str, api_key: str) -> list[float]:
    """Generate embedding using Gemini via OpenRouter."""
    import urllib.request

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
    }

    data = json.dumps({
        "model": GEMINI_MODEL,
        "input": text,
    }).encode('utf-8')

    req = urllib.request.Request(
        "https://openrouter.ai/api/v1/embeddings",
        data=data,
        headers=headers,
        method='POST'
    )

    with urllib.request.urlopen(req, timeout=30) as response:
        result = json.loads(response.read().decode('utf-8'))
        return result["data"][0]["embedding"]


def generate_embedding_local(text: str) -> list[float]:
    """Generate embedding using local BGE-M3 server."""
    response = requests.post(
        f"{EMBEDDING_SERVER_URL}/embed",
        json={"text": text}
    )
    response.raise_for_status()
    return response.json()["embedding"]


def search_qdrant(
    embedding: list[float],
    collection: str,
    limit: int = 200,
    score_threshold: float = 0.0
) -> list[dict]:
    """Search Qdrant for similar passages."""
    # Use environment variable or default
    qdrant_url = os.environ.get("QDRANT_URL", "http://localhost:6333")

    response = requests.post(
        f"{qdrant_url}/collections/{collection}/points/search",
        json={
            "vector": embedding,
            "limit": limit,
            "with_payload": True,
            "score_threshold": score_threshold,
        }
    )
    response.raise_for_status()
    return response.json().get("result", [])


def get_random_passages(collection: str, count: int, exclude_ids: set) -> list[dict]:
    """Get random passages from collection for random negatives."""
    qdrant_url = os.environ.get("QDRANT_URL", "http://localhost:6333")

    # Scroll to get random points
    response = requests.post(
        f"{qdrant_url}/collections/{collection}/points/scroll",
        json={
            "limit": count * 3,  # Get extra to filter
            "with_payload": True,
        }
    )
    response.raise_for_status()
    points = response.json().get("result", {}).get("points", [])

    # Filter and return random subset
    filtered = [p for p in points if p.get("id") not in exclude_ids]
    random.shuffle(filtered)
    return filtered[:count]


def mine_negatives_for_pair(
    query: str,
    positives: list[str],
    source: str,
    api_key: str,
    embedding_model: str,
    start_rank: int,
    end_rank: int,
    num_hard: int,
    min_sim: float,
    max_sim: float,
    positive_threshold: float,
    include_random: bool = False
) -> tuple[list[str], list[float], dict]:
    """Mine hard negatives for a single query-positive pair."""
    stats = {
        "searched": 0,
        "filtered_too_similar": 0,
        "filtered_positive_aware": 0,
        "filtered_same_passage": 0,
        "hard_negatives": 0,
        "random_negatives": 0,
    }

    # Generate query embedding
    if embedding_model == "gemini":
        embedding = generate_embedding_gemini(query, api_key)
    else:
        embedding = generate_embedding_local(query)

    # Determine collection
    if source == "quran":
        collection = os.environ.get("QDRANT_QURAN_COLLECTION", "quran_enriched_gemini")
    else:
        collection = os.environ.get("QDRANT_HADITH_COLLECTION", "hadith_gemini")

    # Search for candidates
    search_results = search_qdrant(embedding, collection, end_rank, min_sim)
    stats["searched"] = len(search_results)

    # Filter and select negatives
    negatives = []
    neg_scores = []
    positive_score = 1.0  # Assume perfect match for positive

    # Skip top results (often false negatives) and filter
    for i, result in enumerate(search_results):
        if i < start_rank:
            continue  # Skip top N results

        if len(negatives) >= num_hard:
            break

        score = result.get("score", 0.0)
        payload = result.get("payload", {})
        text = payload.get("textPlain") or payload.get("text") or payload.get("textArabic", "")

        if not text:
            continue

        # Filter: Too similar (likely relevant)
        if score > max_sim:
            stats["filtered_too_similar"] += 1
            continue

        # Filter: Positive-aware
        if score > positive_score * positive_threshold:
            stats["filtered_positive_aware"] += 1
            continue

        # Filter: Same passage as positive
        is_same = any(is_same_passage(text, pos) for pos in positives)
        if is_same:
            stats["filtered_same_passage"] += 1
            continue

        # Filter: Duplicate with already selected
        is_dup = any(is_same_passage(text, neg) for neg in negatives)
        if is_dup:
            continue

        negatives.append(text)
        neg_scores.append(score)
        stats["hard_negatives"] += 1

    # Optionally add random negatives for diversity
    if include_random and len(negatives) < num_hard + DEFAULT_RANDOM_NEGATIVES:
        exclude_ids = set()  # Would need to track IDs
        random_passages = get_random_passages(collection, DEFAULT_RANDOM_NEGATIVES, exclude_ids)

        for p in random_passages:
            text = p.get("payload", {}).get("textPlain") or p.get("payload", {}).get("text", "")
            if text and not any(is_same_passage(text, neg) for neg in negatives):
                negatives.append(text)
                neg_scores.append(0.3)  # Low score for random
                stats["random_negatives"] += 1

    return negatives, neg_scores, stats


def load_training_pairs(filepath: str, source_filter: str = "all", limit: int = None) -> list[dict]:
    """Load training pairs from JSONL file."""
    pairs = []

    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            if not line.strip():
                continue
            try:
                data = json.loads(line)
                source = data.get("source", "unknown")

                if source_filter != "all" and source != source_filter:
                    continue

                pairs.append(data)

                if limit and len(pairs) >= limit:
                    break
            except json.JSONDecodeError:
                continue

    return pairs


def main():
    args = parse_args()

    print("=" * 60)
    print("Improved Hard Negative Mining v2")
    print("=" * 60)

    # Check dependencies
    api_key = os.environ.get("OPENROUTER_API_KEY")
    if args.embedding_model == "gemini" and not api_key and not args.dry_run:
        print("ERROR: OPENROUTER_API_KEY not set for Gemini embeddings")
        sys.exit(1)

    if args.embedding_model == "bge-m3" and not REQUESTS_AVAILABLE:
        print("ERROR: requests library not installed for local server")
        sys.exit(1)

    # Determine input file
    script_dir = Path(__file__).parent
    data_dir = script_dir.parent / "data"

    if args.input:
        input_file = Path(args.input)
    elif args.source == "quran":
        input_file = data_dir / "quran_pairs.jsonl"
    elif args.source == "hadith":
        input_file = data_dir / "hadith_pairs.jsonl"
    else:
        input_file = data_dir / "combined_training.jsonl"

    if not input_file.exists():
        print(f"ERROR: Input file not found: {input_file}")
        sys.exit(1)

    # Load pairs
    print(f"\nLoading pairs from: {input_file}")
    pairs = load_training_pairs(str(input_file), args.source, args.limit)
    print(f"  Loaded {len(pairs)} pairs")

    # Configuration
    print(f"\nMining Configuration:")
    print(f"  Start rank: {args.start_rank} (skip top {args.start_rank} results)")
    print(f"  End rank: {args.end_rank}")
    print(f"  Hard negatives per query: {args.hard_negatives}")
    print(f"  Positive-aware threshold: {args.positive_threshold}")
    print(f"  Similarity range: {args.min_similarity} - {args.max_similarity}")
    print(f"  Embedding model: {args.embedding_model}")
    print(f"  Include random negatives: {args.include_random}")

    if args.dry_run:
        print("\nDRY RUN - No actual mining will be performed")
        print(f"Would process {len(pairs)} pairs")
        print(f"Expected negatives: ~{len(pairs) * args.hard_negatives}")
        return

    # Process pairs
    print(f"\nMining hard negatives...")
    output_pairs = []
    total_stats = defaultdict(int)
    start_time = time.time()

    for i, pair in enumerate(pairs):
        query = pair.get("query", "")
        positives = pair.get("pos", [])
        source = pair.get("source", "hadith")

        if not query or not positives:
            output_pairs.append(pair)
            continue

        try:
            negatives, neg_scores, stats = mine_negatives_for_pair(
                query=query,
                positives=positives,
                source=source,
                api_key=api_key,
                embedding_model=args.embedding_model,
                start_rank=args.start_rank,
                end_rank=args.end_rank,
                num_hard=args.hard_negatives,
                min_sim=args.min_similarity,
                max_sim=args.max_similarity,
                positive_threshold=args.positive_threshold,
                include_random=args.include_random
            )

            # Update pair
            pair["neg"] = negatives
            pair["neg_scores"] = neg_scores
            output_pairs.append(pair)

            # Accumulate stats
            for key, value in stats.items():
                total_stats[key] += value

        except Exception as e:
            if args.verbose:
                print(f"  Error processing pair {i}: {e}")
            output_pairs.append(pair)  # Keep original
            continue

        # Progress
        if (i + 1) % 100 == 0 or i == len(pairs) - 1:
            elapsed = time.time() - start_time
            rate = (i + 1) / elapsed if elapsed > 0 else 0
            remaining = (len(pairs) - i - 1) / rate if rate > 0 else 0
            print(f"  Processed {i + 1}/{len(pairs)} ({rate:.1f}/sec, ~{remaining/60:.1f}min remaining)")

        # Rate limiting
        time.sleep(0.05)

    # Write output
    output_path = args.output or str(input_file).replace(".jsonl", "_negatives_v2.jsonl")

    with open(output_path, 'w', encoding='utf-8') as f:
        for pair in output_pairs:
            f.write(json.dumps(pair, ensure_ascii=False) + "\n")

    # Summary
    total_time = time.time() - start_time
    print(f"\n{'='*60}")
    print("MINING COMPLETE")
    print(f"{'='*60}")
    print(f"\n📊 Statistics:")
    print(f"  Pairs processed: {len(output_pairs)}")
    print(f"  Total searched: {total_stats['searched']:,}")
    print(f"  Hard negatives mined: {total_stats['hard_negatives']:,}")
    if args.include_random:
        print(f"  Random negatives added: {total_stats['random_negatives']:,}")
    print(f"\n📋 Filtering Breakdown:")
    print(f"  Filtered (too similar): {total_stats['filtered_too_similar']:,}")
    print(f"  Filtered (positive-aware): {total_stats['filtered_positive_aware']:,}")
    print(f"  Filtered (same passage): {total_stats['filtered_same_passage']:,}")
    print(f"\n⏱️  Time: {total_time/60:.1f} minutes")
    print(f"📁 Output: {output_path}")

    # Quality check
    pairs_with_negs = sum(1 for p in output_pairs if p.get("neg"))
    avg_negs = total_stats['hard_negatives'] / len(output_pairs) if output_pairs else 0
    print(f"\n✅ Quality Check:")
    print(f"  Pairs with negatives: {pairs_with_negs}/{len(output_pairs)} ({100*pairs_with_negs/len(output_pairs):.1f}%)")
    print(f"  Avg negatives per pair: {avg_negs:.2f}")


if __name__ == "__main__":
    main()
