#!/usr/bin/env python3
"""
Generate Arabic Queries for Training Data

Uses Gemini to generate diverse Arabic search queries for existing Arabic passages.
This addresses the critical imbalance: 99% English queries vs 0.7% Arabic queries.

Target: 50% Arabic queries in final training set

Strategies:
1. Native Arabic queries (فصحى) - formal Arabic
2. Colloquial-style Arabic queries - how users actually search
3. Arabic keyword queries - key terms from text
4. Mixed Arabic-English queries - common search pattern

Output format (JSONL):
{"query": "Arabic query", "pos": ["Arabic passage"], "neg": [], "source": "arabic_synthetic", "query_type": "native_ar", "language": "ar"}

Usage:
    python generate-arabic-queries.py --input combined_training.jsonl --output arabic_queries.jsonl
    python generate-arabic-queries.py --dry-run --limit 100  # Test run
    python generate-arabic-queries.py --pilot  # 500 passages only
"""

import argparse
import json
import os
import random
import sys
import time
from pathlib import Path
from typing import Any

# OpenRouter API configuration
OPENROUTER_API_URL = "https://openrouter.ai/api/v1/chat/completions"
MODEL = "google/gemini-2.0-flash-001"
REQUEST_DELAY_MS = 100
MAX_RETRIES = 3
RETRY_DELAY_MS = 2000

# Query type distribution for balanced training
QUERY_TYPES = {
    "native_question": 0.30,      # Natural Arabic questions: ما هو حكم...؟
    "keyword_ar": 0.25,           # Arabic keywords: الصبر الصلاة التوكل
    "conceptual_ar": 0.20,        # Conceptual: آيات عن الصبر
    "colloquial_ar": 0.15,        # Common search patterns: حديث عن الصدق
    "mixed_ar_en": 0.10,          # Mixed: hadith الأعمال بالنيات
}


def parse_args():
    parser = argparse.ArgumentParser(description="Generate Arabic queries for training data")
    parser.add_argument("--input", "-i", type=str, help="Input JSONL file with existing training pairs")
    parser.add_argument("--output", "-o", type=str, default="arabic_queries.jsonl", help="Output file")
    parser.add_argument("--limit", "-l", type=int, default=None, help="Limit number of passages to process")
    parser.add_argument("--batch-size", "-b", type=int, default=5, help="Passages per API call")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done")
    parser.add_argument("--pilot", action="store_true", help="Process 500 passages only")
    parser.add_argument("--queries-per-passage", type=int, default=3, help="Number of queries per passage")
    parser.add_argument("--source-filter", type=str, choices=["quran", "hadith", "all"], default="all",
                        help="Filter by source type")
    return parser.parse_args()


def load_training_data(filepath: str, source_filter: str = "all", limit: int = None) -> list[dict]:
    """Load existing training pairs and extract unique Arabic passages."""
    passages = []
    seen_texts = set()

    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            if not line.strip():
                continue
            try:
                data = json.loads(line)
                source = data.get("source", "unknown")

                # Filter by source if specified
                if source_filter != "all" and source != source_filter:
                    continue

                # Get Arabic passage (from positive)
                positives = data.get("pos", [])
                for pos in positives:
                    # Check if it's Arabic text (contains Arabic characters)
                    if any('\u0600' <= c <= '\u06FF' for c in pos) and pos not in seen_texts:
                        seen_texts.add(pos)
                        passages.append({
                            "text": pos,
                            "source": source,
                            "original_query": data.get("query", "")
                        })

                        if limit and len(passages) >= limit:
                            return passages
            except json.JSONDecodeError:
                continue

    return passages


def build_arabic_query_prompt(passages: list[dict]) -> str:
    """Build prompt for generating diverse Arabic queries."""
    passage_list = []
    for i, p in enumerate(passages, 1):
        source_label = "قرآن" if p["source"] == "quran" else "حديث"
        passage_list.append(f"{i}. [{source_label}]\n{p['text'][:500]}")

    passages_text = "\n\n".join(passage_list)

    return f"""أنت خبير في البحث عن النصوص الإسلامية. مهمتك توليد استعلامات بحث عربية متنوعة وطبيعية لكل نص.

المطلوب: توليد 3 استعلامات بحث عربية مختلفة لكل نص. يجب أن تكون الاستعلامات:

1. سؤال طبيعي بالعربية الفصحى (native_question):
   مثال: "ما حكم الصيام في السفر؟" أو "كيف يتوكل المسلم على الله؟"

2. كلمات مفتاحية عربية (keyword_ar):
   مثال: "الصبر الصلاة الاستعانة" أو "التوحيد الإيمان العقيدة"

3. استعلام مفاهيمي/موضوعي (conceptual_ar):
   مثال: "آيات عن الصبر والمصائب" أو "أحاديث في فضل الصدقة"

معايير الجودة:
- يجب أن يكون الاستعلام محدداً بما يكفي لإيجاد هذا النص تحديداً
- استخدم مصطلحات يبحث بها المستخدمون فعلاً
- تجنب الاستعلامات العامة جداً مثل "آية قرآنية" أو "حديث نبوي"
- للنصوص القرآنية: ضمّن إشارة للسورة أو الموضوع
- للأحاديث: ضمّن الموضوع الرئيسي أو الحكم المذكور

النصوص العربية:
{passages_text}

أعد الإجابة بتنسيق JSON فقط:
[
  {{
    "passage_index": 1,
    "queries": [
      {{"text": "الاستعلام الأول", "type": "native_question"}},
      {{"text": "الاستعلام الثاني", "type": "keyword_ar"}},
      {{"text": "الاستعلام الثالث", "type": "conceptual_ar"}}
    ]
  }}
]

مهم: أعد JSON صالح فقط، بدون أي نص آخر."""


def call_openrouter_api(prompt: str, api_key: str, retries: int = 0) -> dict:
    """Call OpenRouter API to generate queries."""
    import urllib.request
    import urllib.error

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
        "HTTP-Referer": "https://github.com/arabic-texts-library",
        "X-Title": "Arabic Query Generator"
    }

    data = json.dumps({
        "model": MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.7,
        "max_tokens": 4096
    }).encode('utf-8')

    req = urllib.request.Request(OPENROUTER_API_URL, data=data, headers=headers, method='POST')

    try:
        with urllib.request.urlopen(req, timeout=60) as response:
            return json.loads(response.read().decode('utf-8'))
    except urllib.error.HTTPError as e:
        error_body = e.read().decode('utf-8') if e.fp else str(e)
        if retries < MAX_RETRIES:
            print(f"  Retry {retries + 1}/{MAX_RETRIES} after error: {e.code}")
            time.sleep(RETRY_DELAY_MS / 1000)
            return call_openrouter_api(prompt, api_key, retries + 1)
        raise Exception(f"API error {e.code}: {error_body}")
    except Exception as e:
        if retries < MAX_RETRIES:
            print(f"  Retry {retries + 1}/{MAX_RETRIES} after error: {e}")
            time.sleep(RETRY_DELAY_MS / 1000)
            return call_openrouter_api(prompt, api_key, retries + 1)
        raise


def parse_llm_response(content: str) -> list[dict]:
    """Parse JSON response from LLM."""
    # Handle markdown code blocks
    content = content.strip()
    if content.startswith("```json"):
        content = content[7:]
    elif content.startswith("```"):
        content = content[3:]
    if content.endswith("```"):
        content = content[:-3]
    content = content.strip()

    return json.loads(content)


def generate_queries_batch(passages: list[dict], api_key: str, dry_run: bool = False) -> list[dict]:
    """Generate Arabic queries for a batch of passages."""
    if dry_run:
        # Return placeholder queries for dry run
        results = []
        for p in passages:
            results.append({
                "query": f"[DRY RUN] استعلام عربي لـ: {p['text'][:50]}...",
                "pos": [p["text"]],
                "neg": [],
                "source": "arabic_synthetic",
                "query_type": "native_question",
                "language": "ar"
            })
        return results

    prompt = build_arabic_query_prompt(passages)
    response = call_openrouter_api(prompt, api_key)

    content = response.get("choices", [{}])[0].get("message", {}).get("content", "")
    if not content:
        return []

    parsed = parse_llm_response(content)

    # Convert to training pairs
    results = []
    for item in parsed:
        idx = item.get("passage_index", 1) - 1  # Convert to 0-indexed
        if idx < 0 or idx >= len(passages):
            continue

        passage = passages[idx]
        queries = item.get("queries", [])

        for q in queries:
            results.append({
                "query": q.get("text", ""),
                "pos": [passage["text"]],
                "neg": [],
                "source": "arabic_synthetic",
                "query_type": q.get("type", "native_question"),
                "language": "ar"
            })

    return results


def main():
    args = parse_args()

    print("=" * 60)
    print("Arabic Query Generation for BGE-M3 Training")
    print("=" * 60)
    print()

    # Check API key
    api_key = os.environ.get("OPENROUTER_API_KEY")
    if not api_key and not args.dry_run:
        print("ERROR: OPENROUTER_API_KEY environment variable not set.")
        print("Set it or use --dry-run to test.")
        sys.exit(1)

    # Determine input file
    script_dir = Path(__file__).parent
    data_dir = script_dir.parent / "data"

    if args.input:
        input_file = Path(args.input)
    else:
        input_file = data_dir / "combined_training.jsonl"

    if not input_file.exists():
        print(f"ERROR: Input file not found: {input_file}")
        sys.exit(1)

    # Load passages
    print(f"Loading passages from: {input_file}")
    limit = 500 if args.pilot else args.limit
    passages = load_training_data(str(input_file), args.source_filter, limit)

    # Shuffle for diversity
    random.shuffle(passages)

    print(f"  Loaded {len(passages)} unique Arabic passages")
    print(f"  Source filter: {args.source_filter}")
    print(f"  Queries per passage: {args.queries_per_passage}")
    print(f"  Expected output: ~{len(passages) * args.queries_per_passage} training pairs")
    print()

    if len(passages) == 0:
        print("ERROR: No passages found. Check input file format.")
        sys.exit(1)

    # Cost estimate
    est_input_tokens = len(passages) * 400
    est_output_tokens = len(passages) * 80
    est_cost = (est_input_tokens * 0.1 + est_output_tokens * 0.4) / 1_000_000
    print(f"Cost estimate: ~${est_cost:.2f}")
    print()

    if args.dry_run:
        print("DRY RUN - No API calls will be made.")
        print()
        # Show sample prompt
        sample = passages[:min(3, len(passages))]
        print("Sample prompt:")
        print("-" * 40)
        print(build_arabic_query_prompt(sample)[:1500] + "...")
        return

    # Output file
    output_path = data_dir / args.output
    with open(output_path, 'w', encoding='utf-8') as f:
        pass  # Clear file

    # Process in batches
    total_pairs = 0
    processed = 0
    start_time = time.time()

    print("Generating Arabic queries...")
    for i in range(0, len(passages), args.batch_size):
        batch = passages[i:i + args.batch_size]

        try:
            pairs = generate_queries_batch(batch, api_key, dry_run=False)

            # Write to file
            with open(output_path, 'a', encoding='utf-8') as f:
                for pair in pairs:
                    f.write(json.dumps(pair, ensure_ascii=False) + "\n")

            total_pairs += len(pairs)
            processed += len(batch)

            # Progress
            elapsed = time.time() - start_time
            rate = processed / elapsed if elapsed > 0 else 0
            remaining = (len(passages) - processed) / rate if rate > 0 else 0
            print(f"  Processed {processed}/{len(passages)} passages "
                  f"({total_pairs} pairs, {rate:.1f}/sec, ~{remaining/60:.1f}min remaining)")

            # Small delay
            time.sleep(REQUEST_DELAY_MS / 1000)

        except Exception as e:
            print(f"  Error processing batch {i//args.batch_size + 1}: {e}")
            continue

    # Summary
    total_time = time.time() - start_time
    print()
    print("=" * 60)
    print("GENERATION COMPLETE")
    print("=" * 60)
    print(f"  Total passages processed: {processed}")
    print(f"  Total training pairs: {total_pairs}")
    print(f"  Time: {total_time/60:.1f} minutes")
    print(f"  Output: {output_path}")

    # Show sample
    print()
    print("Sample output:")
    print("-" * 40)
    with open(output_path, 'r', encoding='utf-8') as f:
        for i, line in enumerate(f):
            if i >= 3:
                break
            pair = json.loads(line)
            print(f"Query ({pair['query_type']}): {pair['query'][:60]}...")
            print(f"Positive: {pair['pos'][0][:50]}...")
            print()


if __name__ == "__main__":
    main()
