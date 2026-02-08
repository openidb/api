#!/usr/bin/env python3
"""
Generate Conceptual/Thematic Queries for Training Data

Creates queries that ask about Islamic concepts and themes rather than literal text matching.
This addresses the critical imbalance: 99% translation pairs vs 0.4% conceptual queries.

Target: 20% conceptual queries in final training set

Query Types:
1. Thematic queries: "آيات عن الصبر" (verses about patience)
2. Conceptual queries: "التوحيد في الإسلام" (monotheism in Islam)
3. Topic-based queries: "أحكام الصيام" (rulings on fasting)
4. Wisdom/Teaching queries: "حكمة الابتلاء" (wisdom of trials)

This script uses a curated list of Islamic themes and generates queries
that should retrieve relevant passages based on meaning, not exact text.

Output format (JSONL):
{"query": "conceptual query", "pos": ["relevant passage"], "neg": [], "source": "conceptual", "query_type": "thematic", "language": "ar|en"}
"""

import argparse
import json
import os
import random
import sys
import time
from pathlib import Path

# OpenRouter API configuration
OPENROUTER_API_URL = "https://openrouter.ai/api/v1/chat/completions"
MODEL = "google/gemini-2.0-flash-001"
MAX_RETRIES = 3
RETRY_DELAY_MS = 2000

# Islamic themes and concepts for query generation
ISLAMIC_THEMES = {
    "quran": {
        "العقيدة والتوحيد": [
            "توحيد الألوهية", "توحيد الربوبية", "أسماء الله الحسنى",
            "الإيمان بالملائكة", "الإيمان بالكتب", "الإيمان بالرسل",
            "الإيمان باليوم الآخر", "الإيمان بالقدر"
        ],
        "العبادات": [
            "الصلاة وأحكامها", "الصيام وفضله", "الزكاة ومصارفها",
            "الحج والعمرة", "الذكر والدعاء", "قراءة القرآن"
        ],
        "الأخلاق والآداب": [
            "الصبر على البلاء", "الشكر على النعم", "التوكل على الله",
            "الصدق والأمانة", "بر الوالدين", "صلة الرحم",
            "حسن الخلق", "التواضع", "الحلم والعفو"
        ],
        "القصص القرآني": [
            "قصة آدم عليه السلام", "قصة نوح عليه السلام",
            "قصة إبراهيم عليه السلام", "قصة موسى عليه السلام",
            "قصة عيسى عليه السلام", "قصة يوسف عليه السلام",
            "أصحاب الكهف", "ذو القرنين"
        ],
        "الآخرة": [
            "أهوال يوم القيامة", "الحساب والميزان", "الصراط",
            "الجنة ونعيمها", "النار وعذابها", "علامات الساعة"
        ],
        "المعاملات": [
            "العدل والقسط", "التجارة الحلال", "تحريم الربا",
            "حقوق الجار", "حقوق اليتيم", "الوفاء بالعهد"
        ]
    },
    "hadith": {
        "أركان الإسلام": [
            "الشهادتان", "إقامة الصلاة", "إيتاء الزكاة",
            "صوم رمضان", "حج البيت"
        ],
        "أركان الإيمان": [
            "الإيمان بالله", "الإيمان بالملائكة", "الإيمان بالكتب",
            "الإيمان بالرسل", "الإيمان باليوم الآخر", "الإيمان بالقدر"
        ],
        "فضائل الأعمال": [
            "فضل الصلاة في وقتها", "فضل صلاة الجماعة",
            "فضل الصدقة", "فضل ذكر الله", "فضل طلب العلم",
            "فضل الصيام", "فضل قيام الليل"
        ],
        "الأخلاق النبوية": [
            "الرحمة والشفقة", "الصدق في القول", "الأمانة",
            "الوفاء بالوعد", "حسن الجوار", "إكرام الضيف"
        ],
        "الأحكام الشرعية": [
            "أحكام الطهارة", "أحكام الصلاة", "أحكام الصيام",
            "أحكام البيوع", "أحكام النكاح", "أحكام الطلاق"
        ],
        "التحذيرات": [
            "التحذير من الكذب", "التحذير من الغيبة والنميمة",
            "التحذير من الربا", "التحذير من أذية الجار",
            "التحذير من قطيعة الرحم"
        ]
    }
}

# Query templates for different query types
QUERY_TEMPLATES = {
    "thematic_ar": [
        "آيات عن {topic}",
        "أحاديث في {topic}",
        "ما ورد في {topic}",
        "{topic} في القرآن",
        "{topic} في السنة",
    ],
    "conceptual_ar": [
        "مفهوم {topic} في الإسلام",
        "معنى {topic}",
        "{topic} وأهميته",
        "تعريف {topic}",
    ],
    "question_ar": [
        "ما هو {topic}؟",
        "ما حكم {topic}؟",
        "كيف نفهم {topic}؟",
        "ما فضل {topic}؟",
    ],
    "thematic_en": [
        "verses about {topic}",
        "hadith about {topic}",
        "what Islam says about {topic}",
        "Quran on {topic}",
        "Prophet's teachings on {topic}",
    ],
    "question_en": [
        "What is {topic} in Islam?",
        "How to achieve {topic}?",
        "What is the reward for {topic}?",
        "Islamic guidance on {topic}",
    ]
}

# English translations for themes (for bilingual queries)
THEME_TRANSLATIONS = {
    "الصبر على البلاء": "patience during trials",
    "الشكر على النعم": "gratitude for blessings",
    "التوكل على الله": "trust in Allah",
    "بر الوالدين": "honoring parents",
    "صلة الرحم": "maintaining family ties",
    "الصدق والأمانة": "truthfulness and honesty",
    "فضل الصلاة": "virtue of prayer",
    "فضل الصدقة": "virtue of charity",
    "أهوال يوم القيامة": "horrors of Day of Judgment",
    "الجنة ونعيمها": "Paradise and its blessings",
    "توحيد الألوهية": "monotheism in worship",
    "الإيمان بالقدر": "belief in destiny",
}


def parse_args():
    parser = argparse.ArgumentParser(description="Generate conceptual queries for training data")
    parser.add_argument("--input", "-i", type=str, help="Input JSONL file with passages to match")
    parser.add_argument("--output", "-o", type=str, default="quran_conceptual_queries.jsonl")
    parser.add_argument("--limit", "-l", type=int, default=None, help="Limit number of themes")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done")
    parser.add_argument("--source", type=str, choices=["quran", "hadith", "all"], default="all")
    parser.add_argument("--batch-size", type=int, default=10, help="Themes per API batch")
    return parser.parse_args()


def load_passages_by_theme(filepath: str) -> dict:
    """Load passages and group by detected theme keywords."""
    theme_passages = {}

    # Keyword to theme mapping
    keyword_themes = {
        "صبر": "الصبر على البلاء",
        "شكر": "الشكر على النعم",
        "توكل": "التوكل على الله",
        "والد": "بر الوالدين",
        "رحم": "صلة الرحم",
        "صدق": "الصدق والأمانة",
        "أمان": "الصدق والأمانة",
        "صلاة": "فضل الصلاة",
        "صلوة": "فضل الصلاة",
        "زكاة": "فضل الصدقة",
        "صدقة": "فضل الصدقة",
        "قيامة": "أهوال يوم القيامة",
        "جنة": "الجنة ونعيمها",
        "نار": "النار وعذابها",
        "توحيد": "توحيد الألوهية",
        "إله": "توحيد الألوهية",
        "قدر": "الإيمان بالقدر",
        "صوم": "فضل الصيام",
        "صيام": "فضل الصيام",
        "حج": "الحج والعمرة",
        "ذكر": "فضل ذكر الله",
    }

    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            if not line.strip():
                continue
            try:
                data = json.loads(line)
                for pos in data.get("pos", []):
                    # Check which themes this passage matches
                    for keyword, theme in keyword_themes.items():
                        if keyword in pos:
                            if theme not in theme_passages:
                                theme_passages[theme] = []
                            if len(theme_passages[theme]) < 20:  # Limit per theme
                                theme_passages[theme].append(pos)
            except json.JSONDecodeError:
                continue

    return theme_passages


def build_conceptual_prompt(themes_with_passages: list[tuple]) -> str:
    """Build prompt for generating conceptual queries with passage matching."""
    theme_list = []
    for i, (theme, passages) in enumerate(themes_with_passages, 1):
        sample_passages = passages[:3]  # Show 3 examples
        passages_text = "\n".join([f"  - {p[:150]}..." for p in sample_passages])
        theme_list.append(f"{i}. الموضوع: {theme}\nنماذج النصوص:\n{passages_text}")

    themes_text = "\n\n".join(theme_list)

    return f"""أنت خبير في البحث الدلالي عن النصوص الإسلامية. مهمتك توليد استعلامات بحث مفاهيمية/موضوعية.

الفرق بين الاستعلام الحرفي والمفاهيمي:
- حرفي: "إنما الأعمال بالنيات" (يبحث عن النص بالضبط)
- مفاهيمي: "أهمية النية في العبادات" (يبحث عن المعنى)

المطلوب: لكل موضوع، أنشئ 4 استعلامات مفاهيمية متنوعة:

1. استعلام موضوعي بالعربية (thematic_ar):
   مثال: "آيات عن الصبر والمصائب" أو "أحاديث في فضل الذكر"

2. سؤال مفاهيمي بالعربية (question_ar):
   مثال: "ما فضل الصبر في الإسلام؟" أو "كيف يحقق المسلم التوكل؟"

3. استعلام موضوعي بالإنجليزية (thematic_en):
   مثال: "verses about patience" أو "hadith about gratitude"

4. سؤال مفاهيمي بالإنجليزية (question_en):
   مثال: "What does Islam say about patience?" أو "How to be grateful in Islam?"

الموضوعات والنصوص المرتبطة:
{themes_text}

أعد الإجابة بتنسيق JSON فقط:
[
  {{
    "theme_index": 1,
    "theme": "الصبر على البلاء",
    "queries": [
      {{"text": "آيات عن الصبر في القرآن", "type": "thematic_ar", "language": "ar"}},
      {{"text": "ما أجر الصابرين في الإسلام؟", "type": "question_ar", "language": "ar"}},
      {{"text": "Quran verses about patience during hardship", "type": "thematic_en", "language": "en"}},
      {{"text": "What is the reward for patience in Islam?", "type": "question_en", "language": "en"}}
    ]
  }}
]

مهم: أعد JSON صالح فقط، بدون أي نص آخر."""


def call_openrouter_api(prompt: str, api_key: str, retries: int = 0) -> dict:
    """Call OpenRouter API."""
    import urllib.request
    import urllib.error

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
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
    except Exception as e:
        if retries < MAX_RETRIES:
            print(f"  Retry {retries + 1}/{MAX_RETRIES}: {e}")
            time.sleep(RETRY_DELAY_MS / 1000)
            return call_openrouter_api(prompt, api_key, retries + 1)
        raise


def generate_conceptual_queries(
    theme_passages: dict,
    api_key: str,
    batch_size: int = 10,
    dry_run: bool = False
) -> list[dict]:
    """Generate conceptual queries for themes with matched passages."""
    all_pairs = []
    themes_list = list(theme_passages.items())

    for i in range(0, len(themes_list), batch_size):
        batch = themes_list[i:i + batch_size]

        if dry_run:
            # Generate placeholder pairs
            for theme, passages in batch:
                for qtype in ["thematic_ar", "question_ar", "thematic_en", "question_en"]:
                    lang = "ar" if qtype.endswith("_ar") else "en"
                    all_pairs.append({
                        "query": f"[DRY RUN] {qtype}: {theme}",
                        "pos": passages[:1],
                        "neg": [],
                        "source": "conceptual",
                        "query_type": qtype,
                        "language": lang,
                        "theme": theme
                    })
            continue

        prompt = build_conceptual_prompt(batch)
        response = call_openrouter_api(prompt, api_key)

        content = response.get("choices", [{}])[0].get("message", {}).get("content", "")
        if not content:
            continue

        # Parse response
        content = content.strip()
        if content.startswith("```json"):
            content = content[7:]
        elif content.startswith("```"):
            content = content[3:]
        if content.endswith("```"):
            content = content[:-3]

        try:
            parsed = json.loads(content.strip())

            for item in parsed:
                idx = item.get("theme_index", 1) - 1
                if idx < 0 or idx >= len(batch):
                    continue

                theme, passages = batch[idx]
                queries = item.get("queries", [])

                for q in queries:
                    all_pairs.append({
                        "query": q.get("text", ""),
                        "pos": passages[:3],  # Up to 3 relevant passages
                        "neg": [],
                        "source": "conceptual",
                        "query_type": q.get("type", "thematic_ar"),
                        "language": q.get("language", "ar"),
                        "theme": theme
                    })
        except json.JSONDecodeError as e:
            print(f"  Error parsing response: {e}")
            continue

        print(f"  Processed {min(i + batch_size, len(themes_list))}/{len(themes_list)} themes")
        time.sleep(0.1)

    return all_pairs


def main():
    args = parse_args()

    print("=" * 60)
    print("Conceptual Query Generation for BGE-M3 Training")
    print("=" * 60)
    print()

    api_key = os.environ.get("OPENROUTER_API_KEY")
    if not api_key and not args.dry_run:
        print("ERROR: OPENROUTER_API_KEY not set")
        sys.exit(1)

    script_dir = Path(__file__).parent
    data_dir = script_dir.parent / "data"

    # Load passages and group by theme
    input_file = Path(args.input) if args.input else data_dir / "combined_training.jsonl"

    if not input_file.exists():
        print(f"ERROR: Input file not found: {input_file}")
        sys.exit(1)

    print(f"Loading passages from: {input_file}")
    theme_passages = load_passages_by_theme(str(input_file))

    print(f"  Found {len(theme_passages)} themes with passages:")
    for theme, passages in list(theme_passages.items())[:5]:
        print(f"    - {theme}: {len(passages)} passages")
    if len(theme_passages) > 5:
        print(f"    ... and {len(theme_passages) - 5} more themes")
    print()

    # Add themes without passages from our predefined list
    for source, categories in ISLAMIC_THEMES.items():
        if args.source != "all" and source != args.source:
            continue
        for category, themes in categories.items():
            for theme in themes:
                if theme not in theme_passages:
                    # We'll generate queries but mark them for later passage matching
                    theme_passages[theme] = []

    # Filter to themes with passages
    theme_passages = {k: v for k, v in theme_passages.items() if len(v) > 0}

    if args.limit:
        theme_passages = dict(list(theme_passages.items())[:args.limit])

    print(f"Processing {len(theme_passages)} themes with passages")
    print()

    if args.dry_run:
        print("DRY RUN - No API calls")
        print()
        print("Sample themes and passages:")
        for theme, passages in list(theme_passages.items())[:3]:
            print(f"\n{theme}:")
            for p in passages[:2]:
                print(f"  - {p[:80]}...")
        return

    # Generate queries
    print("Generating conceptual queries...")
    pairs = generate_conceptual_queries(
        theme_passages,
        api_key,
        args.batch_size,
        args.dry_run
    )

    # Write output
    output_path = data_dir / args.output
    with open(output_path, 'w', encoding='utf-8') as f:
        for pair in pairs:
            f.write(json.dumps(pair, ensure_ascii=False) + "\n")

    print()
    print("=" * 60)
    print("GENERATION COMPLETE")
    print("=" * 60)
    print(f"  Total pairs generated: {len(pairs)}")
    print(f"  Output: {output_path}")

    # Stats
    ar_count = sum(1 for p in pairs if p["language"] == "ar")
    en_count = sum(1 for p in pairs if p["language"] == "en")
    print(f"  Arabic queries: {ar_count}")
    print(f"  English queries: {en_count}")

    # Sample
    print()
    print("Sample output:")
    print("-" * 40)
    for pair in pairs[:3]:
        print(f"Query ({pair['query_type']}): {pair['query']}")
        print(f"Theme: {pair.get('theme', 'N/A')}")
        print()


if __name__ == "__main__":
    main()
