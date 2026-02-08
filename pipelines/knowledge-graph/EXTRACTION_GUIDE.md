# Knowledge Graph Entity Extraction Guide

Reference for extracting entities and relationships from Quran ayahs + Ibn Kathir tafsir. Follow this guide for consistency when scaling beyond the pilot surahs.

## Input

Read `data/surah-{N}.json` dump files. Each contains ayah groups with:
- `id`: Group identifier (e.g. `28:7` or `108:1-3`)
- `ayahTextArabic`: Uthmani text with ayah markers
- `tafsirText`: Ibn Kathir tafsir for those ayahs

## Output

Write `data/extracted-{N}.json` with this structure:

```json
{
  "entities": [
    {
      "id": "type-slug:english-name-slug",
      "type": "EntityType",
      "nameArabic": "الاسم بالعربية",
      "nameEnglish": "English Name (Transliteration if needed)",
      "descriptionArabic": "وصف مختصر بالعربية",
      "descriptionEnglish": "Brief English description",
      "sources": [
        { "type": "quran", "ref": "28:7" },
        { "type": "hadith", "ref": "bukhari:3394" },
        { "type": "tafsir", "ref": "ibn-kathir:28:7" }
      ]
    }
  ],
  "relationships": [
    {
      "source": "entity-id",
      "target": "entity-id",
      "type": "RELATIONSHIP_TYPE",
      "description": "English description of the relationship",
      "sources": [
        { "type": "quran", "ref": "28:36-37" }
      ]
    },
    {
      "source": "entity-id",
      "target": "28:7",
      "type": "MENTIONED_IN",
      "role": "primary|secondary|referenced",
      "context": "Brief English description of the entity's role in this passage"
    }
  ]
}
```

## Entity ID Format

`{type-slug}:{english-name-slug}`

- Type slug is lowercase, hyphenated: `prophet`, `person`, `place`, `afterlife-place`, `divine-attribute`, `event`, `concept`, `nation`, `angel`, `ruling`, `scripture`, `object`, `time-reference`
- Name slug is lowercase, hyphenated English: `moses`, `al-kawthar`, `children-of-israel`
- IDs must be globally unique across all surahs (same entity = same ID)

Examples:
- `prophet:moses`, `prophet:muhammad`, `prophet:ibrahim`
- `person:pharaoh`, `person:asiyah`, `person:qarun`
- `place:egypt`, `place:midian`, `place:makkah`
- `afterlife-place:paradise`, `afterlife-place:al-kawthar`
- `divine-attribute:allah`, `divine-attribute:al-ahad`
- `event:flight-to-midian`, `event:killing-the-copt`
- `concept:tawhid`, `concept:sabr`, `concept:tawakkul`
- `nation:bani-israel`, `nation:quraysh`

## Entity Types

Extract **fine-grained named entities** — specific names, not generic categories.

| Type | Arabic examples | English examples |
|------|----------------|-----------------|
| `Prophet` | موسى عليه السلام، محمد ﷺ | Moses (Musa), Muhammad ﷺ |
| `Person` | فرعون، آسية بنت مزاحم، قارون | Pharaoh, Asiyah wife of Pharaoh, Qarun |
| `Angel` | جبريل، ميكائيل | Gabriel (Jibril), Michael (Mika'il) |
| `Nation` | بني إسرائيل، قريش، أهل مدين | Children of Israel, Quraysh, People of Midian |
| `Place` | مصر، مدين، مكة، طور سيناء | Egypt, Midian, Mecca, Mount Sinai |
| `AfterlifePlace` | الجنة، جهنم، الكوثر | Paradise (Jannah), Hellfire (Jahannam), Al-Kawthar |
| `Event` | قتل القبطي، خروج بني إسرائيل | Killing of the Copt, Exodus of the Children of Israel |
| `Concept` | توحيد، صبر، توكل | Monotheism (Tawhid), Patience (Sabr), Tawakkul |
| `DivineAttribute` | الأحد، الصمد، الرحمن | The One (Al-Ahad), The Eternal Refuge (As-Samad) |
| `Ruling` | الصلاة، النحر | Prayer (Salah), Sacrifice (Nahr) |
| `Scripture` | التوراة، القرآن | Torah, Quran |
| `Object` | عصا موسى، التابوت | Staff of Moses, The Chest/Ark |
| `TimeReference` | يوم القيامة، ليلة القدر | Day of Judgment, Night of Decree |

## Bilingual Convention

- **Arabic fields** (`nameArabic`, `descriptionArabic`): Use authentic Islamic Arabic terminology
- **English fields** (`nameEnglish`, `descriptionEnglish`): Use standard English that LLMs understand well; include transliteration in parentheses when helpful: "Moses (Musa)", "Patience (Sabr)"
- Entity IDs use English slugs for global deduplication and LLM traversability

## Relationship Types

Relationships are **dynamic** — use whatever type best describes the connection. Do not limit to a fixed set. Use SCREAMING_SNAKE_CASE.

### Common types for reference

**Structural:** `PART_OF`, `LOCATED_IN`, `INSTANCE_OF`, `LEVEL_OF`

**Interpersonal:** `PARENT_OF`, `CHILD_OF`, `SPOUSE_OF`, `SIBLING_OF`, `SENT_TO`, `ADOPTED_BY`, `OPPOSED`, `SERVED`, `MARRIED`, `WARNED`

**Event-entity:** `PARTICIPATED_IN`, `OCCURRED_AT`, `CAUSED_BY`, `RESULTED_IN`, `RESULTED_FROM`, `CAUSED`, `LED_TO`, `COMMANDED_BY`

**Theological:** `ATTRIBUTE_OF`, `REVEALED_TO`, `COMMANDED`, `PROMISED_TO`, `NEGATED_FOR`, `BELIEVED_IN`, `SPOKE_TO`, `INSPIRED`

**New types are encouraged** when the text describes relationships not listed above (e.g. `FLED_TO`, `NURSED_BY`, `WITNESSED`, `REJECTED`, `BUILT`, `EXEMPLIFIED`).

### MENTIONED_IN edges

Every entity gets `MENTIONED_IN` edges to the ayah groups where it appears.

- `source`: entity ID
- `target`: ayah group ID (e.g. `28:7`, `108:1-3`) — **without** the `ayahgroup:` prefix
- `role`: one of:
  - `primary` — the entity is a main subject of this passage
  - `secondary` — the entity plays a supporting role
  - `referenced` — the entity is mentioned in passing (e.g. hadith narrators, scholarly references)
- `context`: brief English description of the entity's role in this specific passage

## Deduplication

The same entity appearing across multiple surahs must use the **same ID**. When extracting a new surah, check existing `extracted-*.json` files for entities that already exist and reuse their IDs.

Common cross-surah entities: `divine-attribute:allah`, `prophet:muhammad`, `prophet:moses`, `afterlife-place:paradise`, `time-reference:day-of-judgment`, `scripture:quran`.

## Golden Examples

These are real examples from the pilot extraction (surahs 28, 108, 112). Use them as a reference for tone, granularity, and style.

### Entity examples

**Prophet** — Include honorifics in Arabic, transliteration in English:
```json
{
  "id": "prophet:moses",
  "type": "Prophet",
  "nameArabic": "موسى عليه السلام",
  "nameEnglish": "Moses (Musa)",
  "descriptionArabic": "نبي أرسله الله إلى فرعون وبني إسرائيل، نشأ في بيت فرعون ثم فر إلى مدين ثم عاد برسالة الله",
  "descriptionEnglish": "Prophet sent by Allah to Pharaoh and the Children of Israel, raised in Pharaoh's household, fled to Midian, then returned with Allah's message"
}
```

**Person** — Non-prophets with their defining relationship:
```json
{
  "id": "person:asiyah",
  "type": "Person",
  "nameArabic": "آسية بنت مزاحم",
  "nameEnglish": "Asiyah wife of Pharaoh",
  "descriptionArabic": "امرأة فرعون، التقطت موسى من النهر وأقنعت فرعون بتبنيه، آمنت بالله",
  "descriptionEnglish": "Wife of Pharaoh, rescued baby Moses from the river and convinced Pharaoh to adopt him, believed in Allah"
}
```

**AfterlifePlace** — Explain what it is in English for LLM clarity:
```json
{
  "id": "afterlife-place:al-kawthar",
  "type": "AfterlifePlace",
  "nameArabic": "الكوثر",
  "nameEnglish": "Al-Kawthar (river in Paradise)",
  "descriptionArabic": "نهر في الجنة أعطاه الله للنبي محمد ﷺ، حافتاه من ذهب وترابه المسك وحصباؤه اللؤلؤ",
  "descriptionEnglish": "A river in Paradise granted to Prophet Muhammad ﷺ, its banks are of gold, its soil is musk, and its pebbles are pearls"
}
```

**DivineAttribute** — Name of Allah with its meaning:
```json
{
  "id": "divine-attribute:as-samad",
  "type": "DivineAttribute",
  "nameArabic": "الصمد",
  "nameEnglish": "The Eternal Refuge (As-Samad)",
  "descriptionArabic": "السيد الذي كمل في سؤدده، الذي يصمد إليه الخلائق في حوائجهم، الذي لا جوف له",
  "descriptionEnglish": "The Master who is perfect in His sovereignty, to whom all creation turns in need, the Self-Sufficient who has no hollow"
}
```

**Event** — Specific named event, not generic:
```json
{
  "id": "event:killing-the-copt",
  "type": "Event",
  "nameArabic": "قتل القبطي",
  "nameEnglish": "Killing of the Copt",
  "descriptionArabic": "دخل موسى المدينة فوجد إسرائيلياً يتقاتل مع قبطي فوكز القبطي فقضى عليه",
  "descriptionEnglish": "Moses entered the city and found an Israelite fighting a Copt; he struck the Copt and killed him"
}
```

**Concept** — Abstract theological/moral concept with transliteration:
```json
{
  "id": "concept:tawakkul",
  "type": "Concept",
  "nameArabic": "التوكل على الله",
  "nameEnglish": "Reliance on Allah (Tawakkul)",
  "descriptionArabic": "الاعتماد على الله والثقة بوعده، كما فعلت أم موسى حين ألقته في اليمّ",
  "descriptionEnglish": "Trusting in Allah and His promise, as Moses' mother did when she cast him into the river"
}
```

**Object** — Physical item mentioned in the narrative:
```json
{
  "id": "object:staff-of-moses",
  "type": "Object",
  "nameArabic": "عصا موسى",
  "nameEnglish": "Staff of Moses",
  "descriptionArabic": "العصا التي ألقاها موسى فصارت حية تسعى، آية من آيات الله",
  "descriptionEnglish": "The staff Moses cast down and it became a living serpent — a sign from Allah"
}
```

**Ruling** — An Islamic command or obligation:
```json
{
  "id": "ruling:nahr",
  "type": "Ruling",
  "nameArabic": "النحر",
  "nameEnglish": "Sacrifice (Nahr)",
  "descriptionArabic": "ذبح الأضاحي والبدن لله وحده",
  "descriptionEnglish": "Slaughtering sacrificial animals for the sake of Allah alone"
}
```

**Nation** — A people or group:
```json
{
  "id": "nation:bani-israel",
  "type": "Nation",
  "nameArabic": "بنو إسرائيل",
  "nameEnglish": "Children of Israel (Bani Israel)",
  "descriptionArabic": "القوم الذين استضعفهم فرعون وذبّح أبناءهم، أرسل الله إليهم موسى لتحريرهم",
  "descriptionEnglish": "The people oppressed by Pharaoh who slaughtered their sons; Allah sent Moses to liberate them"
}
```

### Relationship examples

**Interpersonal** — between people/prophets:
```json
{"source": "prophet:moses", "target": "person:pharaoh", "type": "SENT_TO", "description": "Moses was sent by Allah as a messenger to Pharaoh"}
{"source": "prophet:moses", "target": "person:asiyah", "type": "ADOPTED_BY", "description": "Asiyah wife of Pharaoh convinced Pharaoh to adopt baby Moses"}
{"source": "person:pharaoh", "target": "prophet:moses", "type": "OPPOSED", "description": "Pharaoh rejected Moses' message, tried to kill him, and persecuted his followers"}
{"source": "person:pharaoh", "target": "nation:bani-israel", "type": "OPPRESSED", "description": "Pharaoh enslaved the Children of Israel, slaughtered their sons, and spared their women"}
{"source": "person:believing-man", "target": "prophet:moses", "type": "WARNED", "description": "Came running to warn Moses that the chiefs were plotting to kill him"}
{"source": "prophet:moses", "target": "person:daughter-of-shuayb", "type": "MARRIED", "description": "Moses married one of Shu'ayb's daughters after serving him for eight years"}
```

**Event-entity** — linking events to participants, places, causes:
```json
{"source": "event:killing-the-copt", "target": "event:flight-to-midian", "type": "CAUSED", "description": "The killing of the Copt forced Moses to flee Egypt to avoid execution"}
{"source": "event:burning-bush", "target": "place:valley-of-tuwa", "type": "OCCURRED_AT", "description": "Allah addressed Moses at the fire in the sacred valley near Mount Tur"}
{"source": "event:watering-at-midian", "target": "event:marriage-of-moses", "type": "LED_TO", "description": "Moses watering the flock led to meeting Shu'ayb and marrying his daughter"}
{"source": "event:slaughter-of-sons", "target": "event:casting-of-baby-moses", "type": "CAUSED", "description": "The slaughter campaign forced Moses' mother to cast him into the river"}
{"source": "event:adoption-by-pharaoh", "target": "concept:divine-qadr", "type": "EXEMPLIFIED", "description": "Moses raised by the very person who sought to kill Israelite boys — illustrating divine decree"}
```

**Theological** — divine attributes, commands, promises:
```json
{"source": "divine-attribute:al-ahad", "target": "divine-attribute:allah", "type": "ATTRIBUTE_OF", "description": "Al-Ahad (The One) is an attribute and name of Allah affirming His absolute uniqueness"}
{"source": "divine-attribute:allah", "target": "prophet:moses", "type": "SPOKE_TO", "description": "Allah directly spoke to Moses at the burning bush in the sacred valley"}
{"source": "divine-attribute:allah", "target": "person:mother-of-moses", "type": "INSPIRED", "description": "Allah inspired Moses' mother to nurse him and cast him into the river"}
{"source": "afterlife-place:al-kawthar", "target": "prophet:muhammad", "type": "PROMISED_TO", "description": "Allah granted Al-Kawthar to Prophet Muhammad ﷺ"}
{"source": "scripture:torah", "target": "prophet:moses", "type": "REVEALED_TO", "description": "The Torah was revealed to Moses as guidance and mercy"}
{"source": "concept:tanzih", "target": "divine-attribute:allah", "type": "APPLIED_TO", "description": "Tanzih negates offspring, parentage, and any equal from Allah"}
```

**Structural** — hierarchical or containment:
```json
{"source": "afterlife-place:al-kawthar", "target": "afterlife-place:paradise", "type": "PART_OF", "description": "Al-Kawthar is a river located in Paradise"}
{"source": "afterlife-place:al-hawdh", "target": "afterlife-place:al-kawthar", "type": "FED_BY", "description": "The Pool on the Day of Judgment is fed by two channels from Al-Kawthar"}
{"source": "person:qarun", "target": "nation:bani-israel", "type": "PART_OF", "description": "Qarun was from the people of Moses (Children of Israel)"}
```

### MENTIONED_IN examples

**Primary** — the entity is a main subject of the passage:
```json
{"source": "person:pharaoh", "target": "28:4", "type": "MENTIONED_IN", "role": "primary", "context": "Pharaoh's tyranny described: he divided the people, oppressed a group, slaughtered their sons"}
{"source": "event:casting-of-baby-moses", "target": "28:7", "type": "MENTIONED_IN", "role": "primary", "context": "The divine command to cast baby Moses into the river"}
{"source": "divine-attribute:al-ahad", "target": "112:1", "type": "MENTIONED_IN", "role": "primary", "context": "The name Al-Ahad is explicitly stated — He is unique, not applied to anyone else in the affirmative"}
{"source": "ruling:salah", "target": "108:2-3", "type": "MENTIONED_IN", "role": "primary", "context": "Command to pray exclusively for Allah as gratitude for Al-Kawthar"}
```

**Secondary** — the entity plays a supporting role:
```json
{"source": "afterlife-place:paradise", "target": "108:1", "type": "MENTIONED_IN", "role": "secondary", "context": "Paradise is the location of Al-Kawthar as explained in tafsir"}
{"source": "angel:jibril", "target": "108:1", "type": "MENTIONED_IN", "role": "secondary", "context": "Jibril identified Al-Kawthar for the Prophet during the Night Journey"}
{"source": "nation:christians", "target": "112:2-4", "type": "MENTIONED_IN", "role": "referenced", "context": "Their claim that the Messiah is son of God is implicitly refuted by 'He begets not'"}
```

**Referenced** — mentioned in passing (tafsir narrators, tangential figures):
```json
{"source": "person:anas-ibn-malik", "target": "108:1", "type": "MENTIONED_IN", "role": "referenced", "context": "Primary narrator of the hadiths about Al-Kawthar"}
{"source": "person:ubayy-ibn-kaab", "target": "112:1", "type": "MENTIONED_IN", "role": "referenced", "context": "Narrated the hadith about the cause of revelation"}
{"source": "prophet:ibrahim", "target": "28:4", "type": "MENTIONED_IN", "role": "referenced", "context": "Ibn Kathir mentions Abraham's prophecy about the destruction of Egypt's king through his lineage"}
```

### Style notes from the examples

- **Descriptions are 1-2 sentences** — enough to understand the entity, not a full biography
- **Arabic descriptions** use Islamic phrasing naturally (عليه السلام، ﷺ، رضي الله عنه not needed for every person)
- **English descriptions** are plain, informative, and avoid transliteration jargon unless it adds clarity
- **Relationship descriptions** are one clear sentence explaining the specific connection
- **MENTIONED_IN context** explains the entity's specific role in *that* passage, not a general description
- **Events are named from the narrative perspective** — "Casting of Baby Moses into the River" not "Moses being put in water"
- **Concepts tie to concrete examples** when possible — Tawakkul is described via Moses' mother's act, not as an abstract definition alone

## Sources (Citation Provenance)

Every entity and entity-entity relationship must include a `sources` array that references specific locations in the library. Sources are the **evidence** — they link back to actual text in PostgreSQL so the frontend can display real Quran/hadith/tafsir text as citations, not LLM-generated descriptions.

### Source types and ref format

| Type | Ref format | Example | PostgreSQL lookup |
|------|-----------|---------|-------------------|
| `quran` | `{surah}:{ayah}` or `{surah}:{start}-{end}` | `"28:7"`, `"28:36-37"` | `Ayah WHERE surahNumber AND ayahNumber` |
| `hadith` | `{collection}:{number}` | `"bukhari:3394"` | `Hadith WHERE collectionSlug AND hadithNumber` |
| `tafsir` | `{source}:{surah}:{ayah}` | `"ibn-kathir:28:7"` | `AyahTafsir WHERE source AND surahNumber AND ayahNumber` |
| `book` | `book:{bookId}:{page}` | `"book:12345:42"` | `Page WHERE bookId AND pageNumber` |

### On entities — "Where is this entity discussed?"

Derive from MENTIONED_IN edges plus cross-library references:
- Include quran refs for all ayah groups where the entity is mentioned (primary and secondary roles)
- Include tafsir refs for the primary ayah mentions (`ibn-kathir:{surah}:{ayah}`)
- Include hadith refs for well-known hadiths that discuss this entity (only include refs you're confident about)

### On entity-entity relationships — "What evidence supports this claim?"

Each relationship's `sources` should list the specific ayahs/hadiths that describe THIS particular relationship:
- Focus on 1-4 of the most relevant refs
- Prefer refs from the current surah, but include cross-surah refs if the relationship is discussed elsewhere

### MENTIONED_IN edges do NOT get sources

MENTIONED_IN edges inherently point to an ayah group — they ARE the source link. No additional `sources` field needed.

## Extraction Principles

1. **Be specific**: Extract "Pharaoh" not "tyrant"; "Killing of the Copt" not "a killing"
2. **Follow the text**: Only extract entities and relationships that appear in the ayah text or are directly discussed in the tafsir
3. **Bilingual always**: Every entity must have both Arabic and English names/descriptions
4. **Capture narrative structure**: Events should link to participants, locations, and causes/effects
5. **Don't over-extract hadith narrators**: Only include narrators mentioned in the tafsir as `referenced` role if they are historically significant (e.g. Anas ibn Malik, Ibn Abbas), not every chain narrator
6. **Theological concepts matter**: Extract divine attributes, rulings, and abstract concepts — these are important for knowledge graph traversal
