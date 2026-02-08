/**
 * Arabic to English/Latin Transliteration using OpenRouter API
 *
 * Transliterates Arabic names to their standard English/Latin scholarly equivalents.
 * Uses Google Gemini via OpenRouter for high-quality, contextually appropriate transliterations.
 */

const OPENROUTER_API_URL = "https://openrouter.ai/api/v1/chat/completions";
const MODEL = "google/gemini-2.0-flash-001";
const BATCH_SIZE = 15; // Smaller batches to ensure complete responses

interface TransliterationResult {
  arabic: string;
  latin: string;
}

interface OpenRouterResponse {
  choices: {
    message: {
      content: string;
    };
  }[];
}

/**
 * Transliterate a batch of Arabic names to English/Latin
 *
 * @param names Array of Arabic names to transliterate
 * @returns Map of Arabic names to their Latin transliterations
 */
export async function transliterateArabicBatch(
  names: string[]
): Promise<Map<string, string>> {
  const results = new Map<string, string>();

  if (names.length === 0) {
    return results;
  }

  // Format names as numbered list for clear parsing
  const numberedNames = names.map((name, i) => `${i + 1}. ${name}`).join("\n");

  const prompt = `You are an expert in Arabic-English transliteration for Islamic scholarly texts.

Transliterate the following Arabic names to their standard English/Latin equivalents. Use the most commonly recognized scholarly transliteration for famous scholars. For book titles, use standard academic transliteration conventions.

Guidelines:
- Use established English spellings for well-known scholars (e.g., Ibn Taymiyyah, Ibn Kathir, Ibn al-Jawzi)
- For book titles, use "Kitab" for كتاب, use standard scholarly transliteration
- Use "al-" for the Arabic definite article
- Maintain consistency with Library of Congress romanization standards where applicable
- Do NOT include diacritical marks (no macrons, dots under letters, etc.)
- Keep transliterations clean and readable
- IMPORTANT: Transliterate ALL Arabic text to Latin script. Do not leave any Arabic characters in the output.
  - ضمن should be transliterated as "dimin" or "min"
  - Every single Arabic word must be converted to Latin letters

Respond with ONLY a JSON array of objects with "arabic" and "latin" keys, in the same order as the input.
Example response format:
[{"arabic": "ابن تيمية", "latin": "Ibn Taymiyyah"}, {"arabic": "الكافي", "latin": "Al-Kafi"}]

Arabic names to transliterate:
${numberedNames}`;

  const apiKey = process.env.OPENROUTER_API_KEY;
  if (!apiKey) {
    throw new Error("OPENROUTER_API_KEY not set in environment");
  }

  try {
    const response = await fetch(OPENROUTER_API_URL, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
        "HTTP-Referer": "https://arabic-texts-library.local",
        "X-Title": "Arabic Texts Library",
      },
      body: JSON.stringify({
        model: MODEL,
        messages: [
          {
            role: "user",
            content: prompt,
          },
        ],
        max_tokens: 8192,
      }),
    });

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`OpenRouter API error: ${response.status} - ${errorText}`);
    }

    const data = (await response.json()) as OpenRouterResponse;

    // Extract text content from response
    const textContent = data.choices?.[0]?.message?.content;
    if (!textContent) {
      throw new Error("No text content in response");
    }

    // Parse JSON response
    const jsonText = textContent.trim();
    // Handle potential markdown code blocks
    const cleanJson = jsonText.replace(/```json\n?|\n?```/g, "").trim();

    let parsed: TransliterationResult[];
    try {
      parsed = JSON.parse(cleanJson);
    } catch (parseError) {
      console.error("Failed to parse JSON response:");
      console.error("Raw response:", textContent.substring(0, 500));
      throw new Error(`JSON parse failed: ${parseError}`);
    }

    // Map results back
    for (const item of parsed) {
      results.set(item.arabic, item.latin);
    }

    // Verify all names were transliterated
    for (const name of names) {
      if (!results.has(name)) {
        console.warn(`  Warning: No transliteration returned for "${name}"`);
      }
    }

    return results;
  } catch (error) {
    console.error("Transliteration API error:", error);
    throw error;
  }
}

/**
 * Transliterate multiple Arabic names with automatic batching
 *
 * @param names Array of Arabic names to transliterate
 * @param onProgress Optional callback for progress updates
 * @returns Map of Arabic names to their Latin transliterations
 */
export async function transliterateArabic(
  names: string[],
  onProgress?: (completed: number, total: number) => void
): Promise<Map<string, string>> {
  const allResults = new Map<string, string>();
  const uniqueNames = [...new Set(names)];

  // Process in batches
  for (let i = 0; i < uniqueNames.length; i += BATCH_SIZE) {
    const batch = uniqueNames.slice(i, i + BATCH_SIZE);
    const batchResults = await transliterateArabicBatch(batch);

    for (const [arabic, latin] of batchResults) {
      allResults.set(arabic, latin);
    }

    if (onProgress) {
      onProgress(
        Math.min(i + BATCH_SIZE, uniqueNames.length),
        uniqueNames.length
      );
    }

    // Small delay between batches to avoid rate limiting
    if (i + BATCH_SIZE < uniqueNames.length) {
      await new Promise((resolve) => setTimeout(resolve, 500));
    }
  }

  return allResults;
}

/**
 * Transliterate a single Arabic name
 *
 * @param name Arabic name to transliterate
 * @returns Latin transliteration
 */
export async function transliterateSingle(name: string): Promise<string> {
  const results = await transliterateArabicBatch([name]);
  return results.get(name) || name;
}
