import { getCachedExpansion, setCachedExpansion } from "../../query-expansion-cache";
import { callOpenRouter } from "../../lib/openrouter";
import type { ExpandedQuery } from "./types";

export function getQueryExpansionModelId(model: string): string {
  switch (model) {
    case "gpt-oss-120b":
      return "openai/gpt-oss-120b";
    case "gemini-flash":
    default:
      return "google/gemini-3-flash-preview";
  }
}

export async function expandQuery(query: string, model: string = "gemini-flash"): Promise<ExpandedQuery[]> {
  const cached = getCachedExpansion(query);
  if (cached) {
    return cached;
  }

  try {
    const prompt = `You are a search query expansion expert for an Arabic/Islamic text search engine covering Quran, Hadith, and classical Islamic books.

User Query: "${query}"

Your task: Generate 4 alternative search queries that will help find what the user is actually looking for.

EXPANSION STRATEGIES (use the most relevant):

1. **ANSWER-ORIENTED** (if query is a question)
2. **TOPIC VARIANTS** - Arabic equivalents, root variations, related terminology
3. **CONTEXTUAL EXPANSION** - What sources would discuss this topic?
4. **SEMANTIC BRIDGES** - English query to Arabic content terms

Return ONLY a JSON array of query strings:
["expanded query 1", "expanded query 2", "expanded query 3", "expanded query 4"]

IMPORTANT:
- Prioritize queries that would find ANSWERS, not just mentions
- Include at least one Arabic query if the original is English (and vice versa)
- Keep queries 2-5 words, focused and searchable
- Don't include the original query in your response`;

    const modelId = getQueryExpansionModelId(model);
    const result = await callOpenRouter({
      model: modelId,
      messages: [{ role: "user", content: prompt }],
      temperature: 0.3,
      timeoutMs: 15000,
    });

    if (!result) {
      return [{ query, weight: 1.0, reason: "Original query" }];
    }

    const match = result.content.match(/\[[\s\S]*\]/);
    if (!match) {
      console.warn("Query expansion returned invalid format");
      return [{ query, weight: 1.0, reason: "Original query" }];
    }

    const expanded: string[] = JSON.parse(match[0]);

    const results: ExpandedQuery[] = [
      { query, weight: 1.0, reason: "Original query" },
    ];

    for (let i = 0; i < Math.min(expanded.length, 4); i++) {
      const expQuery = typeof expanded[i] === 'string' ? expanded[i] : (expanded[i] as any)?.query;
      if (expQuery && expQuery.trim() && expQuery !== query) {
        results.push({
          query: expQuery.trim(),
          weight: 0.7,
          reason: `Expanded query ${i + 1}`,
        });
      }
    }

    setCachedExpansion(query, results);
    return results;
  } catch (err) {
    console.warn("Query expansion error:", err);
    return [{ query, weight: 1.0, reason: "Original query" }];
  }
}

export async function expandQueryWithCacheInfo(query: string, model: string = "gemini-flash"): Promise<{ queries: ExpandedQuery[]; cached: boolean }> {
  const cached = getCachedExpansion(query);
  if (cached) {
    return { queries: cached, cached: true };
  }
  const queries = await expandQuery(query, model);
  return { queries, cached: false };
}
