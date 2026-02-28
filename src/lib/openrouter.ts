const OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions";

async function fetchWithTimeout(
  url: string,
  options: RequestInit,
  timeoutMs: number
): Promise<Response> {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, { ...options, signal: controller.signal });
  } finally {
    clearTimeout(timeoutId);
  }
}

export async function callOpenRouter(opts: {
  model: string;
  messages: Array<{ role: string; content: string }>;
  temperature?: number;
  timeoutMs?: number;
  maxTokens?: number;
}): Promise<{ content: string } | null> {
  const apiKey = process.env.OPENROUTER_API_KEY;
  if (!apiKey) return null;

  const { model, messages, temperature = 0, timeoutMs = 15000, maxTokens } = opts;

  const body: Record<string, any> = { model, messages, temperature };
  if (maxTokens) body.max_tokens = maxTokens;

  const response = await fetchWithTimeout(
    OPENROUTER_URL,
    {
      method: "POST",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify(body),
    },
    timeoutMs
  );

  if (!response.ok) {
    console.error(`[openrouter] API error: ${response.status} ${response.statusText}`);
    throw new Error("Translation service error");
  }

  const data = await response.json();
  const content = data.choices?.[0]?.message?.content || "";
  const finishReason = data.choices?.[0]?.finish_reason;
  if (finishReason && finishReason !== "stop") {
    console.warn(`[openrouter] finish_reason: ${finishReason} (usage: ${JSON.stringify(data.usage || {})})`);
  }
  return { content };
}
