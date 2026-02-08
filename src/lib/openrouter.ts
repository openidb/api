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
}): Promise<{ content: string } | null> {
  const apiKey = process.env.OPENROUTER_API_KEY;
  if (!apiKey) return null;

  const { model, messages, temperature = 0, timeoutMs = 15000 } = opts;

  const response = await fetchWithTimeout(
    OPENROUTER_URL,
    {
      method: "POST",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ model, messages, temperature }),
    },
    timeoutMs
  );

  if (!response.ok) {
    throw new Error(`OpenRouter API error: ${response.statusText}`);
  }

  const data = await response.json();
  const content = data.choices?.[0]?.message?.content || "";
  return { content };
}
