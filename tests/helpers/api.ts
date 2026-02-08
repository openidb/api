const BASE_URL = process.env.OPENIDB_TEST_URL || "http://localhost:4000";

interface ApiResponse<T> {
  status: number;
  data: T;
  headers: Headers;
}

export async function get<T = any>(
  path: string,
  params?: Record<string, string | number | boolean>
): Promise<ApiResponse<T>> {
  const url = new URL(path, BASE_URL);
  if (params) {
    for (const [key, value] of Object.entries(params)) {
      url.searchParams.set(key, String(value));
    }
  }
  const res = await fetch(url.toString());
  const data = (await res.json()) as T;
  return { status: res.status, data, headers: res.headers };
}

export async function post<T = any>(
  path: string,
  body: unknown
): Promise<ApiResponse<T>> {
  const url = new URL(path, BASE_URL);
  const res = await fetch(url.toString(), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  const data = (await res.json()) as T;
  return { status: res.status, data, headers: res.headers };
}

export function expectOk<T>(res: ApiResponse<T>): T {
  if (res.status < 200 || res.status >= 300) {
    throw new Error(
      `Expected 2xx status, got ${res.status}: ${JSON.stringify(res.data)}`
    );
  }
  return res.data;
}

export function expectPagination(data: any) {
  expect(typeof data.total).toBe("number");
  expect(data.total).toBeGreaterThanOrEqual(0);
  expect(typeof data.limit).toBe("number");
  expect(data.limit).toBeGreaterThan(0);
  expect(typeof data.offset).toBe("number");
  expect(data.offset).toBeGreaterThanOrEqual(0);
}

export function expectSources(data: any) {
  expect(data._sources).toBeDefined();
  expect(Array.isArray(data._sources)).toBe(true);
  for (const source of data._sources) {
    expect(typeof source.name).toBe("string");
    expect(typeof source.url).toBe("string");
    expect(typeof source.type).toBe("string");
  }
}
