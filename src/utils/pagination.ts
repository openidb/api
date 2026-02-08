export function parseBoundedInt(value: string | undefined, defaultVal: number, min: number, max: number): number {
  const parsed = parseInt(value || String(defaultVal), 10);
  return Math.min(Math.max(isNaN(parsed) ? defaultVal : parsed, min), max);
}

export function parseBoundedFloat(value: string | undefined, defaultVal: number, min: number, max: number): number {
  const parsed = parseFloat(value || String(defaultVal));
  return Math.min(Math.max(isNaN(parsed) ? defaultVal : parsed, min), max);
}

export function parsePagination(
  limitParam: string | undefined,
  offsetParam: string | undefined,
  defaultLimit = 20,
  maxLimit = 100
): { limit: number; offset: number } {
  return {
    limit: parseBoundedInt(limitParam, defaultLimit, 1, maxLimit),
    offset: Math.max(parseInt(offsetParam || "0", 10), 0),
  };
}
