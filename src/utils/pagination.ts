export function parsePagination(
  limitParam: string | undefined,
  offsetParam: string | undefined,
  defaultLimit = 20,
  maxLimit = 100
): { limit: number; offset: number } {
  return {
    limit: Math.min(Math.max(parseInt(limitParam || String(defaultLimit), 10), 1), maxLimit),
    offset: Math.max(parseInt(offsetParam || "0", 10), 0),
  };
}
