export function startTimer(): () => number {
  const start = Date.now();
  return () => Date.now() - start;
}
