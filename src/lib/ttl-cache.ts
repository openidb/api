interface CacheEntry<T> {
  value: T;
  timestamp: number;
}

export class TTLCache<T> {
  private cache = new Map<string, CacheEntry<T>>();
  private maxSize: number;
  private ttlMs: number;
  private evictionCount: number;
  private label: string;

  constructor(opts: { maxSize: number; ttlMs: number; evictionCount: number; label?: string }) {
    this.maxSize = opts.maxSize;
    this.ttlMs = opts.ttlMs;
    this.evictionCount = opts.evictionCount;
    this.label = opts.label || "Cache";
  }

  get(key: string): T | null {
    const entry = this.cache.get(key);
    if (!entry) return null;
    if (Date.now() - entry.timestamp > this.ttlMs) {
      this.cache.delete(key);
      return null;
    }
    return entry.value;
  }

  getMany(keys: string[]): Map<string, T> {
    const result = new Map<string, T>();
    const now = Date.now();
    for (const key of keys) {
      const entry = this.cache.get(key);
      if (entry) {
        if (now - entry.timestamp > this.ttlMs) {
          this.cache.delete(key);
        } else {
          result.set(key, entry.value);
        }
      }
    }
    return result;
  }

  set(key: string, value: T): void {
    if (this.cache.size >= this.maxSize) this.evict();
    this.cache.set(key, { value, timestamp: Date.now() });
  }

  setMany(entries: Array<{ key: string; value: T }>): void {
    if (this.cache.size + entries.length >= this.maxSize) this.evict();
    const now = Date.now();
    for (const { key, value } of entries) {
      this.cache.set(key, { value, timestamp: now });
    }
  }

  get stats(): { size: number; maxSize: number; ttlMs: number } {
    return { size: this.cache.size, maxSize: this.maxSize, ttlMs: this.ttlMs };
  }

  clear(): void {
    this.cache.clear();
  }

  private evict(): void {
    const entries = [...this.cache.entries()];
    entries.sort((a, b) => a[1].timestamp - b[1].timestamp);
    const toEvict = entries.slice(0, this.evictionCount);
    for (const [key] of toEvict) {
      this.cache.delete(key);
    }
    console.log(`[${this.label}Cache] Evicted ${toEvict.length} entries, cache size: ${this.cache.size}`);
  }
}
