import { SortedSet } from './sortedset.js';
import type { RedisType, StoredEntry } from './types.js';
import { now } from './util.js';

export class Database {
  private map = new Map<string, StoredEntry>();

  size(): number {
    this.reap();
    return this.map.size;
  }

  keys(): string[] {
    this.reap();
    return [...this.map.keys()];
  }

  getEntry(key: string): StoredEntry | undefined {
    const e = this.map.get(key);
    if (!e) return undefined;
    if (e.expiresAt !== undefined && e.expiresAt <= now()) {
      this.map.delete(key);
      return undefined;
    }
    return e;
  }

  has(key: string): boolean {
    return this.getEntry(key) !== undefined;
  }

  type(key: string): RedisType {
    return this.getEntry(key)?.type ?? 'none';
  }

  setEntry(key: string, entry: StoredEntry): void {
    this.map.set(key, entry);
  }

  delete(key: string): boolean {
    return this.map.delete(key);
  }

  clear(): void {
    this.map.clear();
  }

  ttlMs(key: string): number {
    const e = this.getEntry(key);
    if (!e) return -2;
    if (e.expiresAt === undefined) return -1;
    return Math.max(0, e.expiresAt - now());
  }

  setExpireAt(key: string, ts: number | undefined): boolean {
    const e = this.getEntry(key);
    if (!e) return false;
    if (ts === undefined) delete e.expiresAt;
    else e.expiresAt = ts;
    return true;
  }

  // typed helpers — auto-create empty structures on demand
  getOrCreateString(key: string): { entry: StoredEntry; value: Buffer } {
    const e = this.getEntry(key);
    if (!e) {
      const entry: StoredEntry = { type: 'string', value: Buffer.alloc(0) };
      this.map.set(key, entry);
      return { entry, value: entry.value as Buffer };
    }
    if (e.type !== 'string') throw new Error('WRONGTYPE Operation against a key holding the wrong kind of value');
    return { entry: e, value: e.value as Buffer };
  }

  getOrCreateList(key: string): string[] {
    const e = this.getEntry(key);
    if (!e) {
      const arr: string[] = [];
      this.map.set(key, { type: 'list', value: arr });
      return arr;
    }
    if (e.type !== 'list') throw new Error('WRONGTYPE Operation against a key holding the wrong kind of value');
    return e.value as string[];
  }

  getOrCreateHash(key: string): Map<string, string> {
    const e = this.getEntry(key);
    if (!e) {
      const m = new Map<string, string>();
      this.map.set(key, { type: 'hash', value: m });
      return m;
    }
    if (e.type !== 'hash') throw new Error('WRONGTYPE Operation against a key holding the wrong kind of value');
    return e.value as Map<string, string>;
  }

  getOrCreateSet(key: string): Set<string> {
    const e = this.getEntry(key);
    if (!e) {
      const s = new Set<string>();
      this.map.set(key, { type: 'set', value: s });
      return s;
    }
    if (e.type !== 'set') throw new Error('WRONGTYPE Operation against a key holding the wrong kind of value');
    return e.value as Set<string>;
  }

  getOrCreateZSet(key: string): SortedSet {
    const e = this.getEntry(key);
    if (!e) {
      const z = new SortedSet();
      this.map.set(key, { type: 'zset', value: z });
      return z;
    }
    if (e.type !== 'zset') throw new Error('WRONGTYPE Operation against a key holding the wrong kind of value');
    return e.value as SortedSet;
  }

  private reap(): void {
    const t = now();
    for (const [k, e] of this.map) {
      if (e.expiresAt !== undefined && e.expiresAt <= t) this.map.delete(k);
    }
  }
}
