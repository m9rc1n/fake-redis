// Simple sorted set backed by a Map + sorted array. Good enough for tests.
export class SortedSet {
  private scores = new Map<string, number>();

  get size(): number {
    return this.scores.size;
  }

  has(member: string): boolean {
    return this.scores.has(member);
  }

  score(member: string): number | undefined {
    return this.scores.get(member);
  }

  set(member: string, score: number): void {
    this.scores.set(member, score);
  }

  delete(member: string): boolean {
    return this.scores.delete(member);
  }

  entries(): Array<[string, number]> {
    return [...this.scores.entries()].sort((a, b) => {
      if (a[1] !== b[1]) return a[1] - b[1];
      return a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0;
    });
  }

  rangeByIndex(start: number, stop: number, rev = false): Array<[string, number]> {
    const arr = rev ? this.entries().reverse() : this.entries();
    const len = arr.length;
    const s = start < 0 ? Math.max(0, len + start) : Math.min(start, len);
    const e = stop < 0 ? len + stop : Math.min(stop, len - 1);
    if (s > e) return [];
    return arr.slice(s, e + 1);
  }

  rangeByScore(
    min: number,
    max: number,
    minExclusive: boolean,
    maxExclusive: boolean,
    rev = false,
  ): Array<[string, number]> {
    const filt = this.entries().filter(([, s]) => {
      const okMin = minExclusive ? s > min : s >= min;
      const okMax = maxExclusive ? s < max : s <= max;
      return okMin && okMax;
    });
    return rev ? filt.reverse() : filt;
  }

  rangeByLex(
    min: string,
    max: string,
    minExclusive: boolean,
    maxExclusive: boolean,
    minUnbounded: boolean,
    maxUnbounded: boolean,
    rev = false,
  ): Array<[string, number]> {
    const filt = this.entries().filter(([m]) => {
      const okMin = minUnbounded || (minExclusive ? m > min : m >= min);
      const okMax = maxUnbounded || (maxExclusive ? m < max : m <= max);
      return okMin && okMax;
    });
    return rev ? filt.reverse() : filt;
  }

  rank(member: string, rev = false): number | null {
    const arr = rev ? this.entries().reverse() : this.entries();
    const i = arr.findIndex(([m]) => m === member);
    return i === -1 ? null : i;
  }
}
