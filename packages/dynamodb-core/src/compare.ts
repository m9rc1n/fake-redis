import type { AttributeValue } from './types.js';

export const typeOf = (v: AttributeValue): string => Object.keys(v)[0]!;

export const equal = (a: AttributeValue, b: AttributeValue): boolean => {
  const ta = typeOf(a), tb = typeOf(b);
  if (ta !== tb) return false;
  const va = (a as any)[ta], vb = (b as any)[tb];
  if (ta === 'B') return bytesEqual(va, vb);
  if (ta === 'L') return va.length === vb.length && va.every((x: AttributeValue, i: number) => equal(x, vb[i]));
  if (ta === 'M') {
    const ka = Object.keys(va), kb = Object.keys(vb);
    if (ka.length !== kb.length) return false;
    return ka.every((k) => k in vb && equal(va[k], vb[k]));
  }
  if (ta === 'SS' || ta === 'NS') {
    if (va.length !== vb.length) return false;
    const sa = new Set<string>(va); for (const x of vb) if (!sa.has(x)) return false;
    return true;
  }
  if (ta === 'BS') {
    if (va.length !== vb.length) return false;
    const setA = va.map(toHex);
    return vb.every((x: Uint8Array) => setA.includes(toHex(x)));
  }
  return va === vb;
};

const toHex = (b: Uint8Array) => Buffer.from(b).toString('hex');
const bytesEqual = (a: Uint8Array, b: Uint8Array) =>
  a.length === b.length && a.every((x, i) => x === b[i]);

/** Compare two AttributeValues of the same scalar type. Returns -1 / 0 / 1. */
export const compare = (a: AttributeValue, b: AttributeValue): number => {
  const ta = typeOf(a), tb = typeOf(b);
  if (ta !== tb) throw new Error(`cannot compare ${ta} with ${tb}`);
  if (ta === 'S') return stringCmp((a as any).S, (b as any).S);
  if (ta === 'N') {
    const na = Number((a as any).N), nb = Number((b as any).N);
    return na < nb ? -1 : na > nb ? 1 : 0;
  }
  if (ta === 'B') return bytesCmp((a as any).B, (b as any).B);
  throw new Error(`unsupported compare for type ${ta}`);
};

const stringCmp = (a: string, b: string) => (a < b ? -1 : a > b ? 1 : 0);
const bytesCmp = (a: Uint8Array, b: Uint8Array) => {
  const n = Math.min(a.length, b.length);
  for (let i = 0; i < n; i++) if (a[i] !== b[i]) return a[i]! < b[i]! ? -1 : 1;
  return a.length - b.length;
};

export const hashKey = (v: AttributeValue): string => {
  const t = typeOf(v);
  if (t === 'S') return `S:${(v as any).S}`;
  if (t === 'N') return `N:${Number((v as any).N)}`;
  if (t === 'B') return `B:${toHex((v as any).B)}`;
  throw new Error(`invalid key type ${t}`);
};

export const sizeOf = (v: AttributeValue): number => {
  const t = typeOf(v);
  const x = (v as any)[t];
  if (t === 'S' || t === 'N') return String(x).length;
  if (t === 'B') return x.length;
  if (t === 'L' || t === 'SS' || t === 'NS' || t === 'BS') return x.length;
  if (t === 'M') return Object.keys(x).length;
  return 0;
};
