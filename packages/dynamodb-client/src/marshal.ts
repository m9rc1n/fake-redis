import type { AttributeValue, Item } from '@fake-redis/dynamodb-core';

export interface MarshalOptions {
  removeUndefined?: boolean;
  convertEmptyValues?: boolean;
  convertClassInstanceToMap?: boolean;
}

export const marshall = (native: unknown, opts: MarshalOptions = {}): AttributeValue => {
  if (native === null) return { NULL: true };
  if (native === undefined) {
    if (opts.removeUndefined) return { NULL: true };
    throw new Error('Cannot marshall undefined — pass { removeUndefined: true }');
  }
  if (typeof native === 'boolean') return { BOOL: native };
  if (typeof native === 'number') {
    if (!Number.isFinite(native)) throw new Error(`Cannot marshall non-finite number ${native}`);
    return { N: String(native) };
  }
  if (typeof native === 'bigint') return { N: native.toString() };
  if (typeof native === 'string') {
    if (native === '' && opts.convertEmptyValues) return { NULL: true };
    return { S: native };
  }
  if (native instanceof Uint8Array) return { B: native };
  if (native instanceof Set) {
    const arr = [...native];
    if (arr.length === 0) throw new Error('Cannot marshall empty Set');
    const first = arr[0];
    if (typeof first === 'string') return { SS: arr as string[] };
    if (typeof first === 'number' || typeof first === 'bigint') return { NS: (arr as (number | bigint)[]).map(String) };
    if (first instanceof Uint8Array) return { BS: arr as Uint8Array[] };
    throw new Error(`Unsupported Set element type: ${typeof first}`);
  }
  if (Array.isArray(native)) return { L: native.map((v) => marshall(v, opts)) };
  if (typeof native === 'object') {
    const m: Record<string, AttributeValue> = {};
    for (const [k, v] of Object.entries(native as Record<string, unknown>)) {
      if (v === undefined && opts.removeUndefined) continue;
      m[k] = marshall(v, opts);
    }
    return { M: m };
  }
  throw new Error(`Cannot marshall value of type ${typeof native}`);
};

export const marshallItem = (native: Record<string, unknown>, opts: MarshalOptions = {}): Item => {
  const out: Item = {};
  for (const [k, v] of Object.entries(native)) {
    if (v === undefined && opts.removeUndefined) continue;
    out[k] = marshall(v, opts);
  }
  return out;
};

export const unmarshall = (av: AttributeValue): unknown => {
  if ('S' in av) return av.S;
  if ('N' in av) return Number(av.N);
  if ('B' in av) return av.B;
  if ('BOOL' in av) return av.BOOL;
  if ('NULL' in av) return null;
  if ('L' in av) return av.L.map(unmarshall);
  if ('M' in av) {
    const out: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(av.M)) out[k] = unmarshall(v);
    return out;
  }
  if ('SS' in av) return new Set(av.SS);
  if ('NS' in av) return new Set(av.NS.map(Number));
  if ('BS' in av) return new Set(av.BS);
  return undefined;
};

export const unmarshallItem = (item: Item): Record<string, unknown> => {
  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(item)) out[k] = unmarshall(v);
  return out;
};
