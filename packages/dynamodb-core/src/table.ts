import type { AttributeValue, GSI, Item, KeySchemaElement, LSI, TableDefinition } from './types.js';
import { hashKey as encodeKey, compare } from './compare.js';
import { ValidationException } from './errors.js';

const hashKeyAttr = (ks: KeySchemaElement[]) => ks.find((k) => k.KeyType === 'HASH')?.AttributeName;
const rangeKeyAttr = (ks: KeySchemaElement[]) => ks.find((k) => k.KeyType === 'RANGE')?.AttributeName;

export class Table {
  readonly def: TableDefinition;
  readonly created = new Date();
  // partitions[hashStr] -> Map<sortStr, item>. For tables with no sort key, sortStr is ''.
  readonly partitions = new Map<string, Map<string, Item>>();

  constructor(def: TableDefinition) {
    this.def = def;
    const hk = hashKeyAttr(def.KeySchema);
    if (!hk) throw ValidationException('Table must have a HASH key');
  }

  get hashAttr(): string { return hashKeyAttr(this.def.KeySchema)!; }
  get sortAttr(): string | undefined { return rangeKeyAttr(this.def.KeySchema); }

  getIndex(name: string): { keySchema: KeySchemaElement[]; kind: 'gsi' | 'lsi' } {
    const gsi = this.def.GlobalSecondaryIndexes?.find((g) => g.IndexName === name);
    if (gsi) return { keySchema: gsi.KeySchema, kind: 'gsi' };
    const lsi = this.def.LocalSecondaryIndexes?.find((l) => l.IndexName === name);
    if (lsi) return { keySchema: lsi.KeySchema, kind: 'lsi' };
    throw ValidationException(`Index '${name}' does not exist on table ${this.def.TableName}`);
  }

  keyOf(item: Item): { h: string; s: string } {
    const hv = item[this.hashAttr];
    if (!hv) throw ValidationException(`Missing hash key attribute ${this.hashAttr}`);
    const sAttr = this.sortAttr;
    if (!sAttr) return { h: encodeKey(hv), s: '' };
    const sv = item[sAttr];
    if (!sv) throw ValidationException(`Missing sort key attribute ${sAttr}`);
    return { h: encodeKey(hv), s: encodeKey(sv) };
  }

  get(item: Item): Item | undefined {
    const { h, s } = this.keyOf(item);
    return this.partitions.get(h)?.get(s);
  }

  put(item: Item): Item | undefined {
    const { h, s } = this.keyOf(item);
    let p = this.partitions.get(h);
    if (!p) { p = new Map(); this.partitions.set(h, p); }
    const prev = p.get(s);
    p.set(s, item);
    return prev;
  }

  delete(item: Item): Item | undefined {
    const { h, s } = this.keyOf(item);
    const p = this.partitions.get(h);
    if (!p) return undefined;
    const prev = p.get(s);
    p.delete(s);
    if (p.size === 0) this.partitions.delete(h);
    return prev;
  }

  /** Partition items for a given hash key, sorted by sort key asc. */
  partitionSorted(hashValue: AttributeValue): Item[] {
    const p = this.partitions.get(encodeKey(hashValue));
    if (!p) return [];
    if (!this.sortAttr) return [...p.values()];
    const arr = [...p.values()];
    arr.sort((a, b) => compare(a[this.sortAttr!]!, b[this.sortAttr!]!));
    return arr;
  }

  /** All items. Used by Scan and GSI queries. */
  allItems(): Item[] {
    const out: Item[] = [];
    for (const p of this.partitions.values()) for (const it of p.values()) out.push(it);
    return out;
  }

  itemCount(): number {
    let n = 0; for (const p of this.partitions.values()) n += p.size; return n;
  }
}
