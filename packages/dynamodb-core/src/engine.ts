import type {
  AttributeValue, ExpressionInputs, Item, KeySchemaElement, TableDefinition, TableDescription, ReturnValues,
} from './types.js';
import { Table } from './table.js';
import { applyUpdate, evalCondition, evalKeyCondition, project } from './expression.js';
import { compare, equal, hashKey, typeOf } from './compare.js';
import {
  ConditionalCheckFailedException, ResourceInUseException, ResourceNotFoundException,
  TransactionCanceledException, ValidationException,
} from './errors.js';

export interface PutItemInput extends ExpressionInputs {
  TableName: string;
  Item: Item;
  ConditionExpression?: string;
  ReturnValues?: ReturnValues;
}
export interface GetItemInput {
  TableName: string;
  Key: Item;
  ConsistentRead?: boolean;
  ProjectionExpression?: string;
  ExpressionAttributeNames?: Record<string, string>;
}
export interface UpdateItemInput extends ExpressionInputs {
  TableName: string;
  Key: Item;
  UpdateExpression: string;
  ConditionExpression?: string;
  ReturnValues?: ReturnValues;
}
export interface DeleteItemInput extends ExpressionInputs {
  TableName: string;
  Key: Item;
  ConditionExpression?: string;
  ReturnValues?: ReturnValues;
}
export interface QueryInput extends ExpressionInputs {
  TableName: string;
  IndexName?: string;
  KeyConditionExpression: string;
  FilterExpression?: string;
  ProjectionExpression?: string;
  Limit?: number;
  ScanIndexForward?: boolean;
  ExclusiveStartKey?: Item;
  Select?: 'ALL_ATTRIBUTES' | 'COUNT' | 'SPECIFIC_ATTRIBUTES' | 'ALL_PROJECTED_ATTRIBUTES';
}
export interface ScanInput extends ExpressionInputs {
  TableName: string;
  IndexName?: string;
  FilterExpression?: string;
  ProjectionExpression?: string;
  Limit?: number;
  ExclusiveStartKey?: Item;
  Segment?: number;
  TotalSegments?: number;
}

export class Engine {
  readonly tables = new Map<string, Table>();

  createTable(def: TableDefinition): TableDescription {
    if (this.tables.has(def.TableName)) throw ResourceInUseException(`Table ${def.TableName} already exists`);
    const t = new Table(def);
    this.tables.set(def.TableName, t);
    return this.describeTable(def.TableName);
  }
  deleteTable(name: string): TableDescription {
    const t = this.tables.get(name);
    if (!t) throw ResourceNotFoundException(`Table ${name} not found`);
    const desc = this.describeTable(name);
    this.tables.delete(name);
    return desc;
  }
  describeTable(name: string): TableDescription {
    const t = this.tables.get(name);
    if (!t) throw ResourceNotFoundException(`Table ${name} not found`);
    return { ...t.def, TableStatus: 'ACTIVE', CreationDateTime: t.created, ItemCount: t.itemCount(), TableSizeBytes: 0 };
  }
  listTables(): string[] { return [...this.tables.keys()]; }

  private table(name: string): Table {
    const t = this.tables.get(name);
    if (!t) throw ResourceNotFoundException(`Table ${name} not found`);
    return t;
  }

  putItem(input: PutItemInput): { Attributes?: Item } {
    const t = this.table(input.TableName);
    this.validateKeyAttrs(t.def.KeySchema, t.def.AttributeDefinitions, input.Item);
    const existing = t.get(input.Item) ?? {};
    if (input.ConditionExpression) {
      if (!evalCondition(input.ConditionExpression, existing, input)) throw ConditionalCheckFailedException();
    }
    const prev = t.put(input.Item);
    if (input.ReturnValues === 'ALL_OLD' && prev) return { Attributes: prev };
    return {};
  }

  getItem(input: GetItemInput): { Item?: Item } {
    const t = this.table(input.TableName);
    const existing = t.get(input.Key);
    if (!existing) return {};
    return { Item: project(existing, input.ProjectionExpression, input.ExpressionAttributeNames) };
  }

  updateItem(input: UpdateItemInput): { Attributes?: Item } {
    const t = this.table(input.TableName);
    this.validateKeyAttrs(t.def.KeySchema, t.def.AttributeDefinitions, input.Key);
    const existing = t.get(input.Key) ?? { ...input.Key };
    const before = structuredClone(existing) as Item;
    if (input.ConditionExpression) {
      if (!evalCondition(input.ConditionExpression, t.get(input.Key) ?? {}, input))
        throw ConditionalCheckFailedException();
    }
    applyUpdate(input.UpdateExpression, existing, input);
    // Ensure key attrs preserved
    for (const k of t.def.KeySchema) existing[k.AttributeName] = input.Key[k.AttributeName]!;
    t.put(existing);
    const rv = input.ReturnValues ?? 'NONE';
    if (rv === 'ALL_NEW') return { Attributes: existing };
    if (rv === 'ALL_OLD') return { Attributes: before };
    if (rv === 'UPDATED_NEW') return { Attributes: diffAttrs(before, existing) };
    if (rv === 'UPDATED_OLD') return { Attributes: diffAttrs(existing, before) };
    return {};
  }

  deleteItem(input: DeleteItemInput): { Attributes?: Item } {
    const t = this.table(input.TableName);
    const existing = t.get(input.Key);
    if (input.ConditionExpression) {
      if (!evalCondition(input.ConditionExpression, existing ?? {}, input))
        throw ConditionalCheckFailedException();
    }
    const prev = t.delete(input.Key);
    if (input.ReturnValues === 'ALL_OLD' && prev) return { Attributes: prev };
    return {};
  }

  query(input: QueryInput): { Items: Item[]; Count: number; ScannedCount: number; LastEvaluatedKey?: Item } {
    const t = this.table(input.TableName);
    const ks = input.IndexName ? t.getIndex(input.IndexName).keySchema : t.def.KeySchema;
    const hashAttr = ks.find((k) => k.KeyType === 'HASH')!.AttributeName;
    const sortAttr = ks.find((k) => k.KeyType === 'RANGE')?.AttributeName;
    const kc = evalKeyCondition(input.KeyConditionExpression, hashAttr, sortAttr, input);

    let candidates: Item[];
    if (!input.IndexName) {
      candidates = t.partitionSorted(kc.hashValue);
    } else {
      candidates = t.allItems()
        .filter((it) => it[hashAttr] && equal(it[hashAttr]!, kc.hashValue));
      if (sortAttr) candidates.sort((a, b) => {
        if (!a[sortAttr] || !b[sortAttr]) return 0;
        return compare(a[sortAttr]!, b[sortAttr]!);
      });
    }
    if (sortAttr && kc.sortPred) candidates = candidates.filter((it) => it[sortAttr] && kc.sortPred!(it[sortAttr]!));
    if (input.ScanIndexForward === false) candidates.reverse();

    if (input.ExclusiveStartKey) {
      const ek = input.ExclusiveStartKey;
      const idx = candidates.findIndex((it) => t.def.KeySchema.every((k) => it[k.AttributeName] && equal(it[k.AttributeName]!, ek[k.AttributeName]!)));
      if (idx >= 0) candidates = candidates.slice(idx + 1);
    }

    let scanned = candidates.length;
    if (input.FilterExpression) {
      const filtered: Item[] = [];
      for (const it of candidates) if (evalCondition(input.FilterExpression, it, input)) filtered.push(it);
      candidates = filtered;
    }

    let last: Item | undefined;
    if (input.Limit && candidates.length > input.Limit) {
      last = candidates[input.Limit - 1];
      candidates = candidates.slice(0, input.Limit);
      scanned = Math.min(scanned, input.Limit);
    }
    const items = candidates.map((it) => project(it, input.ProjectionExpression, input.ExpressionAttributeNames));
    const res: { Items: Item[]; Count: number; ScannedCount: number; LastEvaluatedKey?: Item } = {
      Items: input.Select === 'COUNT' ? [] : items,
      Count: items.length,
      ScannedCount: scanned,
    };
    if (last) res.LastEvaluatedKey = keyFor(last, t.def.KeySchema);
    return res;
  }

  scan(input: ScanInput): { Items: Item[]; Count: number; ScannedCount: number; LastEvaluatedKey?: Item } {
    const t = this.table(input.TableName);
    let items = t.allItems();
    if (typeof input.Segment === 'number' && typeof input.TotalSegments === 'number') {
      items = items.filter((_it, i) => i % input.TotalSegments! === input.Segment);
    }
    if (input.ExclusiveStartKey) {
      const ek = input.ExclusiveStartKey;
      const idx = items.findIndex((it) => t.def.KeySchema.every((k) => equal(it[k.AttributeName]!, ek[k.AttributeName]!)));
      if (idx >= 0) items = items.slice(idx + 1);
    }
    const scanned = items.length;
    if (input.FilterExpression) {
      items = items.filter((it) => evalCondition(input.FilterExpression!, it, input));
    }
    let last: Item | undefined;
    if (input.Limit && items.length > input.Limit) {
      last = items[input.Limit - 1];
      items = items.slice(0, input.Limit);
    }
    const res: { Items: Item[]; Count: number; ScannedCount: number; LastEvaluatedKey?: Item } = {
      Items: items.map((it) => project(it, input.ProjectionExpression, input.ExpressionAttributeNames)),
      Count: items.length,
      ScannedCount: scanned,
    };
    if (last) res.LastEvaluatedKey = keyFor(last, t.def.KeySchema);
    return res;
  }

  batchGetItem(input: { RequestItems: Record<string, { Keys: Item[]; ProjectionExpression?: string; ExpressionAttributeNames?: Record<string, string> }> }) {
    const out: Record<string, Item[]> = {};
    for (const [name, req] of Object.entries(input.RequestItems)) {
      out[name] = [];
      for (const k of req.Keys) {
        const getInput: GetItemInput = { TableName: name, Key: k };
        if (req.ProjectionExpression) getInput.ProjectionExpression = req.ProjectionExpression;
        if (req.ExpressionAttributeNames) getInput.ExpressionAttributeNames = req.ExpressionAttributeNames;
        const r = this.getItem(getInput);
        if (r.Item) out[name]!.push(r.Item);
      }
    }
    return { Responses: out, UnprocessedKeys: {} };
  }

  batchWriteItem(input: { RequestItems: Record<string, Array<{ PutRequest?: { Item: Item }; DeleteRequest?: { Key: Item } }>> }) {
    for (const [name, ops] of Object.entries(input.RequestItems)) {
      for (const op of ops) {
        if (op.PutRequest) this.putItem({ TableName: name, Item: op.PutRequest.Item });
        else if (op.DeleteRequest) this.deleteItem({ TableName: name, Key: op.DeleteRequest.Key });
      }
    }
    return { UnprocessedItems: {} };
  }

  transactWriteItems(input: { TransactItems: Array<{
    Put?: PutItemInput; Update?: UpdateItemInput; Delete?: DeleteItemInput;
    ConditionCheck?: { TableName: string; Key: Item; ConditionExpression: string } & ExpressionInputs;
  }>; }) {
    const reasons: Array<{ Code: string; Message?: string }> = [];
    // First pass: check all conditions against current state without mutating
    for (const item of input.TransactItems) {
      try {
        if (item.ConditionCheck) {
          const t = this.table(item.ConditionCheck.TableName);
          const existing = t.get(item.ConditionCheck.Key) ?? {};
          if (!evalCondition(item.ConditionCheck.ConditionExpression, existing, item.ConditionCheck))
            throw ConditionalCheckFailedException();
          reasons.push({ Code: 'None' });
        } else if (item.Put) {
          const t = this.table(item.Put.TableName);
          const existing = t.get(item.Put.Item) ?? {};
          if (item.Put.ConditionExpression && !evalCondition(item.Put.ConditionExpression, existing, item.Put))
            throw ConditionalCheckFailedException();
          reasons.push({ Code: 'None' });
        } else if (item.Update) {
          const t = this.table(item.Update.TableName);
          const existing = t.get(item.Update.Key) ?? {};
          if (item.Update.ConditionExpression && !evalCondition(item.Update.ConditionExpression, existing, item.Update))
            throw ConditionalCheckFailedException();
          reasons.push({ Code: 'None' });
        } else if (item.Delete) {
          const t = this.table(item.Delete.TableName);
          const existing = t.get(item.Delete.Key) ?? {};
          if (item.Delete.ConditionExpression && !evalCondition(item.Delete.ConditionExpression, existing, item.Delete))
            throw ConditionalCheckFailedException();
          reasons.push({ Code: 'None' });
        } else {
          reasons.push({ Code: 'ValidationError', Message: 'empty transact item' });
        }
      } catch (e: any) {
        reasons.push({ Code: 'ConditionalCheckFailed', Message: e.message });
      }
    }
    if (reasons.some((r) => r.Code !== 'None')) throw TransactionCanceledException(reasons);
    // Second pass: apply (conditions already validated)
    const stripCond = <T extends { ConditionExpression?: string }>(x: T): T => {
      const { ConditionExpression: _c, ...rest } = x;
      return rest as T;
    };
    for (const item of input.TransactItems) {
      if (item.Put) this.putItem(stripCond(item.Put));
      else if (item.Update) this.updateItem(stripCond(item.Update));
      else if (item.Delete) this.deleteItem(stripCond(item.Delete));
    }
    return {};
  }

  transactGetItems(input: { TransactItems: Array<{ Get: GetItemInput }> }) {
    return { Responses: input.TransactItems.map((i) => ({ Item: this.getItem(i.Get).Item })) };
  }

  private validateKeyAttrs(ks: KeySchemaElement[], defs: { AttributeName: string; AttributeType: string }[], item: Item) {
    for (const k of ks) {
      const v = item[k.AttributeName];
      if (!v) throw ValidationException(`Missing key attribute ${k.AttributeName}`);
      const def = defs.find((d) => d.AttributeName === k.AttributeName);
      if (def && typeOf(v) !== def.AttributeType)
        throw ValidationException(`Key attribute ${k.AttributeName} has wrong type (expected ${def.AttributeType})`);
    }
  }
}

const keyFor = (item: Item, ks: KeySchemaElement[]): Item => {
  const out: Item = {};
  for (const k of ks) if (item[k.AttributeName]) out[k.AttributeName] = item[k.AttributeName]!;
  return out;
};

const diffAttrs = (before: Item, after: Item): Item => {
  const out: Item = {};
  for (const k of Object.keys(after)) {
    if (!(k in before) || !equal(before[k]!, after[k]!)) out[k] = after[k]!;
  }
  return out;
};

// silence unused
void hashKey;
