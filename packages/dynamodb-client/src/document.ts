import type { AttributeValue, Item } from '@fake-redis/dynamodb-core';
import { FakeDynamoDBClient, type FakeDynamoDBClientConfig } from './client.js';
import { inferOperation } from './commands.js';
import { marshall, marshallItem, unmarshall, unmarshallItem, type MarshalOptions } from './marshal.js';

export interface TranslateConfig {
  marshallOptions?: MarshalOptions;
  unmarshallOptions?: { wrapNumbers?: boolean };
}

const marshalValueMap = (obj: Record<string, unknown> | undefined, opts: MarshalOptions): Record<string, AttributeValue> | undefined => {
  if (!obj) return undefined;
  const out: Record<string, AttributeValue> = {};
  for (const [k, v] of Object.entries(obj)) out[k] = marshall(v, opts);
  return out;
};

/** Marshal the native-shape input for one of the document-style commands into AV-shape. */
export const marshallInput = (op: string, input: any, opts: MarshalOptions = {}): any => {
  if (!input) return input;
  const out: any = { ...input };
  if (out.Item) out.Item = marshallItem(out.Item, opts);
  if (out.Key) out.Key = marshallItem(out.Key, opts);
  if (out.ExpressionAttributeValues) out.ExpressionAttributeValues = marshalValueMap(out.ExpressionAttributeValues, opts);
  if (out.ExclusiveStartKey) out.ExclusiveStartKey = marshallItem(out.ExclusiveStartKey, opts);
  if (op === 'BatchGetItem' && out.RequestItems) {
    const ri: any = {};
    for (const [t, v] of Object.entries<any>(out.RequestItems))
      ri[t] = { ...v, Keys: v.Keys?.map((k: any) => marshallItem(k, opts)) };
    out.RequestItems = ri;
  }
  if (op === 'BatchWriteItem' && out.RequestItems) {
    const ri: any = {};
    for (const [t, ops] of Object.entries<any>(out.RequestItems)) {
      ri[t] = (ops as any[]).map((o) => {
        if (o.PutRequest) return { PutRequest: { Item: marshallItem(o.PutRequest.Item, opts) } };
        if (o.DeleteRequest) return { DeleteRequest: { Key: marshallItem(o.DeleteRequest.Key, opts) } };
        return o;
      });
    }
    out.RequestItems = ri;
  }
  if (op === 'TransactWriteItems' && out.TransactItems) {
    out.TransactItems = out.TransactItems.map((t: any) => {
      const r: any = {};
      for (const op of ['Put', 'Update', 'Delete', 'ConditionCheck'] as const) {
        if (t[op]) {
          const v = { ...t[op] };
          if (v.Item) v.Item = marshallItem(v.Item, opts);
          if (v.Key) v.Key = marshallItem(v.Key, opts);
          if (v.ExpressionAttributeValues) v.ExpressionAttributeValues = marshalValueMap(v.ExpressionAttributeValues, opts);
          r[op] = v;
        }
      }
      return r;
    });
  }
  if (op === 'TransactGetItems' && out.TransactItems) {
    out.TransactItems = out.TransactItems.map((t: any) => ({
      Get: { ...t.Get, Key: marshallItem(t.Get.Key, opts) },
    }));
  }
  return out;
};

/** Unmarshal AV-shape output into native JS. */
export const unmarshallOutput = (op: string, out: any): any => {
  if (!out || typeof out !== 'object') return out;
  const r: any = { ...out };
  if (r.Item) r.Item = unmarshallItem(r.Item as Item);
  if (r.Attributes) r.Attributes = unmarshallItem(r.Attributes as Item);
  if (Array.isArray(r.Items)) r.Items = r.Items.map((i: Item) => unmarshallItem(i));
  if (r.LastEvaluatedKey) r.LastEvaluatedKey = unmarshallItem(r.LastEvaluatedKey as Item);
  if (op === 'BatchGetItem' && r.Responses) {
    const resp: any = {};
    for (const [t, items] of Object.entries<any>(r.Responses))
      resp[t] = (items as Item[]).map(unmarshallItem);
    r.Responses = resp;
  }
  if (op === 'TransactGetItems' && Array.isArray(r.Responses)) {
    r.Responses = r.Responses.map((resp: any) => resp.Item ? { Item: unmarshallItem(resp.Item) } : resp);
  }
  return r;
};

/**
 * Drop-in fake for `@aws-sdk/lib-dynamodb`'s `DynamoDBDocumentClient`.
 * Accepts native JS in inputs (no `{S:...}` wrapping) and returns native JS in outputs.
 *
 * Usage:
 *   const client = new FakeDynamoDBDocumentClient();
 *   await client.send(new PutCommand({ TableName: 'T', Item: { pk: 'a', name: 'Alice' } }));
 */
export class FakeDynamoDBDocumentClient {
  readonly client: FakeDynamoDBClient;
  readonly translate: TranslateConfig;

  constructor(client?: FakeDynamoDBClient | FakeDynamoDBClientConfig, translate: TranslateConfig = {}) {
    this.client = client instanceof FakeDynamoDBClient ? client : new FakeDynamoDBClient(client);
    this.translate = translate;
  }

  /** Static factory mirroring the AWS SDK's `DynamoDBDocumentClient.from(...)`. */
  static from(client: FakeDynamoDBClient, translate: TranslateConfig = {}): FakeDynamoDBDocumentClient {
    return new FakeDynamoDBDocumentClient(client, translate);
  }

  async send<T = any>(command: { input?: any; constructor?: { name?: string }; operation?: string }): Promise<T> {
    const raw = inferOperation(command)!;
    const op = docOpAlias[raw] ?? raw;
    const marshalOpts = this.translate.marshallOptions ?? {};
    const marshaled = marshallInput(op, command.input ?? {}, marshalOpts);
    const res = await this.client.send({ operation: op, input: marshaled });
    return unmarshallOutput(op, res) as T;
  }

  destroy() { this.client.destroy(); }
}

// Document-style command classes (mirror lib-dynamodb's {Put,Get,Update,Delete,Query,Scan,Batch*,Transact*}Command)
// They behave identically to the base commands — the op is inferred by the constructor name ("PutCommand" → "Put").
// But users will typically get the 'Put' → 'PutItem' mapping. Map those explicitly.

const docOpAlias: Record<string, string> = {
  Put: 'PutItem', Get: 'GetItem', Update: 'UpdateItem', Delete: 'DeleteItem',
};

export abstract class DocumentCommand<TInput = unknown, TOutput = unknown> {
  readonly input: TInput;
  declare readonly _output: TOutput;
  abstract readonly _op: string;
  get operation(): string { return docOpAlias[this._op] ?? this._op; }
  constructor(input: TInput) { this.input = input; }
}
export class PutCommand extends DocumentCommand { readonly _op = 'Put'; }
export class GetCommand extends DocumentCommand { readonly _op = 'Get'; }
export class UpdateCommand extends DocumentCommand { readonly _op = 'Update'; }
export class DeleteCommand extends DocumentCommand { readonly _op = 'Delete'; }
export class QueryCommand extends DocumentCommand { readonly _op = 'Query'; }
export class ScanCommand extends DocumentCommand { readonly _op = 'Scan'; }
export class BatchGetCommand extends DocumentCommand { readonly _op = 'BatchGetItem'; }
export class BatchWriteCommand extends DocumentCommand { readonly _op = 'BatchWriteItem'; }
export class TransactWriteCommand extends DocumentCommand { readonly _op = 'TransactWriteItems'; }
export class TransactGetCommand extends DocumentCommand { readonly _op = 'TransactGetItems'; }

// Silence unused imports
void unmarshall;
