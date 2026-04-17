import { Engine } from '@fake-redis/dynamodb-core';
import { inferOperation } from './commands.js';

export interface FakeDynamoDBClientConfig {
  /** Share a backing engine across multiple clients (for multi-client test scenarios). */
  engine?: Engine;
  /** AWS SDK config fields — accepted for parity, ignored. */
  region?: string;
  endpoint?: string;
  credentials?: unknown;
  maxAttempts?: number;
}

/**
 * Drop-in fake for `@aws-sdk/client-dynamodb`'s `DynamoDBClient`.
 * Accepts either our own command classes or the real AWS SDK command classes
 * (matched by constructor name), and also plain objects with `{ operation, input }`.
 */
export class FakeDynamoDBClient {
  readonly engine: Engine;
  readonly config: FakeDynamoDBClientConfig;

  constructor(config: FakeDynamoDBClientConfig = {}) {
    this.engine = config.engine ?? new Engine();
    this.config = config;
  }

  async send<T = unknown>(command: { input?: any; constructor?: { name?: string }; operation?: string }): Promise<T> {
    const op = inferOperation(command);
    if (!op) throw new Error('Unable to determine operation for command');
    const input = command.input ?? {};
    switch (op) {
      case 'CreateTable':        return this.engine.createTable(input) as T;
      case 'DeleteTable':        return this.engine.deleteTable(input.TableName) as T;
      case 'DescribeTable':      return { Table: this.engine.describeTable(input.TableName) } as T;
      case 'ListTables':         return { TableNames: this.engine.listTables() } as T;
      case 'PutItem':            return this.engine.putItem(input) as T;
      case 'GetItem':            return this.engine.getItem(input) as T;
      case 'UpdateItem':         return this.engine.updateItem(input) as T;
      case 'DeleteItem':         return this.engine.deleteItem(input) as T;
      case 'Query':              return this.engine.query(input) as T;
      case 'Scan':               return this.engine.scan(input) as T;
      case 'BatchGetItem':       return this.engine.batchGetItem(input) as T;
      case 'BatchWriteItem':     return this.engine.batchWriteItem(input) as T;
      case 'TransactWriteItems': return this.engine.transactWriteItems(input) as T;
      case 'TransactGetItems':   return this.engine.transactGetItems(input) as T;
      default: throw new Error(`Unsupported operation: ${op}`);
    }
  }

  destroy(): void {
    // no-op, parity with aws-sdk
  }
}
