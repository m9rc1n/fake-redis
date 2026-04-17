/**
 * Command classes mirroring `@aws-sdk/client-dynamodb`.
 * Each carries an `input` and a stable `operation` tag used by the client dispatcher.
 */
export abstract class DynamoDBCommand<TInput = unknown, TOutput = unknown> {
  abstract readonly operation: string;
  readonly input: TInput;
  // Phantom type helper — keeps TS happy without unused-var warnings.
  declare readonly _output: TOutput;
  constructor(input: TInput) { this.input = input; }
}

export class CreateTableCommand extends DynamoDBCommand { readonly operation = 'CreateTable'; }
export class DeleteTableCommand extends DynamoDBCommand { readonly operation = 'DeleteTable'; }
export class DescribeTableCommand extends DynamoDBCommand { readonly operation = 'DescribeTable'; }
export class ListTablesCommand extends DynamoDBCommand { readonly operation = 'ListTables'; }
export class PutItemCommand extends DynamoDBCommand { readonly operation = 'PutItem'; }
export class GetItemCommand extends DynamoDBCommand { readonly operation = 'GetItem'; }
export class UpdateItemCommand extends DynamoDBCommand { readonly operation = 'UpdateItem'; }
export class DeleteItemCommand extends DynamoDBCommand { readonly operation = 'DeleteItem'; }
export class QueryCommand extends DynamoDBCommand { readonly operation = 'Query'; }
export class ScanCommand extends DynamoDBCommand { readonly operation = 'Scan'; }
export class BatchGetItemCommand extends DynamoDBCommand { readonly operation = 'BatchGetItem'; }
export class BatchWriteItemCommand extends DynamoDBCommand { readonly operation = 'BatchWriteItem'; }
export class TransactWriteItemsCommand extends DynamoDBCommand { readonly operation = 'TransactWriteItems'; }
export class TransactGetItemsCommand extends DynamoDBCommand { readonly operation = 'TransactGetItems'; }

/** Map a command instance (ours or aws-sdk's) to an operation name by constructor name. */
export const inferOperation = (cmd: { constructor?: { name?: string }; operation?: string }): string | undefined => {
  if (cmd.operation) return cmd.operation;
  const n = cmd.constructor?.name;
  if (!n) return undefined;
  // "PutItemCommand" → "PutItem"
  return n.endsWith('Command') ? n.slice(0, -'Command'.length) : n;
};
