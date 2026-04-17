// Low-level (AV-shape) — mirrors @aws-sdk/client-dynamodb
export {
  DynamoDBCommand, inferOperation,
  CreateTableCommand, DeleteTableCommand, DescribeTableCommand, ListTablesCommand,
  PutItemCommand, GetItemCommand, UpdateItemCommand, DeleteItemCommand,
  QueryCommand, ScanCommand,
  BatchGetItemCommand, BatchWriteItemCommand,
  TransactWriteItemsCommand, TransactGetItemsCommand,
} from './commands.js';
export { FakeDynamoDBClient, type FakeDynamoDBClientConfig } from './client.js';

// High-level (native-shape) — mirrors @aws-sdk/lib-dynamodb
// NOTE: lib-dynamodb also exports QueryCommand/ScanCommand; in our single-package build
// they are exported as DocQueryCommand / DocScanCommand to avoid name collisions.
export {
  FakeDynamoDBDocumentClient, marshallInput, unmarshallOutput,
  DocumentCommand,
  PutCommand, GetCommand, UpdateCommand, DeleteCommand,
  BatchGetCommand, BatchWriteCommand,
  TransactWriteCommand, TransactGetCommand,
  QueryCommand as DocQueryCommand, ScanCommand as DocScanCommand,
} from './document.js';

export * from './marshal.js';
export { Engine } from '@fake-redis/dynamodb-core';
