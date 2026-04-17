export type ScalarType = 'S' | 'N' | 'B';
export type KeyType = 'HASH' | 'RANGE';

export type AttributeValue =
  | { S: string }
  | { N: string }
  | { B: Uint8Array }
  | { BOOL: boolean }
  | { NULL: true }
  | { L: AttributeValue[] }
  | { M: Record<string, AttributeValue> }
  | { SS: string[] }
  | { NS: string[] }
  | { BS: Uint8Array[] };

export type Item = Record<string, AttributeValue>;

export interface AttributeDefinition {
  AttributeName: string;
  AttributeType: ScalarType;
}
export interface KeySchemaElement {
  AttributeName: string;
  KeyType: KeyType;
}
export interface Projection {
  ProjectionType?: 'ALL' | 'KEYS_ONLY' | 'INCLUDE';
  NonKeyAttributes?: string[];
}
export interface GSI {
  IndexName: string;
  KeySchema: KeySchemaElement[];
  Projection?: Projection;
}
export interface LSI {
  IndexName: string;
  KeySchema: KeySchemaElement[];
  Projection?: Projection;
}

export interface TableDefinition {
  TableName: string;
  AttributeDefinitions: AttributeDefinition[];
  KeySchema: KeySchemaElement[];
  GlobalSecondaryIndexes?: GSI[];
  LocalSecondaryIndexes?: LSI[];
  BillingMode?: 'PROVISIONED' | 'PAY_PER_REQUEST';
}

export interface TableDescription extends TableDefinition {
  TableStatus: 'ACTIVE';
  CreationDateTime: Date;
  ItemCount: number;
  TableSizeBytes: number;
}

export type ReturnValues = 'NONE' | 'ALL_OLD' | 'UPDATED_OLD' | 'ALL_NEW' | 'UPDATED_NEW';

export interface ExpressionInputs {
  ExpressionAttributeNames?: Record<string, string>;
  ExpressionAttributeValues?: Record<string, AttributeValue>;
}
