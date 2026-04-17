export class DynamoDBError extends Error {
  override readonly name: string;
  constructor(name: string, message: string) {
    super(message);
    this.name = name;
  }
}
export const ResourceNotFoundException = (msg = 'Resource not found') =>
  new DynamoDBError('ResourceNotFoundException', msg);
export const ResourceInUseException = (msg: string) =>
  new DynamoDBError('ResourceInUseException', msg);
export const ConditionalCheckFailedException = (msg = 'The conditional request failed') =>
  new DynamoDBError('ConditionalCheckFailedException', msg);
export const ValidationException = (msg: string) =>
  new DynamoDBError('ValidationException', msg);
export const TransactionCanceledException = (
  reasons: Array<{ Code: string; Message?: string }>,
) => {
  const e = new DynamoDBError('TransactionCanceledException', 'Transaction cancelled');
  (e as any).CancellationReasons = reasons;
  return e;
};
