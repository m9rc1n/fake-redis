# @fake-redis/dynamodb-client

## 0.2.0

### Minor Changes

- Initial release of `fake-dynamodb`.

  - `@fake-redis/dynamodb-core`: in-memory DynamoDB engine with full expression support (ConditionExpression, FilterExpression, UpdateExpression incl. SET / REMOVE / ADD / DELETE, `if_not_exists`, `list_append`, arithmetic), key conditions (BETWEEN, begins_with), functions (`attribute_exists`, `attribute_not_exists`, `attribute_type`, `contains`, `size`), GSI/LSI queries, batch ops, and transactional writes/reads.
  - `@fake-redis/dynamodb-client`: AWS SDK v3 drop-in. Accepts real `@aws-sdk/client-dynamodb` and `@aws-sdk/lib-dynamodb` command instances via duck-typing, plus native-shape `DynamoDBDocumentClient` with marshall/unmarshall.

### Patch Changes

- Updated dependencies
  - @fake-redis/dynamodb-core@0.2.0
