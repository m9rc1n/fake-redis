import { describe, expect, it } from 'vitest';
// Real AWS SDK command classes — used against our fake client.
import {
  CreateTableCommand as AwsCreateTableCommand,
  PutItemCommand as AwsPutItemCommand,
  GetItemCommand as AwsGetItemCommand,
  UpdateItemCommand as AwsUpdateItemCommand,
  QueryCommand as AwsQueryCommand,
  TransactWriteItemsCommand as AwsTransactWriteItemsCommand,
} from '@aws-sdk/client-dynamodb';
import {
  PutCommand as AwsPutCommand,
  GetCommand as AwsGetCommand,
  UpdateCommand as AwsUpdateCommand,
  QueryCommand as AwsDocQueryCommand,
} from '@aws-sdk/lib-dynamodb';

import { FakeDynamoDBClient, FakeDynamoDBDocumentClient } from '../src/index.js';

describe('aws-sdk drop-in parity', () => {
  it('FakeDynamoDBClient accepts real @aws-sdk/client-dynamodb command classes', async () => {
    const client = new FakeDynamoDBClient();
    await client.send(new AwsCreateTableCommand({
      TableName: 'T',
      AttributeDefinitions: [
        { AttributeName: 'pk', AttributeType: 'S' },
        { AttributeName: 'sk', AttributeType: 'S' },
      ],
      KeySchema: [
        { AttributeName: 'pk', KeyType: 'HASH' },
        { AttributeName: 'sk', KeyType: 'RANGE' },
      ],
    }));

    await client.send(new AwsPutItemCommand({
      TableName: 'T',
      Item: { pk: { S: 'u' }, sk: { S: '1' }, counter: { N: '0' } },
    }));

    await client.send(new AwsUpdateItemCommand({
      TableName: 'T',
      Key: { pk: { S: 'u' }, sk: { S: '1' } },
      UpdateExpression: 'ADD #c :one',
      ExpressionAttributeNames: { '#c': 'counter' },
      ExpressionAttributeValues: { ':one': { N: '5' } },
    }));

    const got: any = await client.send(new AwsGetItemCommand({
      TableName: 'T', Key: { pk: { S: 'u' }, sk: { S: '1' } },
    }));
    expect(got.Item?.counter).toEqual({ N: '5' });

    const q: any = await client.send(new AwsQueryCommand({
      TableName: 'T',
      KeyConditionExpression: 'pk = :p',
      ExpressionAttributeValues: { ':p': { S: 'u' } },
    }));
    expect(q.Count).toBe(1);

    await client.send(new AwsTransactWriteItemsCommand({
      TransactItems: [
        { Put: { TableName: 'T', Item: { pk: { S: 'u' }, sk: { S: '2' } } } },
        { Update: {
            TableName: 'T',
            Key: { pk: { S: 'u' }, sk: { S: '1' } },
            UpdateExpression: 'SET counter = :n',
            ExpressionAttributeValues: { ':n': { N: '99' } },
        } },
      ],
    }));
    const final: any = await client.send(new AwsGetItemCommand({
      TableName: 'T', Key: { pk: { S: 'u' }, sk: { S: '1' } },
    }));
    expect(final.Item?.counter).toEqual({ N: '99' });
  });

  it('FakeDynamoDBDocumentClient accepts real @aws-sdk/lib-dynamodb command classes', async () => {
    const base = new FakeDynamoDBClient();
    await base.send(new AwsCreateTableCommand({
      TableName: 'T',
      AttributeDefinitions: [
        { AttributeName: 'pk', AttributeType: 'S' },
        { AttributeName: 'sk', AttributeType: 'S' },
      ],
      KeySchema: [
        { AttributeName: 'pk', KeyType: 'HASH' },
        { AttributeName: 'sk', KeyType: 'RANGE' },
      ],
    }));
    const doc = FakeDynamoDBDocumentClient.from(base);

    await doc.send(new AwsPutCommand({
      TableName: 'T',
      Item: { pk: 'u1', sk: 'profile', name: 'Alice', tags: ['a', 'b'], active: true, score: 10 },
    }));
    const got: any = await doc.send(new AwsGetCommand({
      TableName: 'T', Key: { pk: 'u1', sk: 'profile' },
    }));
    expect(got.Item).toEqual({ pk: 'u1', sk: 'profile', name: 'Alice', tags: ['a', 'b'], active: true, score: 10 });

    await doc.send(new AwsUpdateCommand({
      TableName: 'T', Key: { pk: 'u1', sk: 'profile' },
      UpdateExpression: 'SET score = score + :n',
      ExpressionAttributeValues: { ':n': 7 },
    }));
    const q: any = await doc.send(new AwsDocQueryCommand({
      TableName: 'T',
      KeyConditionExpression: 'pk = :p',
      ExpressionAttributeValues: { ':p': 'u1' },
    }));
    expect(q.Items[0].score).toBe(17);
  });
});
