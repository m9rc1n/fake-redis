import { describe, expect, it } from 'vitest';
import {
  FakeDynamoDBClient, FakeDynamoDBDocumentClient,
  CreateTableCommand, PutItemCommand, GetItemCommand, QueryCommand, UpdateItemCommand,
  PutCommand, GetCommand, UpdateCommand, QueryCommand as DocQueryCommand, BatchWriteCommand, TransactWriteCommand,
} from '../src/index.js';

const defineTable = async (client: FakeDynamoDBClient) => {
  await client.send(new CreateTableCommand({
    TableName: 'Users',
    AttributeDefinitions: [
      { AttributeName: 'pk', AttributeType: 'S' },
      { AttributeName: 'sk', AttributeType: 'S' },
    ],
    KeySchema: [
      { AttributeName: 'pk', KeyType: 'HASH' },
      { AttributeName: 'sk', KeyType: 'RANGE' },
    ],
  }));
};

describe('FakeDynamoDBClient (AV-shape)', () => {
  it('put + get via command objects', async () => {
    const client = new FakeDynamoDBClient();
    await defineTable(client);
    await client.send(new PutItemCommand({
      TableName: 'Users',
      Item: { pk: { S: 'u1' }, sk: { S: 'p' }, name: { S: 'Alice' } },
    }));
    const r: any = await client.send(new GetItemCommand({
      TableName: 'Users', Key: { pk: { S: 'u1' }, sk: { S: 'p' } },
    }));
    expect(r.Item.name).toEqual({ S: 'Alice' });
  });

  it('accepts duck-typed AWS SDK style command by constructor name', async () => {
    const client = new FakeDynamoDBClient();
    await defineTable(client);
    // Simulate an aws-sdk command: class named 'PutItemCommand' from a separate source.
    const PutItemCommandShim = class { input: any; constructor(i: any) { this.input = i; } };
    Object.defineProperty(PutItemCommandShim, 'name', { value: 'PutItemCommand' });
    const cmd = new PutItemCommandShim({ TableName: 'Users', Item: { pk: { S: 'a' }, sk: { S: 'b' } } });
    await client.send(cmd);
    const r: any = await client.send(new GetItemCommand({ TableName: 'Users', Key: { pk: { S: 'a' }, sk: { S: 'b' } } }));
    expect(r.Item).toBeDefined();
  });

  it('Query with condition and Update', async () => {
    const client = new FakeDynamoDBClient();
    await defineTable(client);
    for (const sk of ['a', 'b', 'c']) {
      await client.send(new PutItemCommand({
        TableName: 'Users', Item: { pk: { S: 'u' }, sk: { S: sk }, n: { N: '1' } },
      }));
    }
    await client.send(new UpdateItemCommand({
      TableName: 'Users', Key: { pk: { S: 'u' }, sk: { S: 'b' } },
      UpdateExpression: 'ADD n :one',
      ExpressionAttributeValues: { ':one': { N: '10' } },
    }));
    const r: any = await client.send(new QueryCommand({
      TableName: 'Users',
      KeyConditionExpression: 'pk = :pk',
      ExpressionAttributeValues: { ':pk': { S: 'u' } },
    }));
    expect(r.Count).toBe(3);
    const b = r.Items.find((i: any) => i.sk.S === 'b');
    expect(b.n).toEqual({ N: '11' });
  });
});

describe('FakeDynamoDBDocumentClient (native-shape)', () => {
  it('native round trip', async () => {
    const base = new FakeDynamoDBClient();
    await defineTable(base);
    const doc = FakeDynamoDBDocumentClient.from(base);

    await doc.send(new PutCommand({
      TableName: 'Users',
      Item: { pk: 'u1', sk: 'p', name: 'Alice', tags: ['a', 'b'], meta: { active: true, count: 3 } },
    }));
    const r: any = await doc.send(new GetCommand({ TableName: 'Users', Key: { pk: 'u1', sk: 'p' } }));
    expect(r.Item).toEqual({
      pk: 'u1', sk: 'p', name: 'Alice',
      tags: ['a', 'b'],
      meta: { active: true, count: 3 },
    });
  });

  it('native Update + Query', async () => {
    const doc = new FakeDynamoDBDocumentClient();
    await defineTable(doc.client);
    await doc.send(new PutCommand({ TableName: 'Users', Item: { pk: 'x', sk: 'a', count: 1 } }));
    await doc.send(new UpdateCommand({
      TableName: 'Users', Key: { pk: 'x', sk: 'a' },
      UpdateExpression: 'SET #c = #c + :inc',
      ExpressionAttributeNames: { '#c': 'count' },
      ExpressionAttributeValues: { ':inc': 4 },
      ReturnValues: 'ALL_NEW',
    }));
    const q: any = await doc.send(new DocQueryCommand({
      TableName: 'Users',
      KeyConditionExpression: 'pk = :p',
      ExpressionAttributeValues: { ':p': 'x' },
    }));
    expect(q.Items[0].count).toBe(5);
  });

  it('BatchWrite and TransactWrite', async () => {
    const doc = new FakeDynamoDBDocumentClient();
    await defineTable(doc.client);
    await doc.send(new BatchWriteCommand({
      RequestItems: {
        Users: [
          { PutRequest: { Item: { pk: 'a', sk: 'x', v: 1 } } },
          { PutRequest: { Item: { pk: 'b', sk: 'x', v: 2 } } },
        ],
      },
    }));
    await doc.send(new TransactWriteCommand({
      TransactItems: [
        { Put: { TableName: 'Users', Item: { pk: 'c', sk: 'x', v: 3 } } },
        { Update: {
            TableName: 'Users', Key: { pk: 'a', sk: 'x' },
            UpdateExpression: 'SET v = :n',
            ExpressionAttributeValues: { ':n': 99 },
        } },
      ],
    }));
    const r: any = await doc.send(new DocQueryCommand({
      TableName: 'Users',
      KeyConditionExpression: 'pk = :p',
      ExpressionAttributeValues: { ':p': 'a' },
    }));
    expect(r.Items[0].v).toBe(99);
  });
});

describe('Shared engine across clients', () => {
  it('two clients see the same data', async () => {
    const a = new FakeDynamoDBClient();
    await defineTable(a);
    const b = new FakeDynamoDBClient({ engine: a.engine });
    await a.send(new PutItemCommand({
      TableName: 'Users', Item: { pk: { S: 'u' }, sk: { S: 'p' }, v: { S: 'hi' } },
    }));
    const r: any = await b.send(new GetItemCommand({
      TableName: 'Users', Key: { pk: { S: 'u' }, sk: { S: 'p' } },
    }));
    expect(r.Item.v).toEqual({ S: 'hi' });
  });
});
