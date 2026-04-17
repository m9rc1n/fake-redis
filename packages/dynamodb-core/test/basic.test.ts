import { describe, expect, it } from 'vitest';
import { Engine } from '../src/index.js';

const makeEngine = () => {
  const e = new Engine();
  e.createTable({
    TableName: 'Users',
    AttributeDefinitions: [
      { AttributeName: 'pk', AttributeType: 'S' },
      { AttributeName: 'sk', AttributeType: 'S' },
    ],
    KeySchema: [
      { AttributeName: 'pk', KeyType: 'HASH' },
      { AttributeName: 'sk', KeyType: 'RANGE' },
    ],
  });
  return e;
};

describe('basic CRUD', () => {
  it('put/get round trip', () => {
    const e = makeEngine();
    e.putItem({ TableName: 'Users', Item: { pk: { S: 'u1' }, sk: { S: 'profile' }, name: { S: 'Alice' } } });
    const got = e.getItem({ TableName: 'Users', Key: { pk: { S: 'u1' }, sk: { S: 'profile' } } });
    expect(got.Item?.name).toEqual({ S: 'Alice' });
  });

  it('PutItem conditional: attribute_not_exists', () => {
    const e = makeEngine();
    const item = { pk: { S: 'u1' }, sk: { S: 'profile' }, name: { S: 'Alice' } };
    e.putItem({ TableName: 'Users', Item: item });
    expect(() => e.putItem({
      TableName: 'Users', Item: item,
      ConditionExpression: 'attribute_not_exists(pk)',
    })).toThrow(expect.objectContaining({ name: 'ConditionalCheckFailedException' }));
  });

  it('DeleteItem', () => {
    const e = makeEngine();
    e.putItem({ TableName: 'Users', Item: { pk: { S: 'u1' }, sk: { S: 'x' }, v: { N: '1' } } });
    e.deleteItem({ TableName: 'Users', Key: { pk: { S: 'u1' }, sk: { S: 'x' } } });
    expect(e.getItem({ TableName: 'Users', Key: { pk: { S: 'u1' }, sk: { S: 'x' } } }).Item).toBeUndefined();
  });
});

describe('UpdateExpression', () => {
  it('SET and REMOVE', () => {
    const e = makeEngine();
    e.putItem({ TableName: 'Users', Item: { pk: { S: 'u1' }, sk: { S: 'p' }, name: { S: 'A' }, age: { N: '20' } } });
    e.updateItem({
      TableName: 'Users', Key: { pk: { S: 'u1' }, sk: { S: 'p' } },
      UpdateExpression: 'SET #n = :n REMOVE age',
      ExpressionAttributeNames: { '#n': 'name' },
      ExpressionAttributeValues: { ':n': { S: 'B' } },
    });
    const got = e.getItem({ TableName: 'Users', Key: { pk: { S: 'u1' }, sk: { S: 'p' } } }).Item!;
    expect(got.name).toEqual({ S: 'B' });
    expect(got.age).toBeUndefined();
  });

  it('ADD increments N', () => {
    const e = makeEngine();
    e.putItem({ TableName: 'Users', Item: { pk: { S: 'u1' }, sk: { S: 'c' }, count: { N: '1' } } });
    e.updateItem({
      TableName: 'Users', Key: { pk: { S: 'u1' }, sk: { S: 'c' } },
      UpdateExpression: 'ADD #c :one',
      ExpressionAttributeNames: { '#c': 'count' },
      ExpressionAttributeValues: { ':one': { N: '1' } },
    });
    const got = e.getItem({ TableName: 'Users', Key: { pk: { S: 'u1' }, sk: { S: 'c' } } }).Item!;
    expect(got.count).toEqual({ N: '2' });
  });

  it('SET with if_not_exists and arithmetic', () => {
    const e = makeEngine();
    e.putItem({ TableName: 'Users', Item: { pk: { S: 'u1' }, sk: { S: 'c' }, count: { N: '5' } } });
    e.updateItem({
      TableName: 'Users', Key: { pk: { S: 'u1' }, sk: { S: 'c' } },
      UpdateExpression: 'SET count = count + :one, extra = if_not_exists(extra, :d)',
      ExpressionAttributeValues: { ':one': { N: '3' }, ':d': { S: 'hello' } },
    });
    const got = e.getItem({ TableName: 'Users', Key: { pk: { S: 'u1' }, sk: { S: 'c' } } }).Item!;
    expect(got.count).toEqual({ N: '8' });
    expect(got.extra).toEqual({ S: 'hello' });
  });

  it('SET list_append', () => {
    const e = makeEngine();
    e.putItem({ TableName: 'Users', Item: { pk: { S: 'u1' }, sk: { S: 'l' }, tags: { L: [{ S: 'a' }] } } });
    e.updateItem({
      TableName: 'Users', Key: { pk: { S: 'u1' }, sk: { S: 'l' } },
      UpdateExpression: 'SET tags = list_append(tags, :new)',
      ExpressionAttributeValues: { ':new': { L: [{ S: 'b' }, { S: 'c' }] } },
    });
    const got = e.getItem({ TableName: 'Users', Key: { pk: { S: 'u1' }, sk: { S: 'l' } } }).Item!;
    expect(got.tags).toEqual({ L: [{ S: 'a' }, { S: 'b' }, { S: 'c' }] });
  });

  it('conditional update fails when condition not met', () => {
    const e = makeEngine();
    e.putItem({ TableName: 'Users', Item: { pk: { S: 'u1' }, sk: { S: 'c' }, count: { N: '5' } } });
    expect(() => e.updateItem({
      TableName: 'Users', Key: { pk: { S: 'u1' }, sk: { S: 'c' } },
      UpdateExpression: 'SET count = :n',
      ConditionExpression: 'count = :expected',
      ExpressionAttributeValues: { ':n': { N: '10' }, ':expected': { N: '99' } },
    })).toThrow(expect.objectContaining({ name: 'ConditionalCheckFailedException' }));
  });
});

describe('Query', () => {
  it('partition + sort range', () => {
    const e = makeEngine();
    for (const sk of ['a', 'b', 'c', 'd']) {
      e.putItem({ TableName: 'Users', Item: { pk: { S: 'u1' }, sk: { S: sk }, v: { S: sk } } });
    }
    const r = e.query({
      TableName: 'Users',
      KeyConditionExpression: 'pk = :pk AND sk BETWEEN :a AND :c',
      ExpressionAttributeValues: { ':pk': { S: 'u1' }, ':a': { S: 'b' }, ':c': { S: 'c' } },
    });
    expect(r.Items.map((i) => i.sk)).toEqual([{ S: 'b' }, { S: 'c' }]);
  });

  it('begins_with on sort key', () => {
    const e = makeEngine();
    for (const sk of ['post#1', 'post#2', 'comment#1']) {
      e.putItem({ TableName: 'Users', Item: { pk: { S: 'u1' }, sk: { S: sk } } });
    }
    const r = e.query({
      TableName: 'Users',
      KeyConditionExpression: 'pk = :pk AND begins_with(sk, :p)',
      ExpressionAttributeValues: { ':pk': { S: 'u1' }, ':p': { S: 'post#' } },
    });
    expect(r.Count).toBe(2);
  });

  it('FilterExpression and Limit and ScanIndexForward=false', () => {
    const e = makeEngine();
    for (let i = 0; i < 5; i++)
      e.putItem({ TableName: 'Users', Item: { pk: { S: 'u1' }, sk: { S: `${i}` }, score: { N: `${i * 10}` } } });
    const r = e.query({
      TableName: 'Users',
      KeyConditionExpression: 'pk = :pk',
      FilterExpression: 'score > :m',
      Limit: 2,
      ScanIndexForward: false,
      ExpressionAttributeValues: { ':pk': { S: 'u1' }, ':m': { N: '10' } },
    });
    expect(r.Items.map((i) => i.sk)).toEqual([{ S: '4' }, { S: '3' }]);
  });
});

describe('Scan', () => {
  it('scans all items with filter', () => {
    const e = makeEngine();
    for (let i = 0; i < 10; i++)
      e.putItem({ TableName: 'Users', Item: { pk: { S: `u${i}` }, sk: { S: 'x' }, n: { N: `${i}` } } });
    const r = e.scan({
      TableName: 'Users',
      FilterExpression: 'n >= :m',
      ExpressionAttributeValues: { ':m': { N: '7' } },
    });
    expect(r.Count).toBe(3);
    expect(r.ScannedCount).toBe(10);
  });
});

describe('Transactions', () => {
  it('transactWriteItems rolls back if any condition fails', () => {
    const e = makeEngine();
    e.putItem({ TableName: 'Users', Item: { pk: { S: 'a' }, sk: { S: 'x' }, v: { N: '1' } } });
    expect(() => e.transactWriteItems({
      TransactItems: [
        { Put: { TableName: 'Users', Item: { pk: { S: 'b' }, sk: { S: 'x' }, v: { N: '2' } } } },
        { Update: {
            TableName: 'Users', Key: { pk: { S: 'a' }, sk: { S: 'x' } },
            UpdateExpression: 'SET v = :v',
            ConditionExpression: 'v = :expected',
            ExpressionAttributeValues: { ':v': { N: '99' }, ':expected': { N: '77' } },
        } },
      ],
    })).toThrow(/Transaction cancelled/);
    // ensure 'b' was NOT written
    expect(e.getItem({ TableName: 'Users', Key: { pk: { S: 'b' }, sk: { S: 'x' } } }).Item).toBeUndefined();
  });

  it('successful transactWriteItems applies all', () => {
    const e = makeEngine();
    e.transactWriteItems({
      TransactItems: [
        { Put: { TableName: 'Users', Item: { pk: { S: 'a' }, sk: { S: 'x' } } } },
        { Put: { TableName: 'Users', Item: { pk: { S: 'b' }, sk: { S: 'x' } } } },
      ],
    });
    const r = e.scan({ TableName: 'Users' });
    expect(r.Count).toBe(2);
  });
});

describe('Indexes', () => {
  it('GSI query', () => {
    const e = new Engine();
    e.createTable({
      TableName: 'T',
      AttributeDefinitions: [
        { AttributeName: 'pk', AttributeType: 'S' },
        { AttributeName: 'sk', AttributeType: 'S' },
        { AttributeName: 'gsi_pk', AttributeType: 'S' },
      ],
      KeySchema: [{ AttributeName: 'pk', KeyType: 'HASH' }, { AttributeName: 'sk', KeyType: 'RANGE' }],
      GlobalSecondaryIndexes: [{
        IndexName: 'byCategory',
        KeySchema: [{ AttributeName: 'gsi_pk', KeyType: 'HASH' }],
      }],
    });
    e.putItem({ TableName: 'T', Item: { pk: { S: 'a' }, sk: { S: '1' }, gsi_pk: { S: 'cat1' } } });
    e.putItem({ TableName: 'T', Item: { pk: { S: 'b' }, sk: { S: '2' }, gsi_pk: { S: 'cat1' } } });
    e.putItem({ TableName: 'T', Item: { pk: { S: 'c' }, sk: { S: '3' }, gsi_pk: { S: 'cat2' } } });
    const r = e.query({
      TableName: 'T', IndexName: 'byCategory',
      KeyConditionExpression: 'gsi_pk = :v',
      ExpressionAttributeValues: { ':v': { S: 'cat1' } },
    });
    expect(r.Count).toBe(2);
  });
});

describe('ConditionExpression evaluator', () => {
  it('handles AND/OR/NOT and functions', () => {
    const e = makeEngine();
    e.putItem({ TableName: 'Users', Item: { pk: { S: 'u' }, sk: { S: 'x' }, name: { S: 'alice' }, tags: { SS: ['a', 'b'] } } });
    expect(() => e.putItem({
      TableName: 'Users',
      Item: { pk: { S: 'u' }, sk: { S: 'x' } },
      ConditionExpression: '(attribute_exists(name) AND contains(tags, :t)) AND NOT begins_with(name, :p)',
      ExpressionAttributeValues: { ':t': { S: 'b' }, ':p': { S: 'bob' } },
    })).not.toThrow();
  });
});
