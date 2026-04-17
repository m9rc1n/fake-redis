import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import Redis from 'ioredis';
import { startServer, type RunningServer } from '../../src/index.js';

describe('e2e: real ioredis against fake-redis RESP server', () => {
  let server: RunningServer;
  let client: Redis;

  beforeAll(async () => {
    server = await startServer();
    client = new Redis({ port: server.port, host: server.host, lazyConnect: true, maxRetriesPerRequest: 1 });
    await client.connect();
  });

  afterAll(async () => {
    await client.quit().catch(() => undefined);
    await server.close();
  });

  it('basic string ops', async () => {
    expect(await client.set('k', 'v')).toBe('OK');
    expect(await client.get('k')).toBe('v');
    expect(await client.del('k')).toBe(1);
  });

  it('list ops', async () => {
    await client.rpush('l', 'a', 'b', 'c');
    expect(await client.lrange('l', 0, -1)).toEqual(['a', 'b', 'c']);
  });

  it('hash ops', async () => {
    await client.hset('h', 'a', '1', 'b', '2');
    expect(await client.hget('h', 'a')).toBe('1');
  });

  it('pipeline', async () => {
    const res = await client.pipeline().set('x', '1').incr('x').get('x').exec();
    expect(res).toEqual([[null, 'OK'], [null, 2], [null, '2']]);
  });

  it('transaction', async () => {
    const res = await client.multi().set('t', '1').incr('t').exec();
    expect(res).toEqual([[null, 'OK'], [null, 2]]);
  });
});
