import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { createClient, type RedisClientType } from 'redis';
import { startServer, type RunningServer } from '../../src/index.js';

describe('e2e: real node-redis against fake-redis RESP server', () => {
  let server: RunningServer;
  let client: RedisClientType;

  beforeAll(async () => {
    server = await startServer();
    client = createClient({ url: server.url });
    client.on('error', () => undefined);
    await client.connect();
  });

  afterAll(async () => {
    if (client.isOpen) await client.quit();
    await server.close();
  });

  it('SET/GET', async () => {
    expect(await client.set('k', 'v')).toBe('OK');
    expect(await client.get('k')).toBe('v');
  });

  it('INCR', async () => {
    await client.set('n', '0');
    expect(await client.incr('n')).toBe(1);
    expect(await client.incr('n')).toBe(2);
  });

  it('LPUSH/LRANGE', async () => {
    await client.rPush('l', ['a', 'b', 'c']);
    expect(await client.lRange('l', 0, -1)).toEqual(['a', 'b', 'c']);
  });

  it('multi', async () => {
    const res = await client.multi().set('m', '1').incr('m').exec();
    expect(res).toEqual(['OK', 2]);
  });
});
