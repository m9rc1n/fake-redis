import { describe, it, expect, afterEach } from 'vitest';
import { createClient, FakeRedisClient } from '../../src/index.js';

let client: FakeRedisClient | null = null;
afterEach(async () => { await client?.quit(); client = null; });

describe('node-redis adapter', () => {
  it('connect + set/get', async () => {
    client = createClient();
    await client.connect();
    expect(await client.set('k', 'v')).toBe('OK');
    expect(await client.get('k')).toBe('v');
  });

  it('hSet / hGetAll method names', async () => {
    client = createClient();
    await client.connect();
    await client.hSet('h', 'a', '1');
    const v = await client.hGet('h', 'a');
    expect(v).toBe('1');
  });

  it('multi transaction', async () => {
    client = createClient();
    await client.connect();
    const res = await client.multi().set('a', '1').incr('a').exec();
    expect(res).toEqual(['OK', 2]);
  });

  it('sendCommand raw', async () => {
    client = createClient();
    await client.connect();
    expect(await client.sendCommand(['SET', 'k', 'v'])).toBe('OK');
    expect(await client.sendCommand(['GET', 'k'])).toBe('v');
  });

  it('pub/sub', async () => {
    client = createClient();
    await client.connect();
    const sub = client.duplicate();
    await sub.connect();
    const received = new Promise<[string, string]>((resolve) => {
      sub.subscribe('ch', (msg, ch) => resolve([ch, msg]));
    });
    await new Promise((r) => setTimeout(r, 5));
    await client.publish('ch', 'hello');
    expect(await received).toEqual(['ch', 'hello']);
    await sub.quit();
  });
});
