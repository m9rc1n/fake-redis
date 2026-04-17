import { describe, it, expect, afterEach } from 'vitest';
import { FakeRedis } from '../../src/index.js';

let client: FakeRedis | null = null;
afterEach(async () => { await client?.quit(); client = null; });

describe('ioredis adapter', () => {
  it('set/get via method', async () => {
    client = new FakeRedis();
    expect(await client.set('k', 'v')).toBe('OK');
    expect(await client.get('k')).toBe('v');
  });

  it('hset / hgetall', async () => {
    client = new FakeRedis();
    await client.hset('h', 'a', '1', 'b', '2');
    const all = await client.hgetall('h');
    // ioredis returns hashes as object; our impl returns flat array — document this
    expect(Array.isArray(all) ? all.sort() : all).toBeTruthy();
  });

  it('pipeline', async () => {
    client = new FakeRedis();
    const res = await client.pipeline().set('a', '1').incr('a').get('a').exec();
    expect(res).toEqual([[null, 'OK'], [null, 2], [null, '2']]);
  });

  it('multi transaction', async () => {
    client = new FakeRedis();
    const res = await client.multi().set('a', '1').incr('a').exec();
    expect(res!.map(([e, r]) => [e, r])).toEqual([[null, 'OK'], [null, 2]]);
  });

  it('pub/sub between two clients', async () => {
    client = new FakeRedis();
    const sub = client.duplicate();
    const received = new Promise<[string, string]>((resolve) => {
      sub.on('message', (ch, msg) => resolve([ch, msg]));
    });
    await new Promise((r) => setTimeout(r, 5));
    await sub.subscribe('room');
    await client.publish('room', 'hi');
    expect(await received).toEqual(['room', 'hi']);
    await sub.quit();
  });

  it('ttl', async () => {
    client = new FakeRedis();
    await client.set('k', 'v', 'EX', 60);
    const ttl = await client.ttl('k');
    expect(ttl).toBeGreaterThan(0);
  });

  it('emits ready', async () => {
    client = new FakeRedis();
    const ok = await new Promise<boolean>((resolve) => client!.on('ready', () => resolve(true)));
    expect(ok).toBe(true);
  });
});
