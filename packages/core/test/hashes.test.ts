import { describe, it, expect, beforeEach } from 'vitest';
import { Connection, Engine } from '../src/index.js';

describe('hashes', () => {
  let c: Connection;
  beforeEach(() => { c = new Engine().createConnection(); });

  it('HSET/HGET/HGETALL', async () => {
    expect(await c.call('HSET', 'h', 'a', '1', 'b', '2')).toBe(2);
    expect(await c.call('HGET', 'h', 'a')).toBe('1');
    const all = (await c.call('HGETALL', 'h')) as string[];
    expect(all.sort()).toEqual(['1', '2', 'a', 'b']);
  });

  it('HINCRBY/HINCRBYFLOAT', async () => {
    expect(await c.call('HINCRBY', 'h', 'n', 5)).toBe(5);
    expect(await c.call('HINCRBY', 'h', 'n', 10)).toBe(15);
    expect(await c.call('HINCRBYFLOAT', 'h', 'f', '0.5')).toBe('0.5');
  });

  it('HDEL/HEXISTS/HLEN', async () => {
    await c.call('HSET', 'h', 'a', '1', 'b', '2');
    expect(await c.call('HEXISTS', 'h', 'a')).toBe(1);
    expect(await c.call('HDEL', 'h', 'a', 'missing')).toBe(1);
    expect(await c.call('HLEN', 'h')).toBe(1);
  });

  it('HKEYS/HVALS/HMGET', async () => {
    await c.call('HSET', 'h', 'a', '1', 'b', '2');
    expect(((await c.call('HKEYS', 'h')) as string[]).sort()).toEqual(['a', 'b']);
    expect(((await c.call('HVALS', 'h')) as string[]).sort()).toEqual(['1', '2']);
    expect(await c.call('HMGET', 'h', 'a', 'missing', 'b')).toEqual(['1', null, '2']);
  });

  it('HSETNX', async () => {
    expect(await c.call('HSETNX', 'h', 'a', '1')).toBe(1);
    expect(await c.call('HSETNX', 'h', 'a', '2')).toBe(0);
    expect(await c.call('HGET', 'h', 'a')).toBe('1');
  });
});
