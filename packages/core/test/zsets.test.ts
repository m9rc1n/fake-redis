import { describe, it, expect, beforeEach } from 'vitest';
import { Connection, Engine } from '../src/index.js';

describe('sorted sets', () => {
  let c: Connection;
  beforeEach(() => { c = new Engine().createConnection(); });

  it('ZADD/ZSCORE/ZCARD', async () => {
    expect(await c.call('ZADD', 'z', 1, 'a', 2, 'b', 3, 'c')).toBe(3);
    expect(await c.call('ZSCORE', 'z', 'b')).toBe('2');
    expect(await c.call('ZCARD', 'z')).toBe(3);
  });

  it('ZRANGE by index, score and with WITHSCORES', async () => {
    await c.call('ZADD', 'z', 1, 'a', 2, 'b', 3, 'c');
    expect(await c.call('ZRANGE', 'z', 0, -1)).toEqual(['a', 'b', 'c']);
    expect(await c.call('ZRANGE', 'z', 0, -1, 'WITHSCORES')).toEqual(['a', '1', 'b', '2', 'c', '3']);
    expect(await c.call('ZRANGEBYSCORE', 'z', 1, 2)).toEqual(['a', 'b']);
    expect(await c.call('ZRANGEBYSCORE', 'z', '(1', 3)).toEqual(['b', 'c']);
  });

  it('ZINCRBY / ZRANK / ZREVRANK', async () => {
    await c.call('ZADD', 'z', 1, 'a', 2, 'b');
    expect(await c.call('ZINCRBY', 'z', 5, 'a')).toBe('6');
    expect(await c.call('ZRANK', 'z', 'b')).toBe(0);
    expect(await c.call('ZREVRANK', 'z', 'b')).toBe(1);
  });

  it('ZPOPMIN/ZPOPMAX', async () => {
    await c.call('ZADD', 'z', 1, 'a', 2, 'b', 3, 'c');
    expect(await c.call('ZPOPMIN', 'z')).toEqual(['a', '1']);
    expect(await c.call('ZPOPMAX', 'z')).toEqual(['c', '3']);
  });

  it('ZREM', async () => {
    await c.call('ZADD', 'z', 1, 'a', 2, 'b');
    expect(await c.call('ZREM', 'z', 'a', 'missing')).toBe(1);
  });

  it('ZREMRANGEBYSCORE', async () => {
    await c.call('ZADD', 'z', 1, 'a', 2, 'b', 3, 'c', 4, 'd');
    expect(await c.call('ZREMRANGEBYSCORE', 'z', 2, 3)).toBe(2);
    expect(await c.call('ZRANGE', 'z', 0, -1)).toEqual(['a', 'd']);
  });
});
