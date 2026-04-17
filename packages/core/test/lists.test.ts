import { describe, it, expect, beforeEach } from 'vitest';
import { Connection, Engine } from '../src/index.js';

describe('lists', () => {
  let c: Connection;
  beforeEach(() => { c = new Engine().createConnection(); });

  it('LPUSH/RPUSH/LRANGE', async () => {
    await c.call('RPUSH', 'l', 'a', 'b', 'c');
    expect(await c.call('LRANGE', 'l', 0, -1)).toEqual(['a', 'b', 'c']);
    await c.call('LPUSH', 'l', 'z');
    expect(await c.call('LRANGE', 'l', 0, 0)).toEqual(['z']);
  });

  it('LPOP/RPOP with count', async () => {
    await c.call('RPUSH', 'l', 'a', 'b', 'c', 'd');
    expect(await c.call('LPOP', 'l', 2)).toEqual(['a', 'b']);
    expect(await c.call('RPOP', 'l')).toBe('d');
  });

  it('LLEN/LINDEX/LSET', async () => {
    await c.call('RPUSH', 'l', 'a', 'b', 'c');
    expect(await c.call('LLEN', 'l')).toBe(3);
    expect(await c.call('LINDEX', 'l', 1)).toBe('b');
    expect(await c.call('LINDEX', 'l', -1)).toBe('c');
    await c.call('LSET', 'l', 0, 'A');
    expect(await c.call('LINDEX', 'l', 0)).toBe('A');
  });

  it('LREM/LTRIM/LINSERT', async () => {
    await c.call('RPUSH', 'l', 'a', 'b', 'a', 'c');
    expect(await c.call('LREM', 'l', 1, 'a')).toBe(1);
    expect(await c.call('LRANGE', 'l', 0, -1)).toEqual(['b', 'a', 'c']);
    await c.call('LINSERT', 'l', 'BEFORE', 'a', 'X');
    expect(await c.call('LRANGE', 'l', 0, -1)).toEqual(['b', 'X', 'a', 'c']);
    await c.call('LTRIM', 'l', 1, 2);
    expect(await c.call('LRANGE', 'l', 0, -1)).toEqual(['X', 'a']);
  });

  it('LMOVE/RPOPLPUSH', async () => {
    await c.call('RPUSH', 'src', 'a', 'b', 'c');
    expect(await c.call('LMOVE', 'src', 'dst', 'LEFT', 'RIGHT')).toBe('a');
    expect(await c.call('LRANGE', 'dst', 0, -1)).toEqual(['a']);
    expect(await c.call('RPOPLPUSH', 'src', 'dst')).toBe('c');
  });

  it('LPOS', async () => {
    await c.call('RPUSH', 'l', 'a', 'b', 'a', 'c', 'a');
    expect(await c.call('LPOS', 'l', 'a')).toBe(0);
    expect(await c.call('LPOS', 'l', 'a', 'RANK', 2)).toBe(2);
    expect(await c.call('LPOS', 'l', 'a', 'COUNT', 0)).toEqual([0, 2, 4]);
  });
});
