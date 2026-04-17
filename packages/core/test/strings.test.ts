import { describe, it, expect, beforeEach } from 'vitest';
import { Connection, Engine } from '../src/index.js';

describe('strings', () => {
  let c: Connection;
  beforeEach(() => { c = new Engine().createConnection(); });

  it('SET/GET', async () => {
    expect(await c.call('SET', 'k', 'v')).toBe('OK');
    expect(await c.call('GET', 'k')).toBe('v');
  });

  it('SET NX/XX', async () => {
    await c.call('SET', 'k', 'v');
    expect(await c.call('SET', 'k', 'w', 'NX')).toBeNull();
    expect(await c.call('GET', 'k')).toBe('v');
    expect(await c.call('SET', 'missing', 'v', 'XX')).toBeNull();
    expect(await c.call('GET', 'missing')).toBeNull();
  });

  it('INCR/DECR', async () => {
    expect(await c.call('INCR', 'n')).toBe(1);
    expect(await c.call('INCRBY', 'n', 10)).toBe(11);
    expect(await c.call('DECR', 'n')).toBe(10);
    expect(await c.call('DECRBY', 'n', 5)).toBe(5);
  });

  it('INCR on non-integer throws', async () => {
    await c.call('SET', 'k', 'hi');
    await expect(c.call('INCR', 'k')).rejects.toThrow(/not an integer/);
  });

  it('APPEND/STRLEN', async () => {
    expect(await c.call('APPEND', 'k', 'hello')).toBe(5);
    expect(await c.call('APPEND', 'k', ' world')).toBe(11);
    expect(await c.call('STRLEN', 'k')).toBe(11);
    expect(await c.call('GET', 'k')).toBe('hello world');
  });

  it('MSET/MGET', async () => {
    await c.call('MSET', 'a', '1', 'b', '2', 'c', '3');
    expect(await c.call('MGET', 'a', 'b', 'missing', 'c')).toEqual(['1', '2', null, '3']);
  });

  it('GETRANGE/SETRANGE', async () => {
    await c.call('SET', 'k', 'Hello World');
    expect(await c.call('GETRANGE', 'k', 0, 4)).toBe('Hello');
    expect(await c.call('GETRANGE', 'k', -5, -1)).toBe('World');
    await c.call('SETRANGE', 'k', 6, 'Redis');
    expect(await c.call('GET', 'k')).toBe('Hello Redis');
  });

  it('INCRBYFLOAT', async () => {
    await c.call('SET', 'k', '10');
    expect(await c.call('INCRBYFLOAT', 'k', '0.5')).toBe('10.5');
  });
});
