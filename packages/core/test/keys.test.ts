import { describe, it, expect, beforeEach } from 'vitest';
import { Connection, Engine } from '../src/index.js';

describe('keys / TTL', () => {
  let c: Connection;
  beforeEach(() => { c = new Engine().createConnection(); });

  it('EXISTS/DEL/TYPE', async () => {
    await c.call('SET', 'k', 'v');
    expect(await c.call('EXISTS', 'k')).toBe(1);
    expect(await c.call('TYPE', 'k')).toBe('string');
    expect(await c.call('DEL', 'k')).toBe(1);
    expect(await c.call('EXISTS', 'k')).toBe(0);
    expect(await c.call('TYPE', 'k')).toBe('none');
  });

  it('EXPIRE/TTL/PERSIST', async () => {
    await c.call('SET', 'k', 'v');
    expect(await c.call('EXPIRE', 'k', 100)).toBe(1);
    expect(await c.call('TTL', 'k')).toBeGreaterThan(0);
    expect(await c.call('PERSIST', 'k')).toBe(1);
    expect(await c.call('TTL', 'k')).toBe(-1);
    expect(await c.call('TTL', 'missing')).toBe(-2);
  });

  it('EXPIRE NX/XX/GT/LT', async () => {
    await c.call('SET', 'k', 'v');
    expect(await c.call('EXPIRE', 'k', 100, 'XX')).toBe(0);
    expect(await c.call('EXPIRE', 'k', 100, 'NX')).toBe(1);
    expect(await c.call('EXPIRE', 'k', 50, 'NX')).toBe(0);
    expect(await c.call('EXPIRE', 'k', 200, 'GT')).toBe(1);
    expect(await c.call('EXPIRE', 'k', 50, 'LT')).toBe(1);
  });

  it('PEXPIRE and expiration on GET', async () => {
    await c.call('SET', 'k', 'v');
    await c.call('PEXPIRE', 'k', 1);
    await new Promise((r) => setTimeout(r, 10));
    expect(await c.call('GET', 'k')).toBeNull();
  });

  it('KEYS pattern', async () => {
    await c.call('MSET', 'foo', '1', 'bar', '2', 'foz', '3');
    const r = (await c.call('KEYS', 'fo*')) as string[];
    expect(r.sort()).toEqual(['foo', 'foz']);
  });

  it('RENAME', async () => {
    await c.call('SET', 'a', '1');
    await c.call('RENAME', 'a', 'b');
    expect(await c.call('GET', 'b')).toBe('1');
    expect(await c.call('EXISTS', 'a')).toBe(0);
  });

  it('COPY with REPLACE', async () => {
    await c.call('SET', 'a', '1');
    await c.call('SET', 'b', '2');
    expect(await c.call('COPY', 'a', 'b')).toBe(0);
    expect(await c.call('COPY', 'a', 'b', 'REPLACE')).toBe(1);
    expect(await c.call('GET', 'b')).toBe('1');
  });
});
