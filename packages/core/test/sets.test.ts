import { describe, it, expect, beforeEach } from 'vitest';
import { Connection, Engine } from '../src/index.js';

describe('sets', () => {
  let c: Connection;
  beforeEach(() => { c = new Engine().createConnection(); });

  it('SADD/SMEMBERS/SCARD', async () => {
    expect(await c.call('SADD', 's', 'a', 'b', 'a', 'c')).toBe(3);
    expect(((await c.call('SMEMBERS', 's')) as string[]).sort()).toEqual(['a', 'b', 'c']);
    expect(await c.call('SCARD', 's')).toBe(3);
  });

  it('SISMEMBER/SMISMEMBER', async () => {
    await c.call('SADD', 's', 'a', 'b');
    expect(await c.call('SISMEMBER', 's', 'a')).toBe(1);
    expect(await c.call('SISMEMBER', 's', 'z')).toBe(0);
    expect(await c.call('SMISMEMBER', 's', 'a', 'z', 'b')).toEqual([1, 0, 1]);
  });

  it('SINTER/SUNION/SDIFF', async () => {
    await c.call('SADD', 'a', '1', '2', '3');
    await c.call('SADD', 'b', '2', '3', '4');
    expect(((await c.call('SINTER', 'a', 'b')) as string[]).sort()).toEqual(['2', '3']);
    expect(((await c.call('SUNION', 'a', 'b')) as string[]).sort()).toEqual(['1', '2', '3', '4']);
    expect(((await c.call('SDIFF', 'a', 'b')) as string[]).sort()).toEqual(['1']);
  });

  it('SREM/SMOVE', async () => {
    await c.call('SADD', 's', 'a', 'b');
    expect(await c.call('SREM', 's', 'a', 'missing')).toBe(1);
    await c.call('SMOVE', 's', 'dst', 'b');
    expect(await c.call('SMEMBERS', 'dst')).toEqual(['b']);
  });
});
