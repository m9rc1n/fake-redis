import { describe, it, expect } from 'vitest';
import { Engine } from '../src/index.js';

describe('MULTI/EXEC', () => {
  it('queues and returns array', async () => {
    const c = new Engine().createConnection();
    await c.call('MULTI');
    expect(await c.call('SET', 'a', '1')).toBe('QUEUED');
    expect(await c.call('INCR', 'a')).toBe('QUEUED');
    const r = (await c.call('EXEC')) as unknown[];
    expect(r).toEqual(['OK', 2]);
  });

  it('DISCARD aborts', async () => {
    const c = new Engine().createConnection();
    await c.call('MULTI');
    await c.call('SET', 'a', '1');
    expect(await c.call('DISCARD')).toBe('OK');
    expect(await c.call('GET', 'a')).toBeNull();
  });

  it('errors in queued commands appear in result', async () => {
    const c = new Engine().createConnection();
    await c.call('SET', 'k', 'hi');
    await c.call('MULTI');
    await c.call('INCR', 'k');
    const r = (await c.call('EXEC')) as unknown[];
    expect(r[0]).toBeInstanceOf(Error);
  });
});
