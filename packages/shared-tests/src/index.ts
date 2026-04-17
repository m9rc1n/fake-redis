import { expect, it } from 'vitest';

/** A minimal client interface the conformance suite exercises via raw sendCommand. */
export interface ConformanceClient {
  sendCommand(args: (string | number)[]): Promise<unknown>;
  close(): Promise<void>;
}

/** Register conformance tests on a client factory. Each call gets a fresh client. */
export const runConformance = (
  label: string,
  makeClient: () => Promise<ConformanceClient>,
) => {
  const cmd = async (c: ConformanceClient, ...a: (string | number)[]) =>
    c.sendCommand(a);

  it(`[${label}] string ops`, async () => {
    const c = await makeClient();
    try {
      expect(await cmd(c, 'SET', 'k', 'v')).toBe('OK');
      expect(await cmd(c, 'GET', 'k')).toBe('v');
      expect(await cmd(c, 'APPEND', 'k', '!')).toBe(2);
      expect(await cmd(c, 'GET', 'k')).toBe('v!');
    } finally { await c.close(); }
  });

  it(`[${label}] numeric ops`, async () => {
    const c = await makeClient();
    try {
      expect(await cmd(c, 'INCR', 'n')).toBe(1);
      expect(await cmd(c, 'INCRBY', 'n', 10)).toBe(11);
    } finally { await c.close(); }
  });

  it(`[${label}] lists`, async () => {
    const c = await makeClient();
    try {
      await cmd(c, 'RPUSH', 'l', 'a', 'b', 'c');
      expect(await cmd(c, 'LRANGE', 'l', 0, -1)).toEqual(['a', 'b', 'c']);
    } finally { await c.close(); }
  });

  it(`[${label}] hashes`, async () => {
    const c = await makeClient();
    try {
      await cmd(c, 'HSET', 'h', 'a', '1', 'b', '2');
      expect(await cmd(c, 'HGET', 'h', 'a')).toBe('1');
    } finally { await c.close(); }
  });

  it(`[${label}] sets`, async () => {
    const c = await makeClient();
    try {
      await cmd(c, 'SADD', 's', 'a', 'b', 'c');
      expect(await cmd(c, 'SCARD', 's')).toBe(3);
    } finally { await c.close(); }
  });

  it(`[${label}] zsets`, async () => {
    const c = await makeClient();
    try {
      await cmd(c, 'ZADD', 'z', 1, 'a', 2, 'b');
      expect(await cmd(c, 'ZRANGE', 'z', 0, -1)).toEqual(['a', 'b']);
    } finally { await c.close(); }
  });

  it(`[${label}] TTL`, async () => {
    const c = await makeClient();
    try {
      await cmd(c, 'SET', 'k', 'v');
      await cmd(c, 'EXPIRE', 'k', 100);
      const ttl = (await cmd(c, 'TTL', 'k')) as number;
      expect(ttl).toBeGreaterThan(0);
    } finally { await c.close(); }
  });
};
