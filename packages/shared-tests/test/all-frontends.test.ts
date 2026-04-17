import { describe } from 'vitest';
import Redis from 'ioredis';
import { createClient } from 'redis';
import { FakeRedis } from '@fake-redis/ioredis';
import { createClient as fakeCreateClient } from '@fake-redis/node-redis';
import { startServer } from '@fake-redis/server';
import { runConformance, type ConformanceClient } from '../src/index.js';

describe('conformance: fake ioredis adapter', () => {
  runConformance('fake-ioredis', async () => {
    const c = new FakeRedis();
    return {
      sendCommand: (args) => c.call(String(args[0]), ...args.slice(1)),
      close: async () => { await c.quit(); },
    } satisfies ConformanceClient;
  });
});

describe('conformance: fake node-redis adapter', () => {
  runConformance('fake-node-redis', async () => {
    const c = fakeCreateClient();
    await c.connect();
    return {
      sendCommand: (args) => c.sendCommand(args as any),
      close: async () => { await c.quit(); },
    } satisfies ConformanceClient;
  });
});

describe('conformance: real ioredis via RESP server', () => {
  runConformance('real-ioredis', async () => {
    const server = await startServer();
    const r = new Redis({ port: server.port, host: server.host, lazyConnect: true, maxRetriesPerRequest: 1 });
    await r.connect();
    return {
      sendCommand: (args) =>
        r.call(String(args[0]), ...args.slice(1).map(String)) as Promise<unknown>,
      close: async () => { await r.quit().catch(() => undefined); await server.close(); },
    } satisfies ConformanceClient;
  });
});

describe('conformance: real node-redis via RESP server', () => {
  runConformance('real-node-redis', async () => {
    const server = await startServer();
    const r = createClient({ url: server.url });
    r.on('error', () => undefined);
    await r.connect();
    return {
      sendCommand: (args) => r.sendCommand(args.map(String)) as Promise<unknown>,
      close: async () => { if (r.isOpen) await r.quit(); await server.close(); },
    } satisfies ConformanceClient;
  });
});
