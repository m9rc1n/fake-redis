# fake-redis

[![CI](https://github.com/marcinnurbanski/fake-redis/actions/workflows/ci.yml/badge.svg)](https://github.com/marcinnurbanski/fake-redis/actions/workflows/ci.yml)
[![npm](https://img.shields.io/npm/v/@fake-redis/ioredis.svg)](https://www.npmjs.com/package/@fake-redis/ioredis)
[![license](https://img.shields.io/github/license/marcinnurbanski/fake-redis.svg)](./LICENSE)

**In-memory Redis fake for Node.js tests.** Drop-in for `ioredis` and `node-redis`. No Docker, no TCP, no external process — just a fast, in-process state machine with the same command semantics your code expects.

## Why

| | `fake-redis` | `ioredis-mock` | `redis-memory-server` |
|---|---|---|---|
| Works with `ioredis` | ✅ | ✅ | ✅ (spawns redis) |
| Works with `node-redis` | ✅ | ❌ | ✅ |
| Zero external process | ✅ | ✅ | ❌ (downloads redis) |
| Optional RESP TCP server | ✅ | ❌ | ✅ |
| Shared in-memory state across clients | ✅ | partial | ✅ |
| ESM + CJS, Node 20/22/24 | ✅ | partial | partial |

## Install

```bash
# ioredis users
pnpm add -D @fake-redis/ioredis

# node-redis users
pnpm add -D @fake-redis/node-redis

# or point any client at a real TCP port
pnpm add -D @fake-redis/server
```

## Quickstart

### ioredis

```ts
import { FakeRedis } from '@fake-redis/ioredis';

const redis = new FakeRedis();
await redis.set('hello', 'world');
console.log(await redis.get('hello')); // 'world'
```

Swap it in via Jest/Vitest mock:

```ts
// vitest.setup.ts
vi.mock('ioredis', async () => ({ default: (await import('@fake-redis/ioredis')).FakeRedis }));
```

### node-redis

```ts
import { createClient } from '@fake-redis/node-redis';

const client = createClient();
await client.connect();
await client.set('hello', 'world');
```

### RESP TCP server (point any client at it)

```ts
import { startServer } from '@fake-redis/server';
import Redis from 'ioredis';

const server = await startServer();
const redis = new Redis({ port: server.port, host: server.host });
// ...run your tests...
await redis.quit();
await server.close();
```

## Sharing state across clients

```ts
import { Engine } from '@fake-redis/core';
import { FakeRedis } from '@fake-redis/ioredis';

const engine = new Engine();
const writer = new FakeRedis({ engine });
const reader = new FakeRedis({ engine });

await writer.set('k', '1');
await reader.get('k'); // '1'
```

## Command coverage

Strings, keys, TTL, lists, hashes, sets, sorted sets (full range/score/lex APIs), pub/sub, MULTI/EXEC, SCAN family, bit ops, HyperLogLog (approximate via set), streams (basic XADD/XLEN/XRANGE), geo (stub), scripting (stub), cluster (stub).

See [docs/compatibility.md](docs/compatibility.md) for the full matrix.

## Packages

- [`@fake-redis/core`](packages/core) — command engine, no client deps
- [`@fake-redis/ioredis`](packages/ioredis) — `ioredis`-compatible class
- [`@fake-redis/node-redis`](packages/node-redis) — `node-redis`-compatible `createClient`
- [`@fake-redis/server`](packages/server) — RESP TCP server wrapping the core

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). TL;DR: `pnpm install && pnpm test`.

## License

MIT — see [LICENSE](LICENSE).
