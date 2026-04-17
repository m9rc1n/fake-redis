# @fake-redis/node-redis

## 0.2.0

### Minor Changes

- Initial public release.

  - `@fake-redis/core`: in-memory Redis command engine covering strings, lists, hashes, sets, sorted sets, streams (basic), pub/sub, TTL, transactions (MULTI/EXEC), and ~150 commands.
  - `@fake-redis/ioredis`: drop-in `ioredis`-compatible client with Pipeline/Multi, pub/sub, and full command surface.
  - `@fake-redis/node-redis`: drop-in `node-redis` v4/v5 compatible `createClient` with camelCase methods and Multi.
  - `@fake-redis/server`: RESP2 TCP server so real Redis clients can connect to the in-memory engine.

### Patch Changes

- Updated dependencies
  - @fake-redis/core@0.2.0
