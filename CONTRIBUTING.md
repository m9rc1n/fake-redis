# Contributing to fake-redis

Thanks for helping! This repo is a pnpm monorepo.

## Setup

```bash
pnpm install
pnpm build
pnpm test
```

Node 20+ required. pnpm 10+.

## Project layout

```
packages/
  core/           ← command engine, no client deps
  ioredis/        ← ioredis-compatible facade
  node-redis/     ← node-redis-compatible facade
  server/         ← RESP TCP server wrapping core
  shared-tests/   ← conformance suite run across frontends
```

## Adding a command

1. Add a handler in [`packages/core/src/commands.ts`](packages/core/src/commands.ts) with `registerCommand('NAME', (engine, db, args) => ...)`.
2. Add the name to `IOREDIS_COMMANDS` and `REDIS_COMMAND_NAMES` if it's a standard Redis command.
3. Write a unit test in `packages/core/test/<topic>.test.ts`.
4. Add a line to the conformance suite in `packages/shared-tests/src/index.ts` if it's a staple command.
5. Run `pnpm test` — all four frontends (fake ioredis, fake node-redis, real ioredis over RESP, real node-redis over RESP) must pass.
6. Add a changeset: `pnpm changeset`.

## Conventions

- TypeScript strict mode, no `any` in public API.
- No runtime dependencies in `@fake-redis/core`.
- All client-facing adapters must keep parity with upstream method names.
- Every behavioral change needs a test.

## Releasing

Maintainers only. Merging the "Version Packages" PR opened by the Changesets bot triggers publish to npm with provenance via GitHub Actions OIDC.

## Reporting security issues

Please use GitHub's private security advisory flow, not public issues. See [SECURITY.md](SECURITY.md).
