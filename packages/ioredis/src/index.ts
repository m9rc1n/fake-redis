import { EventEmitter } from 'node:events';
import { Connection, Engine, ReplyError } from '@fake-redis/core';
import type { CommandArg, Reply } from '@fake-redis/core';
import { IOREDIS_COMMANDS } from './commands.js';

export type FakeRedisOptions = {
  data?: Record<string, string>;
  /** Share a backing engine across multiple FakeRedis instances (multi-client tests). */
  engine?: Engine;
  /** ioredis-compatible options accepted for API parity but ignored */
  host?: string;
  port?: number;
  db?: number;
  password?: string;
  keyPrefix?: string;
  lazyConnect?: boolean;
  connectionName?: string;
};

const globalEngines = new WeakMap<object, Engine>();

/**
 * Drop-in fake for `ioredis` with an in-memory backend.
 * Usage: `const redis = new FakeRedis();`
 * or: `jest.mock('ioredis', () => require('@fake-redis/ioredis').default);`
 */
export class FakeRedis extends EventEmitter {
  readonly connection: Connection;
  readonly options: FakeRedisOptions;
  status: 'wait' | 'connecting' | 'connect' | 'ready' | 'end' = 'wait';

  constructor(options: FakeRedisOptions | string | number = {}) {
    super();
    this.options = typeof options === 'object' ? options : {};
    const engine = this.options.engine ?? new Engine();
    this.connection = engine.createConnection();
    if (this.options.db) void this.connection.call('SELECT', this.options.db);
    if (this.options.data) {
      for (const [k, v] of Object.entries(this.options.data)) void this.connection.call('SET', k, v);
    }
    this.connection.on('message', (m: any) => {
      if (m.kind === 'message') this.emit('message', m.channel, m.message);
      else this.emit('pmessage', m.pattern, m.channel, m.message);
    });
    // simulate async connect
    queueMicrotask(() => {
      this.status = 'ready';
      this.emit('connect');
      this.emit('ready');
    });
  }

  async connect(): Promise<void> {
    this.status = 'ready';
  }

  disconnect(): void { this.quit(); }
  async quit(): Promise<'OK'> {
    this.connection.close();
    this.status = 'end';
    this.emit('end');
    this.emit('close');
    return 'OK';
  }

  duplicate(overrides?: FakeRedisOptions): FakeRedis {
    return new FakeRedis({ ...this.options, ...overrides, engine: this.connection.engine });
  }

  async call(command: string, ...args: CommandArg[]): Promise<Reply> {
    return this.connection.call(command, ...args);
  }

  sendCommand = this.call.bind(this);

  pipeline(): Pipeline {
    return new Pipeline(this, false);
  }
  multi(): Pipeline {
    return new Pipeline(this, true);
  }

  subscribe(...channels: string[]): Promise<number> {
    return this.connection.call('SUBSCRIBE', ...channels).then(() => channels.length);
  }
  unsubscribe(...channels: string[]): Promise<number> {
    return this.connection.call('UNSUBSCRIBE', ...channels).then(() => channels.length);
  }
  psubscribe(...patterns: string[]): Promise<number> {
    return this.connection.call('PSUBSCRIBE', ...patterns).then(() => patterns.length);
  }
  punsubscribe(...patterns: string[]): Promise<number> {
    return this.connection.call('PUNSUBSCRIBE', ...patterns).then(() => patterns.length);
  }

  defineCommand(name: string, _opts: { numberOfKeys?: number; lua: string }): void {
    (this as any)[name] = (...args: CommandArg[]) => this.connection.call('EVAL', _opts.lua, 0, ...args);
  }
}

export class Pipeline {
  private _queue: Array<[string, CommandArg[]]> = [];
  private _client: FakeRedis;
  private _transactional: boolean;
  constructor(client: FakeRedis, transactional: boolean) {
    this._client = client;
    this._transactional = transactional;
    for (const cmd of IOREDIS_COMMANDS) {
      if (cmd === 'exec' || cmd === 'multi' || cmd === 'discard') continue;
      (this as any)[cmd] = (...args: CommandArg[]) => {
        this._queue.push([cmd, args]);
        return this;
      };
    }
  }

  async exec(): Promise<Array<[Error | null, Reply]>> {
    if (this._transactional) await this._client.call('MULTI');
    const results: Array<[Error | null, Reply]> = [];
    if (this._transactional) {
      for (const [name, args] of this._queue) {
        await this._client.call(name, ...args);
      }
      const execRes = (await this._client.call('EXEC')) as Reply[];
      return (execRes as Reply[]).map((r) => (r instanceof Error ? [r, null] : [null, r]));
    }
    for (const [name, args] of this._queue) {
      try { results.push([null, await this._client.call(name, ...args)]); }
      catch (e) { results.push([e as Error, null]); }
    }
    return results;
  }
}

// Attach all commands as methods returning promises.
for (const cmd of IOREDIS_COMMANDS) {
  if (cmd in FakeRedis.prototype) continue;
  (FakeRedis.prototype as any)[cmd] = function (this: FakeRedis, ...args: CommandArg[]) {
    // ioredis supports optional callback last arg
    let cb: ((err: Error | null, res?: Reply) => void) | undefined;
    if (typeof args[args.length - 1] === 'function') cb = args.pop() as any;
    const p = this.connection.call(cmd, ...args);
    if (cb) {
      p.then((r) => cb!(null, r), (e) => cb!(e));
      return undefined as any;
    }
    return p;
  };
  // ioredis also provides a `${cmd}Buffer` variant
  (FakeRedis.prototype as any)[`${cmd}Buffer`] = function (this: FakeRedis, ...args: CommandArg[]) {
    return this.connection.call(cmd, ...args).then((r) => {
      if (typeof r === 'string') return Buffer.from(r);
      if (Array.isArray(r)) return (r as unknown[]).map((x) => (typeof x === 'string' ? Buffer.from(x) : x));
      return r;
    });
  };
}

// Type declaration merging — all commands return Promise<any>.
export interface FakeRedis {
  [K: string]: any;
}

export { Engine, Connection, ReplyError };
export default FakeRedis;
