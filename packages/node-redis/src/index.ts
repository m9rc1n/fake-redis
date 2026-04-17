import { EventEmitter } from 'node:events';
import { Connection, Engine, ReplyError } from '@fake-redis/core';
import type { CommandArg, Reply } from '@fake-redis/core';
import { REDIS_COMMAND_NAMES } from './commands.js';

export type CreateClientOptions = {
  url?: string;
  socket?: { host?: string; port?: number };
  database?: number;
  password?: string;
  username?: string;
  /** Share a backing engine across clients */
  engine?: Engine;
};

/** node-redis compatible client. Use `createClient(opts)` like the real `redis` package. */
export class FakeRedisClient extends EventEmitter {
  readonly connection: Connection;
  isOpen = false;
  isReady = false;

  constructor(public readonly options: CreateClientOptions = {}) {
    super();
    const engine = options.engine ?? new Engine();
    this.connection = engine.createConnection();
    if (options.database) void this.connection.call('SELECT', options.database);
    this.connection.on('message', (m: any) => {
      if (m.kind === 'message') this.emit('message', m.message, m.channel);
      else this.emit('pmessage', m.pattern, m.channel, m.message);
    });
  }

  async connect(): Promise<this> {
    this.isOpen = true;
    this.isReady = true;
    this.emit('connect');
    this.emit('ready');
    return this;
  }

  async quit(): Promise<'OK'> { return this.disconnect(); }
  async disconnect(): Promise<'OK'> {
    this.connection.close();
    this.isOpen = false;
    this.isReady = false;
    this.emit('end');
    return 'OK';
  }

  async sendCommand<T = Reply>(args: CommandArg[] | [string, ...CommandArg[]]): Promise<T> {
    const [cmd, ...rest] = args;
    return (await this.connection.call(String(cmd), ...rest)) as T;
  }

  duplicate(overrides?: CreateClientOptions): FakeRedisClient {
    return new FakeRedisClient({ ...this.options, ...overrides, engine: this.connection.engine });
  }

  multi(): Multi {
    return new Multi(this, true);
  }

  // pub/sub
  async subscribe(channels: string | string[], listener?: (message: string, channel: string) => void): Promise<void> {
    const chans = Array.isArray(channels) ? channels : [channels];
    await this.connection.call('SUBSCRIBE', ...chans);
    if (listener) this.on('message', listener);
  }
  async unsubscribe(channels?: string | string[]): Promise<void> {
    const chans = channels === undefined ? [] : Array.isArray(channels) ? channels : [channels];
    await this.connection.call('UNSUBSCRIBE', ...chans);
  }
  async pSubscribe(patterns: string | string[], listener?: (message: string, channel: string, pattern: string) => void): Promise<void> {
    const pats = Array.isArray(patterns) ? patterns : [patterns];
    await this.connection.call('PSUBSCRIBE', ...pats);
    if (listener) this.on('pmessage', listener);
  }
  async pUnsubscribe(patterns?: string | string[]): Promise<void> {
    const pats = patterns === undefined ? [] : Array.isArray(patterns) ? patterns : [patterns];
    await this.connection.call('PUNSUBSCRIBE', ...pats);
  }
}

export class Multi {
  private queue: Array<[string, CommandArg[]]> = [];
  private _client: FakeRedisClient;
  private _transactional: boolean;
  constructor(client: FakeRedisClient, transactional: boolean) {
    this._client = client;
    this._transactional = transactional;
    for (const upper of REDIS_COMMAND_NAMES) {
      if (upper === 'EXEC' || upper === 'MULTI' || upper === 'DISCARD') continue;
      const method = toMethodName(upper);
      (this as any)[method] = (...args: CommandArg[]) => {
        this.queue.push([upper, args]);
        return this;
      };
    }
  }

  addCommand(args: [string, ...CommandArg[]]): this {
    this.queue.push([args[0], args.slice(1) as CommandArg[]]);
    return this;
  }

  async exec(): Promise<Reply[]> {
    if (this._transactional) await this._client.connection.call('MULTI');
    if (this._transactional) {
      for (const [n, a] of this.queue) await this._client.connection.call(n, ...a);
      const res = (await this._client.connection.call('EXEC')) as Reply[];
      return res;
    }
    const out: Reply[] = [];
    for (const [n, a] of this.queue) {
      try { out.push(await this._client.connection.call(n, ...a)); }
      catch (e) { out.push(e as Error); }
    }
    return out;
  }
  execAsPipeline(): Promise<Reply[]> { return this.exec(); }
}

/** Convert Redis command name to node-redis method name. Splits on common type-prefixes and verbs. */
const toMethodName = (upper: string): string => {
  // Handle SUFFIX_RO → suffixRo and dots
  let s = upper.toLowerCase().replace(/_([a-z])/g, (_, c) => c.toUpperCase());
  // Split known single-letter type prefixes (h/l/z/s/x/p) followed by a verb.
  // Words that tend to be verbs in Redis commands:
  const verbs = ['getall', 'getex', 'setex', 'setnx', 'setrange', 'getrange', 'getbit', 'setbit',
    'incrbyfloat', 'incrby', 'decrby', 'random', 'randmember', 'randfield',
    'rangebyscore', 'rangebylex', 'rangestore', 'revrange', 'revrangebyscore', 'revrangebylex',
    'remrangebyrank', 'remrangebyscore', 'remrangebylex', 'revrank',
    'popmin', 'popmax', 'score', 'rank', 'count', 'lexcount', 'mscore',
    'range', 'mpop', 'intercard', 'interstore', 'unionstore', 'diffstore',
    'ismember', 'mismember', 'move', 'members', 'card', 'scan',
    'pushx', 'push', 'pop', 'len', 'index', 'insert', 'set', 'get', 'rem', 'trim', 'pos',
    'exists', 'incr', 'decr', 'add', 'diff', 'inter', 'union', 'merge', 'keys', 'vals', 'del',
    'strlen', 'setinfo', 'setname', 'getname', 'expire', 'expireat', 'expiretime', 'ttl',
  ];
  const sorted = [...verbs].sort((a, b) => b.length - a.length);
  for (const prefix of ['h', 'l', 'z', 's', 'x', 'p', 'b', 'g']) {
    if (s.startsWith(prefix)) {
      const rest = s.slice(prefix.length);
      for (const v of sorted) {
        if (rest === v) return prefix + v[0]!.toUpperCase() + v.slice(1);
        if (rest.startsWith(v) && !/^[a-z]/.test(rest.slice(v.length))) {
          return prefix + v[0]!.toUpperCase() + v.slice(1);
        }
      }
    }
  }
  return s;
};

// Attach camelCase method for every Redis command. Also attach the all-lowercase name as alias.
for (const upper of REDIS_COMMAND_NAMES) {
  const methods = new Set([toMethodName(upper), upper.toLowerCase().replace(/_/g, '')]);
  const impl = function (this: FakeRedisClient, ...args: CommandArg[]) {
    const flat: CommandArg[] = [];
    for (const a of args) {
      if (Array.isArray(a)) { for (const x of a) flat.push(x as CommandArg); }
      else if (Buffer.isBuffer(a) || typeof a !== 'object' || a === null) flat.push(a as CommandArg);
      else flat.push(JSON.stringify(a));
    }
    return this.connection.call(upper, ...flat);
  };
  for (const m of methods) {
    if (m in FakeRedisClient.prototype) continue;
    (FakeRedisClient.prototype as any)[m] = impl;
  }
}

export interface FakeRedisClient { [K: string]: any; }

export const createClient = (options: CreateClientOptions = {}): FakeRedisClient => new FakeRedisClient(options);

export const createCluster = (options: CreateClientOptions = {}): FakeRedisClient => new FakeRedisClient(options);

export { Engine, Connection, ReplyError };
export default { createClient, createCluster, FakeRedisClient };
