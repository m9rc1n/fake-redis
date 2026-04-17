import { EventEmitter } from 'node:events';
import './commands.js';
import { Database } from './database.js';
import { getCommand } from './commands.js';
import { PubSubHub } from './pubsub.js';
import { type CommandArg, type Reply, ReplyError } from './types.js';
import { toStr } from './util.js';

export interface EngineOptions {
  databases?: number;
}

/** Multi-database server. Create one per "Redis instance"; create Connections for each client. */
export class Engine {
  readonly pubsub = new PubSubHub();
  private dbs: Database[] = [];

  constructor(opts: EngineOptions = {}) {
    const n = opts.databases ?? 16;
    for (let i = 0; i < n; i++) this.dbs.push(new Database());
  }

  db(idx: number): Database {
    const d = this.dbs[idx];
    if (!d) throw new ReplyError('DB index is out of range');
    return d;
  }

  get databaseCount(): number { return this.dbs.length; }

  select(_idx: number): void {
    // per-connection state — handled by Connection; this is a no-op placeholder
  }

  flushAll(): void {
    for (const d of this.dbs) d.clear();
  }

  createConnection(): Connection {
    return new Connection(this);
  }
}

export class Connection extends EventEmitter {
  private selectedDb = 0;
  private multiQueue: Array<{ name: string; args: CommandArg[] }> | null = null;
  private subscribedChannels = new Set<string>();
  private subscribedPatterns = new Set<string>();

  constructor(public readonly engine: Engine) {
    super();
    this.on('pubsub', (m) => this.emit('message', m));
  }

  get db(): Database { return this.engine.db(this.selectedDb); }
  get dbIndex(): number { return this.selectedDb; }

  async call(name: string, ...args: CommandArg[]): Promise<Reply> {
    const upper = name.toUpperCase();

    // transaction lifecycle
    if (upper === 'MULTI') {
      if (this.multiQueue) throw new ReplyError('MULTI calls can not be nested');
      this.multiQueue = [];
      return 'OK';
    }
    if (upper === 'DISCARD') {
      if (!this.multiQueue) throw new ReplyError('DISCARD without MULTI');
      this.multiQueue = null;
      return 'OK';
    }
    if (upper === 'EXEC') {
      if (!this.multiQueue) throw new ReplyError('EXEC without MULTI');
      const q = this.multiQueue;
      this.multiQueue = null;
      const results: Reply[] = [];
      for (const c of q) {
        try { results.push(await this._execOne(c.name, c.args)); }
        catch (e) { results.push(e instanceof Error ? e : new Error(String(e))); }
      }
      return results;
    }
    if (this.multiQueue) {
      this.multiQueue.push({ name: upper, args });
      return 'QUEUED';
    }

    // pub/sub state
    if (upper === 'SUBSCRIBE') {
      const out: Reply[] = [];
      for (const c of args.map(toStr)) {
        this.subscribedChannels.add(c);
        const n = this.engine.pubsub.subscribe(this, c);
        out.push(['subscribe', c, n]);
      }
      return out.length === 1 ? out[0]! : out;
    }
    if (upper === 'UNSUBSCRIBE') {
      const chans = args.length === 0 ? [...this.subscribedChannels] : args.map(toStr);
      const out: Reply[] = [];
      for (const c of chans) {
        this.engine.pubsub.unsubscribe(this, c);
        this.subscribedChannels.delete(c);
        out.push(['unsubscribe', c, this.subscribedChannels.size]);
      }
      return out;
    }
    if (upper === 'PSUBSCRIBE') {
      const out: Reply[] = [];
      for (const p of args.map(toStr)) {
        this.subscribedPatterns.add(p);
        const n = this.engine.pubsub.psubscribe(this, p);
        out.push(['psubscribe', p, n]);
      }
      return out;
    }
    if (upper === 'PUNSUBSCRIBE') {
      const pats = args.length === 0 ? [...this.subscribedPatterns] : args.map(toStr);
      const out: Reply[] = [];
      for (const p of pats) {
        this.engine.pubsub.punsubscribe(this, p);
        this.subscribedPatterns.delete(p);
        out.push(['punsubscribe', p, this.subscribedPatterns.size]);
      }
      return out;
    }

    return this._execOne(upper, args);
  }

  private async _execOne(upper: string, args: CommandArg[]): Promise<Reply> {
    if (upper === 'SELECT') {
      const n = Number(toStr(args[0]!));
      if (!Number.isInteger(n) || n < 0 || n >= this.engine.databaseCount) {
        throw new ReplyError('DB index is out of range');
      }
      this.selectedDb = n;
      return 'OK';
    }
    const handler = getCommand(upper);
    if (!handler) throw new ReplyError(`unknown command '${upper}'`);
    const engineProxy: Engine = {
      ...this.engine,
      select: (n: number) => {
        if (!Number.isInteger(n) || n < 0 || n >= this.engine.databaseCount) {
          throw new ReplyError('DB index is out of range');
        }
        this.selectedDb = n;
      },
      flushAll: () => this.engine.flushAll(),
      pubsub: this.engine.pubsub,
      db: (i: number) => this.engine.db(i),
      databaseCount: this.engine.databaseCount,
      createConnection: () => this.engine.createConnection(),
    } as Engine;
    return handler(engineProxy, this.db, args);
  }

  close(): void {
    for (const c of this.subscribedChannels) this.engine.pubsub.unsubscribe(this, c);
    for (const p of this.subscribedPatterns) this.engine.pubsub.punsubscribe(this, p);
    this.subscribedChannels.clear();
    this.subscribedPatterns.clear();
    this.removeAllListeners();
  }
}
