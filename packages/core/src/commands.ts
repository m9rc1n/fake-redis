import type { Database } from './database.js';
import type { Engine } from './engine.js';
import { ReplyError, type CommandArg, type Reply } from './types.js';
import { formatFloat, matchGlob, now, toFloat, toInt, toStr } from './util.js';

export type CommandHandler = (engine: Engine, db: Database, args: CommandArg[]) => Reply | Promise<Reply>;

const reg = new Map<string, CommandHandler>();
export const registerCommand = (name: string, fn: CommandHandler) => {
  reg.set(name.toLowerCase(), fn);
};
export const getCommand = (name: string): CommandHandler | undefined => reg.get(name.toLowerCase());
export const commandList = (): string[] => [...reg.keys()];

const need = (args: CommandArg[], min: number, max = Infinity, name: string) => {
  if (args.length < min || args.length > max) {
    throw new ReplyError(`wrong number of arguments for '${name.toLowerCase()}' command`);
  }
};

// ------------------------ connection / server ------------------------
registerCommand('PING', (_e, _db, args) => {
  if (args.length === 0) return 'PONG';
  if (args.length === 1) return toStr(args[0]!);
  throw new ReplyError("wrong number of arguments for 'ping' command");
});
registerCommand('ECHO', (_e, _db, args) => {
  need(args, 1, 1, 'ECHO');
  return toStr(args[0]!);
});
registerCommand('SELECT', (e, _db, args) => {
  need(args, 1, 1, 'SELECT');
  e.select(toInt(args[0]!));
  return 'OK';
});
registerCommand('FLUSHDB', (_e, db) => {
  db.clear();
  return 'OK';
});
registerCommand('FLUSHALL', (e) => {
  e.flushAll();
  return 'OK';
});
registerCommand('DBSIZE', (_e, db) => db.size());
registerCommand('AUTH', () => 'OK');
registerCommand('HELLO', (_e, _db, args) => {
  const reply: Reply[] = [
    'server', 'fake-redis',
    'version', '0.1.0',
    'proto', args.length > 0 ? toInt(args[0]!) : 2,
    'id', 1,
    'mode', 'standalone',
    'role', 'master',
    'modules', [],
  ];
  return reply;
});
registerCommand('CLIENT', (_e, _db, args) => {
  const sub = toStr(args[0] ?? 'unknown').toUpperCase();
  if (sub === 'SETNAME' || sub === 'SETINFO') return 'OK';
  if (sub === 'GETNAME') return null;
  if (sub === 'ID') return 1;
  if (sub === 'LIST') return '';
  if (sub === 'NO-EVICT' || sub === 'NO-TOUCH' || sub === 'REPLY' || sub === 'UNPAUSE' || sub === 'PAUSE') return 'OK';
  return 'OK';
});
registerCommand('COMMAND', () => []);
registerCommand('INFO', () => '# Server\nredis_version:7.4.0-fake\nredis_mode:standalone\n');
registerCommand('TIME', () => {
  const ms = now();
  return [String(Math.floor(ms / 1000)), String((ms % 1000) * 1000)];
});
registerCommand('DEBUG', (_e, _db, args) => {
  const sub = toStr(args[0] ?? '').toUpperCase();
  if (sub === 'SLEEP') {
    const ms = toFloat(args[1]!) * 1000;
    return new Promise<Reply>((resolve) => setTimeout(() => resolve('OK'), ms));
  }
  return 'OK';
});
registerCommand('WAIT', () => 0);
registerCommand('RESET', () => 'RESET');
registerCommand('CONFIG', (_e, _db, args) => {
  const sub = toStr(args[0] ?? '').toUpperCase();
  if (sub === 'GET') return [];
  if (sub === 'SET') return 'OK';
  if (sub === 'RESETSTAT') return 'OK';
  if (sub === 'REWRITE') return 'OK';
  return 'OK';
});
registerCommand('MEMORY', () => 0);
registerCommand('SCRIPT', () => 'OK');
registerCommand('SHUTDOWN', () => 'OK');
registerCommand('LASTSAVE', () => Math.floor(now() / 1000));
registerCommand('BGSAVE', () => 'Background saving started');
registerCommand('BGREWRITEAOF', () => 'Background append only file rewriting started');
registerCommand('SAVE', () => 'OK');
registerCommand('SWAPDB', () => 'OK');

// ------------------------ keys ------------------------
registerCommand('EXISTS', (_e, db, args) => {
  need(args, 1, Infinity, 'EXISTS');
  return args.reduce<number>((acc, k) => acc + (db.has(toStr(k)) ? 1 : 0), 0);
});
registerCommand('DEL', (_e, db, args) => {
  need(args, 1, Infinity, 'DEL');
  let n = 0;
  for (const k of args) if (db.delete(toStr(k))) n++;
  return n;
});
registerCommand('UNLINK', (_e, db, args) => {
  need(args, 1, Infinity, 'UNLINK');
  let n = 0;
  for (const k of args) if (db.delete(toStr(k))) n++;
  return n;
});
registerCommand('TYPE', (_e, db, args) => {
  need(args, 1, 1, 'TYPE');
  return db.type(toStr(args[0]!));
});
registerCommand('KEYS', (_e, db, args) => {
  need(args, 1, 1, 'KEYS');
  const p = toStr(args[0]!);
  return db.keys().filter((k) => matchGlob(p, k));
});
registerCommand('RANDOMKEY', (_e, db) => {
  const ks = db.keys();
  return ks.length === 0 ? null : ks[Math.floor(Math.random() * ks.length)]!;
});
registerCommand('RENAME', (_e, db, args) => {
  need(args, 2, 2, 'RENAME');
  const src = toStr(args[0]!);
  const dst = toStr(args[1]!);
  const e = db.getEntry(src);
  if (!e) throw new ReplyError('no such key');
  db.delete(dst);
  db.setEntry(dst, { ...e });
  db.delete(src);
  return 'OK';
});
registerCommand('RENAMENX', (_e, db, args) => {
  need(args, 2, 2, 'RENAMENX');
  const src = toStr(args[0]!);
  const dst = toStr(args[1]!);
  const e = db.getEntry(src);
  if (!e) throw new ReplyError('no such key');
  if (db.has(dst)) return 0;
  db.setEntry(dst, { ...e });
  db.delete(src);
  return 1;
});
registerCommand('COPY', (_e, db, args) => {
  need(args, 2, Infinity, 'COPY');
  const src = toStr(args[0]!);
  const dst = toStr(args[1]!);
  let replace = false;
  for (let i = 2; i < args.length; i++) {
    if (toStr(args[i]!).toUpperCase() === 'REPLACE') replace = true;
  }
  const e = db.getEntry(src);
  if (!e) return 0;
  if (db.has(dst) && !replace) return 0;
  // deep-ish copy
  const copied = { ...e, value: cloneValue(e.type, e.value) };
  db.setEntry(dst, copied);
  return 1;
});

const cloneValue = (type: string, v: unknown): unknown => {
  if (type === 'string') return Buffer.from(v as Buffer);
  if (type === 'list') return [...(v as string[])];
  if (type === 'hash') return new Map(v as Map<string, string>);
  if (type === 'set') return new Set(v as Set<string>);
  if (type === 'zset') {
    const { SortedSet } = require('./sortedset.js') as typeof import('./sortedset.js');
    const z = new SortedSet();
    for (const [m, s] of (v as import('./sortedset.js').SortedSet).entries()) z.set(m, s);
    return z;
  }
  return v;
};

registerCommand('TOUCH', (_e, db, args) => {
  need(args, 1, Infinity, 'TOUCH');
  return args.reduce<number>((a, k) => a + (db.has(toStr(k)) ? 1 : 0), 0);
});

registerCommand('EXPIRE', (_e, db, args) => {
  need(args, 2, 5, 'EXPIRE');
  const key = toStr(args[0]!);
  const seconds = toInt(args[1]!);
  return applyExpire(db, key, seconds * 1000, args.slice(2), false);
});
registerCommand('PEXPIRE', (_e, db, args) => {
  need(args, 2, 5, 'PEXPIRE');
  return applyExpire(db, toStr(args[0]!), toInt(args[1]!), args.slice(2), false);
});
registerCommand('EXPIREAT', (_e, db, args) => {
  need(args, 2, 5, 'EXPIREAT');
  return applyExpire(db, toStr(args[0]!), toInt(args[1]!) * 1000, args.slice(2), true);
});
registerCommand('PEXPIREAT', (_e, db, args) => {
  need(args, 2, 5, 'PEXPIREAT');
  return applyExpire(db, toStr(args[0]!), toInt(args[1]!), args.slice(2), true);
});

function applyExpire(db: Database, key: string, amountMs: number, opts: CommandArg[], isAt: boolean): number {
  const e = db.getEntry(key);
  if (!e) return 0;
  const target = isAt ? amountMs : now() + amountMs;
  let nx = false, xx = false, gt = false, lt = false;
  for (const o of opts) {
    const u = toStr(o).toUpperCase();
    if (u === 'NX') nx = true;
    else if (u === 'XX') xx = true;
    else if (u === 'GT') gt = true;
    else if (u === 'LT') lt = true;
  }
  const cur = e.expiresAt;
  if (nx && cur !== undefined) return 0;
  if (xx && cur === undefined) return 0;
  if (gt && cur !== undefined && target <= cur) return 0;
  if (lt && cur !== undefined && target >= cur) return 0;
  if (gt && cur === undefined) return 0;
  db.setExpireAt(key, target);
  return 1;
}

registerCommand('TTL', (_e, db, args) => {
  need(args, 1, 1, 'TTL');
  const ms = db.ttlMs(toStr(args[0]!));
  return ms < 0 ? ms : Math.floor(ms / 1000);
});
registerCommand('PTTL', (_e, db, args) => {
  need(args, 1, 1, 'PTTL');
  return db.ttlMs(toStr(args[0]!));
});
registerCommand('EXPIRETIME', (_e, db, args) => {
  const ms = db.ttlMs(toStr(args[0]!));
  if (ms < 0) return ms;
  return Math.floor((now() + ms) / 1000);
});
registerCommand('PEXPIRETIME', (_e, db, args) => {
  const ms = db.ttlMs(toStr(args[0]!));
  if (ms < 0) return ms;
  return now() + ms;
});
registerCommand('PERSIST', (_e, db, args) => {
  need(args, 1, 1, 'PERSIST');
  const e = db.getEntry(toStr(args[0]!));
  if (!e || e.expiresAt === undefined) return 0;
  delete e.expiresAt;
  return 1;
});
registerCommand('OBJECT', (_e, db, args) => {
  const sub = toStr(args[0] ?? '').toUpperCase();
  const key = toStr(args[1] ?? '');
  const e = db.getEntry(key);
  if (sub === 'ENCODING') {
    if (!e) return null;
    if (e.type === 'string') return 'raw';
    if (e.type === 'list') return 'listpack';
    if (e.type === 'hash') return 'listpack';
    if (e.type === 'set') return 'listpack';
    if (e.type === 'zset') return 'listpack';
    return 'raw';
  }
  if (sub === 'IDLETIME' || sub === 'FREQ' || sub === 'REFCOUNT') return 0;
  if (sub === 'HELP') return ['OBJECT fake'];
  return null;
});

registerCommand('SCAN', (_e, db, args) => {
  need(args, 1, Infinity, 'SCAN');
  return scanImpl(db.keys(), args);
});

function scanImpl(all: string[], args: CommandArg[]): Reply {
  const cursor = toInt(args[0]!);
  let match = '*';
  let count = 10;
  let typeFilter: string | null = null;
  for (let i = 1; i < args.length; i++) {
    const u = toStr(args[i]!).toUpperCase();
    if (u === 'MATCH') match = toStr(args[++i]!);
    else if (u === 'COUNT') count = toInt(args[++i]!);
    else if (u === 'TYPE') typeFilter = toStr(args[++i]!);
  }
  void count;
  void typeFilter;
  const matched = all.filter((k) => matchGlob(match, k));
  // simple impl: return all in one pass
  void cursor;
  return ['0', matched];
}

// ------------------------ strings ------------------------
registerCommand('SET', (_e, db, args) => {
  need(args, 2, Infinity, 'SET');
  const key = toStr(args[0]!);
  const val = Buffer.isBuffer(args[1]) ? args[1]! : Buffer.from(toStr(args[1]!), 'utf8');
  let ex: number | undefined;
  let nx = false, xx = false, get = false, keepttl = false;
  let i = 2;
  while (i < args.length) {
    const u = toStr(args[i]!).toUpperCase();
    if (u === 'EX') ex = toInt(args[++i]!) * 1000;
    else if (u === 'PX') ex = toInt(args[++i]!);
    else if (u === 'EXAT') ex = toInt(args[++i]!) * 1000 - now();
    else if (u === 'PXAT') ex = toInt(args[++i]!) - now();
    else if (u === 'NX') nx = true;
    else if (u === 'XX') xx = true;
    else if (u === 'GET') get = true;
    else if (u === 'KEEPTTL') keepttl = true;
    i++;
  }
  const existing = db.getEntry(key);
  let old: Reply = null;
  if (get) {
    if (existing && existing.type !== 'string') throw new ReplyError('WRONGTYPE Operation against a key holding the wrong kind of value');
    old = existing ? (existing.value as Buffer).toString('utf8') : null;
  }
  if (nx && existing) return get ? old : null;
  if (xx && !existing) return get ? old : null;
  const entry = { type: 'string' as const, value: val, ...(keepttl && existing?.expiresAt !== undefined ? { expiresAt: existing.expiresAt } : {}) };
  if (ex !== undefined) entry.expiresAt = now() + ex;
  db.setEntry(key, entry);
  return get ? old : 'OK';
});
registerCommand('GET', (_e, db, args) => {
  need(args, 1, 1, 'GET');
  const e = db.getEntry(toStr(args[0]!));
  if (!e) return null;
  if (e.type !== 'string') throw new ReplyError('WRONGTYPE Operation against a key holding the wrong kind of value');
  return (e.value as Buffer).toString('utf8');
});
registerCommand('GETDEL', (_e, db, args) => {
  need(args, 1, 1, 'GETDEL');
  const key = toStr(args[0]!);
  const e = db.getEntry(key);
  if (!e) return null;
  if (e.type !== 'string') throw new ReplyError('WRONGTYPE Operation against a key holding the wrong kind of value');
  db.delete(key);
  return (e.value as Buffer).toString('utf8');
});
registerCommand('GETEX', (_e, db, args) => {
  need(args, 1, Infinity, 'GETEX');
  const key = toStr(args[0]!);
  const e = db.getEntry(key);
  if (!e) return null;
  if (e.type !== 'string') throw new ReplyError('WRONGTYPE Operation against a key holding the wrong kind of value');
  for (let i = 1; i < args.length; i++) {
    const u = toStr(args[i]!).toUpperCase();
    if (u === 'EX') e.expiresAt = now() + toInt(args[++i]!) * 1000;
    else if (u === 'PX') e.expiresAt = now() + toInt(args[++i]!);
    else if (u === 'EXAT') e.expiresAt = toInt(args[++i]!) * 1000;
    else if (u === 'PXAT') e.expiresAt = toInt(args[++i]!);
    else if (u === 'PERSIST') delete e.expiresAt;
  }
  return (e.value as Buffer).toString('utf8');
});
registerCommand('GETSET', (_e, db, args) => {
  need(args, 2, 2, 'GETSET');
  const key = toStr(args[0]!);
  const old = db.getEntry(key);
  if (old && old.type !== 'string') throw new ReplyError('WRONGTYPE Operation against a key holding the wrong kind of value');
  const val = Buffer.from(toStr(args[1]!), 'utf8');
  db.setEntry(key, { type: 'string', value: val });
  return old ? (old.value as Buffer).toString('utf8') : null;
});
registerCommand('SETNX', (_e, db, args) => {
  need(args, 2, 2, 'SETNX');
  const key = toStr(args[0]!);
  if (db.has(key)) return 0;
  db.setEntry(key, { type: 'string', value: Buffer.from(toStr(args[1]!), 'utf8') });
  return 1;
});
registerCommand('SETEX', (_e, db, args) => {
  need(args, 3, 3, 'SETEX');
  db.setEntry(toStr(args[0]!), {
    type: 'string',
    value: Buffer.from(toStr(args[2]!), 'utf8'),
    expiresAt: now() + toInt(args[1]!) * 1000,
  });
  return 'OK';
});
registerCommand('PSETEX', (_e, db, args) => {
  need(args, 3, 3, 'PSETEX');
  db.setEntry(toStr(args[0]!), {
    type: 'string',
    value: Buffer.from(toStr(args[2]!), 'utf8'),
    expiresAt: now() + toInt(args[1]!),
  });
  return 'OK';
});
registerCommand('MSET', (_e, db, args) => {
  if (args.length === 0 || args.length % 2 !== 0) throw new ReplyError("wrong number of arguments for 'mset' command");
  for (let i = 0; i < args.length; i += 2) {
    db.setEntry(toStr(args[i]!), { type: 'string', value: Buffer.from(toStr(args[i + 1]!), 'utf8') });
  }
  return 'OK';
});
registerCommand('MSETNX', (_e, db, args) => {
  if (args.length === 0 || args.length % 2 !== 0) throw new ReplyError("wrong number of arguments for 'msetnx' command");
  for (let i = 0; i < args.length; i += 2) if (db.has(toStr(args[i]!))) return 0;
  for (let i = 0; i < args.length; i += 2) {
    db.setEntry(toStr(args[i]!), { type: 'string', value: Buffer.from(toStr(args[i + 1]!), 'utf8') });
  }
  return 1;
});
registerCommand('MGET', (_e, db, args) => {
  need(args, 1, Infinity, 'MGET');
  return args.map((k) => {
    const e = db.getEntry(toStr(k));
    if (!e || e.type !== 'string') return null;
    return (e.value as Buffer).toString('utf8');
  });
});
registerCommand('APPEND', (_e, db, args) => {
  need(args, 2, 2, 'APPEND');
  const { entry, value } = db.getOrCreateString(toStr(args[0]!));
  const add = Buffer.from(toStr(args[1]!), 'utf8');
  const nv = Buffer.concat([value, add]);
  entry.value = nv;
  return nv.length;
});
registerCommand('STRLEN', (_e, db, args) => {
  need(args, 1, 1, 'STRLEN');
  const e = db.getEntry(toStr(args[0]!));
  if (!e) return 0;
  if (e.type !== 'string') throw new ReplyError('WRONGTYPE');
  return (e.value as Buffer).length;
});
registerCommand('INCR', (_e, db, args) => incrBy(db, toStr(args[0]!), 1));
registerCommand('DECR', (_e, db, args) => incrBy(db, toStr(args[0]!), -1));
registerCommand('INCRBY', (_e, db, args) => incrBy(db, toStr(args[0]!), toInt(args[1]!)));
registerCommand('DECRBY', (_e, db, args) => incrBy(db, toStr(args[0]!), -toInt(args[1]!)));
registerCommand('INCRBYFLOAT', (_e, db, args) => {
  const key = toStr(args[0]!);
  const inc = toFloat(args[1]!);
  const e = db.getEntry(key);
  let cur = 0;
  if (e) {
    if (e.type !== 'string') throw new ReplyError('WRONGTYPE');
    cur = Number((e.value as Buffer).toString('utf8'));
    if (Number.isNaN(cur)) throw new ReplyError('value is not a valid float');
  }
  const nv = cur + inc;
  db.setEntry(key, { type: 'string', value: Buffer.from(formatFloat(nv), 'utf8'), ...(e?.expiresAt !== undefined ? { expiresAt: e.expiresAt } : {}) });
  return formatFloat(nv);
});

function incrBy(db: Database, key: string, by: number): number {
  const e = db.getEntry(key);
  let cur = 0;
  if (e) {
    if (e.type !== 'string') throw new ReplyError('WRONGTYPE');
    const s = (e.value as Buffer).toString('utf8');
    if (!/^-?\d+$/.test(s)) throw new ReplyError('value is not an integer or out of range');
    cur = Number(s);
  }
  const nv = cur + by;
  db.setEntry(key, { type: 'string', value: Buffer.from(String(nv), 'utf8'), ...(e?.expiresAt !== undefined ? { expiresAt: e.expiresAt } : {}) });
  return nv;
}

registerCommand('GETRANGE', (_e, db, args) => {
  need(args, 3, 3, 'GETRANGE');
  const e = db.getEntry(toStr(args[0]!));
  if (!e) return '';
  if (e.type !== 'string') throw new ReplyError('WRONGTYPE');
  const s = (e.value as Buffer).toString('utf8');
  let start = toInt(args[1]!);
  let end = toInt(args[2]!);
  if (start < 0) start = Math.max(0, s.length + start);
  if (end < 0) end = s.length + end;
  return s.slice(start, end + 1);
});
registerCommand('SUBSTR', (e, db, args) => getCommand('GETRANGE')!(e, db, args));
registerCommand('SETRANGE', (_e, db, args) => {
  need(args, 3, 3, 'SETRANGE');
  const key = toStr(args[0]!);
  const offset = toInt(args[1]!);
  const val = Buffer.from(toStr(args[2]!), 'utf8');
  const { entry, value } = db.getOrCreateString(key);
  const minLen = offset + val.length;
  const buf = Buffer.alloc(Math.max(minLen, value.length));
  value.copy(buf);
  val.copy(buf, offset);
  entry.value = buf;
  return buf.length;
});
registerCommand('LCS', () => '');

// ------------------------ bit ops ------------------------
registerCommand('SETBIT', (_e, db, args) => {
  need(args, 3, 3, 'SETBIT');
  const key = toStr(args[0]!);
  const offset = toInt(args[1]!);
  const bit = toInt(args[2]!);
  if (bit !== 0 && bit !== 1) throw new ReplyError('bit is not an integer or out of range');
  const { entry, value } = db.getOrCreateString(key);
  const byteIdx = Math.floor(offset / 8);
  const bitIdx = 7 - (offset % 8);
  const minLen = byteIdx + 1;
  const buf = Buffer.alloc(Math.max(minLen, value.length));
  value.copy(buf);
  const prev = (buf[byteIdx]! >> bitIdx) & 1;
  if (bit) buf[byteIdx] = buf[byteIdx]! | (1 << bitIdx);
  else buf[byteIdx] = buf[byteIdx]! & ~(1 << bitIdx);
  entry.value = buf;
  return prev;
});
registerCommand('GETBIT', (_e, db, args) => {
  need(args, 2, 2, 'GETBIT');
  const e = db.getEntry(toStr(args[0]!));
  if (!e) return 0;
  const buf = e.value as Buffer;
  const offset = toInt(args[1]!);
  const byteIdx = Math.floor(offset / 8);
  if (byteIdx >= buf.length) return 0;
  return (buf[byteIdx]! >> (7 - (offset % 8))) & 1;
});
registerCommand('BITCOUNT', (_e, db, args) => {
  const e = db.getEntry(toStr(args[0]!));
  if (!e) return 0;
  const buf = e.value as Buffer;
  let start = args.length > 1 ? toInt(args[1]!) : 0;
  let end = args.length > 2 ? toInt(args[2]!) : buf.length - 1;
  if (start < 0) start = Math.max(0, buf.length + start);
  if (end < 0) end = buf.length + end;
  end = Math.min(end, buf.length - 1);
  let n = 0;
  for (let i = start; i <= end; i++) {
    let b = buf[i]!;
    while (b) { n += b & 1; b >>>= 1; }
  }
  return n;
});
registerCommand('BITOP', (_e, db, args) => {
  need(args, 3, Infinity, 'BITOP');
  const op = toStr(args[0]!).toUpperCase();
  const dst = toStr(args[1]!);
  const srcs = args.slice(2).map((k) => {
    const e = db.getEntry(toStr(k));
    return e && e.type === 'string' ? (e.value as Buffer) : Buffer.alloc(0);
  });
  const maxLen = Math.max(...srcs.map((b) => b.length), 0);
  const out = Buffer.alloc(maxLen);
  for (let i = 0; i < maxLen; i++) {
    let v = srcs[0]?.[i] ?? 0;
    for (let j = 1; j < srcs.length; j++) {
      const b = srcs[j]?.[i] ?? 0;
      if (op === 'AND') v &= b;
      else if (op === 'OR') v |= b;
      else if (op === 'XOR') v ^= b;
    }
    if (op === 'NOT') v = ~(srcs[0]?.[i] ?? 0) & 0xff;
    out[i] = v;
  }
  if (maxLen === 0) db.delete(dst);
  else db.setEntry(dst, { type: 'string', value: out });
  return maxLen;
});
registerCommand('BITPOS', () => -1);
registerCommand('BITFIELD', () => []);
registerCommand('BITFIELD_RO', () => []);

// ------------------------ lists ------------------------
registerCommand('LPUSH', (_e, db, args) => {
  need(args, 2, Infinity, 'LPUSH');
  const list = db.getOrCreateList(toStr(args[0]!));
  for (let i = 1; i < args.length; i++) list.unshift(toStr(args[i]!));
  return list.length;
});
registerCommand('RPUSH', (_e, db, args) => {
  need(args, 2, Infinity, 'RPUSH');
  const list = db.getOrCreateList(toStr(args[0]!));
  for (let i = 1; i < args.length; i++) list.push(toStr(args[i]!));
  return list.length;
});
registerCommand('LPUSHX', (_e, db, args) => {
  const key = toStr(args[0]!);
  if (!db.has(key)) return 0;
  const list = db.getOrCreateList(key);
  for (let i = 1; i < args.length; i++) list.unshift(toStr(args[i]!));
  return list.length;
});
registerCommand('RPUSHX', (_e, db, args) => {
  const key = toStr(args[0]!);
  if (!db.has(key)) return 0;
  const list = db.getOrCreateList(key);
  for (let i = 1; i < args.length; i++) list.push(toStr(args[i]!));
  return list.length;
});
registerCommand('LPOP', (_e, db, args) => {
  const key = toStr(args[0]!);
  const e = db.getEntry(key);
  if (!e) return args.length > 1 ? null : null;
  if (e.type !== 'list') throw new ReplyError('WRONGTYPE');
  const list = e.value as string[];
  const n = args.length > 1 ? toInt(args[1]!) : 1;
  const out: string[] = [];
  for (let i = 0; i < n && list.length > 0; i++) out.push(list.shift()!);
  if (list.length === 0) db.delete(key);
  if (args.length === 1) return out[0] ?? null;
  return out.length === 0 ? null : out;
});
registerCommand('RPOP', (_e, db, args) => {
  const key = toStr(args[0]!);
  const e = db.getEntry(key);
  if (!e) return null;
  if (e.type !== 'list') throw new ReplyError('WRONGTYPE');
  const list = e.value as string[];
  const n = args.length > 1 ? toInt(args[1]!) : 1;
  const out: string[] = [];
  for (let i = 0; i < n && list.length > 0; i++) out.push(list.pop()!);
  if (list.length === 0) db.delete(key);
  if (args.length === 1) return out[0] ?? null;
  return out.length === 0 ? null : out;
});
registerCommand('LLEN', (_e, db, args) => {
  const e = db.getEntry(toStr(args[0]!));
  if (!e) return 0;
  if (e.type !== 'list') throw new ReplyError('WRONGTYPE');
  return (e.value as string[]).length;
});
registerCommand('LRANGE', (_e, db, args) => {
  need(args, 3, 3, 'LRANGE');
  const e = db.getEntry(toStr(args[0]!));
  if (!e) return [];
  if (e.type !== 'list') throw new ReplyError('WRONGTYPE');
  const list = e.value as string[];
  let start = toInt(args[1]!);
  let stop = toInt(args[2]!);
  if (start < 0) start = Math.max(0, list.length + start);
  if (stop < 0) stop = list.length + stop;
  return list.slice(start, stop + 1);
});
registerCommand('LINDEX', (_e, db, args) => {
  need(args, 2, 2, 'LINDEX');
  const e = db.getEntry(toStr(args[0]!));
  if (!e) return null;
  if (e.type !== 'list') throw new ReplyError('WRONGTYPE');
  const list = e.value as string[];
  let i = toInt(args[1]!);
  if (i < 0) i = list.length + i;
  return list[i] ?? null;
});
registerCommand('LSET', (_e, db, args) => {
  need(args, 3, 3, 'LSET');
  const e = db.getEntry(toStr(args[0]!));
  if (!e) throw new ReplyError('no such key');
  if (e.type !== 'list') throw new ReplyError('WRONGTYPE');
  const list = e.value as string[];
  let i = toInt(args[1]!);
  if (i < 0) i = list.length + i;
  if (i < 0 || i >= list.length) throw new ReplyError('index out of range');
  list[i] = toStr(args[2]!);
  return 'OK';
});
registerCommand('LINSERT', (_e, db, args) => {
  need(args, 4, 4, 'LINSERT');
  const e = db.getEntry(toStr(args[0]!));
  if (!e) return 0;
  if (e.type !== 'list') throw new ReplyError('WRONGTYPE');
  const list = e.value as string[];
  const where = toStr(args[1]!).toUpperCase();
  const pivot = toStr(args[2]!);
  const val = toStr(args[3]!);
  const idx = list.indexOf(pivot);
  if (idx === -1) return -1;
  list.splice(where === 'BEFORE' ? idx : idx + 1, 0, val);
  return list.length;
});
registerCommand('LREM', (_e, db, args) => {
  need(args, 3, 3, 'LREM');
  const key = toStr(args[0]!);
  const e = db.getEntry(key);
  if (!e) return 0;
  if (e.type !== 'list') throw new ReplyError('WRONGTYPE');
  const list = e.value as string[];
  const count = toInt(args[1]!);
  const val = toStr(args[2]!);
  let removed = 0;
  if (count >= 0) {
    const limit = count === 0 ? Infinity : count;
    for (let i = 0; i < list.length && removed < limit; ) {
      if (list[i] === val) { list.splice(i, 1); removed++; } else i++;
    }
  } else {
    const limit = -count;
    for (let i = list.length - 1; i >= 0 && removed < limit; i--) {
      if (list[i] === val) { list.splice(i, 1); removed++; }
    }
  }
  if (list.length === 0) db.delete(key);
  return removed;
});
registerCommand('LTRIM', (_e, db, args) => {
  need(args, 3, 3, 'LTRIM');
  const key = toStr(args[0]!);
  const e = db.getEntry(key);
  if (!e) return 'OK';
  if (e.type !== 'list') throw new ReplyError('WRONGTYPE');
  const list = e.value as string[];
  let start = toInt(args[1]!);
  let stop = toInt(args[2]!);
  if (start < 0) start = Math.max(0, list.length + start);
  if (stop < 0) stop = list.length + stop;
  const n = list.splice(0, list.length, ...list.slice(start, stop + 1));
  void n;
  if (list.length === 0) db.delete(key);
  return 'OK';
});
registerCommand('LPOS', (_e, db, args) => {
  const key = toStr(args[0]!);
  const el = toStr(args[1]!);
  const e = db.getEntry(key);
  if (!e) return null;
  const list = e.value as string[];
  let rank = 1, count: number | null = null;
  for (let i = 2; i < args.length; i++) {
    const u = toStr(args[i]!).toUpperCase();
    if (u === 'RANK') rank = toInt(args[++i]!);
    else if (u === 'COUNT') count = toInt(args[++i]!);
    else if (u === 'MAXLEN') ++i;
  }
  const matches: number[] = [];
  const step = rank < 0 ? -1 : 1;
  const absRank = Math.abs(rank);
  let found = 0;
  const indices = step > 0 ? [...list.keys()] : [...list.keys()].reverse();
  for (const i of indices) {
    if (list[i] === el) {
      found++;
      if (found >= absRank) matches.push(i);
      if (count !== null && count > 0 && matches.length >= count) break;
      if (count === null && matches.length >= 1) break;
    }
  }
  if (count === null) return matches[0] ?? null;
  return matches;
});
registerCommand('RPOPLPUSH', (_e, db, args) => {
  need(args, 2, 2, 'RPOPLPUSH');
  const src = toStr(args[0]!);
  const dst = toStr(args[1]!);
  const e = db.getEntry(src);
  if (!e) return null;
  if (e.type !== 'list') throw new ReplyError('WRONGTYPE');
  const list = e.value as string[];
  const v = list.pop();
  if (v === undefined) return null;
  if (list.length === 0) db.delete(src);
  const dl = db.getOrCreateList(dst);
  dl.unshift(v);
  return v;
});
registerCommand('LMOVE', (_e, db, args) => {
  need(args, 4, 4, 'LMOVE');
  const src = toStr(args[0]!), dst = toStr(args[1]!);
  const from = toStr(args[2]!).toUpperCase();
  const to = toStr(args[3]!).toUpperCase();
  const e = db.getEntry(src);
  if (!e) return null;
  if (e.type !== 'list') throw new ReplyError('WRONGTYPE');
  const list = e.value as string[];
  const v = from === 'LEFT' ? list.shift() : list.pop();
  if (v === undefined) return null;
  if (list.length === 0) db.delete(src);
  const dl = db.getOrCreateList(dst);
  if (to === 'LEFT') dl.unshift(v);
  else dl.push(v);
  return v;
});
// blocking variants resolve immediately if data present, else return null after timeout
const blockingPop = async (db: Database, keys: string[], right: boolean): Promise<Reply> => {
  for (const k of keys) {
    const e = db.getEntry(k);
    if (e && e.type === 'list') {
      const list = e.value as string[];
      const v = right ? list.pop() : list.shift();
      if (v !== undefined) {
        if (list.length === 0) db.delete(k);
        return [k, v];
      }
    }
  }
  return null;
};
registerCommand('BLPOP', async (_e, db, args) => {
  const keys = args.slice(0, -1).map(toStr);
  return blockingPop(db, keys, false);
});
registerCommand('BRPOP', async (_e, db, args) => {
  const keys = args.slice(0, -1).map(toStr);
  return blockingPop(db, keys, true);
});
registerCommand('BRPOPLPUSH', (e, db, args) => getCommand('RPOPLPUSH')!(e, db, args.slice(0, 2)));
registerCommand('BLMOVE', (e, db, args) => getCommand('LMOVE')!(e, db, args.slice(0, 4)));
registerCommand('LMPOP', (_e, db, args) => {
  let i = 0;
  const numKeys = toInt(args[i++]!);
  const keys = args.slice(i, i + numKeys).map(toStr);
  i += numKeys;
  const dir = toStr(args[i++]!).toUpperCase();
  let count = 1;
  if (i < args.length && toStr(args[i]!).toUpperCase() === 'COUNT') count = toInt(args[i + 1]!);
  for (const k of keys) {
    const e = db.getEntry(k);
    if (!e || e.type !== 'list') continue;
    const list = e.value as string[];
    if (list.length === 0) continue;
    const out: string[] = [];
    for (let j = 0; j < count && list.length > 0; j++) out.push(dir === 'LEFT' ? list.shift()! : list.pop()!);
    if (list.length === 0) db.delete(k);
    return [k, out];
  }
  return null;
});
registerCommand('BLMPOP', (e, db, args) => getCommand('LMPOP')!(e, db, args.slice(1)));

// ------------------------ hashes ------------------------
registerCommand('HSET', (_e, db, args) => {
  need(args, 3, Infinity, 'HSET');
  if ((args.length - 1) % 2 !== 0) throw new ReplyError("wrong number of arguments for 'hset' command");
  const h = db.getOrCreateHash(toStr(args[0]!));
  let added = 0;
  for (let i = 1; i < args.length; i += 2) {
    const f = toStr(args[i]!);
    if (!h.has(f)) added++;
    h.set(f, toStr(args[i + 1]!));
  }
  return added;
});
registerCommand('HSETNX', (_e, db, args) => {
  need(args, 3, 3, 'HSETNX');
  const h = db.getOrCreateHash(toStr(args[0]!));
  const f = toStr(args[1]!);
  if (h.has(f)) return 0;
  h.set(f, toStr(args[2]!));
  return 1;
});
registerCommand('HMSET', (e, db, args) => {
  getCommand('HSET')!(e, db, args);
  return 'OK';
});
registerCommand('HGET', (_e, db, args) => {
  need(args, 2, 2, 'HGET');
  const e = db.getEntry(toStr(args[0]!));
  if (!e) return null;
  if (e.type !== 'hash') throw new ReplyError('WRONGTYPE');
  return (e.value as Map<string, string>).get(toStr(args[1]!)) ?? null;
});
registerCommand('HMGET', (_e, db, args) => {
  need(args, 2, Infinity, 'HMGET');
  const e = db.getEntry(toStr(args[0]!));
  const h = e?.type === 'hash' ? (e.value as Map<string, string>) : null;
  return args.slice(1).map((f) => h?.get(toStr(f)) ?? null);
});
registerCommand('HGETALL', (_e, db, args) => {
  const e = db.getEntry(toStr(args[0]!));
  if (!e) return [];
  if (e.type !== 'hash') throw new ReplyError('WRONGTYPE');
  const out: string[] = [];
  for (const [k, v] of e.value as Map<string, string>) { out.push(k, v); }
  return out;
});
registerCommand('HKEYS', (_e, db, args) => {
  const e = db.getEntry(toStr(args[0]!));
  if (!e) return [];
  return [...(e.value as Map<string, string>).keys()];
});
registerCommand('HVALS', (_e, db, args) => {
  const e = db.getEntry(toStr(args[0]!));
  if (!e) return [];
  return [...(e.value as Map<string, string>).values()];
});
registerCommand('HLEN', (_e, db, args) => {
  const e = db.getEntry(toStr(args[0]!));
  return e ? (e.value as Map<string, string>).size : 0;
});
registerCommand('HEXISTS', (_e, db, args) => {
  const e = db.getEntry(toStr(args[0]!));
  return e && (e.value as Map<string, string>).has(toStr(args[1]!)) ? 1 : 0;
});
registerCommand('HDEL', (_e, db, args) => {
  need(args, 2, Infinity, 'HDEL');
  const key = toStr(args[0]!);
  const e = db.getEntry(key);
  if (!e) return 0;
  const h = e.value as Map<string, string>;
  let n = 0;
  for (let i = 1; i < args.length; i++) if (h.delete(toStr(args[i]!))) n++;
  if (h.size === 0) db.delete(key);
  return n;
});
registerCommand('HINCRBY', (_e, db, args) => {
  const h = db.getOrCreateHash(toStr(args[0]!));
  const f = toStr(args[1]!);
  const cur = h.get(f);
  const curN = cur === undefined ? 0 : Number(cur);
  if (!Number.isFinite(curN) || (cur !== undefined && !/^-?\d+$/.test(cur))) throw new ReplyError('hash value is not an integer');
  const nv = curN + toInt(args[2]!);
  h.set(f, String(nv));
  return nv;
});
registerCommand('HINCRBYFLOAT', (_e, db, args) => {
  const h = db.getOrCreateHash(toStr(args[0]!));
  const f = toStr(args[1]!);
  const cur = Number(h.get(f) ?? '0');
  const nv = cur + toFloat(args[2]!);
  h.set(f, formatFloat(nv));
  return formatFloat(nv);
});
registerCommand('HSTRLEN', (_e, db, args) => {
  const e = db.getEntry(toStr(args[0]!));
  if (!e) return 0;
  return ((e.value as Map<string, string>).get(toStr(args[1]!)) ?? '').length;
});
registerCommand('HRANDFIELD', (_e, db, args) => {
  const e = db.getEntry(toStr(args[0]!));
  if (!e) return null;
  const keys = [...(e.value as Map<string, string>).keys()];
  if (args.length === 1) return keys[Math.floor(Math.random() * keys.length)] ?? null;
  const count = toInt(args[1]!);
  const withValues = args.length > 2 && toStr(args[2]!).toUpperCase() === 'WITHVALUES';
  const h = e.value as Map<string, string>;
  const picked = [...keys].sort(() => Math.random() - 0.5).slice(0, Math.abs(count));
  if (!withValues) return picked;
  const out: string[] = [];
  for (const k of picked) { out.push(k, h.get(k) ?? ''); }
  return out;
});
registerCommand('HSCAN', (_e, db, args) => {
  const e = db.getEntry(toStr(args[0]!));
  const h = e?.type === 'hash' ? (e.value as Map<string, string>) : new Map();
  const all: string[] = [];
  let match = '*';
  for (let i = 2; i < args.length; i++) {
    if (toStr(args[i]!).toUpperCase() === 'MATCH') match = toStr(args[++i]!);
  }
  for (const [k, v] of h) { if (matchGlob(match, k)) all.push(k, v); }
  return ['0', all];
});

// ------------------------ sets ------------------------
registerCommand('SADD', (_e, db, args) => {
  need(args, 2, Infinity, 'SADD');
  const s = db.getOrCreateSet(toStr(args[0]!));
  let added = 0;
  for (let i = 1; i < args.length; i++) {
    const v = toStr(args[i]!);
    if (!s.has(v)) { s.add(v); added++; }
  }
  return added;
});
registerCommand('SREM', (_e, db, args) => {
  need(args, 2, Infinity, 'SREM');
  const key = toStr(args[0]!);
  const e = db.getEntry(key);
  if (!e) return 0;
  const s = e.value as Set<string>;
  let n = 0;
  for (let i = 1; i < args.length; i++) if (s.delete(toStr(args[i]!))) n++;
  if (s.size === 0) db.delete(key);
  return n;
});
registerCommand('SMEMBERS', (_e, db, args) => {
  const e = db.getEntry(toStr(args[0]!));
  return e ? [...(e.value as Set<string>)] : [];
});
registerCommand('SCARD', (_e, db, args) => {
  const e = db.getEntry(toStr(args[0]!));
  return e ? (e.value as Set<string>).size : 0;
});
registerCommand('SISMEMBER', (_e, db, args) => {
  const e = db.getEntry(toStr(args[0]!));
  return e && (e.value as Set<string>).has(toStr(args[1]!)) ? 1 : 0;
});
registerCommand('SMISMEMBER', (_e, db, args) => {
  const e = db.getEntry(toStr(args[0]!));
  const s = e ? (e.value as Set<string>) : null;
  return args.slice(1).map((m) => (s && s.has(toStr(m)) ? 1 : 0));
});
registerCommand('SPOP', (_e, db, args) => {
  const key = toStr(args[0]!);
  const e = db.getEntry(key);
  if (!e) return args.length > 1 ? [] : null;
  const s = e.value as Set<string>;
  const count = args.length > 1 ? toInt(args[1]!) : 1;
  const members = [...s];
  const picked: string[] = [];
  for (let i = 0; i < count && members.length > 0; i++) {
    const idx = Math.floor(Math.random() * members.length);
    picked.push(members[idx]!);
    s.delete(members[idx]!);
    members.splice(idx, 1);
  }
  if (s.size === 0) db.delete(key);
  if (args.length === 1) return picked[0] ?? null;
  return picked;
});
registerCommand('SRANDMEMBER', (_e, db, args) => {
  const e = db.getEntry(toStr(args[0]!));
  if (!e) return args.length > 1 ? [] : null;
  const members = [...(e.value as Set<string>)];
  if (args.length === 1) return members[Math.floor(Math.random() * members.length)] ?? null;
  const count = toInt(args[1]!);
  if (count >= 0) {
    return [...members].sort(() => Math.random() - 0.5).slice(0, count);
  }
  const out: string[] = [];
  for (let i = 0; i < -count; i++) out.push(members[Math.floor(Math.random() * members.length)]!);
  return out;
});
registerCommand('SMOVE', (_e, db, args) => {
  const src = toStr(args[0]!), dst = toStr(args[1]!), m = toStr(args[2]!);
  const se = db.getEntry(src);
  if (!se) return 0;
  const ss = se.value as Set<string>;
  if (!ss.has(m)) return 0;
  ss.delete(m);
  if (ss.size === 0) db.delete(src);
  const ds = db.getOrCreateSet(dst);
  ds.add(m);
  return 1;
});
const setOp = (db: Database, keys: string[], op: 'inter' | 'union' | 'diff'): Set<string> => {
  const sets = keys.map((k) => {
    const e = db.getEntry(k);
    return e?.type === 'set' ? (e.value as Set<string>) : new Set<string>();
  });
  if (sets.length === 0) return new Set();
  if (op === 'union') { const o = new Set<string>(); for (const s of sets) for (const v of s) o.add(v); return o; }
  if (op === 'diff') { const o = new Set(sets[0]); for (let i = 1; i < sets.length; i++) for (const v of sets[i]!) o.delete(v); return o; }
  const o = new Set(sets[0]);
  for (let i = 1; i < sets.length; i++) for (const v of [...o]) if (!sets[i]!.has(v)) o.delete(v);
  return o;
};
registerCommand('SINTER', (_e, db, args) => [...setOp(db, args.map(toStr), 'inter')]);
registerCommand('SUNION', (_e, db, args) => [...setOp(db, args.map(toStr), 'union')]);
registerCommand('SDIFF', (_e, db, args) => [...setOp(db, args.map(toStr), 'diff')]);
registerCommand('SINTERSTORE', (_e, db, args) => {
  const dst = toStr(args[0]!);
  const r = setOp(db, args.slice(1).map(toStr), 'inter');
  if (r.size === 0) db.delete(dst); else db.setEntry(dst, { type: 'set', value: r });
  return r.size;
});
registerCommand('SUNIONSTORE', (_e, db, args) => {
  const dst = toStr(args[0]!);
  const r = setOp(db, args.slice(1).map(toStr), 'union');
  if (r.size === 0) db.delete(dst); else db.setEntry(dst, { type: 'set', value: r });
  return r.size;
});
registerCommand('SDIFFSTORE', (_e, db, args) => {
  const dst = toStr(args[0]!);
  const r = setOp(db, args.slice(1).map(toStr), 'diff');
  if (r.size === 0) db.delete(dst); else db.setEntry(dst, { type: 'set', value: r });
  return r.size;
});
registerCommand('SINTERCARD', (_e, db, args) => {
  const n = toInt(args[0]!);
  const keys = args.slice(1, 1 + n).map(toStr);
  return setOp(db, keys, 'inter').size;
});
registerCommand('SSCAN', (_e, db, args) => {
  const e = db.getEntry(toStr(args[0]!));
  const s = e?.type === 'set' ? (e.value as Set<string>) : new Set<string>();
  let match = '*';
  for (let i = 2; i < args.length; i++) {
    if (toStr(args[i]!).toUpperCase() === 'MATCH') match = toStr(args[++i]!);
  }
  return ['0', [...s].filter((v) => matchGlob(match, v))];
});

// ------------------------ sorted sets ------------------------
registerCommand('ZADD', (_e, db, args) => {
  const key = toStr(args[0]!);
  let i = 1;
  let nx = false, xx = false, gt = false, lt = false, ch = false, incr = false;
  while (i < args.length) {
    const u = toStr(args[i]!).toUpperCase();
    if (u === 'NX') nx = true;
    else if (u === 'XX') xx = true;
    else if (u === 'GT') gt = true;
    else if (u === 'LT') lt = true;
    else if (u === 'CH') ch = true;
    else if (u === 'INCR') incr = true;
    else break;
    i++;
  }
  const z = db.getOrCreateZSet(key);
  let added = 0, changed = 0;
  let lastScore: number | null = null;
  for (; i < args.length; i += 2) {
    const score = toFloat(args[i]!);
    const member = toStr(args[i + 1]!);
    const existing = z.score(member);
    if (nx && existing !== undefined) { lastScore = existing; continue; }
    if (xx && existing === undefined) continue;
    let newScore = score;
    if (incr) newScore = (existing ?? 0) + score;
    if (gt && existing !== undefined && newScore <= existing) { lastScore = existing; continue; }
    if (lt && existing !== undefined && newScore >= existing) { lastScore = existing; continue; }
    if (existing === undefined) added++;
    else if (existing !== newScore) changed++;
    z.set(member, newScore);
    lastScore = newScore;
  }
  if (incr) return lastScore === null ? null : formatFloat(lastScore);
  return ch ? added + changed : added;
});
registerCommand('ZREM', (_e, db, args) => {
  const key = toStr(args[0]!);
  const e = db.getEntry(key);
  if (!e) return 0;
  const z = e.value as import('./sortedset.js').SortedSet;
  let n = 0;
  for (let i = 1; i < args.length; i++) if (z.delete(toStr(args[i]!))) n++;
  if (z.size === 0) db.delete(key);
  return n;
});
registerCommand('ZCARD', (_e, db, args) => {
  const e = db.getEntry(toStr(args[0]!));
  return e ? (e.value as import('./sortedset.js').SortedSet).size : 0;
});
registerCommand('ZSCORE', (_e, db, args) => {
  const e = db.getEntry(toStr(args[0]!));
  const s = e ? (e.value as import('./sortedset.js').SortedSet).score(toStr(args[1]!)) : undefined;
  return s === undefined ? null : formatFloat(s);
});
registerCommand('ZMSCORE', (_e, db, args) => {
  const e = db.getEntry(toStr(args[0]!));
  const z = e ? (e.value as import('./sortedset.js').SortedSet) : null;
  return args.slice(1).map((m) => {
    const s = z?.score(toStr(m));
    return s === undefined ? null : formatFloat(s);
  });
});
registerCommand('ZINCRBY', (_e, db, args) => {
  const z = db.getOrCreateZSet(toStr(args[0]!));
  const by = toFloat(args[1]!);
  const m = toStr(args[2]!);
  const nv = (z.score(m) ?? 0) + by;
  z.set(m, nv);
  return formatFloat(nv);
});
registerCommand('ZRANK', (_e, db, args) => {
  const e = db.getEntry(toStr(args[0]!));
  if (!e) return null;
  const z = e.value as import('./sortedset.js').SortedSet;
  const m = toStr(args[1]!);
  const withScore = args.length > 2 && toStr(args[2]!).toUpperCase() === 'WITHSCORE';
  const r = z.rank(m);
  if (r === null) return withScore ? null : null;
  return withScore ? [r, formatFloat(z.score(m)!)] : r;
});
registerCommand('ZREVRANK', (_e, db, args) => {
  const e = db.getEntry(toStr(args[0]!));
  if (!e) return null;
  const z = e.value as import('./sortedset.js').SortedSet;
  const m = toStr(args[1]!);
  const withScore = args.length > 2 && toStr(args[2]!).toUpperCase() === 'WITHSCORE';
  const r = z.rank(m, true);
  if (r === null) return null;
  return withScore ? [r, formatFloat(z.score(m)!)] : r;
});
const parseScore = (s: string): { v: number; exclusive: boolean } => {
  let exclusive = false;
  if (s.startsWith('(')) { exclusive = true; s = s.slice(1); }
  return { v: toFloat(s), exclusive };
};
const parseLex = (s: string): { v: string; exclusive: boolean; unbounded: boolean } => {
  if (s === '-') return { v: '', exclusive: false, unbounded: true };
  if (s === '+') return { v: '', exclusive: false, unbounded: true };
  if (s.startsWith('(')) return { v: s.slice(1), exclusive: true, unbounded: false };
  if (s.startsWith('[')) return { v: s.slice(1), exclusive: false, unbounded: false };
  throw new ReplyError('min or max not valid string range item');
};
const zrangeGeneric = (db: Database, args: CommandArg[], rev: boolean): Reply => {
  const key = toStr(args[0]!);
  const e = db.getEntry(key);
  if (!e) return [];
  const z = e.value as import('./sortedset.js').SortedSet;
  const rawStart = toStr(args[1]!);
  const rawStop = toStr(args[2]!);
  let mode: 'index' | 'score' | 'lex' = 'index';
  let withScores = false;
  let limitOffset = 0;
  let limitCount = -1;
  let reverse = rev;
  for (let i = 3; i < args.length; i++) {
    const u = toStr(args[i]!).toUpperCase();
    if (u === 'BYSCORE') mode = 'score';
    else if (u === 'BYLEX') mode = 'lex';
    else if (u === 'REV') reverse = !reverse;
    else if (u === 'WITHSCORES') withScores = true;
    else if (u === 'LIMIT') { limitOffset = toInt(args[++i]!); limitCount = toInt(args[++i]!); }
  }
  let res: Array<[string, number]>;
  if (mode === 'score') {
    const a = parseScore(reverse ? rawStop : rawStart);
    const b = parseScore(reverse ? rawStart : rawStop);
    res = z.rangeByScore(a.v, b.v, a.exclusive, b.exclusive, reverse);
  } else if (mode === 'lex') {
    const a = parseLex(reverse ? rawStop : rawStart);
    const b = parseLex(reverse ? rawStart : rawStop);
    res = z.rangeByLex(a.v, b.v, a.exclusive, b.exclusive, a.unbounded, b.unbounded, reverse);
  } else {
    res = z.rangeByIndex(toInt(rawStart), toInt(rawStop), reverse);
  }
  if (limitCount >= 0 || limitOffset > 0) {
    res = res.slice(limitOffset, limitCount < 0 ? undefined : limitOffset + limitCount);
  }
  if (withScores) {
    const out: string[] = [];
    for (const [m, s] of res) out.push(m, formatFloat(s));
    return out;
  }
  return res.map(([m]) => m);
};
registerCommand('ZRANGE', (_e, db, args) => zrangeGeneric(db, args, false));
registerCommand('ZREVRANGE', (_e, db, args) => zrangeGeneric(db, args, true));
registerCommand('ZRANGEBYSCORE', (_e, db, args) => zrangeGeneric(db, [args[0]!, args[1]!, args[2]!, 'BYSCORE', ...args.slice(3)], false));
registerCommand('ZREVRANGEBYSCORE', (_e, db, args) => zrangeGeneric(db, [args[0]!, args[1]!, args[2]!, 'BYSCORE', 'REV', ...args.slice(3)], false));
registerCommand('ZRANGEBYLEX', (_e, db, args) => zrangeGeneric(db, [args[0]!, args[1]!, args[2]!, 'BYLEX', ...args.slice(3)], false));
registerCommand('ZREVRANGEBYLEX', (_e, db, args) => zrangeGeneric(db, [args[0]!, args[1]!, args[2]!, 'BYLEX', 'REV', ...args.slice(3)], false));
registerCommand('ZRANGESTORE', (_e, db, args) => {
  const dst = toStr(args[0]!);
  const rest = args.slice(1);
  const result = zrangeGeneric(db, [...rest, 'WITHSCORES'], false) as string[];
  const z = db.getOrCreateZSet(dst);
  for (const k of [...db.keys()]) if (k === dst) db.delete(k);
  const z2 = db.getOrCreateZSet(dst);
  void z;
  for (let i = 0; i < result.length; i += 2) z2.set(result[i]!, Number(result[i + 1]));
  return z2.size;
});
registerCommand('ZCOUNT', (_e, db, args) => {
  const e = db.getEntry(toStr(args[0]!));
  if (!e) return 0;
  const a = parseScore(toStr(args[1]!));
  const b = parseScore(toStr(args[2]!));
  return (e.value as import('./sortedset.js').SortedSet).rangeByScore(a.v, b.v, a.exclusive, b.exclusive).length;
});
registerCommand('ZLEXCOUNT', (_e, db, args) => {
  const e = db.getEntry(toStr(args[0]!));
  if (!e) return 0;
  const a = parseLex(toStr(args[1]!));
  const b = parseLex(toStr(args[2]!));
  return (e.value as import('./sortedset.js').SortedSet).rangeByLex(a.v, b.v, a.exclusive, b.exclusive, a.unbounded, b.unbounded).length;
});
registerCommand('ZREMRANGEBYRANK', (_e, db, args) => {
  const key = toStr(args[0]!);
  const e = db.getEntry(key);
  if (!e) return 0;
  const z = e.value as import('./sortedset.js').SortedSet;
  const range = z.rangeByIndex(toInt(args[1]!), toInt(args[2]!));
  for (const [m] of range) z.delete(m);
  if (z.size === 0) db.delete(key);
  return range.length;
});
registerCommand('ZREMRANGEBYSCORE', (_e, db, args) => {
  const key = toStr(args[0]!);
  const e = db.getEntry(key);
  if (!e) return 0;
  const z = e.value as import('./sortedset.js').SortedSet;
  const a = parseScore(toStr(args[1]!));
  const b = parseScore(toStr(args[2]!));
  const range = z.rangeByScore(a.v, b.v, a.exclusive, b.exclusive);
  for (const [m] of range) z.delete(m);
  if (z.size === 0) db.delete(key);
  return range.length;
});
registerCommand('ZREMRANGEBYLEX', (_e, db, args) => {
  const key = toStr(args[0]!);
  const e = db.getEntry(key);
  if (!e) return 0;
  const z = e.value as import('./sortedset.js').SortedSet;
  const a = parseLex(toStr(args[1]!));
  const b = parseLex(toStr(args[2]!));
  const range = z.rangeByLex(a.v, b.v, a.exclusive, b.exclusive, a.unbounded, b.unbounded);
  for (const [m] of range) z.delete(m);
  if (z.size === 0) db.delete(key);
  return range.length;
});
registerCommand('ZPOPMIN', (_e, db, args) => {
  const key = toStr(args[0]!);
  const count = args.length > 1 ? toInt(args[1]!) : 1;
  const e = db.getEntry(key);
  if (!e) return [];
  const z = e.value as import('./sortedset.js').SortedSet;
  const out: string[] = [];
  for (const [m, s] of z.rangeByIndex(0, count - 1)) { out.push(m, formatFloat(s)); z.delete(m); }
  if (z.size === 0) db.delete(key);
  return out;
});
registerCommand('ZPOPMAX', (_e, db, args) => {
  const key = toStr(args[0]!);
  const count = args.length > 1 ? toInt(args[1]!) : 1;
  const e = db.getEntry(key);
  if (!e) return [];
  const z = e.value as import('./sortedset.js').SortedSet;
  const out: string[] = [];
  for (const [m, s] of z.rangeByIndex(0, count - 1, true)) { out.push(m, formatFloat(s)); z.delete(m); }
  if (z.size === 0) db.delete(key);
  return out;
});
registerCommand('BZPOPMIN', (e, db, args) => getCommand('ZPOPMIN')!(e, db, args.slice(0, -1)));
registerCommand('BZPOPMAX', (e, db, args) => getCommand('ZPOPMAX')!(e, db, args.slice(0, -1)));
registerCommand('ZMPOP', (_e, db, args) => {
  const num = toInt(args[0]!);
  const keys = args.slice(1, 1 + num).map(toStr);
  const dir = toStr(args[1 + num]!).toUpperCase();
  let count = 1;
  for (let i = 2 + num; i < args.length; i++) {
    if (toStr(args[i]!).toUpperCase() === 'COUNT') count = toInt(args[++i]!);
  }
  for (const k of keys) {
    const e = db.getEntry(k);
    if (!e) continue;
    const z = e.value as import('./sortedset.js').SortedSet;
    if (z.size === 0) continue;
    const range = z.rangeByIndex(0, count - 1, dir === 'MAX');
    const out: Reply[] = [];
    for (const [m, s] of range) { out.push([m, formatFloat(s)]); z.delete(m); }
    if (z.size === 0) db.delete(k);
    return [k, out];
  }
  return null;
});
registerCommand('BZMPOP', (e, db, args) => getCommand('ZMPOP')!(e, db, args.slice(1)));
registerCommand('ZRANDMEMBER', (_e, db, args) => {
  const e = db.getEntry(toStr(args[0]!));
  if (!e) return args.length > 1 ? [] : null;
  const z = e.value as import('./sortedset.js').SortedSet;
  const all = z.entries();
  if (args.length === 1) return all[Math.floor(Math.random() * all.length)]?.[0] ?? null;
  const count = toInt(args[1]!);
  const withScores = args.length > 2 && toStr(args[2]!).toUpperCase() === 'WITHSCORES';
  const picked = [...all].sort(() => Math.random() - 0.5).slice(0, Math.abs(count));
  if (!withScores) return picked.map(([m]) => m);
  const out: string[] = [];
  for (const [m, s] of picked) out.push(m, formatFloat(s));
  return out;
});
const zsetCombine = (db: Database, args: CommandArg[], op: 'inter' | 'union' | 'diff', storeDst?: string): Reply => {
  let i = 0;
  const numKeys = toInt(args[i++]!);
  const keys = args.slice(i, i + numKeys).map(toStr);
  i += numKeys;
  let weights = keys.map(() => 1);
  let agg: 'SUM' | 'MIN' | 'MAX' = 'SUM';
  let withScores = false;
  while (i < args.length) {
    const u = toStr(args[i]!).toUpperCase();
    if (u === 'WEIGHTS') { weights = keys.map((_, j) => toFloat(args[i + 1 + j]!)); i += 1 + keys.length; }
    else if (u === 'AGGREGATE') { agg = toStr(args[++i]!).toUpperCase() as 'SUM' | 'MIN' | 'MAX'; i++; }
    else if (u === 'WITHSCORES') { withScores = true; i++; }
    else i++;
  }
  const maps = keys.map((k, j) => {
    const e = db.getEntry(k);
    const w = weights[j]!;
    const m = new Map<string, number>();
    if (e?.type === 'zset') {
      for (const [mem, s] of (e.value as import('./sortedset.js').SortedSet).entries()) m.set(mem, s * w);
    } else if (e?.type === 'set') {
      for (const mem of e.value as Set<string>) m.set(mem, 1 * w);
    }
    return m;
  });
  const result = new Map<string, number>();
  if (op === 'union') {
    for (const m of maps) for (const [mem, s] of m) {
      if (!result.has(mem)) result.set(mem, s);
      else result.set(mem, agg === 'SUM' ? result.get(mem)! + s : agg === 'MIN' ? Math.min(result.get(mem)!, s) : Math.max(result.get(mem)!, s));
    }
  } else if (op === 'inter') {
    if (maps[0]) {
      for (const [mem, s] of maps[0]) {
        let score = s;
        let ok = true;
        for (let j = 1; j < maps.length; j++) {
          if (!maps[j]!.has(mem)) { ok = false; break; }
          const ss = maps[j]!.get(mem)!;
          score = agg === 'SUM' ? score + ss : agg === 'MIN' ? Math.min(score, ss) : Math.max(score, ss);
        }
        if (ok) result.set(mem, score);
      }
    }
  } else {
    if (maps[0]) {
      for (const [mem, s] of maps[0]) {
        let inOther = false;
        for (let j = 1; j < maps.length; j++) if (maps[j]!.has(mem)) { inOther = true; break; }
        if (!inOther) result.set(mem, s);
      }
    }
  }
  const sorted = [...result.entries()].sort((a, b) => a[1] - b[1]);
  if (storeDst) {
    const { SortedSet } = require('./sortedset.js') as typeof import('./sortedset.js');
    const z = new SortedSet();
    for (const [m, s] of sorted) z.set(m, s);
    if (z.size === 0) db.delete(storeDst);
    else db.setEntry(storeDst, { type: 'zset', value: z });
    return z.size;
  }
  if (withScores) {
    const out: string[] = [];
    for (const [m, s] of sorted) out.push(m, formatFloat(s));
    return out;
  }
  return sorted.map(([m]) => m);
};
registerCommand('ZUNION', (_e, db, args) => zsetCombine(db, args, 'union'));
registerCommand('ZINTER', (_e, db, args) => zsetCombine(db, args, 'inter'));
registerCommand('ZDIFF', (_e, db, args) => zsetCombine(db, args, 'diff'));
registerCommand('ZUNIONSTORE', (_e, db, args) => zsetCombine(db, args.slice(1), 'union', toStr(args[0]!)));
registerCommand('ZINTERSTORE', (_e, db, args) => zsetCombine(db, args.slice(1), 'inter', toStr(args[0]!)));
registerCommand('ZDIFFSTORE', (_e, db, args) => zsetCombine(db, args.slice(1), 'diff', toStr(args[0]!)));
registerCommand('ZINTERCARD', (_e, db, args) => {
  const r = zsetCombine(db, args, 'inter') as string[];
  return r.length;
});
registerCommand('ZSCAN', (_e, db, args) => {
  const e = db.getEntry(toStr(args[0]!));
  const z = e?.type === 'zset' ? (e.value as import('./sortedset.js').SortedSet) : null;
  let match = '*';
  for (let i = 2; i < args.length; i++) if (toStr(args[i]!).toUpperCase() === 'MATCH') match = toStr(args[++i]!);
  const out: string[] = [];
  if (z) for (const [m, s] of z.entries()) if (matchGlob(match, m)) out.push(m, formatFloat(s));
  return ['0', out];
});

// ------------------------ pub/sub ------------------------
registerCommand('PUBLISH', (engine, _db, args) => engine.pubsub.publish(toStr(args[0]!), toStr(args[1]!)));
registerCommand('SPUBLISH', (engine, _db, args) => engine.pubsub.publish(toStr(args[0]!), toStr(args[1]!)));
registerCommand('PUBSUB', (engine, _db, args) => {
  const sub = toStr(args[0] ?? '').toUpperCase();
  if (sub === 'CHANNELS') return engine.pubsub.channelList(args.length > 1 ? toStr(args[1]!) : undefined);
  if (sub === 'NUMSUB') {
    const out: Reply[] = [];
    for (const [c, n] of engine.pubsub.numSub(args.slice(1).map(toStr))) { out.push(c, n); }
    return out;
  }
  if (sub === 'NUMPAT') return engine.pubsub.numPat();
  if (sub === 'SHARDCHANNELS') return [];
  if (sub === 'SHARDNUMSUB') return [];
  return [];
});

// ------------------------ transactions stubs (engine handles real MULTI/EXEC) ------------------------
registerCommand('WATCH', () => 'OK');
registerCommand('UNWATCH', () => 'OK');

// ------------------------ scripting stubs ------------------------
registerCommand('EVAL', () => null);
registerCommand('EVALSHA', () => null);
registerCommand('FUNCTION', () => 'OK');
registerCommand('FCALL', () => null);
registerCommand('FCALL_RO', () => null);

// ------------------------ cluster stubs ------------------------
registerCommand('CLUSTER', (_e, _db, args) => {
  const sub = toStr(args[0] ?? '').toUpperCase();
  if (sub === 'INFO') return 'cluster_enabled:0\ncluster_state:ok\n';
  if (sub === 'NODES') return '';
  if (sub === 'MYID') return '0000000000000000000000000000000000000000';
  if (sub === 'SLOTS' || sub === 'SHARDS' || sub === 'LINKS') return [];
  if (sub === 'KEYSLOT') return 0;
  if (sub === 'COUNTKEYSINSLOT') return 0;
  if (sub === 'GETSLOTSINRANGE') return [];
  return 'OK';
});
registerCommand('READONLY', () => 'OK');
registerCommand('READWRITE', () => 'OK');
registerCommand('ASKING', () => 'OK');
registerCommand('MOVE', (_e, db, args) => {
  const key = toStr(args[0]!);
  const e = db.getEntry(key);
  if (!e) return 0;
  db.delete(key);
  return 1;
});

// ------------------------ geo stubs (treat as zset with simple score) ------------------------
registerCommand('GEOADD', (_e, db, args) => {
  const z = db.getOrCreateZSet(toStr(args[0]!));
  let added = 0, i = 1;
  while (i < args.length) {
    const u = toStr(args[i]!).toUpperCase();
    if (u === 'NX' || u === 'XX' || u === 'CH') { i++; continue; }
    const lng = toFloat(args[i]!), lat = toFloat(args[i + 1]!);
    const m = toStr(args[i + 2]!);
    const score = lng * 1000 + lat;
    if (z.score(m) === undefined) added++;
    z.set(m, score);
    i += 3;
  }
  return added;
});
registerCommand('GEODIST', () => null);
registerCommand('GEOHASH', (_e, _db, args) => args.slice(1).map(() => ''));
registerCommand('GEOPOS', (_e, _db, args) => args.slice(1).map(() => null));
registerCommand('GEORADIUS', () => []);
registerCommand('GEORADIUSBYMEMBER', () => []);
registerCommand('GEOSEARCH', () => []);
registerCommand('GEOSEARCHSTORE', () => 0);
registerCommand('GEORADIUS_RO', () => []);
registerCommand('GEORADIUSBYMEMBER_RO', () => []);

// ------------------------ hyperloglog (approximate as set) ------------------------
registerCommand('PFADD', (_e, db, args) => {
  const s = db.getOrCreateSet(toStr(args[0]!));
  let changed = 0;
  for (let i = 1; i < args.length; i++) if (!s.has(toStr(args[i]!))) { s.add(toStr(args[i]!)); changed = 1; }
  return changed;
});
registerCommand('PFCOUNT', (_e, db, args) => {
  const union = new Set<string>();
  for (const k of args) {
    const e = db.getEntry(toStr(k));
    if (e?.type === 'set') for (const v of e.value as Set<string>) union.add(v);
  }
  return union.size;
});
registerCommand('PFMERGE', (_e, db, args) => {
  const dst = db.getOrCreateSet(toStr(args[0]!));
  for (let i = 1; i < args.length; i++) {
    const e = db.getEntry(toStr(args[i]!));
    if (e?.type === 'set') for (const v of e.value as Set<string>) dst.add(v);
  }
  return 'OK';
});

// ------------------------ streams (minimal) ------------------------
interface StreamEntry { id: string; fields: string[] }
interface Stream { entries: StreamEntry[]; lastId: string; groups: Map<string, StreamGroup> }
interface StreamGroup { lastDelivered: string; pending: Map<string, { consumer: string; time: number }>; consumers: Set<string> }

const nextId = (stream: Stream): string => {
  const [ms, seq] = stream.lastId.split('-').map(Number) as [number, number];
  const nowMs = now();
  if (nowMs > ms) return `${nowMs}-0`;
  return `${ms}-${seq + 1}`;
};
const getOrCreateStream = (db: Database, key: string): Stream => {
  const e = db.getEntry(key);
  if (!e) {
    const s: Stream = { entries: [], lastId: '0-0', groups: new Map() };
    db.setEntry(key, { type: 'stream', value: s });
    return s;
  }
  if (e.type !== 'stream') throw new ReplyError('WRONGTYPE');
  return e.value as Stream;
};
registerCommand('XADD', (_e, db, args) => {
  const key = toStr(args[0]!);
  let i = 1;
  while (toStr(args[i]!).toUpperCase() === 'NOMKSTREAM' || toStr(args[i]!).toUpperCase() === 'MAXLEN' || toStr(args[i]!).toUpperCase() === 'MINID' || toStr(args[i]!) === '~' || toStr(args[i]!) === '=' || toStr(args[i]!) === 'LIMIT') { i++; }
  const idArg = toStr(args[i++]!);
  const s = getOrCreateStream(db, key);
  const id = idArg === '*' ? nextId(s) : idArg;
  s.entries.push({ id, fields: args.slice(i).map(toStr) });
  s.lastId = id;
  return id;
});
registerCommand('XLEN', (_e, db, args) => {
  const e = db.getEntry(toStr(args[0]!));
  return e ? (e.value as Stream).entries.length : 0;
});
registerCommand('XRANGE', (_e, db, args) => {
  const e = db.getEntry(toStr(args[0]!));
  if (!e) return [];
  return (e.value as Stream).entries.map((en) => [en.id, en.fields]);
});
registerCommand('XREVRANGE', (_e, db, args) => {
  const e = db.getEntry(toStr(args[0]!));
  if (!e) return [];
  return [...(e.value as Stream).entries].reverse().map((en) => [en.id, en.fields]);
});
registerCommand('XREAD', () => null);
registerCommand('XREADGROUP', () => null);
registerCommand('XGROUP', () => 'OK');
registerCommand('XACK', () => 0);
registerCommand('XPENDING', () => []);
registerCommand('XCLAIM', () => []);
registerCommand('XAUTOCLAIM', () => ['0-0', [], []]);
registerCommand('XDEL', (_e, db, args) => {
  const e = db.getEntry(toStr(args[0]!));
  if (!e) return 0;
  const s = e.value as Stream;
  const ids = new Set(args.slice(1).map(toStr));
  const before = s.entries.length;
  s.entries = s.entries.filter((en) => !ids.has(en.id));
  return before - s.entries.length;
});
registerCommand('XTRIM', () => 0);
registerCommand('XINFO', () => []);
registerCommand('XSETID', () => 'OK');

// ------------------------ acl stubs ------------------------
registerCommand('ACL', () => 'OK');

// ------------------------ slowlog / latency stubs ------------------------
registerCommand('SLOWLOG', () => []);
registerCommand('LATENCY', () => []);

// ------------------------ replication stubs ------------------------
registerCommand('REPLICAOF', () => 'OK');
registerCommand('SLAVEOF', () => 'OK');

// ------------------------ sort ------------------------
registerCommand('SORT', (_e, db, args) => {
  const e = db.getEntry(toStr(args[0]!));
  if (!e) return [];
  let vals: string[] = [];
  if (e.type === 'list') vals = [...(e.value as string[])];
  else if (e.type === 'set') vals = [...(e.value as Set<string>)];
  else if (e.type === 'zset') vals = (e.value as import('./sortedset.js').SortedSet).entries().map(([m]) => m);
  let alpha = false, desc = false;
  for (let i = 1; i < args.length; i++) {
    const u = toStr(args[i]!).toUpperCase();
    if (u === 'ALPHA') alpha = true;
    else if (u === 'DESC') desc = true;
    else if (u === 'ASC') desc = false;
  }
  vals.sort((a, b) => alpha ? a.localeCompare(b) : Number(a) - Number(b));
  if (desc) vals.reverse();
  return vals;
});
registerCommand('SORT_RO', (e, db, args) => getCommand('SORT')!(e, db, args));

// ------------------------ monitor / subscribe are handled at connection level ------------------------
registerCommand('MONITOR', () => 'OK');
registerCommand('SUBSCRIBE', () => 'OK');
registerCommand('UNSUBSCRIBE', () => 'OK');
registerCommand('PSUBSCRIBE', () => 'OK');
registerCommand('PUNSUBSCRIBE', () => 'OK');
registerCommand('SSUBSCRIBE', () => 'OK');
registerCommand('SUNSUBSCRIBE', () => 'OK');

// ------------------------ misc ------------------------
registerCommand('DUMP', () => null);
registerCommand('RESTORE', () => 'OK');
registerCommand('MIGRATE', () => 'OK');
registerCommand('FAILOVER', () => 'OK');
registerCommand('LOLWUT', () => 'Fake Redis says hi');
registerCommand('QUIT', () => 'OK');
registerCommand('PSYNC', () => 'OK');
registerCommand('SYNC', () => 'OK');
registerCommand('RESP3', () => 'OK');
