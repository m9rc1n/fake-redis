export type RedisValue = string | number | Buffer;

export type CommandArg = string | Buffer | number;

export type Reply =
  | null
  | string
  | number
  | Buffer
  | Error
  | Reply[]
  | { [k: string]: Reply };

export type RedisType = 'string' | 'list' | 'hash' | 'set' | 'zset' | 'stream' | 'none';

export interface StoredEntry {
  type: RedisType;
  value: unknown;
  expiresAt?: number; // epoch ms
}

export class ReplyError extends Error {
  constructor(message: string, public code = 'ERR') {
    super(message);
    this.name = 'ReplyError';
  }
  override toString() {
    return `${this.code} ${this.message}`;
  }
}

export const OK = 'OK';
