// Minimal RESP2/3 encoder + RESP2 decoder (sufficient for ioredis & node-redis).
import type { Reply } from '@fake-redis/core';

const CRLF = '\r\n';

export const encode = (value: Reply | undefined): Buffer => {
  if (value === null || value === undefined) return Buffer.from(`$-1${CRLF}`);
  if (value instanceof Error) {
    const msg = (value as any).code ? `${(value as any).code} ${value.message}` : `ERR ${value.message}`;
    return Buffer.from(`-${msg}${CRLF}`);
  }
  if (typeof value === 'number') return Buffer.from(`:${value}${CRLF}`);
  if (typeof value === 'string') {
    if (value === 'OK' || value === 'PONG' || value === 'QUEUED') return Buffer.from(`+${value}${CRLF}`);
    const b = Buffer.from(value, 'utf8');
    return Buffer.concat([Buffer.from(`$${b.length}${CRLF}`), b, Buffer.from(CRLF)]);
  }
  if (Buffer.isBuffer(value)) {
    return Buffer.concat([Buffer.from(`$${value.length}${CRLF}`), value, Buffer.from(CRLF)]);
  }
  if (Array.isArray(value)) {
    const parts = value.map(encode);
    return Buffer.concat([Buffer.from(`*${value.length}${CRLF}`), ...parts]);
  }
  // map type — encode as flat array
  const entries = Object.entries(value as Record<string, Reply>);
  const flat: Reply[] = [];
  for (const [k, v] of entries) { flat.push(k); flat.push(v); }
  return encode(flat);
};

export class Parser {
  private buf: Buffer = Buffer.alloc(0);
  private readonly onMessage: (args: (string | Buffer)[]) => void;
  private readonly onError: (err: Error) => void;

  constructor(opts: { onMessage: (args: (string | Buffer)[]) => void; onError: (err: Error) => void }) {
    this.onMessage = opts.onMessage;
    this.onError = opts.onError;
  }

  push(chunk: Buffer): void {
    this.buf = this.buf.length === 0 ? chunk : Buffer.concat([this.buf, chunk]);
    while (true) {
      const res = this.parseValue(0);
      if (!res) return;
      const [val, off] = res;
      this.buf = this.buf.slice(off);
      if (Array.isArray(val)) this.onMessage(val as (string | Buffer)[]);
      else if (typeof val === 'string') this.onMessage([val]);
    }
  }

  private parseValue(off: number): [unknown, number] | null {
    if (off >= this.buf.length) return null;
    const marker = this.buf[off]!;
    const eol = this.buf.indexOf('\r\n', off);
    if (eol === -1) return null;
    const line = this.buf.slice(off + 1, eol).toString('utf8');
    if (marker === 0x2b /* + */) return [line, eol + 2];
    if (marker === 0x2d /* - */) { this.onError(new Error(line)); return [line, eol + 2]; }
    if (marker === 0x3a /* : */) return [Number(line), eol + 2];
    if (marker === 0x24 /* $ */) {
      const len = Number(line);
      if (len === -1) return [null, eol + 2];
      const start = eol + 2;
      const end = start + len;
      if (this.buf.length < end + 2) return null;
      return [this.buf.slice(start, end), end + 2];
    }
    if (marker === 0x2a /* * */) {
      const len = Number(line);
      if (len === -1) return [null, eol + 2];
      const arr: unknown[] = [];
      let cur = eol + 2;
      for (let i = 0; i < len; i++) {
        const next = this.parseValue(cur);
        if (!next) return null;
        arr.push(next[0]);
        cur = next[1];
      }
      return [arr, cur];
    }
    // inline command (space-separated)
    const parts = this.buf.slice(off, eol).toString('utf8').split(' ').filter(Boolean);
    return [parts, eol + 2];
  }
}
