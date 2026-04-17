import net from 'node:net';
import { AddressInfo } from 'node:net';
import { Engine, ReplyError, type CommandArg, type Reply } from '@fake-redis/core';
import { encode, Parser } from './resp.js';

export interface ServerOptions {
  engine?: Engine;
  port?: number;
  host?: string;
}

export interface RunningServer {
  readonly port: number;
  readonly host: string;
  readonly engine: Engine;
  readonly url: string;
  close(): Promise<void>;
}

export const startServer = async (opts: ServerOptions = {}): Promise<RunningServer> => {
  const engine = opts.engine ?? new Engine();
  const sockets = new Set<net.Socket>();

  const server = net.createServer((socket) => {
    sockets.add(socket);
    const conn = engine.createConnection();

    conn.on('message', (m: any) => {
      if (m.kind === 'message') {
        socket.write(encode(['message', m.channel, m.message]));
      } else {
        socket.write(encode(['pmessage', m.pattern, m.channel, m.message]));
      }
    });

    const parser = new Parser({
      onMessage: async (args) => {
        if (args.length === 0) return;
        const [cmd, ...rest] = args as [string | Buffer, ...(string | Buffer)[]];
        const cmdStr = Buffer.isBuffer(cmd) ? cmd.toString('utf8') : cmd;
        try {
          const reply = await conn.call(cmdStr, ...(rest as CommandArg[]));
          socket.write(encode(reply as Reply));
        } catch (e) {
          const err = e instanceof ReplyError ? e : new ReplyError((e as Error).message);
          socket.write(encode(err as any));
        }
      },
      onError: () => undefined,
    });

    socket.on('data', (c) => parser.push(c));
    socket.on('close', () => { conn.close(); sockets.delete(socket); });
    socket.on('error', () => { conn.close(); sockets.delete(socket); });
  });

  await new Promise<void>((resolve) => server.listen(opts.port ?? 0, opts.host ?? '127.0.0.1', resolve));
  const addr = server.address() as AddressInfo;

  return {
    port: addr.port,
    host: addr.address,
    engine,
    url: `redis://${addr.address}:${addr.port}`,
    async close(): Promise<void> {
      for (const s of sockets) s.destroy();
      await new Promise<void>((resolve) => server.close(() => resolve()));
    },
  };
};

export { Engine };
