import { describe, it, expect } from 'vitest';
import { Engine } from '../src/index.js';

describe('pub/sub', () => {
  it('SUBSCRIBE and PUBLISH deliver messages', async () => {
    const eng = new Engine();
    const sub = eng.createConnection();
    const pub = eng.createConnection();
    const messages: any[] = [];
    sub.on('message', (m) => messages.push(m));
    await sub.call('SUBSCRIBE', 'ch');
    expect(await pub.call('PUBLISH', 'ch', 'hello')).toBe(1);
    expect(messages[0]).toMatchObject({ kind: 'message', channel: 'ch', message: 'hello' });
  });

  it('PSUBSCRIBE matches patterns', async () => {
    const eng = new Engine();
    const sub = eng.createConnection();
    const pub = eng.createConnection();
    const messages: any[] = [];
    sub.on('message', (m) => messages.push(m));
    await sub.call('PSUBSCRIBE', 'news.*');
    await pub.call('PUBLISH', 'news.tech', 'hi');
    expect(messages[0]).toMatchObject({ kind: 'pmessage', pattern: 'news.*', channel: 'news.tech' });
  });

  it('PUBSUB CHANNELS / NUMSUB', async () => {
    const eng = new Engine();
    const sub = eng.createConnection();
    const q = eng.createConnection();
    await sub.call('SUBSCRIBE', 'a', 'b');
    const channels = (await q.call('PUBSUB', 'CHANNELS')) as string[];
    expect(channels.sort()).toEqual(['a', 'b']);
    expect(await q.call('PUBSUB', 'NUMSUB', 'a')).toEqual(['a', 1]);
  });
});
