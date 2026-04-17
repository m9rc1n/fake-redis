import { EventEmitter } from 'node:events';
import { matchGlob } from './util.js';

export type PubSubMessage =
  | { kind: 'message'; channel: string; message: string }
  | { kind: 'pmessage'; pattern: string; channel: string; message: string };

export class PubSubHub {
  private channels = new Map<string, Set<EventEmitter>>();
  private patterns = new Map<string, Set<EventEmitter>>();

  subscribe(subscriber: EventEmitter, channel: string): number {
    let set = this.channels.get(channel);
    if (!set) {
      set = new Set();
      this.channels.set(channel, set);
    }
    set.add(subscriber);
    return set.size;
  }

  unsubscribe(subscriber: EventEmitter, channel: string): void {
    const set = this.channels.get(channel);
    if (!set) return;
    set.delete(subscriber);
    if (set.size === 0) this.channels.delete(channel);
  }

  psubscribe(subscriber: EventEmitter, pattern: string): number {
    let set = this.patterns.get(pattern);
    if (!set) {
      set = new Set();
      this.patterns.set(pattern, set);
    }
    set.add(subscriber);
    return set.size;
  }

  punsubscribe(subscriber: EventEmitter, pattern: string): void {
    const set = this.patterns.get(pattern);
    if (!set) return;
    set.delete(subscriber);
    if (set.size === 0) this.patterns.delete(pattern);
  }

  publish(channel: string, message: string): number {
    let count = 0;
    const direct = this.channels.get(channel);
    if (direct) {
      for (const s of direct) {
        s.emit('pubsub', { kind: 'message', channel, message } satisfies PubSubMessage);
        count++;
      }
    }
    for (const [pattern, subs] of this.patterns) {
      if (matchGlob(pattern, channel)) {
        for (const s of subs) {
          s.emit('pubsub', { kind: 'pmessage', pattern, channel, message } satisfies PubSubMessage);
          count++;
        }
      }
    }
    return count;
  }

  channelList(pattern?: string): string[] {
    const all = [...this.channels.keys()];
    return pattern ? all.filter((c) => matchGlob(pattern, c)) : all;
  }

  numSub(channels: string[]): Array<[string, number]> {
    return channels.map((c) => [c, this.channels.get(c)?.size ?? 0]);
  }

  numPat(): number {
    return this.patterns.size;
  }
}
