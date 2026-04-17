import type { CommandArg } from './types.js';

export const toStr = (v: CommandArg): string =>
  Buffer.isBuffer(v) ? v.toString('utf8') : typeof v === 'number' ? String(v) : v;

export const toBuf = (v: CommandArg): Buffer =>
  Buffer.isBuffer(v) ? v : Buffer.from(String(v), 'utf8');

export const toInt = (v: CommandArg): number => {
  const s = toStr(v);
  if (!/^-?\d+$/.test(s)) throw new Error('value is not an integer or out of range');
  const n = Number(s);
  if (!Number.isSafeInteger(n)) throw new Error('value is not an integer or out of range');
  return n;
};

export const toFloat = (v: CommandArg): number => {
  const s = toStr(v).toLowerCase();
  if (s === 'inf' || s === '+inf') return Number.POSITIVE_INFINITY;
  if (s === '-inf') return Number.NEGATIVE_INFINITY;
  const n = Number(s);
  if (Number.isNaN(n)) throw new Error('value is not a valid float');
  return n;
};

export const now = (): number => Date.now();

export const matchGlob = (pattern: string, s: string): boolean => {
  let re = '^';
  let i = 0;
  while (i < pattern.length) {
    const c = pattern[i]!;
    if (c === '*') re += '.*';
    else if (c === '?') re += '.';
    else if (c === '[') {
      re += '[';
      i++;
      if (pattern[i] === '^') {
        re += '^';
        i++;
      }
      while (i < pattern.length && pattern[i] !== ']') {
        re += pattern[i]!.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
        i++;
      }
      re += ']';
    } else if (c === '\\' && i + 1 < pattern.length) {
      re += pattern[i + 1]!.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      i++;
    } else {
      re += c.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    }
    i++;
  }
  re += '$';
  return new RegExp(re).test(s);
};

export const formatFloat = (n: number): string => {
  if (n === Number.POSITIVE_INFINITY) return 'inf';
  if (n === Number.NEGATIVE_INFINITY) return '-inf';
  if (Number.isInteger(n)) return String(n);
  return String(n);
};
