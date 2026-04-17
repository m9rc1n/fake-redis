import type { AttributeValue, ExpressionInputs, Item } from './types.js';
import { compare, equal, sizeOf, typeOf } from './compare.js';
import { ValidationException } from './errors.js';

// ---------- Tokenizer ----------

type TokKind =
  | 'IDENT' | 'NAME_REF' | 'VALUE_REF' | 'NUMBER'
  | 'OP' | 'LP' | 'RP' | 'COMMA' | 'DOT' | 'LB' | 'RB'
  | 'AND' | 'OR' | 'NOT' | 'BETWEEN' | 'IN'
  | 'SET' | 'REMOVE' | 'ADD' | 'DELETE'
  | 'PLUS' | 'MINUS' | 'EQ'
  | 'EOF';

interface Tok { kind: TokKind; value: string; }

const KEYWORDS: Record<string, TokKind> = {
  AND: 'AND', OR: 'OR', NOT: 'NOT', BETWEEN: 'BETWEEN', IN: 'IN',
  SET: 'SET', REMOVE: 'REMOVE', ADD: 'ADD', DELETE: 'DELETE',
};

export const tokenize = (src: string): Tok[] => {
  const out: Tok[] = [];
  let i = 0;
  while (i < src.length) {
    const c = src[i]!;
    if (/\s/.test(c)) { i++; continue; }
    if (c === '(') { out.push({ kind: 'LP', value: c }); i++; continue; }
    if (c === ')') { out.push({ kind: 'RP', value: c }); i++; continue; }
    if (c === '[') { out.push({ kind: 'LB', value: c }); i++; continue; }
    if (c === ']') { out.push({ kind: 'RB', value: c }); i++; continue; }
    if (c === ',') { out.push({ kind: 'COMMA', value: c }); i++; continue; }
    if (c === '.') { out.push({ kind: 'DOT', value: c }); i++; continue; }
    if (c === '+') { out.push({ kind: 'PLUS', value: c }); i++; continue; }
    if (c === '-') { out.push({ kind: 'MINUS', value: c }); i++; continue; }
    if (c === '=') { out.push({ kind: 'EQ', value: '=' }); i++; continue; }
    if (c === '<' || c === '>') {
      if (src[i + 1] === '=') { out.push({ kind: 'OP', value: c + '=' }); i += 2; continue; }
      if (c === '<' && src[i + 1] === '>') { out.push({ kind: 'OP', value: '<>' }); i += 2; continue; }
      out.push({ kind: 'OP', value: c }); i++; continue;
    }
    if (c === '#') {
      let j = i + 1;
      while (j < src.length && /[A-Za-z0-9_]/.test(src[j]!)) j++;
      out.push({ kind: 'NAME_REF', value: src.slice(i, j) });
      i = j; continue;
    }
    if (c === ':') {
      let j = i + 1;
      while (j < src.length && /[A-Za-z0-9_]/.test(src[j]!)) j++;
      out.push({ kind: 'VALUE_REF', value: src.slice(i, j) });
      i = j; continue;
    }
    if (/[0-9]/.test(c)) {
      let j = i;
      while (j < src.length && /[0-9]/.test(src[j]!)) j++;
      out.push({ kind: 'NUMBER', value: src.slice(i, j) });
      i = j; continue;
    }
    if (/[A-Za-z_]/.test(c)) {
      let j = i;
      while (j < src.length && /[A-Za-z0-9_]/.test(src[j]!)) j++;
      const word = src.slice(i, j);
      const up = word.toUpperCase();
      if (up in KEYWORDS) out.push({ kind: KEYWORDS[up]!, value: up });
      else out.push({ kind: 'IDENT', value: word });
      i = j; continue;
    }
    throw ValidationException(`Unexpected character '${c}' at position ${i}`);
  }
  out.push({ kind: 'EOF', value: '' });
  return out;
};

// ---------- Path (a.b[0].c or #n.#m[3]) ----------

export type PathSeg = { kind: 'field'; name: string } | { kind: 'index'; index: number };
export type Path = PathSeg[];

export const resolveName = (ident: string, names?: Record<string, string>): string => {
  if (ident.startsWith('#')) {
    if (!names || !(ident in names)) throw ValidationException(`Missing ExpressionAttributeName ${ident}`);
    return names[ident]!;
  }
  return ident;
};

export const resolveValue = (ref: string, values?: Record<string, AttributeValue>): AttributeValue => {
  if (!values || !(ref in values)) throw ValidationException(`Missing ExpressionAttributeValue ${ref}`);
  return values[ref]!;
};

export const getByPath = (item: Item, path: Path): AttributeValue | undefined => {
  let cur: any = item;
  for (const seg of path) {
    if (cur === undefined || cur === null) return undefined;
    if (seg.kind === 'field') {
      if (cur && typeof cur === 'object' && 'M' in cur) cur = cur.M[seg.name];
      else if (cur && typeof cur === 'object' && !('S' in cur) && !('N' in cur)) cur = (cur as any)[seg.name];
      else return undefined;
    } else {
      if (cur && typeof cur === 'object' && 'L' in cur) cur = cur.L[seg.index];
      else return undefined;
    }
  }
  return cur as AttributeValue | undefined;
};

export const setByPath = (item: Item, path: Path, value: AttributeValue): void => {
  if (path.length === 0) throw ValidationException('empty path');
  const head = path[0]!;
  if (head.kind !== 'field') throw ValidationException('top-level path must be a field');
  if (path.length === 1) { item[head.name] = value; return; }
  const first = item[head.name];
  const container: AttributeValue = first ?? (path[1]!.kind === 'index' ? { L: [] } : { M: {} });
  item[head.name] = container;
  let cur: AttributeValue = container;
  for (let i = 1; i < path.length - 1; i++) {
    const seg = path[i]!;
    if (seg.kind === 'field') {
      if (!('M' in cur)) throw ValidationException(`path segment ${seg.name} not a map`);
      const next = cur.M[seg.name];
      const needList = path[i + 1]!.kind === 'index';
      cur.M[seg.name] = next ?? (needList ? { L: [] } : { M: {} });
      cur = cur.M[seg.name]!;
    } else {
      if (!('L' in cur)) throw ValidationException(`path segment [${seg.index}] not a list`);
      const next = cur.L[seg.index];
      const needList = path[i + 1]!.kind === 'index';
      cur.L[seg.index] = next ?? (needList ? { L: [] } : { M: {} });
      cur = cur.L[seg.index]!;
    }
  }
  const last = path[path.length - 1]!;
  if (last.kind === 'field') {
    if (!('M' in cur)) throw ValidationException('parent not a map');
    cur.M[last.name] = value;
  } else {
    if (!('L' in cur)) throw ValidationException('parent not a list');
    cur.L[last.index] = value;
  }
};

export const removeByPath = (item: Item, path: Path): void => {
  if (path.length === 1) {
    const head = path[0]!;
    if (head.kind === 'field') delete item[head.name];
    return;
  }
  const parentPath = path.slice(0, -1);
  const parent = getByPath(item, parentPath);
  if (!parent) return;
  const last = path[path.length - 1]!;
  if (last.kind === 'field' && 'M' in parent) delete parent.M[last.name];
  else if (last.kind === 'index' && 'L' in parent) parent.L.splice(last.index, 1);
};

// ---------- AST for condition expressions ----------

type Node =
  | { t: 'and'; a: Node; b: Node }
  | { t: 'or'; a: Node; b: Node }
  | { t: 'not'; a: Node }
  | { t: 'cmp'; op: '=' | '<>' | '<' | '<=' | '>' | '>='; a: Operand; b: Operand }
  | { t: 'between'; x: Operand; lo: Operand; hi: Operand }
  | { t: 'in'; x: Operand; list: Operand[] }
  | { t: 'fn'; name: string; args: Operand[] };

type Operand =
  | { k: 'path'; p: Path }
  | { k: 'value'; ref: string }
  | { k: 'size'; of: Operand };

class Parser {
  private i = 0;
  constructor(private toks: Tok[]) {}
  peek(off = 0) { return this.toks[this.i + off]!; }
  eat(kind?: TokKind): Tok {
    const t = this.toks[this.i]!;
    if (kind && t.kind !== kind) throw ValidationException(`Expected ${kind}, got ${t.kind} '${t.value}'`);
    this.i++; return t;
  }
  atEnd() { return this.peek().kind === 'EOF'; }

  parseCondition(): Node {
    const n = this.parseOr();
    if (!this.atEnd()) throw ValidationException(`Unexpected token '${this.peek().value}'`);
    return n;
  }
  private parseOr(): Node {
    let left = this.parseAnd();
    while (this.peek().kind === 'OR') { this.eat('OR'); const right = this.parseAnd(); left = { t: 'or', a: left, b: right }; }
    return left;
  }
  private parseAnd(): Node {
    let left = this.parseNot();
    while (this.peek().kind === 'AND') { this.eat('AND'); const right = this.parseNot(); left = { t: 'and', a: left, b: right }; }
    return left;
  }
  private parseNot(): Node {
    if (this.peek().kind === 'NOT') { this.eat('NOT'); return { t: 'not', a: this.parseNot() }; }
    return this.parsePrimary();
  }
  private parsePrimary(): Node {
    if (this.peek().kind === 'LP') {
      this.eat('LP'); const n = this.parseOr(); this.eat('RP'); return n;
    }
    // Function call or path-based comparison
    if (this.peek().kind === 'IDENT' && this.peek(1).kind === 'LP') {
      const fn = this.eat('IDENT').value;
      this.eat('LP');
      const args: Operand[] = [];
      if (this.peek().kind !== 'RP') {
        args.push(this.parseOperand());
        while (this.peek().kind === 'COMMA') { this.eat('COMMA'); args.push(this.parseOperand()); }
      }
      this.eat('RP');
      return { t: 'fn', name: fn, args };
    }
    const left = this.parseOperand();
    const tk = this.peek();
    if (tk.kind === 'EQ') {
      this.eat('EQ'); const right = this.parseOperand();
      return { t: 'cmp', op: '=', a: left, b: right };
    }
    if (tk.kind === 'OP') {
      this.eat('OP'); const right = this.parseOperand();
      return { t: 'cmp', op: tk.value as any, a: left, b: right };
    }
    if (tk.kind === 'BETWEEN') {
      this.eat('BETWEEN'); const lo = this.parseOperand(); this.eat('AND'); const hi = this.parseOperand();
      return { t: 'between', x: left, lo, hi };
    }
    if (tk.kind === 'IN') {
      this.eat('IN'); this.eat('LP');
      const list: Operand[] = [this.parseOperand()];
      while (this.peek().kind === 'COMMA') { this.eat('COMMA'); list.push(this.parseOperand()); }
      this.eat('RP');
      return { t: 'in', x: left, list };
    }
    throw ValidationException(`Expected comparison, got '${tk.value}'`);
  }

  parseOperand(): Operand {
    const tk = this.peek();
    if (tk.kind === 'IDENT' && tk.value.toLowerCase() === 'size' && this.peek(1).kind === 'LP') {
      this.eat('IDENT'); this.eat('LP'); const of = this.parseOperand(); this.eat('RP');
      return { k: 'size', of };
    }
    if (tk.kind === 'VALUE_REF') { this.eat('VALUE_REF'); return { k: 'value', ref: tk.value }; }
    return { k: 'path', p: this.parsePath() };
  }

  parsePath(): Path {
    const segs: Path = [];
    const first = this.peek();
    if (first.kind !== 'IDENT' && first.kind !== 'NAME_REF')
      throw ValidationException(`Expected attribute name, got '${first.value}'`);
    this.i++;
    segs.push({ kind: 'field', name: first.value });
    while (true) {
      if (this.peek().kind === 'DOT') {
        this.eat('DOT');
        const n = this.peek();
        if (n.kind !== 'IDENT' && n.kind !== 'NAME_REF') throw ValidationException('Expected field after .');
        this.i++;
        segs.push({ kind: 'field', name: n.value });
      } else if (this.peek().kind === 'LB') {
        this.eat('LB');
        const n = this.eat('NUMBER');
        this.eat('RB');
        segs.push({ kind: 'index', index: Number(n.value) });
      } else break;
    }
    // Resolve #name refs lazily at eval; keep raw names here.
    return segs;
  }
}

// ---------- Evaluator ----------

const evalOperand = (o: Operand, item: Item, inputs: ExpressionInputs): AttributeValue | undefined => {
  if (o.k === 'value') return resolveValue(o.ref, inputs.ExpressionAttributeValues);
  if (o.k === 'size') {
    const inner = evalOperand(o.of, item, inputs);
    if (!inner) return undefined;
    return { N: String(sizeOf(inner)) };
  }
  const resolved: Path = o.p.map((s) =>
    s.kind === 'field' ? { kind: 'field', name: resolveName(s.name, inputs.ExpressionAttributeNames) } : s,
  );
  return getByPath(item, resolved);
};

export const resolvePath = (p: Path, names?: Record<string, string>): Path =>
  p.map((s) => (s.kind === 'field' ? { kind: 'field', name: resolveName(s.name, names) } : s));

const evalNode = (n: Node, item: Item, inputs: ExpressionInputs): boolean => {
  switch (n.t) {
    case 'and': return evalNode(n.a, item, inputs) && evalNode(n.b, item, inputs);
    case 'or':  return evalNode(n.a, item, inputs) || evalNode(n.b, item, inputs);
    case 'not': return !evalNode(n.a, item, inputs);
    case 'cmp': {
      const a = evalOperand(n.a, item, inputs);
      const b = evalOperand(n.b, item, inputs);
      if (a === undefined || b === undefined) return n.op === '<>';
      if (n.op === '=') return equal(a, b);
      if (n.op === '<>') return !equal(a, b);
      const c = compare(a, b);
      if (n.op === '<') return c < 0;
      if (n.op === '<=') return c <= 0;
      if (n.op === '>') return c > 0;
      return c >= 0;
    }
    case 'between': {
      const x = evalOperand(n.x, item, inputs);
      const lo = evalOperand(n.lo, item, inputs);
      const hi = evalOperand(n.hi, item, inputs);
      if (!x || !lo || !hi) return false;
      return compare(x, lo) >= 0 && compare(x, hi) <= 0;
    }
    case 'in': {
      const x = evalOperand(n.x, item, inputs);
      if (!x) return false;
      return n.list.some((o) => { const v = evalOperand(o, item, inputs); return v ? equal(x, v) : false; });
    }
    case 'fn': {
      const name = n.name.toLowerCase();
      if (name === 'attribute_exists') {
        const v = evalOperand(n.args[0]!, item, inputs); return v !== undefined;
      }
      if (name === 'attribute_not_exists') {
        const v = evalOperand(n.args[0]!, item, inputs); return v === undefined;
      }
      if (name === 'attribute_type') {
        const v = evalOperand(n.args[0]!, item, inputs);
        const t = evalOperand(n.args[1]!, item, inputs);
        if (!v || !t || !('S' in t)) return false;
        return typeOf(v) === (t as any).S;
      }
      if (name === 'begins_with') {
        const v = evalOperand(n.args[0]!, item, inputs);
        const pfx = evalOperand(n.args[1]!, item, inputs);
        if (!v || !pfx) return false;
        if ('S' in v && 'S' in pfx) return v.S.startsWith(pfx.S);
        if ('B' in v && 'B' in pfx) {
          if (pfx.B.length > v.B.length) return false;
          for (let i = 0; i < pfx.B.length; i++) if (v.B[i] !== pfx.B[i]) return false;
          return true;
        }
        return false;
      }
      if (name === 'contains') {
        const v = evalOperand(n.args[0]!, item, inputs);
        const needle = evalOperand(n.args[1]!, item, inputs);
        if (!v || !needle) return false;
        if ('S' in v && 'S' in needle) return v.S.includes(needle.S);
        if ('L' in v) return v.L.some((x) => equal(x, needle));
        if ('SS' in v && 'S' in needle) return v.SS.includes(needle.S);
        if ('NS' in v && 'N' in needle) return v.NS.includes(needle.N);
        return false;
      }
      throw ValidationException(`Unknown function '${n.name}'`);
    }
  }
};

export const evalCondition = (expr: string, item: Item, inputs: ExpressionInputs): boolean => {
  const p = new Parser(tokenize(expr));
  const n = p.parseCondition();
  return evalNode(n, item, inputs);
};

// ---------- Key condition: restricted to pk = :v [AND sk <op> :v | between | begins_with] ----------

export interface KeyConditionResult {
  hashValue: AttributeValue;
  sortPred?: (sortValue: AttributeValue) => boolean;
  sortBounds?: { lo?: AttributeValue; hi?: AttributeValue }; // for ordered range scan planning
}

export const evalKeyCondition = (
  expr: string,
  hashAttr: string,
  sortAttr: string | undefined,
  inputs: ExpressionInputs,
): KeyConditionResult => {
  const p = new Parser(tokenize(expr));
  const n = p.parseCondition();
  const parts: Node[] = [];
  const flatten = (node: Node) => {
    if (node.t === 'and') { flatten(node.a); flatten(node.b); } else parts.push(node);
  };
  flatten(n);
  let hashValue: AttributeValue | undefined;
  let sortPred: ((v: AttributeValue) => boolean) | undefined;
  let sortBounds: { lo?: AttributeValue; hi?: AttributeValue } | undefined;
  const inputs2 = inputs;
  const pathName = (o: Operand): string | undefined => {
    if (o.k !== 'path' || o.p.length !== 1 || o.p[0]!.kind !== 'field') return undefined;
    return resolveName(o.p[0]!.name, inputs.ExpressionAttributeNames);
  };
  for (const part of parts) {
    if (part.t === 'cmp') {
      const name = pathName(part.a);
      if (!name) throw ValidationException('KeyConditionExpression must reference key attribute');
      const val = evalOperand(part.b, {}, inputs2);
      if (!val) throw ValidationException('KeyConditionExpression value missing');
      if (name === hashAttr) {
        if (part.op !== '=') throw ValidationException('Hash key must use equality');
        hashValue = val;
      } else if (sortAttr && name === sortAttr) {
        const op = part.op;
        sortPred = (v) => {
          if (op === '=') return equal(v, val);
          if (op === '<>') return !equal(v, val);
          const c = compare(v, val);
          if (op === '<') return c < 0;
          if (op === '<=') return c <= 0;
          if (op === '>') return c > 0;
          return c >= 0;
        };
        sortBounds = op === '<' || op === '<=' ? { hi: val } : op === '>' || op === '>=' ? { lo: val } : op === '=' ? { lo: val, hi: val } : undefined;
      } else throw ValidationException(`KeyConditionExpression references non-key attribute ${name}`);
    } else if (part.t === 'between') {
      const name = pathName(part.x);
      if (!name || name !== sortAttr) throw ValidationException('BETWEEN only valid on sort key');
      const lo = evalOperand(part.lo, {}, inputs2)!;
      const hi = evalOperand(part.hi, {}, inputs2)!;
      sortPred = (v) => compare(v, lo) >= 0 && compare(v, hi) <= 0;
      sortBounds = { lo, hi };
    } else if (part.t === 'fn' && part.name.toLowerCase() === 'begins_with') {
      const name = pathName(part.args[0]!);
      if (!name || name !== sortAttr) throw ValidationException('begins_with only valid on sort key in KeyCondition');
      const pfx = evalOperand(part.args[1]!, {}, inputs2);
      if (!pfx || !('S' in pfx)) throw ValidationException('begins_with requires string');
      sortPred = (v) => 'S' in v && v.S.startsWith(pfx.S);
    } else throw ValidationException('Unsupported KeyCondition clause');
  }
  if (!hashValue) throw ValidationException('Hash key equality missing from KeyConditionExpression');
  const result: KeyConditionResult = { hashValue };
  if (sortPred) result.sortPred = sortPred;
  if (sortBounds) result.sortBounds = sortBounds;
  return result;
};

// ---------- Update expression ----------

export type UpdateAction =
  | { kind: 'set'; path: Path; expr: UpdateValueExpr }
  | { kind: 'remove'; path: Path }
  | { kind: 'add'; path: Path; value: Operand }
  | { kind: 'delete'; path: Path; value: Operand };

type UpdateValueExpr =
  | { t: 'operand'; o: Operand }
  | { t: 'plus'; a: UpdateValueExpr; b: UpdateValueExpr }
  | { t: 'minus'; a: UpdateValueExpr; b: UpdateValueExpr }
  | { t: 'fn'; name: string; args: UpdateValueExpr[] };

class UpdateParser extends Parser {
  parseUpdate(): UpdateAction[] {
    const actions: UpdateAction[] = [];
    while (!this.atEnd()) {
      const tk = this.peek();
      if (tk.kind === 'SET') { this.eat('SET'); this.parseSetList(actions); }
      else if (tk.kind === 'REMOVE') { this.eat('REMOVE'); this.parseRemoveList(actions); }
      else if (tk.kind === 'ADD') { this.eat('ADD'); this.parseAddDeleteList(actions, 'add'); }
      else if (tk.kind === 'DELETE') { this.eat('DELETE'); this.parseAddDeleteList(actions, 'delete'); }
      else throw ValidationException(`Unexpected token '${tk.value}' in UpdateExpression`);
    }
    return actions;
  }
  private parseSetList(actions: UpdateAction[]) {
    actions.push(this.parseSetAction());
    while (this.peek().kind === 'COMMA') { this.eat('COMMA'); actions.push(this.parseSetAction()); }
  }
  private parseSetAction(): UpdateAction {
    const path = this.parsePath();
    if (this.peek().kind !== 'EQ') throw ValidationException(`Expected '=' in SET`);
    this.eat('EQ');
    const expr = this.parseValueExpr();
    return { kind: 'set', path, expr };
  }
  private parseValueExpr(): UpdateValueExpr {
    let left = this.parseValueTerm();
    while (this.peek().kind === 'PLUS' || this.peek().kind === 'MINUS') {
      const op = this.eat();
      const right = this.parseValueTerm();
      left = { t: op.kind === 'PLUS' ? 'plus' : 'minus', a: left, b: right };
    }
    return left;
  }
  private parseValueTerm(): UpdateValueExpr {
    if (this.peek().kind === 'IDENT' && this.peek(1).kind === 'LP') {
      const fn = this.eat('IDENT').value;
      this.eat('LP');
      const args: UpdateValueExpr[] = [];
      if (this.peek().kind !== 'RP') {
        args.push(this.parseValueExpr());
        while (this.peek().kind === 'COMMA') { this.eat('COMMA'); args.push(this.parseValueExpr()); }
      }
      this.eat('RP');
      return { t: 'fn', name: fn, args };
    }
    return { t: 'operand', o: this.parseOperand() };
  }
  private parseRemoveList(actions: UpdateAction[]) {
    actions.push({ kind: 'remove', path: this.parsePath() });
    while (this.peek().kind === 'COMMA') { this.eat('COMMA'); actions.push({ kind: 'remove', path: this.parsePath() }); }
  }
  private parseAddDeleteList(actions: UpdateAction[], kind: 'add' | 'delete') {
    const parseOne = () => {
      const path = this.parsePath();
      const value = this.parseOperand();
      actions.push({ kind, path, value } as UpdateAction);
    };
    parseOne();
    while (this.peek().kind === 'COMMA') { this.eat('COMMA'); parseOne(); }
  }
}

const evalValueExpr = (e: UpdateValueExpr, item: Item, inputs: ExpressionInputs): AttributeValue => {
  if (e.t === 'operand') {
    const v = evalOperand(e.o, item, inputs);
    if (v === undefined) throw ValidationException('Operand resolved to undefined');
    return v;
  }
  if (e.t === 'plus' || e.t === 'minus') {
    const a = evalValueExpr(e.a, item, inputs);
    const b = evalValueExpr(e.b, item, inputs);
    if (!('N' in a) || !('N' in b)) throw ValidationException('Arithmetic requires N operands');
    const r = e.t === 'plus' ? Number(a.N) + Number(b.N) : Number(a.N) - Number(b.N);
    return { N: String(r) };
  }
  if (e.t === 'fn') {
    const name = e.name.toLowerCase();
    if (name === 'if_not_exists') {
      const op = e.args[0]!;
      if (op.t !== 'operand' || op.o.k !== 'path') throw ValidationException('if_not_exists first arg must be a path');
      const current = evalOperand(op.o, item, inputs);
      if (current !== undefined) return current;
      return evalValueExpr(e.args[1]!, item, inputs);
    }
    if (name === 'list_append') {
      const a = evalValueExpr(e.args[0]!, item, inputs);
      const b = evalValueExpr(e.args[1]!, item, inputs);
      if (!('L' in a) || !('L' in b)) throw ValidationException('list_append requires lists');
      return { L: [...a.L, ...b.L] };
    }
    throw ValidationException(`Unknown update function '${e.name}'`);
  }
  throw ValidationException('Invalid update expression');
};

export const applyUpdate = (
  expr: string,
  item: Item,
  inputs: ExpressionInputs,
): Item => {
  const p = new UpdateParser(tokenize(expr));
  const actions = p.parseUpdate();
  for (const a of actions) {
    const path = resolvePath(a.path, inputs.ExpressionAttributeNames);
    if (a.kind === 'set') {
      const v = evalValueExpr(a.expr, item, inputs);
      setByPath(item, path, v);
    } else if (a.kind === 'remove') {
      removeByPath(item, path);
    } else if (a.kind === 'add') {
      const incoming = evalOperand(a.value, item, inputs)!;
      const current = getByPath(item, path);
      if (current === undefined) { setByPath(item, path, incoming); continue; }
      if ('N' in current && 'N' in incoming) {
        setByPath(item, path, { N: String(Number(current.N) + Number(incoming.N)) });
      } else if ('SS' in current && 'SS' in incoming) {
        const set = new Set([...current.SS, ...incoming.SS]);
        setByPath(item, path, { SS: [...set] });
      } else if ('NS' in current && 'NS' in incoming) {
        const set = new Set([...current.NS, ...incoming.NS]);
        setByPath(item, path, { NS: [...set] });
      } else throw ValidationException('ADD requires compatible types (N/SS/NS)');
    } else if (a.kind === 'delete') {
      const incoming = evalOperand(a.value, item, inputs)!;
      const current = getByPath(item, path);
      if (current === undefined) continue;
      if ('SS' in current && 'SS' in incoming) {
        const rem = new Set(incoming.SS);
        const next = current.SS.filter((x) => !rem.has(x));
        if (next.length === 0) removeByPath(item, path);
        else setByPath(item, path, { SS: next });
      } else if ('NS' in current && 'NS' in incoming) {
        const rem = new Set(incoming.NS);
        const next = current.NS.filter((x) => !rem.has(x));
        if (next.length === 0) removeByPath(item, path);
        else setByPath(item, path, { NS: next });
      } else throw ValidationException('DELETE requires matching SS/NS types');
    }
  }
  return item;
};

// ---------- Projection ----------

export const project = (item: Item, expr: string | undefined, names?: Record<string, string>): Item => {
  if (!expr) return item;
  const paths = expr.split(',').map((s) => s.trim()).filter(Boolean);
  const out: Item = {};
  for (const pathStr of paths) {
    const p = new Parser(tokenize(pathStr));
    const path = resolvePath(p.parsePath(), names);
    const v = getByPath(item, path);
    if (v !== undefined) setByPath(out, path, v);
  }
  return out;
};
