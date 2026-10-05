/**
 * Writes send their values as parameters (protocol version 4): the program
 * text holds `$pN` references only, and the engine binds the values without
 * parsing them.
 */

import { describe, it, expect } from 'vitest';
import {
  relation,
  any,
  NOT,
  CompileError,
  KnowledgeGraph,
  Timestamp,
  compileInsert,
  compileValue,
  compileLiteral,
  withParams,
  type Connection,
} from '../src/index';

const Row = relation('Row', { id: 'int', name: 'string', score: 'float', ok: 'bool', at: 'timestamp' });
const Vec = relation('Vec', { id: 'int', v: 'vector' });
const Attempt = relation('Attempt', { order: 'string', tool: 'string', attempt: 'string' });

/** A knowledge graph whose connection records each program and its params. */
function recording(rows: unknown[][] = []) {
  const sent: { iql: string; params: unknown }[] = [];
  const conn = {
    execute: async (iql: string, opts?: { params?: unknown }) => {
      sent.push({ iql, params: opts?.params });
      return { columns: [], rows, errors: [] };
    },
  };
  return { sent, kg: new KnowledgeGraph('kg', conn as unknown as Connection) };
}

describe('withParams', () => {
  it('types each value exactly as its literal would be', () => {
    const { result, params } = withParams(() =>
      [
        compileValue(true),
        compileValue('a"b'),
        compileValue(42, 'int'),
        compileValue(0.1, 'float'),
        compileValue(1e21, 'float'),
        compileValue(9007199254740993n, 'int'),
        compileValue(new Timestamp(1_700_000_000_123), 'timestamp'),
        compileValue(new Date(5), 'timestamp'),
        compileValue([0.5, 2]),
      ].join(' '),
    );
    expect(result).toBe('$p0 $p1 $p2 $p3 $p4 $p5 $p6 $p7 $p8');
    expect(params).toEqual({
      p0: true,
      p1: 'a"b',
      p2: 42,
      p3: 0.1,
      p4: { float: 1e21 },
      p5: { int: '9007199254740993' },
      p6: 1_700_000_000_123,
      p7: 5,
      p8: [0.5, 2],
    });
    // The literals they replace, for comparison.
    expect(compileLiteral(1e21, 'float')).toBe('1e+21');
    expect(compileLiteral(0.1, 'float')).toBe('0.1');
  });

  it('names each distinct value once, so equal values compile to equal text', () => {
    const { result, params } = withParams(() =>
      [compileValue('x'), compileValue(1), compileValue('x'), compileValue(1, 'int'), compileValue('1')].join(' '),
    );
    expect(result).toBe('$p0 $p1 $p0 $p1 $p2');
    expect(params).toEqual({ p0: 'x', p1: 1, p2: '1' });
  });

  it('still refuses what has no exact value, sending nothing', () => {
    for (const bad of [NaN, Infinity, -Infinity]) {
      expect(() => withParams(() => compileValue(bad, 'float'))).toThrow(CompileError);
    }
    expect(() => withParams(() => compileValue(2 ** 53 + 2, 'int'))).toThrow(/BigInt/);
    expect(() => withParams(() => compileValue(2n ** 63n, 'int'))).toThrow(CompileError);
    expect(() => withParams(() => compileValue([1, NaN]))).toThrow(CompileError);
    expect(() => withParams(() => compileValue(null))).toThrow(/no null/);
  });

  it('writes literals outside its scope, and restores the outer scope', () => {
    expect(compileValue('x')).toBe('"x"');
    const outer = withParams(() => {
      const inner = withParams(() => compileValue('a'));
      return `${compileValue('b')} ${inner.result}`;
    });
    expect(outer.result).toBe('$p0 $p0');
    expect(outer.params).toEqual({ p0: 'b' });
    expect(compileValue('x')).toBe('"x"');
  });

  it('leaves the literal compilers literal', () => {
    expect(compileInsert(Vec, { id: 1, v: [0.5] })).toBe('+vec(1, [0.5])');
  });
});

describe('writes send values as params', () => {
  const hostile = 'x"), +evil(1) <- a(1)\n?b(X)';

  it('insert, bulk insert and delete', async () => {
    const { sent, kg } = recording();
    const row = { id: 1, name: hostile, score: 2, ok: false, at: new Timestamp(9) };
    await kg.insert(Row, row);
    await kg.insert(Row, [row, { ...row, id: 3 }]);
    await kg.delete(Row, row);
    expect(sent.map((s) => s.iql)).toEqual([
      '+row($p0, $p1, $p2, $p3, $p4)',
      '+row[($p0, $p1, $p2, $p3, $p4), ($p5, $p1, $p2, $p3, $p4)]',
      // An equal value is one parameter: score 2 and a second id 2 would share $p2.
      '-row($p0, $p1, $p2, $p3, $p4)',
    ]);
    expect(sent[0].params).toEqual({ p0: 1, p1: hostile, p2: 2, p3: false, p4: 9 });
    expect(sent[1].params).toEqual({ p0: 1, p1: hostile, p2: 2, p3: false, p4: 9, p5: 3 });
    for (const { iql } of sent) expect(iql).not.toContain('evil');
  });

  it('conditional delete', async () => {
    const { sent, kg } = recording();
    await kg.delete(Row, Row.col('name').eq(hostile));
    expect(sent[0].iql).not.toContain('evil');
    expect(Object.values(sent[0].params as object)).toEqual([hostile]);
  });

  it('a guarded program: values, the token and the guard constants', async () => {
    const { sent, kg } = recording();
    await kg
      .program()
      .insert(Attempt, { order: hostile, tool: 't', attempt: 'a1' })
      .when(any(Attempt, { order: 'ORD-1' }), NOT(any(Attempt, { order: 'ORD-1', tool: 't' })))
      .commit({ strict: false })
      // The recording engine answers no statement; only what was sent matters.
      .catch(() => undefined);
    const { iql, params } = sent.at(-1)!;
    // Only the SDK's own fixed anchors stay literal (`il_txn("")`).
    expect(iql).not.toMatch(/evil|ORD-1|"t"|a1|t-/);
    expect(Object.values(params as object)).toEqual(expect.arrayContaining([hostile, 'ORD-1', 't', 'a1']));
    expect(Object.values(params as object).some((v) => typeof v === 'string' && v.startsWith('t-'))).toBe(true);
  });

  it('claim', async () => {
    const { sent, kg } = recording();
    await kg.claim(Attempt, { order: hostile, tool: 't', attempt: 'a1' }, { key: ['order', 'tool'] });
    expect(sent[0].iql).not.toMatch(/evil|"t"|a1/);
    expect(Object.values(sent[0].params as object)).toEqual(expect.arrayContaining([hostile, 't', 'a1']));
  });

  it('the result of a program names what was sent', async () => {
    const { kg } = recording([["Inserted 1 fact(s) into 'attempt'."]]);
    const result = await kg.program().insert(Attempt, { order: 'o', tool: 't', attempt: 'a' }).commit();
    expect(result.iql).toBe('+attempt($p0, $p1, $p2)');
    expect(result.params).toEqual({ p0: 'o', p1: 't', p2: 'a' });
  });

  it('queries keep literals: they also feed .subscribe and .why, which take no params', async () => {
    const { sent, kg } = recording();
    await kg.query({ select: [Attempt], where: Attempt.col('order').eq('o') });
    expect(sent[0].iql).toContain('"o"');
    expect(sent[0].params).toBeUndefined();
  });
});
