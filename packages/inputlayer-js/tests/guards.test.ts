/**
 * Compiled text of any()/NOT(any()), programs with .when() and claim().
 * The live tests in guards.integration.test.ts run the same forms against an
 * engine.
 */

import { describe, it, expect } from 'vitest';
import {
  relation,
  any,
  from,
  AND,
  NOT,
  CompileError,
  Program,
  KnowledgeGraph,
  type Connection,
  compileRule,
  compileConditionalDelete,
} from '../src/index';
import { compileQueryPlan } from '../src/compiler';
import { compileClaim, parseWriteMessage } from '../src/program';

const Shipment = relation('Shipment', { order: 'string', shipment: 'string' });
const Eta = relation('Eta', { shipment: 'string', due: 'string' });
const Promised = relation('Promised', { order: 'string', due: 'string' });
const ToolPolicy = relation('ToolPolicy', { tool: 'string', mode: 'string' });
const KillSwitch = relation('KillSwitch', { tool: 'string' });
const Attempt = relation('Attempt', { order: 'string', tool: 'string', attempt: 'string' });
const AttemptDone = relation('AttemptDone', { attempt: 'string', status: 'string' });
const CheckNeeded = relation('CheckNeeded', { order: 'string', shipment: 'string' });
const Cursor = relation('UtteranceCursor', { session: 'string', last: 'int' });
const PackVersion = relation('PackVersion', { name: 'string', version: 'string' });
const Doc = relation('Doc', { id: 'int', emb: 'vector[2]', ok: 'bool', score: 'float' });

describe('any() in rules', () => {
  it('compiles the hero rule: NOT(any()) bound to a joined column, after the positive atoms', () => {
    const clause = from(Shipment, Eta, Promised, ToolPolicy)
      .where((s, e, p, t) =>
        AND(
          e.col('shipment').eq(s.col('shipment')),
          p.col('order').eq(s.col('order')),
          e.col('due').gt(p.col('due')),
          t.col('tool').eq('carrier_check'),
          t.col('mode').eq('auto'),
          NOT(any(KillSwitch, { tool: t.col('tool') })),
        ),
      )
      .select({ order: Shipment.col('order'), shipment: Shipment.col('shipment') });
    expect(compileRule('check_needed', ['order', 'shipment'], clause)).toBe(
      '+check_needed(Order, Shipment) <- shipment(Order, Shipment), eta(Shipment, Due), promised(Order, Due_1), ' +
        'tool_policy(Tool, Mode), !kill_switch(Tool), Due > Due_1, Tool = "carrier_check", Mode = "auto"',
    );
  });

  it('shares a variable with an equality already pinning the constant', () => {
    const clause = from(ToolPolicy)
      .where((t) => AND(t.col('tool').eq('carrier_check'), NOT(any(KillSwitch, { tool: 'carrier_check' }))))
      .select({ tool: ToolPolicy.col('tool') });
    expect(compileRule('allowed', ['tool'], clause)).toBe(
      '+allowed(Tool) <- tool_policy(Tool, _), !kill_switch(Tool), Tool = "carrier_check"',
    );
  });

  it('refuses a constant-only negation it cannot rewrite soundly in a rule', () => {
    const clause = from(ToolPolicy)
      .where(NOT(any(KillSwitch, { tool: 'carrier_check' })))
      .select({ tool: ToolPolicy.col('tool') });
    expect(() => compileRule('allowed', ['tool'], clause)).toThrow(CompileError);
  });

  it('refuses a negation bound to a relation the body does not join', () => {
    const clause = from(ToolPolicy)
      .where(NOT(any(KillSwitch, { tool: Attempt.col('tool') })))
      .select({ tool: ToolPolicy.col('tool') });
    expect(() => compileRule('allowed', ['tool'], clause)).toThrow(/no positive atom/);
  });

  it('compiles a positive any() as an existence atom with _ for unbound columns', () => {
    const clause = from(Shipment)
      .where((s) => any(Eta, { shipment: s.col('shipment') }))
      .select({ order: Shipment.col('order') });
    expect(compileRule('has_eta', ['order'], clause)).toBe(
      '+has_eta(Order) <- shipment(Order, Shipment), eta(Shipment, _)',
    );
  });

  it('refuses an unknown column', () => {
    expect(() => any(KillSwitch, { nope: 'x' } as never)).toThrow(CompileError);
  });

  it('works as RelationDef.any()', () => {
    const iql = compileConditionalDelete(Attempt, NOT(AttemptDone.any({ attempt: Attempt.col('attempt') })));
    expect(iql).toBe('-attempt(X0, X1, X2) <- attempt(X0, X1, X2), !attempt_done(X2, _)');
  });
});

describe('any() in queries', () => {
  it('binds a constant-only negation through a program-local session fact', () => {
    const plan = compileQueryPlan({
      select: [ToolPolicy],
      where: NOT(any(KillSwitch, { tool: 'carrier_check' })),
    });
    expect(plan.programs).toEqual([
      'il_const_s("carrier_check")\n?tool_policy(Tool, Mode), il_const_s(K), !kill_switch(K), K = "carrier_check"',
    ]);
    expect(plan.outputs.map((o) => o.variable)).toEqual(['Tool', 'Mode']);
  });

  it('adds a positive any() atom without new result columns', () => {
    const plan = compileQueryPlan({
      select: [Shipment],
      where: AND(any(Eta, { shipment: Shipment.col('shipment') }), NOT(any(Attempt, { order: Shipment.col('order') }))),
    });
    expect(plan.programs).toEqual(['?shipment(Order, Shipment), eta(Shipment, _), !attempt(Order, _, _)']);
  });
});

describe('debug() and why()', () => {
  it('refuse a query whose NOT(any()) binds only constants, without contacting the engine', async () => {
    const sent: string[] = [];
    const conn = { execute: async (iql: string) => { sent.push(iql); throw new Error('unreachable'); } };
    const kg = new KnowledgeGraph('kg', conn as unknown as Connection);
    const opts = { select: [ToolPolicy], where: NOT(any(KillSwitch, { tool: 'carrier_check' })) };
    await expect(kg.debug(opts)).rejects.toBeInstanceOf(CompileError);
    await expect(kg.why(opts)).rejects.toBeInstanceOf(CompileError);
    expect(sent).toEqual([]);
  });
});

describe('Program', () => {
  const T = 't-1';

  it('commits plain statements as one program', () => {
    const p = new Program()
      .retract(Eta, { shipment: 'S-77' })
      .insert(Eta, { shipment: 'S-77', due: '2026-10-10' })
      .retract(Attempt, { order: 'O', tool: 'x', attempt: 'a' });
    expect(p.compile(true, T)).toEqual({
      iql:
        '-eta("S-77", Due) <- eta("S-77", Due)\n' +
        '+eta("S-77", "2026-10-10")\n' +
        '-attempt("O", "x", "a")',
      writeIndexes: [0, 1, 2],
    });
  });

  it('compiles .when(strict: false) to the token form', () => {
    const p = new Program()
      .retract(Cursor, { session: 's-42' })
      .insert(Cursor, { session: 's-42', last: 7 })
      .when(any(Cursor, { session: 's-42' }), Cursor.col('last').lt(7));
    const c = p.compile(false, T);
    expect(c.iql.split('\n')).toEqual([
      '-il_txn(""), +il_txn("t-1") <- utterance_cursor("s-42", Last), Last < 7',
      '-utterance_cursor("s-42", Last) <- utterance_cursor("s-42", Last), il_txn("t-1")',
      '-il_ghost(0), +utterance_cursor("s-42", 7) <- il_txn("t-1")',
      '-il_txn("t-1") <- il_txn("t-1")',
    ]);
    expect(c.tokenIndex).toBe(0);
    expect(c.assertIndex).toBeUndefined();
    expect(c.writeIndexes).toEqual([1, 2]);
  });

  it('compiles .when() with strict (the default) to the abort form', () => {
    const p = new Program()
      .clearRule('late')
      .defineRules('late', ['order'], [from(Shipment).select({ order: Shipment.col('order') })])
      .retract(PackVersion, { name: 'delivery' })
      .insert(PackVersion, { name: 'delivery', version: 'v3' })
      .when(any(PackVersion, { name: 'delivery', version: 'v2' }));
    const c = p.compile(true, 't-d2');
    expect(c.iql.split('\n')).toEqual([
      '+il_txn_pending("t-d2")',
      '-il_txn(""), +il_txn("t-d2") <- pack_version("delivery", "v2")',
      '-il_assert(0), +il_assert("precondition_failed:t-d2") <- il_txn_pending(K), K = "t-d2", !il_txn(K)',
      '.rule clear late',
      '+late(Order) <- shipment(Order, _)',
      '-pack_version("delivery", Version) <- pack_version("delivery", Version), il_txn("t-d2")',
      '-il_ghost(0), +pack_version("delivery", "v3") <- il_txn("t-d2")',
      '-il_txn("t-d2") <- il_txn("t-d2")',
      '-il_txn_pending("t-d2")',
    ]);
    expect(c.tokenIndex).toBe(1);
    expect(c.assertIndex).toBe(2);
    expect(c.writeIndexes).toEqual([5, 6]);
  });

  it('carries the constant of a negation through a positive atom carrying it', () => {
    const p = new Program()
      .insert(AttemptDone, { attempt: 'att-9f3', status: 'ok' })
      .when(any(Attempt, { attempt: 'att-9f3' }), NOT(any(AttemptDone, { attempt: 'att-9f3' })));
    expect(p.compile(false, T).iql.split('\n')[0]).toBe(
      '-il_txn(""), +il_txn("t-1") <- attempt(_, _, Attempt), !attempt_done(Attempt, _), Attempt = "att-9f3"',
    );
  });

  it('binds a negation-only guard through a staged il_txn_const row, deleted last', () => {
    const p = new Program()
      .insert(AttemptDone, { attempt: 'att-y', status: 'ok' })
      .when(NOT(any(AttemptDone, { attempt: 'att-y' })));
    const c = p.compile(true, T);
    expect(c.iql.split('\n')).toEqual([
      '+il_txn_const_s("att-y")',
      '+il_txn_pending("t-1")',
      '-il_txn(""), +il_txn("t-1") <- il_txn_const_s(K), !attempt_done(K, _), K = "att-y"',
      '-il_assert(0), +il_assert("precondition_failed:t-1") <- il_txn_pending(K), K = "t-1", !il_txn(K)',
      '-il_ghost(0), +attempt_done("att-y", "ok") <- il_txn("t-1")',
      '-il_txn("t-1") <- il_txn("t-1")',
      '-il_txn_pending("t-1")',
      '-il_txn_const_s("att-y")',
    ]);
    expect(c.tokenIndex).toBe(2);
    expect(c.assertIndex).toBe(3);
    expect(c.writeIndexes).toEqual([4]);
  });

  it('anchors a guarded insert on the SDK ghost row, never a row of the written relation', () => {
    const p = new Program().insert(Doc, { id: -1, emb: [], ok: false, score: 2.5 }).when(any(PackVersion));
    expect(p.compile(false, T).iql.split('\n')[1]).toBe(
      '-il_ghost(0), +doc(-1, [], false, 2.5) <- il_txn("t-1")',
    );
  });

  it('refuses strict: false for a program holding rules or schema', () => {
    const p = new Program().define(Eta).when(any(PackVersion, { name: 'x' }));
    expect(() => p.compile(false, T)).toThrow(CompileError);
  });

  it('refuses a guard condition over a relation the guard does not bind', () => {
    const p = new Program().insert(Eta, { shipment: 'S', due: 'd' }).when(Cursor.col('last').lt(7));
    expect(() => p.compile(true, T)).toThrow(/no positive any/);
  });

  it('refuses a retract that names no column', () => {
    expect(() => new Program().retract(Eta, {}).compile(true, T)).toThrow(/every row/);
  });

  it('refuses an empty program and a missing column', () => {
    expect(() => new Program().compile(true, T)).toThrow(CompileError);
    expect(() => new Program().insert(Eta, { shipment: 'S' }).when(any(PackVersion)).compile(true, T)).toThrow(
      /missing column 'due'/,
    );
  });

  it('gives each program its own token', () => {
    const p = new Program().insert(Eta, { shipment: 'S', due: 'd' }).when(any(PackVersion));
    expect(p.iql()).not.toBe(p.iql());
  });
});

describe('claim', () => {
  it('compiles the hero claim: guarded insert, then the query on the key', () => {
    const { iql, key } = compileClaim(
      Attempt,
      { order: 'ORD-1', tool: 'carrier_check', attempt: 'a1' },
      {
        when: [any(CheckNeeded, { order: 'ORD-1' })],
        unless: any(Attempt, { order: 'ORD-1', tool: 'carrier_check' }),
      },
    );
    expect(key).toEqual(['order', 'tool']);
    expect(iql.split('\n')).toEqual([
      '-il_ghost(0), +attempt("ORD-1", "carrier_check", "a1") <- check_needed(Order, _), ' +
        '!attempt(Order, "carrier_check", _), Order = "ORD-1"',
      '?attempt("ORD-1", "carrier_check", Attempt)',
    ]);
  });

  it('derives unless from key, and binds a lone negation through il_txn_const', () => {
    const { iql } = compileClaim(Attempt, { order: 'ORD-2', tool: 't', attempt: 'b' }, { key: ['order'] });
    expect(iql.split('\n')).toEqual([
      '+il_txn_const_s("ORD-2")',
      '-il_ghost(0), +attempt("ORD-2", "t", "b") <- il_txn_const_s(K), !attempt(K, _, _), K = "ORD-2"',
      '-il_txn_const_s("ORD-2")',
      '?attempt("ORD-2", Tool, Attempt)',
    ]);
  });

  it('without a guard is a plain insert read back by every column', () => {
    const { iql } = compileClaim(KillSwitch, { tool: 'x' });
    expect(iql).toBe('+kill_switch("x")\n?kill_switch("x")');
  });
});

describe('write reply grammar', () => {
  it('reads every count reply', () => {
    expect(parseWriteMessage("Inserted 2 fact(s) into 'eta'.")).toEqual({ inserted: 2, deleted: 0 });
    expect(parseWriteMessage('Update: 1 deleted, 3 inserted.')).toEqual({ inserted: 3, deleted: 1 });
    expect(parseWriteMessage("Conditional delete: 4 fact(s) deleted from 'eta'.")).toEqual({ inserted: 0, deleted: 4 });
    expect(parseWriteMessage("Deleted 5 facts from 'eta'.")).toEqual({ inserted: 0, deleted: 5 });
  });

  it('refuses an unknown reply rather than guessing', () => {
    expect(() => parseWriteMessage('Rule registered')).toThrow(/Unexpected write reply/);
  });
});
