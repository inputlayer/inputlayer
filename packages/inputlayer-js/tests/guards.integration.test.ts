/**
 * Live tests for any()/NOT(any()), kg.program().when() and kg.claim().
 *
 * Each runs the compiled form against a real engine, so a guard that applies
 * partially, a write form the engine reads as a rule, or a change in the
 * engine's count replies or abort behaviour fails here. Set
 * INPUTLAYER_TEST_SERVER (and INPUTLAYER_TEST_USER / INPUTLAYER_TEST_PASSWORD)
 * to enable; `make js-test-live` starts a server and runs them.
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import {
  InputLayer,
  KnowledgeGraph,
  relation,
  from,
  any,
  AND,
  NOT,
  PreconditionFailed,
  type RowOf,
} from '../src/index';
import { parseWriteMessage } from '../src/program';

const SERVER_URL = process.env.INPUTLAYER_TEST_SERVER ?? '';
const USERNAME = process.env.INPUTLAYER_TEST_USER ?? 'admin';
const PASSWORD = process.env.INPUTLAYER_TEST_PASSWORD ?? 'admin';

const KG_NAME = 'test_guards_js';

// The README hero's declarations.
const Shipment = relation('Shipment', { order: 'string', shipment: 'string' });
const Eta = relation('Eta', { shipment: 'string', due: 'string' });
const Promised = relation('Promised', { order: 'string', due: 'string' });
const ToolPolicy = relation('ToolPolicy', { tool: 'string', mode: 'string' });
const KillSwitch = relation('KillSwitch', { tool: 'string' });
const Attempt = relation('Attempt', { order: 'string', tool: 'string', attempt: 'string' });
const CheckNeeded = relation('CheckNeeded', { order: 'string', shipment: 'string' });

// Write-gate shapes of the design's lab (T-A1 to T-A4, R3-*).
const Session = relation('VoiceSession', { session: 'string', status: 'string' });
const Cursor = relation('UtteranceCursor', { session: 'string', last: 'int' });
const Goal = relation('Goal', { session: 'string', goal: 'string', kind: 'string' });
const AttemptDone = relation('AttemptDone', { attempt: 'string', status: 'string' });
const CarrierNote = relation('CarrierNote', { shipment: 'string', reason: 'string', srcRev: 'int' });
const PackVersion = relation('PackVersion', { name: 'string', version: 'string' });
const Late = relation('Late', { order: 'string' });
const Race = relation('RaceAttempt', { work: 'string', attempt: 'string' });
const PropA = relation('PropA', { k: 'int' });
const PropB = relation('PropB', { k: 'int', v: 'string' });
const Flag = relation('PropFlag', { f: 'string' });
const Probe = relation('GhostProbe', { id: 'int', on: 'bool', tag: 'string' });

async function rows(kg: KnowledgeGraph, iql: string): Promise<unknown[][]> {
  return (await kg.execute(iql)).rows;
}

/** The SDK's guard relations hold nothing once a program is done. */
async function expectNoGuardLeftovers(kg: KnowledgeGraph): Promise<void> {
  for (const q of ['?il_txn(T)', '?il_txn_pending(T)', '?il_assert(V)', '?il_txn_const_s(K)']) {
    expect(await rows(kg, q)).toEqual([]);
  }
}

describe.skipIf(!SERVER_URL)('guards and claims (live)', () => {
  let il: InputLayer;
  let kg: KnowledgeGraph;
  const extra: InputLayer[] = [];
  let apiKey: string | undefined;

  /** Another connection; API-key auth, since the engine throttles bursts of password logins. */
  async function connectAnother(): Promise<KnowledgeGraph> {
    apiKey ??= await il.createApiKey(`test-guards-js-${Date.now()}`);
    const other = new InputLayer({ url: SERVER_URL, apiKey });
    await other.connect();
    extra.push(other);
    return other.knowledgeGraph(KG_NAME);
  }

  beforeAll(async () => {
    il = new InputLayer({ url: SERVER_URL, username: USERNAME, password: PASSWORD });
    await il.connect();
    try {
      await il.knowledgeGraph('default').execute('.kg use default');
      await il.dropKnowledgeGraph(KG_NAME);
    } catch {
      // not there yet
    }
    kg = il.knowledgeGraph(KG_NAME);
    await kg.define(
      Shipment, Eta, Promised, ToolPolicy, KillSwitch, Attempt,
      Session, Cursor, Goal, AttemptDone, CarrierNote, PackVersion, Race, PropA, PropB, Flag, Probe,
    );
  });

  afterAll(async () => {
    for (const other of extra) await other.close();
    try {
      await il.knowledgeGraph('default').execute('.kg use default');
      await il.dropKnowledgeGraph(KG_NAME);
    } catch {
      // ignore
    }
    await il.close();
  });

  it('count_reply_grammar: every write reply the SDK counts from parses', async () => {
    const replies = (
      await rows(
        kg,
        [
          '+flag_grammar("a")',
          '-il_ghost(0), +flag_grammar("b") <- flag_grammar("a")',
          '-flag_grammar(X) <- flag_grammar(X), X = "a"',
          '-flag_grammar("b")',
        ].join('\n'),
      )
    ).map((r) => String(r[0]));
    expect(replies.map(parseWriteMessage)).toEqual([
      { inserted: 1, deleted: 0 },
      { inserted: 1, deleted: 0 },
      { inserted: 0, deleted: 1 },
      { inserted: 0, deleted: 1 },
    ]);
  });

  it('hero: a rule with NOT(any()), a claim once per need, cancel on the kill switch, re-claim when lifted', async () => {
    await kg.defineRules('check_needed', ['order', 'shipment'], [
      from(Shipment, Eta, Promised, ToolPolicy)
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
        .select({ order: Shipment.col('order'), shipment: Shipment.col('shipment') }),
    ]);
    await kg.insert(Shipment, { order: 'ORD-4821', shipment: 'S-77' });
    await kg.insert(Promised, { order: 'ORD-4821', due: '2026-10-08' });
    await kg.insert(ToolPolicy, [
      { tool: 'carrier_check', mode: 'auto' },
      { tool: 'refund', mode: 'confirm' },
    ]);
    await kg.insert(Eta, { shipment: 'S-77', due: '2026-10-10' });
    expect((await kg.query({ select: [CheckNeeded] })).rows).toEqual([['ORD-4821', 'S-77']]);

    const claimFor = (attempt: string) =>
      kg.claim(Attempt, { order: 'ORD-4821', tool: 'carrier_check', attempt }, {
        when: [any(CheckNeeded, { order: 'ORD-4821' })],
        unless: any(Attempt, { order: 'ORD-4821', tool: 'carrier_check' }),
      });
    const first = await claimFor('b-1');
    expect(first).toEqual({ won: true, holder: { order: 'ORD-4821', tool: 'carrier_check', attempt: 'b-1' } });
    const second = await claimFor('b-2');
    expect(second).toEqual({ won: false, holder: { order: 'ORD-4821', tool: 'carrier_check', attempt: 'b-1' } });

    // The kill switch retracts the need; the loop cancels and frees the attempt.
    await kg.insert(KillSwitch, { tool: 'carrier_check' });
    expect((await kg.query({ select: [CheckNeeded] })).rows).toEqual([]);
    const held: RowOf<typeof Attempt> = first.holder!;
    expect(await kg.retract(Attempt, held)).toEqual({ count: 1 });
    expect(await claimFor('b-3')).toEqual({ won: false, holder: null });

    // Lifted: the need comes back and a new claim wins.
    await kg.retract(KillSwitch, { tool: 'carrier_check' });
    expect((await claimFor('b-4')).won).toBe(true);
    expect(await rows(kg, '?attempt(O, T, A)')).toEqual([['ORD-4821', 'carrier_check', 'b-4']]);
  });

  it('read path: a constant-only NOT(any()) binds through a session fact that does not outlive the query', async () => {
    const policies = () =>
      kg.query({
        select: [ToolPolicy],
        where: NOT(any(KillSwitch, { tool: 'carrier_check' })),
        orderBy: ToolPolicy.col('tool').asc(),
      });
    expect((await policies()).rows).toEqual([
      ['carrier_check', 'auto'],
      ['refund', 'confirm'],
    ]);
    await kg.insert(KillSwitch, { tool: 'carrier_check' });
    expect((await policies()).rows).toEqual([]);
    await kg.retract(KillSwitch, { tool: 'carrier_check' });
    expect(await rows(kg, '?il_const_s(K)')).toEqual([]);
    expect(await rows(kg, '.session')).toEqual([['No session data defined.']]);
  });

  it('guard_cursor_advance_applies_whole: a write gate that advances the cursor it guards on', async () => {
    await kg.insert(Session, { session: 's-42', status: 'open' });
    await kg.insert(Cursor, { session: 's-42', last: 6 });
    const utterance = (k: InputLayer['knowledgeGraph'] extends (n: string) => infer K ? K : never, goal: string, at: number) =>
      k
        .program()
        .insert(Goal, { session: 's-42', goal, kind: 'reschedule' })
        .retract(Cursor, { session: 's-42' })
        .insert(Cursor, { session: 's-42', last: at })
        .when(any(Session, { session: 's-42', status: 'open' }), any(Cursor, { session: 's-42' }), Cursor.col('last').lt(at))
        .commit({ strict: false });

    const r = await utterance(kg, 'g-7', 7);
    expect([r.applied, r.inserted, r.deleted]).toEqual([true, 2, 1]);
    expect(await rows(kg, '?utterance_cursor(S, L)')).toEqual([['s-42', 7]]);

    // Replaying the same utterance applies nothing.
    const replay = await utterance(kg, 'g-7b', 7);
    expect([replay.applied, replay.inserted, replay.deleted]).toEqual([false, 0, 0]);
    expect(await rows(kg, '?goal(S, G, K)')).toEqual([['s-42', 'g-7', 'reschedule']]);

    // Two connections race the next utterance: exactly one applies.
    const other = await connectAnother();
    const race = await Promise.all([utterance(kg, 'g-8a', 8), utterance(other, 'g-8b', 8)]);
    expect(race.filter((x) => x.applied)).toHaveLength(1);
    expect(await rows(kg, '?utterance_cursor(S, L)')).toEqual([['s-42', 8]]);
    expect(await rows(kg, '?goal(S, G, K)')).toHaveLength(2);
    await expectNoGuardLeftovers(kg);
  });

  it('guard_first_terminal_outcome: a negation-only guard binds through il_txn_const', async () => {
    const outcome = (status: string) =>
      kg
        .program()
        .insert(AttemptDone, { attempt: 'att-y', status })
        .when(NOT(any(AttemptDone, { attempt: 'att-y' })))
        .commit({ strict: false });
    expect((await outcome('ok')).applied).toBe(true);
    expect((await outcome('failed')).applied).toBe(false);
    expect(await rows(kg, '?attempt_done(A, S)')).toEqual([['att-y', 'ok']]);
    await expectNoGuardLeftovers(kg);
  });

  it('guard_note_replacement: a program that deletes the note its guard reads applies whole', async () => {
    await kg.insert(CarrierNote, { shipment: 'S-77', reason: 'unknown', srcRev: 2100 });
    const r = await kg
      .program()
      .retract(CarrierNote, { shipment: 'S-77' })
      .insert(CarrierNote, { shipment: 'S-77', reason: 'weather_delay', srcRev: 2201 })
      .when(any(CarrierNote, { shipment: 'S-77' }), CarrierNote.col('srcRev').lt(2201))
      .commit();
    expect([r.applied, r.inserted, r.deleted]).toEqual([true, 1, 1]);
    expect(await rows(kg, '?carrier_note(S, R, V)')).toEqual([['S-77', 'weather_delay', 2201]]);
    await expectNoGuardLeftovers(kg);
  });

  it('guarded writes keep a stored row equal to a typed placeholder', async () => {
    await kg.insert(Probe, { id: -1, on: false, tag: '' });
    const r = await kg
      .program()
      .insert(Probe, { id: 1, on: true, tag: 'a' })
      .when(any(Probe, { id: -1 }))
      .commit();
    expect([r.applied, r.inserted, r.deleted]).toEqual([true, 1, 0]);
    const c = await kg.claim(Probe, { id: 2, on: true, tag: 'b' }, { when: any(Probe, { id: -1 }), key: ['id'] });
    expect(c.won).toBe(true);
    expect(await rows(kg, '?ghost_probe(I, O, T)')).toEqual(
      expect.arrayContaining([[-1, false, ''], [1, true, 'a'], [2, true, 'b']]),
    );
    expect(await rows(kg, '?ghost_probe(I, O, T)')).toHaveLength(3);
    await expectNoGuardLeftovers(kg);
  });

  it('strict_guard_raises_precondition_failed_from_error_index', async () => {
    const p = kg
      .program()
      .insert(AttemptDone, { attempt: 'att-z', status: 'ok' })
      .when(any(AttemptDone, { attempt: 'never-there' }));
    await expect(p.commit()).rejects.toBeInstanceOf(PreconditionFailed);
    expect(await rows(kg, '?attempt_done("att-z", S)')).toEqual([]);
    await expectNoGuardLeftovers(kg);
  });

  it('stale_deploy_is_refused_including_rules', async () => {
    await kg.insert(PackVersion, { name: 'delivery', version: 'v2' });
    await kg.defineRules('late', ['order'], [from(Shipment).select({ order: Shipment.col('order') })]);
    const before = await kg.ruleDefinition('late');
    const deploy = (expected: string) =>
      kg
        .program()
        .clearRule('late')
        .defineRules('late', ['order'], [
          from(Shipment, Eta)
            .where((s, e) => e.col('shipment').eq(s.col('shipment')))
            .select({ order: Shipment.col('order') }),
        ])
        .define(Late)
        .retract(PackVersion, { name: 'delivery' })
        .insert(PackVersion, { name: 'delivery', version: 'v3' })
        .when(any(PackVersion, { name: 'delivery', version: expected }))
        .commit();

    await expect(deploy('v1')).rejects.toBeInstanceOf(PreconditionFailed);
    expect(await kg.ruleDefinition('late')).toEqual(before);
    expect(await rows(kg, '?pack_version(N, V)')).toEqual([['delivery', 'v2']]);
    await expectNoGuardLeftovers(kg);
  });

  it('deploy_applies_whole_when_guard_holds', async () => {
    const before = await kg.ruleDefinition('late');
    const r = await kg
      .program()
      .clearRule('late')
      .defineRules('late', ['order'], [
        from(Shipment, Eta)
          .where((s, e) => e.col('shipment').eq(s.col('shipment')))
          .select({ order: Shipment.col('order') }),
      ])
      .retract(PackVersion, { name: 'delivery' })
      .insert(PackVersion, { name: 'delivery', version: 'v3' })
      .when(any(PackVersion, { name: 'delivery', version: 'v2' }))
      .commit();
    expect([r.applied, r.inserted, r.deleted]).toEqual([true, 1, 1]);
    const after = await kg.ruleDefinition('late');
    expect(after).not.toEqual(before);
    expect(after.join('\n')).toContain('eta(');
    expect(await rows(kg, '?pack_version(N, V)')).toEqual([['delivery', 'v3']]);
    await expectNoGuardLeftovers(kg);
  });

  it('claim_twenty_way_race: one winner, nineteen see its row', async () => {
    const kgs = [kg];
    while (kgs.length < 20) kgs.push(await connectAnother());
    // Bind each handle to the graph before the race, so the claims go out together.
    await Promise.all(kgs.map((k) => k.execute('?race_attempt(W, A)')));
    const claims = await Promise.all(
      kgs.map((k, i) => k.claim(Race, { work: 'w-1', attempt: `a-${i}` }, { key: ['work'] })),
    );
    const winners = claims.filter((c) => c.won);
    expect(winners).toHaveLength(1);
    for (const c of claims) expect(c.holder).toEqual(winners[0].holder);
    expect(await rows(kg, '?race_attempt("w-1", A)')).toHaveLength(1);
    await expectNoGuardLeftovers(kg);
  });

  it('guarded_program_is_all_or_nothing: random programs apply whole or not at all', async () => {
    // A connection of its own: the engine limits each connection to 100 messages a second.
    const kg = await connectAnother();
    let seed = 20261004;
    const rand = (n: number) => {
      seed = (seed * 1103515245 + 12345) % 2147483648;
      return seed % n;
    };
    const state = async () => ({
      a: (await rows(kg, '?prop_a(K)')).map((r) => r[0]).sort(),
      b: (await rows(kg, '?prop_b(K, V)')).map((r) => `${r[0]}:${r[1]}`).sort(),
    });
    for (let round = 0; round < 25; round++) {
      // Stay under the engine's per-connection message rate limit.
      await new Promise((ok) => setTimeout(ok, 100));
      const flagOn = rand(2) === 0;
      if (flagOn) await kg.insert(Flag, { f: 'on' });
      else await kg.retract(Flag, { f: 'on' });
      const strict = rand(2) === 0;
      const p = kg.program();
      const before = await state();
      const expectA = new Set(before.a as number[]);
      const expectB = new Map(before.b.map((kv) => kv.split(':')).map(([k, v]) => [Number(k), v]));
      for (let s = 0, n = 1 + rand(5); s < n; s++) {
        const k = rand(6);
        switch (rand(4)) {
          case 0:
            p.insert(PropA, { k });
            expectA.add(k);
            break;
          case 1:
            p.retract(PropA, { k });
            expectA.delete(k);
            break;
          case 2:
            p.retract(PropB, { k });
            p.insert(PropB, { k, v: `r${round}` });
            expectB.set(k, `r${round}`);
            break;
          default:
            p.retract(PropB, { k });
            expectB.delete(k);
        }
      }
      p.when(any(Flag, { f: 'on' }));
      if (!flagOn && strict) {
        await expect(p.commit({ strict })).rejects.toBeInstanceOf(PreconditionFailed);
      } else {
        expect((await p.commit({ strict })).applied).toBe(flagOn);
      }
      const after = await state();
      if (flagOn) {
        expect(after).toEqual({
          a: [...expectA].sort(),
          b: [...expectB].map(([k, v]) => `${k}:${v}`).sort(),
        });
      } else {
        expect(after).toEqual(before);
      }
    }
    await expectNoGuardLeftovers(kg);
  });
});
