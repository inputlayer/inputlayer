/**
 * Live tests of `kg.subscribe()`, `kg.watch()` and `kg.on()` against a real
 * engine: snapshot plus deltas exact over seeded random histories (the
 * differential oracle's universe, `tests/differential_oracle/generate.rs`,
 * with a fresh query as the recompute adapter), projection multiplicity,
 * streamed snapshots and deltas, a `seq` gap and a reset, a slow consumer,
 * a reconnect, an ACL revoke, and the refusals. Set INPUTLAYER_TEST_SERVER
 * (and INPUTLAYER_TEST_USER / INPUTLAYER_TEST_PASSWORD) to enable; `make
 * js-test-live` starts a server and runs them.
 */

import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import {
  type Change,
  type Connection,
  count,
  from,
  InputLayer,
  OR,
  type KnowledgeGraph,
  relation,
  type Row,
  type Subscription,
  SubscriptionRejectedError,
} from '../src/index';

const SERVER_URL = process.env.INPUTLAYER_TEST_SERVER ?? '';
const USERNAME = process.env.INPUTLAYER_TEST_USER ?? 'admin';
const PASSWORD = process.env.INPUTLAYER_TEST_PASSWORD ?? 'admin';
const SKIP = !SERVER_URL;

const PREFIX = 'test_subscriptions_js';
const SEEDS = Number(process.env.INPUTLAYER_ORACLE_SEEDS ?? 6);
const HISTORY_LENGTH = 30;

function client(opts: { username?: string; password?: string } = {}): InputLayer {
  return new InputLayer({
    url: SERVER_URL,
    username: opts.username ?? USERNAME,
    password: opts.password ?? PASSWORD,
    reconnectDelay: 0.05,
  });
}

async function waitFor(predicate: () => boolean | Promise<boolean>, timeoutMs = 10_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!(await predicate())) {
    if (Date.now() > deadline) throw new Error('condition not met in time');
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
}

/** Order-free identity of a set of rows. */
function setOf(rows: unknown[][]): string[] {
  return rows.map((r) => JSON.stringify(r)).sort();
}

/**
 * Applies a subscription's events to a set, as a consumer would, checking
 * each event against the set it applies to: an insert is new, a retract is
 * held, revisions never go back.
 */
class Consumer {
  readonly rows = new Map<string, Row>();
  readonly events: Change[] = [];
  revision = -1;
  verified = false;
  error?: unknown;
  private readonly running: Promise<void>;

  constructor(readonly sub: Subscription) {
    this.running = (async () => {
      try {
        for await (const change of sub) this.apply(change);
      } catch (e) {
        this.error = e;
      }
    })();
  }

  apply(change: Change): void {
    this.events.push(change);
    expect(change.revision).toBeGreaterThanOrEqual(this.revision);
    this.revision = change.revision;
    this.verified = change.verified;
    if (change.kind === 'snapshot') expect(this.rows.size).toBe(0);
    for (const row of change.retracted) {
      const key = JSON.stringify(Object.values(row));
      expect(this.rows.has(key), `retracted ${key}, not held`).toBe(true);
      this.rows.delete(key);
    }
    for (const row of change.inserted) {
      const key = JSON.stringify(Object.values(row));
      expect(this.rows.has(key), `inserted ${key}, already held`).toBe(false);
      this.rows.set(key, row);
    }
  }

  values(): string[] {
    return [...this.rows.keys()].sort();
  }

  kinds(): string[] {
    return this.events.map((e) => (e.kind === 'unverified' ? `unverified:${e.reason}` : e.kind));
  }

  async close(): Promise<void> {
    await this.sub.close();
    await this.running;
  }
}

/**
 * Wait until the consumer holds exactly the engine's current answer to
 * `query`, projected to the columns `project` picks (all of them by default).
 */
async function converges(
  kg: KnowledgeGraph,
  consumer: Consumer,
  query: string,
  project: (row: unknown[]) => unknown[] = (row) => row,
  timeoutMs = 10_000,
): Promise<void> {
  let expected: string[] = [];
  try {
    await waitFor(async () => {
      expected = [...new Set(setOf((await kg.execute(query)).toTuples().map(project)))];
      return consumer.verified && JSON.stringify(consumer.values()) === JSON.stringify(expected);
    }, timeoutMs);
  } catch {
    expect(consumer.values(), `${query} after ${consumer.kinds().join(', ')}`).toEqual(expected);
  }
}

function connectionOf(kg: KnowledgeGraph): Connection {
  return kg.connection;
}

/** The generation the connection routes `sub` under. */
function generationOf(kg: KnowledgeGraph, sub: Subscription): number {
  const routes = (connectionOf(kg) as unknown as { routes: Map<string, { generation?: number }> }).routes;
  return routes.get(sub.id)!.generation!;
}

/** Feed a frame to the connection's reader, as if the server sent it. */
function inject(kg: KnowledgeGraph, frame: Record<string, unknown>): void {
  (connectionOf(kg) as unknown as { onFrame(data: string): void }).onFrame(JSON.stringify(frame));
}

// ── The differential oracle's random histories ──────────────────────

/** SplitMix64, as `tests/differential_oracle/generate.rs`, so a seed means the same history. */
class Rng {
  private state: bigint;
  constructor(seed: number) {
    this.state = BigInt(seed);
  }
  next(): bigint {
    const mask = (1n << 64n) - 1n;
    this.state = (this.state + 0x9e3779b97f4a7c15n) & mask;
    let z = this.state;
    z = ((z ^ (z >> 30n)) * 0xbf58476d1ce4e5b9n) & mask;
    z = ((z ^ (z >> 27n)) * 0x94d049bb133111ebn) & mask;
    return z ^ (z >> 31n);
  }
  below(n: number): number {
    return Number(this.next() % BigInt(n));
  }
  pick<T>(items: readonly T[]): T {
    return items[this.below(items.length)];
  }
}

const NODES = 5;

const Edge = relation('Edge', { x: 'int', y: 'int' });
const Reach = relation('Reach', { x: 'int', y: 'int' });

const RULES: ReadonlyArray<readonly [string, ReadonlyArray<readonly string[]>]> = [
  [
    'reach',
    [
      ['+reach(X, Y) <- edge(X, Y)', '+reach(X, Z) <- reach(X, Y), edge(Y, Z)'],
      ['+reach(X, Y) <- edge(X, Y)', '+reach(X, Z) <- edge(X, Y), reach(Y, Z)'],
      ['+reach(X, Y) <- edge(X, Y), X < Y'],
    ],
  ],
  ['two_hop', [['+two_hop(X, Z) <- edge(X, Y), edge(Y, Z)']]],
  ['linked', [['+linked(X, Y) <- edge(X, Y)', '+linked(X, Y) <- edge(Y, X)']]],
  ['open', [['+open(X, Y) <- reach(X, Y), !blocked(Y)'], ['+open(X, Y) <- edge(X, Y), !blocked(X)']]],
  ['degree', [['+degree(X, count<Y>) <- edge(X, Y)'], ['+degree(X, max<Y>) <- edge(X, Y)']]],
  ['reach_count', [['+reach_count(X, count<Y>) <- reach(X, Y)']]],
  ['weight_sum', [['+weight_sum(sum<Y>) <- edge(_, Y)']]],
];

const QUERIES = [
  '?edge(X, Y)',
  '?blocked(X)',
  '?reach(X, Y)',
  '?reach(0, Y)',
  '?two_hop(X, Y)',
  '?linked(X, Y)',
  '?open(X, Y)',
  '?degree(X, N)',
  '?reach_count(X, N)',
  '?weight_sum(S)',
];

type Step = { execute: string } | { checkpoint: string };

/** `generate::history(seed, length)`, less restarts (a test cannot restart its server). */
function history(seed: number, length: number): Step[] {
  const rng = new Rng(seed);
  const edges = new Map<string, [number, number]>();
  const blocked = new Set<number>();
  const rules = new Set<string>();
  const node = () => rng.below(NODES);
  const steps: Step[] = [];
  for (let i = 0; i < length; i++) {
    const r = rng.below(20);
    if (r <= 6) {
      const n = rng.below(3) + 1;
      const added: Array<[number, number]> = [];
      for (let k = 0; k < n; k++) added.push([node(), node()]);
      for (const [a, b] of added) edges.set(`${a},${b}`, [a, b]);
      steps.push({ execute: `+edge[${added.map(([a, b]) => `(${a}, ${b})`).join(', ')}]` });
    } else if (r <= 11) {
      const existing = [...edges.values()].sort((x, y) => x[0] - y[0] || x[1] - y[1]);
      const [a, b] = existing.length === 0 || rng.below(5) === 0 ? [node(), node()] : rng.pick(existing);
      edges.delete(`${a},${b}`);
      steps.push({ execute: `-edge(${a}, ${b})` });
    } else if (r === 12) {
      const n = node();
      blocked.add(n);
      steps.push({ execute: `+blocked(${n})` });
    } else if (r === 13) {
      const n = blocked.size > 0 ? Math.min(...blocked) : node();
      blocked.delete(n);
      steps.push({ execute: `-blocked(${n})` });
    } else if (r <= 17) {
      const [name, variants] = rng.pick(RULES);
      if (rules.has(name)) steps.push({ execute: `.rule drop ${name}` });
      rules.add(name);
      for (const clause of rng.pick(variants)) steps.push({ execute: clause });
    } else if (r === 18) {
      const defined = [...rules].sort();
      if (defined.length > 0) {
        const name = rng.pick(defined);
        rules.delete(name);
        steps.push({ execute: `.rule drop ${name}` });
      }
    }
    // 19: a restart in the oracle; nothing here.
    if (i % 3 === 2) steps.push({ checkpoint: `c${Math.floor(i / 3)}` });
  }
  return steps;
}

describe.skipIf(SKIP)('Live: subscriptions', () => {
  let il: InputLayer;
  const graphs: string[] = [];

  async function fresh(name: string): Promise<KnowledgeGraph> {
    const full = `${PREFIX}_${name}`;
    await il.dropKnowledgeGraph(full).catch(() => undefined);
    graphs.push(full);
    return il.knowledgeGraph(full);
  }

  beforeAll(async () => {
    il = client();
    await il.connect();
  });

  afterAll(async () => {
    for (const name of graphs) await il?.dropKnowledgeGraph(name).catch(() => undefined);
    await il?.close();
  });

  it('snapshot plus deltas equals a fresh query at every checkpoint of random histories', async () => {
    for (let seed = 1; seed <= SEEDS; seed++) {
      const kg = await fresh(`oracle_${seed}`);
      await kg.execute('+edge(0, 1)\n+blocked(9)\n-edge(0, 1)\n-blocked(9)');
      const consumers = QUERIES.map((iql) => new Consumer(kg.subscribe({ iql })));
      // Projections through the query builder: rows supported several times.
      const projected: Array<[Consumer, string, (row: unknown[]) => unknown[]]> = [
        [new Consumer(kg.subscribe({ select: [Edge.col('x').toAst()], join: [Edge] })), '?edge(X, Y)', (r) => [r[0]]],
        [new Consumer(kg.subscribe({ select: [Reach.col('y').toAst()], join: [Reach] })), '?reach(X, Y)', (r) => [r[1]]],
      ];
      for (const [i, step] of history(seed, HISTORY_LENGTH).entries()) {
        if ('execute' in step) {
          await kg.execute(step.execute);
          continue;
        }
        for (const [q, consumer] of consumers.entries()) {
          await converges(kg, consumer, QUERIES[q]);
          expect(consumer.error, `seed ${seed} step ${i} ${QUERIES[q]}`).toBeUndefined();
        }
        for (const [consumer, query, project] of projected) {
          await converges(kg, consumer, query, project);
          expect(consumer.error, `seed ${seed} step ${i} projected ${query}`).toBeUndefined();
        }
      }
      consumers.push(...projected.map(([consumer]) => consumer));
      for (const consumer of consumers) {
        expect(consumer.kinds().filter((k) => k.startsWith('unverified'))).toEqual([]);
        await consumer.close();
      }
    }
  }, 120_000);

  it('projection_two_supporting_tuples: a projected row leaves with its last support', async () => {
    const kg = await fresh('projection');
    const A = relation('A', { x: 'int', y: 'int' });
    await kg.define(A);
    const consumer = new Consumer(kg.subscribe({ select: [A.col('x').toAst()], join: [A] }));
    await waitFor(() => consumer.events.length >= 1);
    await kg.insert(A, [{ x: 1, y: 1 }, { x: 1, y: 2 }]);
    await waitFor(() => consumer.rows.size === 1);
    await kg.delete(A, { x: 1, y: 1 });
    // A later write proves the delete's delta (if any) arrived first.
    await kg.insert(A, { x: 2, y: 1 });
    await waitFor(() => consumer.rows.size === 2);
    expect(consumer.values()).toEqual(['[1]', '[2]']);
    await kg.delete(A, { x: 1, y: 2 });
    await waitFor(() => consumer.rows.size === 1);
    expect(consumer.values()).toEqual(['[2]']);
    expect(consumer.events.flatMap((e) => e.retracted)).toEqual([{ X: 1 }]);
    await consumer.close();
  });

  it('assembles a streamed snapshot and streamed deltas', async () => {
    const kg = await fresh('streamed');
    const Doc = relation('Doc', { id: 'int', body: 'string' });
    await kg.define(Doc);
    const body = 'x'.repeat(400);
    // A program is at most 1 MiB: insert in parts, then let a rule move all
    // the rows at once, so the snapshot and each delta are over 1 MiB.
    for (let i = 0; i < 4000; i += 1000) {
      await kg.insert(Doc, Array.from({ length: 1000 }, (_, k) => ({ id: i + k, body })));
    }
    await kg.execute('+big(I, B) <- doc(I, B)');
    const frames: string[] = [];
    const conn = connectionOf(kg) as unknown as { onFrame(data: string): void };
    const onFrame = conn.onFrame.bind(conn);
    conn.onFrame = (data: string) => {
      frames.push(String(JSON.parse(data).type));
      onFrame(data);
    };
    const consumer = new Consumer(kg.subscribe({ iql: '?big(I, B)' }));
    await waitFor(() => consumer.rows.size === 4000);
    await kg.execute('.rule drop big');
    await waitFor(() => consumer.rows.size === 0);
    await kg.execute('+big(I, B) <- doc(I, B)');
    await waitFor(() => consumer.rows.size === 4000);
    expect(consumer.kinds()).toEqual(['snapshot', 'delta', 'delta']);
    expect(frames).toContain('result_start');
    expect(frames.filter((t) => t === 'subscription_delta_start')).toHaveLength(2);
    await converges(kg, consumer, '?big(I, B)');
    await consumer.close();
  });

  it('a seq gap and a reset end in unverified, then an exact resync', async () => {
    const kg = await fresh('gap');
    const E = relation('E', { a: 'int', b: 'int' });
    await kg.define(E);
    await kg.insert(E, [{ a: 1, b: 1 }]);
    const sub = kg.subscribe(E);
    const consumer = new Consumer(sub);
    await waitFor(() => consumer.rows.size === 1);

    inject(kg, {
      type: 'subscription_delta', subscription: sub.id, generation: generationOf(kg, sub),
      knowledge_graph: kg.name, seq: 7, revision: 1e9, columns: ['a', 'b'], inserted: [[9, 9]], retracted: [],
    });
    await kg.insert(E, { a: 2, b: 2 });
    await converges(kg, consumer, '?e(A, B)');
    expect(consumer.kinds()).toEqual(['snapshot', 'unverified:seq_gap', 'resync']);
    expect(consumer.events[2].inserted).toEqual([{ a: 2, b: 2 }]);

    // A real reset removes the subscription on the server first.
    await kg.execute(`.unsubscribe ${sub.id}`);
    inject(kg, {
      type: 'subscription_reset', subscription: sub.id, generation: generationOf(kg, sub),
      message: 'Delta 4 has a row over the message limit. The subscription was removed; subscribe again.',
    });
    await kg.delete(E, { a: 1, b: 1 });
    await converges(kg, consumer, '?e(A, B)');
    expect(consumer.kinds().slice(3)).toEqual(['unverified:subscription_reset', 'resync']);
    expect(consumer.events[4].retracted).toEqual([{ a: 1, b: 1 }]);
    expect(sub.stats.resubscribes).toBe(2);
    await consumer.close();
  });

  it('a slow consumer gets unverified, then a resync once it has read the queue', async () => {
    const kg = await fresh('slow');
    const E = relation('E', { a: 'int', b: 'int' });
    await kg.define(E);
    const sub = kg.subscribe(E, { queue: 2 });
    const first = await sub.next();
    expect(first.value?.kind).toBe('snapshot');
    for (let i = 0; i < 6; i++) await kg.insert(E, { a: i, b: i });
    await kg.delete(E, { a: 0, b: 0 });
    // Not reading: the deltas pile up past the queue.
    await new Promise((resolve) => setTimeout(resolve, 500));
    const consumer = new Consumer(sub);
    consumer.apply(first.value!);
    await converges(kg, consumer, '?e(A, B)');
    const kinds = consumer.kinds();
    expect(kinds).toContain('unverified:slow_consumer');
    expect(kinds[kinds.length - 1]).toBe('resync');
    await consumer.close();
  });

  it('reconnects: unverified at once, then a resync with only what changed meanwhile', async () => {
    const listener = client();
    await listener.connect();
    try {
      const writer = await fresh('reconnect');
      const E = relation('E', { a: 'int', b: 'int' });
      await writer.define(E);
      await writer.insert(E, [{ a: 1, b: 1 }, { a: 2, b: 2 }]);
      const kg = listener.knowledgeGraph(writer.name);
      const consumer = new Consumer(kg.subscribe(E));
      await waitFor(() => consumer.rows.size === 2);
      const reconnected = new Promise((resolve) => kg.connection.events.addEventListener('reconnected', resolve));
      (kg.connection as unknown as { ws: { terminate(): void } }).ws.terminate();
      await writer.delete(E, { a: 1, b: 1 });
      await writer.insert(E, { a: 3, b: 3 });
      await reconnected;
      await converges(writer, consumer, '?e(A, B)');
      expect(consumer.kinds()).toEqual(['snapshot', 'unverified:connection_lost', 'resync']);
      const resync = consumer.events[2];
      expect(resync.inserted).toEqual([{ a: 3, b: 3 }]);
      expect(resync.retracted).toEqual([{ a: 1, b: 1 }]);
      await consumer.close();
    } finally {
      await listener.close();
    }
  });

  it('an ACL revoke ends the subscription: unverified, then SubscriptionRejectedError', async () => {
    const kg = await fresh('acl');
    const E = relation('E', { a: 'int', b: 'int' });
    await kg.define(E);
    await kg.insert(E, { a: 1, b: 1 });
    const user = `${PREFIX}_reader`;
    await il.dropUser(user).catch(() => undefined);
    await il.createUser(user, 'reader-password-1', 'viewer');
    await kg.grantAccess(user, 'viewer');
    // Lazy: the reader may read only this graph, so it never opens the default one.
    const reader = client({ username: user, password: 'reader-password-1' });
    try {
      const consumer = new Consumer(reader.knowledgeGraph(kg.name).subscribe(E));
      await waitFor(() => consumer.rows.size === 1);
      await kg.revokeAccess(user);
      await kg.insert(E, { a: 2, b: 2 });
      await waitFor(() => consumer.error !== undefined);
      expect(consumer.kinds()).toEqual(['snapshot', 'unverified:subscription_error']);
      expect(consumer.error).toBeInstanceOf(SubscriptionRejectedError);
      expect((consumer.error as SubscriptionRejectedError).reason).toBe('access_denied');
    } finally {
      await reader.close();
      await il.dropUser(user).catch(() => undefined);
    }
  });

  it('refuses what a standing query cannot track, before or at the engine', async () => {
    const kg = await fresh('refused');
    const E = relation('E', { a: 'int', b: 'int' });
    await kg.define(E);
    await kg.session.defineRules('mine', ['a'], [from(E).select({ a: E.col('a') })]);
    const Mine = relation('Mine', { a: 'int' });
    const cases: Array<[() => Subscription, string]> = [
      [() => kg.subscribe({ select: [E], limit: 1 }), 'limit_offset'],
      [() => kg.subscribe({ select: [E], where: OR(E.col('a').eq(1), E.col('a').eq(2)) }), 'or_branches'],
      [() => kg.subscribe({ select: [E.col('a').toAst(), count(E.col('b'))], join: [E] }), 'session_view'],
      [() => kg.subscribe(Mine), 'session_view'],
      [() => kg.subscribe({ iql: '?e(A, B), limit(1)' }), 'limit_offset'],
    ];
    for (const [open, reason] of cases) {
      const error = await (async () => {
        const sub = open();
        await sub.next();
      })().catch((e: unknown) => e);
      expect(error, reason).toBeInstanceOf(SubscriptionRejectedError);
      expect((error as SubscriptionRejectedError).reason).toBe(reason);
    }
  });

  it('watch yields the whole result; on calls back with each change', async () => {
    const kg = await fresh('watch');
    const E = relation('E', { a: 'int', b: 'int' });
    await kg.define(E);
    await kg.insert(E, { a: 1, b: 1 });
    const seen: Change[] = [];
    const handle = kg.on(E, (change) => {
      seen.push(change);
    });
    const levels = kg.watch(E);
    const first = await levels.next();
    expect(first.value).toMatchObject({ rows: [{ a: 1, b: 1 }], verified: true });
    await kg.insert(E, { a: 2, b: 2 });
    const second = await levels.next();
    expect(second.value?.rows).toEqual([{ a: 1, b: 1 }, { a: 2, b: 2 }]);
    expect(second.value!.revision).toBeGreaterThan(first.value!.revision);
    await waitFor(() => seen.length === 2);
    expect(seen.map((c) => c.kind)).toEqual(['snapshot', 'delta']);
    expect(seen[1].inserted).toEqual([{ a: 2, b: 2 }]);
    // Close while idle: a next() waiting on a quiet result ends at once.
    const idle = levels.next();
    await levels.return!();
    expect(await idle).toEqual({ value: undefined, done: true });
    await handle.close();
  });
});
