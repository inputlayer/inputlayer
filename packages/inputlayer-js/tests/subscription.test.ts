/**
 * Subscriptions against a scripted server: chunk assembly, generation and
 * `seq` checks, projection multiplicity, the bounded queue, resubscribing
 * after a gap, a broken stream, a reset, an error and a reconnect, and the
 * refusals. The live suite (`subscriptions.integration.test.ts`) runs the
 * same behaviour on a real engine.
 */

import { afterEach, describe, expect, it } from 'vitest';
import { WebSocketServer, type WebSocket as ServerSocket } from 'ws';
import {
  type Change,
  ConnectionLostError,
  InputLayer,
  type KnowledgeGraph,
  relation,
  type Subscription,
  SubscriptionRejectedError,
} from '../src/index';

type Frame = Record<string, unknown>;

interface Subscribed {
  socket: ServerSocket;
  id: string;
  query: string;
  generation: number;
}

/**
 * A server that answers `.subscribe` with the snapshot `snapshot` gives,
 * `.unsubscribe` and `.session` with empty results, and lets the test push
 * frames to the subscription it holds.
 */
class Engine {
  readonly programs: string[] = [];
  readonly sockets: ServerSocket[] = [];
  readonly subscribed: Subscribed[] = [];
  snapshot: (query: string) => { columns: string[]; rows: unknown[][]; revision: number } | { error: string } = () => ({
    columns: ['a', 'b'],
    rows: [],
    revision: 1,
  });
  sessionRules: string[] = [];
  /** Requests not answered until `release()`. */
  hold = false;
  private held: Array<() => void> = [];
  private generation = 0;
  private readonly server: WebSocketServer;

  static async start(): Promise<Engine> {
    const engine = new Engine();
    await new Promise((resolve) => engine.server.once('listening', resolve));
    return engine;
  }

  private constructor() {
    this.server = new WebSocketServer({ port: 0, host: '127.0.0.1' });
    this.server.on('connection', (socket) => {
      this.sockets.push(socket);
      socket.on('message', (data) => this.onMessage(socket, JSON.parse(String(data)) as Frame));
    });
  }

  get url(): string {
    const address = this.server.address();
    if (typeof address !== 'object' || address === null) throw new Error('not listening');
    return `ws://127.0.0.1:${address.port}/ws`;
  }

  /** The subscription last opened. */
  get sub(): Subscribed {
    return this.subscribed[this.subscribed.length - 1];
  }

  private onMessage(socket: ServerSocket, msg: Frame): void {
    const send = (frame: Frame) => socket.send(JSON.stringify({ id: msg.id, ...frame }));
    if (msg.type === 'login' || msg.type === 'authenticate') {
      send({
        type: 'authenticated', session_id: 's', knowledge_graph: 'kg', version: 'test',
        role: 'admin', protocol_version: 3, stream_epoch: 'e1',
      });
      return;
    }
    if (msg.type === 'ping') return send({ type: 'pong' });
    if (msg.type !== 'execute') return;
    const program = String(msg.program);
    this.programs.push(program);
    const answer = () => {
      const empty = { type: 'result', columns: [], rows: [], row_count: 0, total_count: 0, truncated: false, execution_time_ms: 0 };
      const m = /^\.subscribe (\S+) (.*)$/s.exec(program);
      if (!m) return send(empty);
      const snapshot = this.snapshot(m[2]);
      if ('error' in snapshot) return send({ type: 'error', message: snapshot.error });
      this.generation += 1;
      this.subscribed.push({ socket, id: m[1], query: m[2], generation: this.generation });
      send({
        ...empty, columns: snapshot.columns, rows: snapshot.rows, row_count: snapshot.rows.length,
        subscribed: { subscription: m[1], generation: this.generation, revision: snapshot.revision },
      });
    };
    if (program === '.session') {
      const rows = this.sessionRules.length === 0
        ? [['No session data defined.']]
        : [[`Session rules (${this.sessionRules.length}):`], ...this.sessionRules.map((r, i) => [`  ${i + 1}. ${r}`])];
      return send({ type: 'result', columns: ['message'], rows, row_count: rows.length, total_count: rows.length, truncated: false, execution_time_ms: 0 });
    }
    if (this.hold) this.held.push(answer);
    else answer();
  }

  release(): void {
    this.hold = false;
    for (const answer of this.held.splice(0)) answer();
  }

  /** Push a frame to the last subscription, with its id and generation unless given. */
  push(frame: Frame): void {
    const { socket, id, generation } = this.sub;
    socket.send(JSON.stringify({ subscription: id, generation, knowledge_graph: 'kg', ...frame }));
  }

  delta(seq: number, revision: number, inserted: unknown[][], retracted: unknown[][] = [], columns = ['a', 'b']): void {
    this.push({ type: 'subscription_delta', seq, revision, columns, inserted, retracted });
  }

  async close(): Promise<void> {
    for (const socket of this.sockets) socket.terminate();
    await new Promise((resolve) => this.server.close(() => resolve(undefined)));
  }
}

let engine: Engine | undefined;
let il: InputLayer | undefined;

afterEach(async () => {
  await il?.close();
  await engine?.close();
  il = undefined;
  engine = undefined;
});

const E = relation('E', { a: 'int', b: 'int' });

async function graph(opts: { autoReconnect?: boolean } = {}): Promise<KnowledgeGraph> {
  engine = await Engine.start();
  il = new InputLayer({
    url: engine.url, username: 'u', password: 'p', keepaliveMs: 0,
    reconnectDelay: 0.01, autoReconnect: opts.autoReconnect ?? false,
  });
  return il.knowledgeGraph('kg');
}

/** Read the next `n` events. */
async function take(sub: Subscription, n: number): Promise<Change[]> {
  const out: Change[] = [];
  for (let i = 0; i < n; i++) {
    const next = await sub.next();
    if (next.done) throw new Error(`ended after ${out.length} event(s)`);
    out.push(next.value);
  }
  return out;
}

const kinds = (changes: Change[]) =>
  changes.map((c) => (c.kind === 'unverified' ? `unverified:${c.reason}` : c.kind));

async function waitFor(predicate: () => boolean, timeoutMs = 5000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error('condition not met in time');
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}

describe('subscribe', () => {
  it('yields the snapshot, then deltas, with revisions and seqs', async () => {
    const kg = await graph();
    engine!.snapshot = () => ({ columns: ['a', 'b'], rows: [[1, 1]], revision: 5 });
    const sub = kg.subscribe(E);
    const [snapshot] = await take(sub, 1);
    expect(snapshot).toEqual({ kind: 'snapshot', inserted: [{ a: 1, b: 1 }], retracted: [], revision: 5, seq: 0, verified: true });
    expect(engine!.sub.query).toBe('?e(A, B)');
    engine!.delta(1, 6, [[2, 2]], [[1, 1]]);
    const [delta] = await take(sub, 1);
    expect(delta).toEqual({ kind: 'delta', inserted: [{ a: 2, b: 2 }], retracted: [{ a: 1, b: 1 }], revision: 6, seq: 1, verified: true });
    await sub.close();
    await waitFor(() => engine!.programs.includes(`.unsubscribe ${sub.id}`));
  });

  it('assembles a streamed delta and checks it against its end', async () => {
    const kg = await graph();
    const sub = kg.subscribe(E);
    await take(sub, 1);
    engine!.push({ type: 'subscription_delta_start', seq: 1, revision: 7, columns: ['a', 'b'] });
    engine!.push({ type: 'subscription_delta_chunk', seq: 1, chunk_index: 0, inserted: [[1, 1]], retracted: [] });
    engine!.push({ type: 'subscription_delta_chunk', seq: 1, chunk_index: 1, inserted: [[2, 2]], retracted: [] });
    engine!.push({ type: 'subscription_delta_end', seq: 1, chunk_count: 2, inserted_count: 2, retracted_count: 0 });
    const [delta] = await take(sub, 1);
    expect(delta).toMatchObject({ kind: 'delta', inserted: [{ a: 1, b: 1 }, { a: 2, b: 2 }], revision: 7, seq: 1 });
    await sub.close();
  });

  it.each([
    ['a missing chunk', [
      { type: 'subscription_delta_start', seq: 1, revision: 7, columns: ['a', 'b'] },
      { type: 'subscription_delta_chunk', seq: 1, chunk_index: 1, inserted: [[1, 1]], retracted: [] },
    ]],
    ['a count mismatch', [
      { type: 'subscription_delta_start', seq: 1, revision: 7, columns: ['a', 'b'] },
      { type: 'subscription_delta_chunk', seq: 1, chunk_index: 0, inserted: [[1, 1]], retracted: [] },
      { type: 'subscription_delta_end', seq: 1, chunk_count: 1, inserted_count: 2, retracted_count: 0 },
    ]],
    ['a delta inside a stream', [
      { type: 'subscription_delta_start', seq: 1, revision: 7, columns: ['a', 'b'] },
      { type: 'subscription_delta', seq: 2, revision: 8, columns: ['a', 'b'], inserted: [[1, 1]], retracted: [] },
    ]],
  ])('a broken stream (%s) is unverified, then a resync', async (_name, frames) => {
    const kg = await graph();
    const sub = kg.subscribe(E);
    await take(sub, 1);
    engine!.snapshot = () => ({ columns: ['a', 'b'], rows: [[1, 1]], revision: 9 });
    for (const frame of frames) engine!.push(frame);
    const events = await take(sub, 2);
    expect(kinds(events)).toEqual(['unverified:broken_stream', 'resync']);
    expect(events[0]).toMatchObject({ inserted: [], retracted: [], verified: false, revision: 1, seq: 0 });
    expect(events[1]).toMatchObject({ inserted: [{ a: 1, b: 1 }], retracted: [], revision: 9, verified: true });
    // The server still held the old subscription: it was ended first.
    const programs = engine!.programs.filter((p) => p.includes(sub.id));
    expect(programs.map((p) => p.split(' ')[0])).toEqual(['.subscribe', '.unsubscribe', '.subscribe']);
    await sub.close();
  });

  it('a seq gap is unverified, then a resync holding only the difference', async () => {
    const kg = await graph();
    engine!.snapshot = () => ({ columns: ['a', 'b'], rows: [[1, 1], [2, 2]], revision: 1 });
    const sub = kg.subscribe(E);
    await take(sub, 1);
    engine!.delta(1, 2, [[3, 3]]);
    await take(sub, 1);
    engine!.snapshot = () => ({ columns: ['a', 'b'], rows: [[2, 2], [3, 3], [4, 4]], revision: 6 });
    engine!.delta(3, 5, [[9, 9]]);
    const events = await take(sub, 2);
    expect(kinds(events)).toEqual(['unverified:seq_gap', 'resync']);
    expect(events[0]).toMatchObject({ revision: 2, seq: 1 });
    expect(events[1]).toMatchObject({ inserted: [{ a: 4, b: 4 }], retracted: [{ a: 1, b: 1 }], revision: 6, seq: 0 });
    // The new generation counts from seq 1 again.
    engine!.delta(1, 7, [[5, 5]]);
    expect((await take(sub, 1))[0]).toMatchObject({ kind: 'delta', inserted: [{ a: 5, b: 5 }], seq: 1 });
    expect(sub.stats.resubscribes).toBe(1);
    await sub.close();
  });

  it('drops pushes of an earlier generation and counts them', async () => {
    const kg = await graph();
    const sub = kg.subscribe(E);
    await take(sub, 1);
    const current = engine!.sub.generation;
    engine!.push({ type: 'subscription_delta', generation: current - 1, seq: 1, revision: 2, columns: ['a', 'b'], inserted: [[8, 8]], retracted: [] });
    engine!.delta(1, 2, [[1, 1]]);
    expect((await take(sub, 1))[0]).toMatchObject({ kind: 'delta', inserted: [{ a: 1, b: 1 }] });
    expect(sub.stats.staleDropped).toBe(1);
    await sub.close();
  });

  it('a reset reopens without unsubscribing; an error unsubscribes first', async () => {
    const kg = await graph();
    const sub = kg.subscribe(E);
    await take(sub, 1);
    engine!.push({ type: 'subscription_reset', message: 'Delta 4 has a row over the limit' });
    const reset = await take(sub, 2);
    expect(kinds(reset)).toEqual(['unverified:subscription_reset', 'resync']);
    expect(reset[0].message).toBe('Delta 4 has a row over the limit');
    engine!.push({ type: 'subscription_error', message: 'Evaluation failed' });
    expect(kinds(await take(sub, 2))).toEqual(['unverified:subscription_error', 'resync']);
    const programs = engine!.programs.filter((p) => p.includes(sub.id)).map((p) => p.split(' ')[0]);
    expect(programs).toEqual(['.subscribe', '.subscribe', '.unsubscribe', '.subscribe']);
    await sub.close();
  });

  it('counts the support of a projected row: retracted only with its last tuple', async () => {
    const kg = await graph();
    engine!.snapshot = () => ({ columns: ['A', 'B'], rows: [[1, 1], [1, 2]], revision: 1 });
    const sub = kg.subscribe({ select: [E.col('a').toAst()], join: [E] });
    const [snapshot] = await take(sub, 1);
    expect(snapshot.inserted).toEqual([{ A: 1 }]);
    engine!.delta(1, 2, [], [[1, 1]], ['A', 'B']);
    // Support moved, the row stayed: nothing to report; the next change is the next event.
    engine!.delta(2, 3, [[2, 1]], [[1, 2]], ['A', 'B']);
    const [delta] = await take(sub, 1);
    expect(delta).toMatchObject({ kind: 'delta', inserted: [{ A: 2 }], retracted: [{ A: 1 }], seq: 2, revision: 3 });
    await sub.close();
  });

  it('a slow consumer gets unverified, and the resync waits until it has read the queue', async () => {
    const kg = await graph();
    const sub = kg.subscribe(E, { queue: 2 });
    await take(sub, 1);
    for (let seq = 1; seq <= 4; seq++) engine!.delta(seq, seq + 1, [[seq, seq]]);
    await waitFor(() => engine!.programs.includes(`.unsubscribe ${sub.id}`));
    engine!.snapshot = () => ({ columns: ['a', 'b'], rows: [[1, 1], [2, 2], [3, 3], [4, 4]], revision: 5 });
    const subscribes = () => engine!.programs.filter((p) => p.startsWith(`.subscribe ${sub.id}`)).length;
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(subscribes()).toBe(1);
    const events = await take(sub, 4);
    expect(kinds(events)).toEqual(['delta', 'delta', 'unverified:slow_consumer', 'resync']);
    expect(events[3]).toMatchObject({ inserted: [{ a: 3, b: 3 }, { a: 4, b: 4 }], retracted: [] });
    expect(subscribes()).toBe(2);
    await sub.close();
  });

  it('a reconnect is unverified at once, then a resync on the new connection', async () => {
    const kg = await graph({ autoReconnect: true });
    engine!.snapshot = () => ({ columns: ['a', 'b'], rows: [[1, 1]], revision: 1 });
    const sub = kg.subscribe(E);
    await take(sub, 1);
    engine!.snapshot = () => ({ columns: ['a', 'b'], rows: [[2, 2]], revision: 4 });
    engine!.sockets[0].terminate();
    const events = await take(sub, 2);
    expect(kinds(events)).toEqual(['unverified:connection_lost', 'resync']);
    expect(events[1]).toMatchObject({ inserted: [{ a: 2, b: 2 }], retracted: [{ a: 1, b: 1 }], revision: 4 });
    expect(engine!.sub.socket).toBe(engine!.sockets[1]);
    await sub.close();
  });

  it('ends with ConnectionLostError when the connection is gone for good', async () => {
    const kg = await graph({ autoReconnect: false });
    const sub = kg.subscribe(E);
    await take(sub, 1);
    engine!.sockets[0].terminate();
    expect(kinds(await take(sub, 1))).toEqual(['unverified:connection_lost']);
    await expect(sub.next()).rejects.toBeInstanceOf(ConnectionLostError);
    expect((await sub.next()).done).toBe(true);
  });

  it('ends quietly when the client closes', async () => {
    const kg = await graph();
    const sub = kg.subscribe(E);
    await take(sub, 1);
    const pending = sub.next();
    await il!.close();
    expect((await pending).done).toBe(true);
  });

  it('holds a push that arrives before the snapshot reply and applies it after', async () => {
    const kg = await graph();
    engine!.hold = true;
    const sub = kg.subscribe(E);
    const first = take(sub, 2);
    await waitFor(() => engine!.programs.some((p) => p.startsWith('.subscribe')));
    const id = /^\.subscribe (\S+)/.exec(engine!.programs.find((p) => p.startsWith('.subscribe'))!)![1];
    // The reply names generation 1; its first delta overtakes it.
    engine!.sockets[0].send(JSON.stringify({
      type: 'subscription_delta', subscription: id, generation: 1, knowledge_graph: 'kg',
      seq: 1, revision: 2, columns: ['a', 'b'], inserted: [[1, 1]], retracted: [],
    }));
    engine!.release();
    expect(kinds(await first)).toEqual(['snapshot', 'delta']);
    await sub.close();
  });
});

describe('refusals', () => {
  it('refuses limit, offset, OR and aggregates before sending anything', async () => {
    const kg = await graph();
    const { count, OR } = await import('../src/index');
    for (const [target, reason] of [
      [{ select: [E], limit: 5 }, 'limit_offset'],
      [{ select: [E], offset: 5 }, 'limit_offset'],
      [{ select: [E], where: OR(E.col('a').eq(1), E.col('b').eq(2)) }, 'or_branches'],
      [{ select: [E.col('a').toAst(), count(E.col('b'))], join: [E] }, 'session_view'],
    ] as const) {
      let error: unknown;
      try {
        kg.subscribe(target as never);
      } catch (e) {
        error = e;
      }
      expect(error, reason).toBeInstanceOf(SubscriptionRejectedError);
      expect((error as SubscriptionRejectedError).reason).toBe(reason);
    }
    expect(engine!.programs).toEqual([]);
  });

  it('refuses a session rule, which the engine would accept and never push', async () => {
    const kg = await graph();
    engine!.sessionRules = ['e(A, B) <- other(A, B)'];
    await expect(kg.subscribe(E).next()).rejects.toMatchObject({ reason: 'session_view' });
    expect(engine!.programs.some((p) => p.startsWith('.subscribe'))).toBe(false);
  });

  it.each([
    ['Subscriptions track the whole result set; remove limit/offset from the query.', 'limit_offset'],
    ['Subscription result exceeds storage.performance.max_result_rows (10); no complete result to deliver. Narrow the query.', 'result_cap'],
    ["Access denied to knowledge graph 'kg'.", 'access_denied'],
    ["Subscription 'x' already exists on this connection. Use .unsubscribe x first or pick another id.", 'id_taken'],
    ['Subscription limit reached (64 per connection, see http.rate_limit.ws_max_subscriptions). Unsubscribe from one first.', 'subscription_limit'],
    ['Parse error', 'rejected'],
  ])('types the engine refusal "%s" as %s', async (message, reason) => {
    const kg = await graph();
    engine!.snapshot = () => ({ error: message });
    const error = await kg.subscribe({ iql: '?e(A, B)' }).next().catch((e: unknown) => e);
    expect(error).toBeInstanceOf(SubscriptionRejectedError);
    expect(error).toMatchObject({ reason, message, iql: `.subscribe ${/il_sub_\d+/.exec(engine!.programs[0])![0]} ?e(A, B)` });
  });

  it('a refused reopen ends the iterator after its unverified event', async () => {
    const kg = await graph();
    const sub = kg.subscribe(E);
    await take(sub, 1);
    engine!.snapshot = () => ({ error: "Access denied to knowledge graph 'kg'." });
    engine!.push({ type: 'subscription_error', message: "Access denied to knowledge graph 'kg'." });
    expect(kinds(await take(sub, 1))).toEqual(['unverified:subscription_error']);
    await expect(sub.next()).rejects.toMatchObject({ reason: 'access_denied' });
    expect((await sub.next()).done).toBe(true);
  });
});

describe('watch and on', () => {
  it('watch yields the whole result, unverified with the rows held', async () => {
    const kg = await graph();
    engine!.snapshot = () => ({ columns: ['a', 'b'], rows: [[1, 1]], revision: 1 });
    const live = kg.watch(E);
    expect((await live.next()).value).toEqual({ rows: [{ a: 1, b: 1 }], revision: 1, verified: true });
    engine!.delta(1, 2, [[2, 2]]);
    expect((await live.next()).value).toEqual({ rows: [{ a: 1, b: 1 }, { a: 2, b: 2 }], revision: 2, verified: true });
    engine!.snapshot = () => ({ columns: ['a', 'b'], rows: [[2, 2]], revision: 3 });
    engine!.delta(5, 9, [[7, 7]]);
    expect((await live.next()).value).toEqual({ rows: [{ a: 1, b: 1 }, { a: 2, b: 2 }], revision: 2, verified: false, reason: 'seq_gap' });
    expect((await live.next()).value).toEqual({ rows: [{ a: 2, b: 2 }], revision: 3, verified: true });
    await live.return();
    await waitFor(() => engine!.programs.some((p) => p.startsWith('.unsubscribe')));
  });

  it('on calls back in order and reports a failing callback without stopping', async () => {
    const kg = await graph();
    const seen: string[] = [];
    const errors: unknown[] = [];
    const handle = kg.on(E, async (change) => {
      seen.push(change.kind);
      if (change.kind === 'snapshot') throw new Error('boom');
    }, { onError: (e) => errors.push(e) });
    await waitFor(() => seen.length === 1);
    engine!.delta(1, 2, [[1, 1]]);
    await waitFor(() => seen.length === 2);
    expect(seen).toEqual(['snapshot', 'delta']);
    expect(errors).toHaveLength(1);
    await handle.close();
  });

  it('on reports the error that ends the subscription', async () => {
    const kg = await graph();
    engine!.snapshot = () => ({ error: "Access denied to knowledge graph 'kg'." });
    const errors: unknown[] = [];
    kg.on(E, () => undefined, { onError: (e) => errors.push(e) });
    await waitFor(() => errors.length === 1);
    expect(errors[0]).toBeInstanceOf(SubscriptionRejectedError);
    expect(errors[0]).not.toBeInstanceOf(ConnectionLostError);
  });
});
