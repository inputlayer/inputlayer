/**
 * Subscriptions against a scripted server: chunk assembly, generation and
 * `seq` checks, projection multiplicity, the bounded queue, resubscribing
 * after a gap, a broken stream, a reset, an error and a reconnect, and the
 * refusals; then the same for subscription groups (all members applied
 * whole or not at all), and snapshot reads. The live suite
 * (`subscriptions.integration.test.ts`) runs the same behaviour on a real
 * engine.
 */

import { afterEach, describe, expect, it } from 'vitest';
import { WebSocketServer, type WebSocket as ServerSocket } from 'ws';
import {
  CancelledError,
  type Change,
  ConnectionError,
  ConnectionLostError,
  DeadlineExceededError,
  type GroupChange,
  type GroupSubscription,
  InputLayer,
  InternalError,
  type KnowledgeGraph,
  type NamedQuery,
  QueryError,
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
  /** A group's queries; unset for `.subscribe`. */
  queries?: NamedQuery[];
}

/** One result of a snapshot the engine answers with. */
interface Answer {
  columns: string[];
  rows: unknown[][];
  truncated?: boolean;
}

/** A `snapshot` reply as frames: one, or streamed a row per chunk. */
function snapshotFrames(
  queries: NamedQuery[],
  answers: Answer[],
  revision: number,
  streamed: boolean,
  subscribed?: Frame,
): Frame[] {
  const head = { knowledge_graph: 'kg', revision, execution_time_ms: 0, ...(subscribed ? { subscribed } : {}) };
  if (!streamed) {
    return [{
      type: 'snapshot', ...head,
      results: answers.map((a, i) => ({
        name: queries[i].name, columns: a.columns, rows: a.rows, total_count: a.rows.length, truncated: a.truncated ?? false,
      })),
    }];
  }
  const frames: Frame[] = [{
    type: 'snapshot_start', ...head,
    results: answers.map((a, i) => ({
      name: queries[i].name, columns: a.columns, row_count: a.rows.length, total_count: a.rows.length, truncated: a.truncated ?? false,
    })),
  }];
  let chunk = 0;
  answers.forEach((a, result) => {
    for (const row of a.rows) frames.push({ type: 'snapshot_chunk', result, chunk_index: chunk++, rows: [row] });
  });
  frames.push({ type: 'snapshot_end', chunk_count: chunk });
  return frames;
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
  /** A group's snapshot, one answer per query (default: empty results at revision 1). */
  groupSnapshot: (queries: NamedQuery[]) => { answers: Answer[]; revision: number } | { error: string } = (queries) => ({
    answers: queries.map(() => ({ columns: ['a', 'b'], rows: [] })),
    revision: 1,
  });
  /** The reply frames to a `read` (default: an empty snapshot at revision 1); none to leave it unanswered. */
  read: (msg: Frame) => Frame[] = (msg) =>
    snapshotFrames(msg.queries as NamedQuery[], (msg.queries as NamedQuery[]).map(() => ({ columns: ['a', 'b'], rows: [] })), 1, false);
  /** Stream group snapshots a row per chunk. */
  streamSnapshots = false;
  /** Every `read`, `subscribe` and `cancel` frame. */
  readonly frames: Frame[] = [];
  sessionRules: string[] = [];
  /** The /ws protocol version the engine reports on login. */
  protocolVersion = 5;
  /** Drop the connection on the next `.session` request. */
  dropOnSession = false;
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
        role: 'admin', protocol_version: this.protocolVersion, stream_epoch: 'e1',
      });
      return;
    }
    if (msg.type === 'ping') return send({ type: 'pong' });
    if (msg.type === 'read' || msg.type === 'cancel') {
      this.frames.push(msg);
      if (msg.type === 'read') for (const frame of this.read(msg)) send(frame);
      return;
    }
    if (msg.type === 'subscribe') {
      this.frames.push(msg);
      const answer = () => {
        const queries = msg.queries as NamedQuery[];
        const snapshot = this.groupSnapshot(queries);
        if ('error' in snapshot) return send({ type: 'error', message: snapshot.error });
        this.generation += 1;
        const id = String(msg.subscription);
        this.subscribed.push({ socket, id, query: '', generation: this.generation, queries });
        const subscribed = { subscription: id, generation: this.generation, revision: snapshot.revision };
        for (const frame of snapshotFrames(queries, snapshot.answers, snapshot.revision, this.streamSnapshots, subscribed)) {
          send(frame);
        }
      };
      if (this.hold) this.held.push(answer);
      else answer();
      return;
    }
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
      if (this.dropOnSession) {
        this.dropOnSession = false;
        return socket.terminate();
      }
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

  /** Push a group delta: each member's inserted and retracted rows, by name, in the group's order. */
  groupDelta(seq: number, revision: number, members: Record<string, [unknown[][], unknown[][]?]>, columns = ['a', 'b']): void {
    this.push({
      type: 'subscription_group_delta', seq, revision,
      members: Object.entries(members).map(([name, [inserted, retracted = []]]) => ({
        name, unchanged: inserted.length === 0 && retracted.length === 0, columns, inserted, retracted,
      })),
    });
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

async function graph(opts: { autoReconnect?: boolean; timeoutGraceMs?: number } = {}): Promise<KnowledgeGraph> {
  engine = await Engine.start();
  il = new InputLayer({
    url: engine.url, username: 'u', password: 'p', keepaliveMs: 0,
    reconnectDelay: 0.01, autoReconnect: opts.autoReconnect ?? false, timeoutGraceMs: opts.timeoutGraceMs,
  });
  return il.knowledgeGraph('kg');
}

/** Read the next `n` events. */
async function take<C = Change>(sub: { next(): Promise<IteratorResult<C>> }, n: number): Promise<C[]> {
  const out: C[] = [];
  for (let i = 0; i < n; i++) {
    const next = await sub.next();
    if (next.done) throw new Error(`ended after ${out.length} event(s)`);
    out.push(next.value);
  }
  return out;
}

const kinds = (changes: Array<{ kind: string; reason?: string }>) =>
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

  it('a connection lost before the first snapshot reopens on the new connection', async () => {
    const kg = await graph({ autoReconnect: true });
    engine!.hold = true;
    engine!.snapshot = () => ({ columns: ['a', 'b'], rows: [[1, 1]], revision: 3 });
    const sub = kg.subscribe(E);
    const first = take(sub, 1);
    await waitFor(() => engine!.programs.some((p) => p.startsWith(`.subscribe ${sub.id}`)));
    engine!.hold = false;
    engine!.sockets[0].terminate();
    const [snapshot] = await first;
    expect(snapshot).toMatchObject({ kind: 'snapshot', inserted: [{ a: 1, b: 1 }], revision: 3, verified: true });
    expect(engine!.sub.socket).toBe(engine!.sockets[1]);
    await sub.close();
  });

  it('a connection lost during the session-rule check still yields the snapshot', async () => {
    const kg = await graph({ autoReconnect: true });
    engine!.dropOnSession = true;
    engine!.snapshot = () => ({ columns: ['a', 'b'], rows: [[1, 1]], revision: 2 });
    const sub = kg.subscribe(E);
    const [snapshot] = await take(sub, 1);
    expect(snapshot).toMatchObject({ kind: 'snapshot', inserted: [{ a: 1, b: 1 }], revision: 2 });
    expect(engine!.programs.filter((p) => p === '.session')).toHaveLength(2);
    expect(engine!.sub.socket).toBe(engine!.sockets[1]);
    await sub.close();
  });

  it('a .subscribe reply lost past its deadline is retried after unsubscribing the id', async () => {
    const kg = await graph({ timeoutGraceMs: 20 });
    engine!.hold = true;
    const sub = kg.subscribe(E, { timeoutMs: 50 });
    const first = take(sub, 1);
    await waitFor(() => engine!.programs.some((p) => p.startsWith(`.subscribe ${sub.id}`)));
    // The first .subscribe is never answered; the retry is.
    engine!.hold = false;
    expect(kinds(await first)).toEqual(['snapshot']);
    const programs = engine!.programs.filter((p) => p.includes(sub.id)).map((p) => p.split(' ')[0]);
    expect(programs).toEqual(['.subscribe', '.unsubscribe', '.subscribe']);
    await sub.close();
  });

  it('closed while its .subscribe reply is lost past the deadline, it still unsubscribes', async () => {
    const kg = await graph({ timeoutGraceMs: 20 });
    engine!.hold = true;
    const sub = kg.subscribe(E, { timeoutMs: 50 });
    const first = sub.next();
    await waitFor(() => engine!.programs.some((p) => p.startsWith(`.subscribe ${sub.id}`)));
    engine!.hold = false;
    await sub.close();
    expect((await first).done).toBe(true);
    await waitFor(() => engine!.programs.includes(`.unsubscribe ${sub.id}`));
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
    await live.return!();
    await waitFor(() => engine!.programs.some((p) => p.startsWith('.unsubscribe')));
  });

  it('watch closes while idle: return() ends a pending next()', async () => {
    const kg = await graph();
    const live = kg.watch(E);
    expect((await live.next()).value).toEqual({ rows: [], revision: 1, verified: true });
    const pending = live.next();
    const closed = await Promise.race([
      live.return!().then(() => 'closed'),
      new Promise((resolve) => setTimeout(() => resolve('hung'), 1000)),
    ]);
    expect(closed).toBe('closed');
    expect(await pending).toEqual({ value: undefined, done: true });
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

// ── Subscription groups ─────────────────────────────────────────────

const F = relation('F', { a: 'int', c: 'int' });

/** A member's change as `[inserted, retracted]` row values, for short assertions. */
const rowsOfMember = (change: GroupChange, name: string) => {
  const member = change.members[name];
  return [member.inserted.map((r) => Object.values(r)), member.retracted.map((r) => Object.values(r))];
};

describe('subscribeGroup', () => {
  it('opens with one subscribe frame and yields every member at one revision', async () => {
    const kg = await graph();
    engine!.groupSnapshot = () => ({
      answers: [{ columns: ['a', 'b'], rows: [[1, 1]] }, { columns: ['a', 'c'], rows: [] }],
      revision: 5,
    });
    const sub = kg.subscribeGroup({ orders: E, etas: F });
    expect(sub.queries).toEqual({ orders: '?e(A, B)', etas: '?f(A, C)' });
    const [snapshot] = await take(sub, 1);
    expect(snapshot).toEqual({
      kind: 'snapshot',
      members: {
        orders: { inserted: [{ a: 1, b: 1 }], retracted: [], unchanged: false },
        etas: { inserted: [], retracted: [], unchanged: true },
      },
      revision: 5, seq: 0, verified: true,
    });
    expect(Object.keys(snapshot.members)).toEqual(['orders', 'etas']);
    const frame = engine!.frames.find((f) => f.type === 'subscribe')!;
    expect(frame).toMatchObject({
      subscription: sub.id,
      queries: [{ name: 'orders', query: '?e(A, B)' }, { name: 'etas', query: '?f(A, C)' }],
    });
    // The engine takes no deadline for a subscribe.
    expect(frame.timeout_ms).toBeUndefined();
    await sub.close();
    await waitFor(() => engine!.programs.includes(`.unsubscribe ${sub.id}`));
  });

  it('ends with the version refusal, not ConnectionLostError, on an engine older than protocol 5', async () => {
    const kg = await graph();
    engine!.protocolVersion = 4;
    const sub = kg.subscribeGroup({ x: E, y: E });
    const error = await sub.next().catch((e: unknown) => e);
    expect(error).toBeInstanceOf(ConnectionError);
    expect(error).not.toBeInstanceOf(ConnectionLostError);
    expect((error as Error).message).toMatch(/need version 5/);
    expect(engine!.frames.some((f) => f.type === 'subscribe')).toBe(false);
  });

  it('assembles a streamed group snapshot', async () => {
    const kg = await graph();
    engine!.streamSnapshots = true;
    engine!.groupSnapshot = () => ({
      answers: [{ columns: ['a', 'b'], rows: [[1, 1], [2, 2]] }, { columns: ['a', 'c'], rows: [] }, { columns: ['a', 'b'], rows: [[3, 3]] }],
      revision: 4,
    });
    const sub = kg.subscribeGroup({ x: E, y: F, z: E });
    const [snapshot] = await take(sub, 1);
    expect(rowsOfMember(snapshot, 'x')).toEqual([[[1, 1], [2, 2]], []]);
    expect(rowsOfMember(snapshot, 'y')).toEqual([[], []]);
    expect(rowsOfMember(snapshot, 'z')).toEqual([[[3, 3]], []]);
    expect(snapshot.revision).toBe(4);
    await sub.close();
  });

  it('applies a group delta to every member, marking the unchanged ones', async () => {
    const kg = await graph();
    engine!.groupSnapshot = () => ({ answers: [{ columns: ['a', 'b'], rows: [[1, 1]] }, { columns: ['a', 'b'], rows: [] }], revision: 1 });
    const sub = kg.subscribeGroup({ x: E, y: E });
    await take(sub, 1);
    engine!.groupDelta(1, 6, { x: [[[2, 2]], [[1, 1]]], y: [[]] });
    const [delta] = await take(sub, 1);
    expect(delta).toEqual({
      kind: 'delta',
      members: {
        x: { inserted: [{ a: 2, b: 2 }], retracted: [{ a: 1, b: 1 }], unchanged: false },
        y: { inserted: [], retracted: [], unchanged: true },
      },
      revision: 6, seq: 1, verified: true,
    });
    await sub.close();
  });

  it('assembles a streamed group delta, member by member', async () => {
    const kg = await graph();
    engine!.groupSnapshot = () => ({ answers: [{ columns: ['a', 'b'], rows: [[9, 9]] }, { columns: ['a', 'b'], rows: [] }, { columns: ['a', 'b'], rows: [] }], revision: 1 });
    const sub = kg.subscribeGroup({ x: E, y: E, z: E });
    await take(sub, 1);
    engine!.push({
      type: 'subscription_group_delta_start', seq: 1, revision: 7,
      members: [
        { name: 'x', unchanged: false, columns: ['a', 'b'], inserted_count: 2, retracted_count: 1 },
        { name: 'y', unchanged: true, columns: ['a', 'b'], inserted_count: 0, retracted_count: 0 },
        { name: 'z', unchanged: false, columns: ['a', 'b'], inserted_count: 1, retracted_count: 0 },
      ],
    });
    engine!.push({ type: 'subscription_group_delta_chunk', seq: 1, chunk_index: 0, member: 0, inserted: [[1, 1]], retracted: [] });
    engine!.push({ type: 'subscription_group_delta_chunk', seq: 1, chunk_index: 1, member: 0, inserted: [[2, 2]], retracted: [[9, 9]] });
    engine!.push({ type: 'subscription_group_delta_chunk', seq: 1, chunk_index: 2, member: 2, inserted: [[3, 3]], retracted: [] });
    engine!.push({ type: 'subscription_group_delta_end', seq: 1, chunk_count: 3 });
    const [delta] = await take(sub, 1);
    expect(delta).toMatchObject({ kind: 'delta', revision: 7, seq: 1 });
    expect(rowsOfMember(delta, 'x')).toEqual([[[1, 1], [2, 2]], [[9, 9]]]);
    expect(delta.members.y.unchanged).toBe(true);
    expect(rowsOfMember(delta, 'z')).toEqual([[[3, 3]], []]);
    await sub.close();
  });

  const start = (counts: Array<[string, number, number]>) => ({
    type: 'subscription_group_delta_start', seq: 1, revision: 7,
    members: counts.map(([name, i, r]) => ({ name, unchanged: i + r === 0, columns: ['a', 'b'], inserted_count: i, retracted_count: r })),
  });
  const chunk = (chunkIndex: number, member: number, inserted: unknown[][]) =>
    ({ type: 'subscription_group_delta_chunk', seq: 1, chunk_index: chunkIndex, member, inserted, retracted: [] });

  it.each([
    ['a member streamed after a later one', [
      start([['x', 1, 0], ['y', 1, 0]]), chunk(0, 1, [[1, 1]]), chunk(1, 0, [[2, 2]]),
    ]],
    ['a chunk past its member\'s count', [
      start([['x', 1, 0], ['y', 0, 0]]), chunk(0, 0, [[1, 1], [2, 2]]),
    ]],
    ['a chunk for no member', [start([['x', 1, 0], ['y', 0, 0]]), chunk(0, 2, [[1, 1]])]],
    ['an empty chunk', [start([['x', 1, 0], ['y', 0, 0]]), chunk(0, 0, [])]],
    ['a missing chunk', [start([['x', 2, 0], ['y', 0, 0]]), chunk(0, 0, [[1, 1]]), chunk(2, 0, [[2, 2]])]],
    ['counts that do not add up', [
      start([['x', 2, 0], ['y', 0, 0]]), chunk(0, 0, [[1, 1]]),
      { type: 'subscription_group_delta_end', seq: 1, chunk_count: 1 },
    ]],
    ['an end announcing more chunks', [
      start([['x', 1, 0], ['y', 0, 0]]), chunk(0, 0, [[1, 1]]),
      { type: 'subscription_group_delta_end', seq: 1, chunk_count: 2 },
    ]],
    ['a header naming other members', [start([['y', 0, 0], ['x', 1, 0]])]],
    ['a delta inside a stream', [
      start([['x', 1, 0], ['y', 0, 0]]),
      { type: 'subscription_group_delta', seq: 2, revision: 8, members: [] },
    ]],
    ['a delta missing a member', [{
      type: 'subscription_group_delta', seq: 1, revision: 7,
      members: [{ name: 'x', unchanged: false, columns: ['a', 'b'], inserted: [[1, 1]], retracted: [] }],
    }]],
    ['a member marked unchanged with rows', [{
      type: 'subscription_group_delta', seq: 1, revision: 7,
      members: [
        { name: 'x', unchanged: true, columns: ['a', 'b'], inserted: [[1, 1]], retracted: [] },
        { name: 'y', unchanged: true, columns: ['a', 'b'], inserted: [], retracted: [] },
      ],
    }]],
    ['a malformed delta', [{ type: 'subscription_group_delta', seq: 1, revision: 7, members: [{ name: 'x' }, { name: 'y' }] }]],
    ['a single query\'s delta', [
      { type: 'subscription_delta', seq: 1, revision: 7, columns: ['a', 'b'], inserted: [[1, 1]], retracted: [] },
    ]],
  ])('a broken group stream (%s) is unverified, then a resync', async (_name, frames) => {
    const kg = await graph();
    const sub = kg.subscribeGroup({ x: E, y: E });
    await take(sub, 1);
    engine!.groupSnapshot = () => ({ answers: [{ columns: ['a', 'b'], rows: [[1, 1]] }, { columns: ['a', 'b'], rows: [] }], revision: 9 });
    for (const frame of frames) engine!.push(frame as Frame);
    const events = await take(sub, 2);
    expect(kinds(events)).toEqual(['unverified:broken_stream', 'resync']);
    expect(events[0]).toEqual({
      kind: 'unverified',
      members: { x: { inserted: [], retracted: [], unchanged: true }, y: { inserted: [], retracted: [], unchanged: true } },
      revision: 1, seq: 0, verified: false, reason: 'broken_stream',
    });
    expect(rowsOfMember(events[1], 'x')).toEqual([[[1, 1]], []]);
    expect(events[1]).toMatchObject({ revision: 9, verified: true });
    // The server still held the old group: it was ended first.
    const programs = engine!.programs.filter((p) => p.includes(sub.id)).map((p) => p.split(' ')[0]);
    expect(programs).toEqual(['.unsubscribe']);
    expect(engine!.frames.filter((f) => f.type === 'subscribe')).toHaveLength(2);
    await sub.close();
  });

  it('applies a delta to every member or to none', async () => {
    const kg = await graph();
    engine!.groupSnapshot = () => ({ answers: [{ columns: ['a', 'b'], rows: [] }, { columns: ['a', 'b'], rows: [[1, 1]] }], revision: 1 });
    const sub = kg.subscribeGroup({ x: E, y: E });
    await take(sub, 1);
    // y retracts a row it does not hold: x's insert must not be applied either.
    engine!.groupSnapshot = () => ({ answers: [{ columns: ['a', 'b'], rows: [[5, 5]] }, { columns: ['a', 'b'], rows: [[1, 1]] }], revision: 3 });
    engine!.groupDelta(1, 2, { x: [[[5, 5]]], y: [[], [[7, 7]]] });
    const events = await take(sub, 2);
    expect(kinds(events)).toEqual(['unverified:broken_stream', 'resync']);
    // The consumer never got x's row, so the resync brings it.
    expect(rowsOfMember(events[1], 'x')).toEqual([[[5, 5]], []]);
    expect(events[1].members.y.unchanged).toBe(true);
    await sub.close();
  });

  it('counts each member\'s projected support, and a delta no member sees delivers nothing', async () => {
    const kg = await graph();
    engine!.groupSnapshot = () => ({
      answers: [{ columns: ['A', 'B'], rows: [[1, 1], [1, 2]] }, { columns: ['a', 'b'], rows: [] }],
      revision: 1,
    });
    const sub = kg.subscribeGroup({ firsts: { select: [E.col('a').toAst()], join: [E] }, all: E });
    const [snapshot] = await take(sub, 1);
    expect(snapshot.members.firsts.inserted).toEqual([{ A: 1 }]);
    // Support of firsts' row moved; all did not change: nothing to report.
    engine!.push({
      type: 'subscription_group_delta', seq: 1, revision: 2,
      members: [
        { name: 'firsts', unchanged: false, columns: ['A', 'B'], inserted: [], retracted: [[1, 1]] },
        { name: 'all', unchanged: true, columns: ['a', 'b'], inserted: [], retracted: [] },
      ],
    });
    engine!.push({
      type: 'subscription_group_delta', seq: 2, revision: 3,
      members: [
        { name: 'firsts', unchanged: false, columns: ['A', 'B'], inserted: [[2, 1]], retracted: [[1, 2]] },
        { name: 'all', unchanged: false, columns: ['a', 'b'], inserted: [[4, 4]], retracted: [] },
      ],
    });
    const [delta] = await take(sub, 1);
    expect(delta).toMatchObject({ kind: 'delta', seq: 2, revision: 3 });
    expect(delta.members.firsts).toEqual({ inserted: [{ A: 2 }], retracted: [{ A: 1 }], unchanged: false });
    expect(delta.members.all).toEqual({ inserted: [{ a: 4, b: 4 }], retracted: [], unchanged: false });
    await sub.close();
  });

  it('a member whose support only moved is unchanged in an event another member changed', async () => {
    const kg = await graph();
    engine!.groupSnapshot = () => ({
      answers: [{ columns: ['A', 'B'], rows: [[1, 1], [1, 2]] }, { columns: ['a', 'b'], rows: [] }],
      revision: 1,
    });
    const sub = kg.subscribeGroup({ firsts: { select: [E.col('a').toAst()], join: [E] }, all: E });
    await take(sub, 1);
    engine!.push({
      type: 'subscription_group_delta', seq: 1, revision: 2,
      members: [
        { name: 'firsts', unchanged: false, columns: ['A', 'B'], inserted: [], retracted: [[1, 1]] },
        { name: 'all', unchanged: false, columns: ['a', 'b'], inserted: [[4, 4]], retracted: [] },
      ],
    });
    const [delta] = await take(sub, 1);
    expect(delta.members.firsts).toEqual({ inserted: [], retracted: [], unchanged: true });
    expect(delta.members.all.unchanged).toBe(false);
    await sub.close();
  });

  it('a seq gap is unverified, then a resync with each member\'s exact difference', async () => {
    const kg = await graph();
    engine!.groupSnapshot = () => ({ answers: [{ columns: ['a', 'b'], rows: [[1, 1], [2, 2]] }, { columns: ['a', 'b'], rows: [[7, 7]] }], revision: 1 });
    const sub = kg.subscribeGroup({ x: E, y: E });
    await take(sub, 1);
    engine!.groupDelta(1, 2, { x: [[[3, 3]]], y: [[]] });
    await take(sub, 1);
    engine!.groupSnapshot = () => ({ answers: [{ columns: ['a', 'b'], rows: [[2, 2], [3, 3], [4, 4]] }, { columns: ['a', 'b'], rows: [[7, 7]] }], revision: 6 });
    engine!.groupDelta(3, 5, { x: [[[9, 9]]], y: [[]] });
    const events = await take(sub, 2);
    expect(kinds(events)).toEqual(['unverified:seq_gap', 'resync']);
    expect(events[0]).toMatchObject({ revision: 2, seq: 1 });
    expect(events[1]).toMatchObject({ revision: 6, seq: 0 });
    expect(rowsOfMember(events[1], 'x')).toEqual([[[4, 4]], [[1, 1]]]);
    expect(events[1].members.y).toEqual({ inserted: [], retracted: [], unchanged: true });
    // The new generation counts from seq 1 again.
    engine!.groupDelta(1, 7, { x: [[[5, 5]]], y: [[]] });
    expect((await take(sub, 1))[0]).toMatchObject({ kind: 'delta', seq: 1 });
    expect(sub.stats.resubscribes).toBe(1);
    await sub.close();
  });

  it('drops pushes of an earlier generation and counts them', async () => {
    const kg = await graph();
    const sub = kg.subscribeGroup({ x: E, y: E });
    await take(sub, 1);
    engine!.push({
      type: 'subscription_group_delta', generation: engine!.sub.generation - 1, seq: 1, revision: 2,
      members: [
        { name: 'x', unchanged: false, columns: ['a', 'b'], inserted: [[8, 8]], retracted: [] },
        { name: 'y', unchanged: true, columns: ['a', 'b'], inserted: [], retracted: [] },
      ],
    });
    engine!.groupDelta(1, 2, { x: [[[1, 1]]], y: [[]] });
    expect(rowsOfMember((await take(sub, 1))[0], 'x')).toEqual([[[1, 1]], []]);
    expect(sub.stats.staleDropped).toBe(1);
    await sub.close();
  });

  it('a reset reopens without unsubscribing; an error unsubscribes first', async () => {
    const kg = await graph();
    const sub = kg.subscribeGroup({ x: E, y: F });
    await take(sub, 1);
    engine!.push({ type: 'subscription_reset', message: 'Delta 4 has a row over the limit' });
    const reset = await take(sub, 2);
    expect(kinds(reset)).toEqual(['unverified:subscription_reset', 'resync']);
    expect(reset[0].message).toBe('Delta 4 has a row over the limit');
    engine!.push({ type: 'subscription_error', message: '?f(A, C): Evaluation failed' });
    const error = await take(sub, 2);
    expect(kinds(error)).toEqual(['unverified:subscription_error', 'resync']);
    expect(error[0].message).toBe('?f(A, C): Evaluation failed');
    const programs = engine!.programs.filter((p) => p.includes(sub.id)).map((p) => p.split(' ')[0]);
    expect(programs).toEqual(['.unsubscribe']);
    expect(engine!.frames.filter((f) => f.type === 'subscribe')).toHaveLength(3);
    await sub.close();
    await waitFor(() => engine!.programs.filter((p) => p === `.unsubscribe ${sub.id}`).length === 2);
  });

  it('a slow consumer gets unverified, and the resync waits until it has read the queue', async () => {
    const kg = await graph();
    const sub = kg.subscribeGroup({ x: E, y: E }, { queue: 2 });
    await take(sub, 1);
    for (let seq = 1; seq <= 4; seq++) engine!.groupDelta(seq, seq + 1, { x: [[[seq, seq]]], y: [[]] });
    await waitFor(() => engine!.programs.includes(`.unsubscribe ${sub.id}`));
    engine!.groupSnapshot = () => ({ answers: [{ columns: ['a', 'b'], rows: [[1, 1], [2, 2], [3, 3]] }, { columns: ['a', 'b'], rows: [] }], revision: 5 });
    const events = await take(sub, 4);
    expect(kinds(events)).toEqual(['delta', 'delta', 'unverified:slow_consumer', 'resync']);
    expect(rowsOfMember(events[3], 'x')).toEqual([[[3, 3]], []]);
    await sub.close();
  });

  it('a reconnect is unverified at once, then a resync on the new connection', async () => {
    const kg = await graph({ autoReconnect: true });
    engine!.groupSnapshot = () => ({ answers: [{ columns: ['a', 'b'], rows: [[1, 1]] }, { columns: ['a', 'b'], rows: [] }], revision: 1 });
    const sub = kg.subscribeGroup({ x: E, y: E });
    await take(sub, 1);
    engine!.groupSnapshot = () => ({ answers: [{ columns: ['a', 'b'], rows: [[2, 2]] }, { columns: ['a', 'b'], rows: [] }], revision: 4 });
    engine!.sockets[0].terminate();
    const events = await take(sub, 2);
    expect(kinds(events)).toEqual(['unverified:connection_lost', 'resync']);
    expect(rowsOfMember(events[1], 'x')).toEqual([[[2, 2]], [[1, 1]]]);
    expect(engine!.sub.socket).toBe(engine!.sockets[1]);
    await sub.close();
  });

  it('closed while its subscribe is unanswered, it still unsubscribes', async () => {
    const kg = await graph();
    engine!.hold = true;
    const sub = kg.subscribeGroup({ x: E, y: E });
    const first = sub.next();
    await waitFor(() => engine!.frames.some((f) => f.type === 'subscribe'));
    await sub.close();
    engine!.release();
    expect((await first).done).toBe(true);
    await waitFor(() => engine!.programs.includes(`.unsubscribe ${sub.id}`));
  });

  it('refuses a member a standing query cannot track, naming it, before sending anything', async () => {
    const kg = await graph();
    const { OR } = await import('../src/index');
    for (const [members, reason, text] of [
      [{ ok: E, paged: { select: [E], limit: 5 } }, 'limit_offset', "Query 'paged': "],
      [{ split: { select: [E], where: OR(E.col('a').eq(1), E.col('b').eq(2)) } }, 'or_branches', "Query 'split': "],
      [{}, 'rejected', 'needs at least one query'],
      [{ '': E }, 'rejected', 'needs a name for every query'],
    ] as const) {
      const error = (() => {
        try {
          kg.subscribeGroup(members as never);
        } catch (e) {
          return e;
        }
      })();
      expect(error, reason).toBeInstanceOf(SubscriptionRejectedError);
      expect(error).toMatchObject({ reason });
      expect((error as Error).message).toContain(text);
    }
    expect(engine!.frames).toEqual([]);
  });

  it('refuses a session rule among the members', async () => {
    const kg = await graph();
    engine!.sessionRules = ['f(A, C) <- other(A, C)'];
    await expect(kg.subscribeGroup({ x: E, y: F }).next()).rejects.toMatchObject({ reason: 'session_view' });
    expect(engine!.frames).toEqual([]);
  });

  it.each([
    ["Subscription 'x' already exists on this connection. Use .unsubscribe x first or pick another id.", 'id_taken'],
    ['Subscription limit reached (4 per connection, see http.rate_limit.ws_max_subscriptions; a group counts each of its queries). Unsubscribe from one first.', 'subscription_limit'],
    ['Subscriptions track the whole result set; remove limit/offset from the query.', 'limit_offset'],
    ["A subscription group names two queries 'a'; names must be unique.", 'rejected'],
  ])('types the engine refusal "%s" as %s', async (message, reason) => {
    const kg = await graph();
    engine!.groupSnapshot = () => ({ error: message });
    const error = await kg.subscribeGroup({ x: E, y: { iql: '?f(A, C)' } }).next().catch((e: unknown) => e);
    expect(error).toBeInstanceOf(SubscriptionRejectedError);
    expect(error).toMatchObject({ reason, message, iql: '?e(A, B)\n?f(A, C)' });
  });

  it('a snapshot answering other queries is not taken as the group\'s', async () => {
    const kg = await graph({ timeoutGraceMs: 20 });
    let calls = 0;
    // The first reply names the members in the wrong order; the retry is right.
    const answers = [{ columns: ['a', 'b'], rows: [[1, 1]] }, { columns: ['a', 'b'], rows: [] }];
    engine!.groupSnapshot = (queries) => {
      calls += 1;
      if (calls === 1) queries.reverse();
      return { answers, revision: 2 };
    };
    const sub = kg.subscribeGroup({ x: E, y: E });
    const [snapshot] = await take(sub, 1);
    expect(snapshot.kind).toBe('snapshot');
    expect(rowsOfMember(snapshot, 'x')).toEqual([[[1, 1]], []]);
    expect(calls).toBe(2);
    await sub.close();
  });
});

// ── Snapshot reads ──────────────────────────────────────────────────

describe('read', () => {
  it('reads every query at one revision, rows shaped as kg.query shapes them', async () => {
    const kg = await graph();
    engine!.read = (msg) => snapshotFrames(msg.queries as NamedQuery[], [
      { columns: ['a', 'b'], rows: [[1, 1], [2, 2]] },
      { columns: ['A', 'B'], rows: [[1, 1], [1, 2]], truncated: true },
      { columns: ['x'], rows: [[7]] },
    ], 12, false);
    const result = await kg.read(
      { all: E, firsts: { select: [E.col('a').toAst()], join: [E] }, raw: { iql: '?g(X)' } },
      { timeoutMs: 5000 },
    );
    expect(result).toEqual({
      revision: 12,
      results: { all: [{ a: 1, b: 1 }, { a: 2, b: 2 }], firsts: [{ A: 1 }, { A: 1 }], raw: [{ x: 7 }] },
      truncated: ['firsts'],
    });
    const frame = engine!.frames.find((f) => f.type === 'read')!;
    expect(frame.queries).toEqual([
      { name: 'all', query: '?e(A, B)' }, { name: 'firsts', query: '?e(A, B)' }, { name: 'raw', query: '?g(X)' },
    ]);
    expect(frame.timeout_ms).toBeGreaterThan(4000);
    expect(frame.timeout_ms).toBeLessThanOrEqual(5000);
  });

  it('assembles a streamed snapshot across results', async () => {
    const kg = await graph();
    engine!.read = (msg) => snapshotFrames(msg.queries as NamedQuery[], [
      { columns: ['a', 'b'], rows: [[1, 1], [2, 2]] },
      { columns: ['a', 'c'], rows: [] },
      { columns: ['a', 'b'], rows: [[3, 3]] },
    ], 3, true);
    const result = await kg.read({ x: E, y: F, z: E });
    expect(result.revision).toBe(3);
    expect(result.results).toEqual({ x: [{ a: 1, b: 1 }, { a: 2, b: 2 }], y: [], z: [{ a: 3, b: 3 }] });
  });

  it('compiles limit, offset with a limit, and orderBy into the query', async () => {
    const kg = await graph();
    await kg.read({
      top: { select: [E], orderBy: E.col('b').desc(), limit: 2 },
      page: { select: [E], limit: 2, offset: 4 },
    });
    const [top, page] = engine!.frames.find((f) => f.type === 'read')!.queries as NamedQuery[];
    expect(top.query).toBe('?e(A, B:desc), limit(2)');
    expect(page.query).toBe('?e(A, B), limit(2, 4)');
  });

  it('refuses what is not one query, naming it, before sending anything', async () => {
    const kg = await graph();
    const { count, OR } = await import('../src/index');
    for (const [queries, reason, text] of [
      [{ split: { select: [E], where: OR(E.col('a').eq(1), E.col('b').eq(2)) } }, 'or_branches', "Query 'split': "],
      [{ agg: { select: [E.col('a').toAst(), count(E.col('b'))], join: [E] } }, 'session_view', "Query 'agg': "],
      [{ skip: { select: [E], offset: 5 } }, 'limit_offset', 'add a limit'],
      [{}, 'rejected', 'A read needs at least one query'],
      [{ '': E }, 'rejected', 'needs a name for every query'],
    ] as const) {
      const error = await kg.read(queries as never).catch((e: unknown) => e);
      expect(error, reason).toBeInstanceOf(SubscriptionRejectedError);
      expect(error).toMatchObject({ reason });
      expect((error as Error).message).toContain(text);
    }
    expect(engine!.frames).toEqual([]);
  });

  it('refuses a session rule, listing the session beside the read; raw IQL needs no listing', async () => {
    const kg = await graph();
    engine!.sessionRules = ['f(A, C) <- other(A, C)'];
    const error = await kg.read({ x: E, y: F }).catch((e: unknown) => e);
    expect(error).toBeInstanceOf(SubscriptionRejectedError);
    expect(error).toMatchObject({ reason: 'session_view', message: expect.stringContaining("'f' is a session rule") });
    expect(engine!.programs).toEqual(['.session']);
    expect(engine!.frames.filter((f) => f.type === 'read')).toHaveLength(1);
    await kg.read({ raw: { iql: '?f(A, C)' } });
    expect(engine!.programs).toEqual(['.session']);
  });

  it('fails as a whole with the engine error naming the query', async () => {
    const kg = await graph();
    engine!.read = () => [{
      type: 'error', code: 'validation', message: "Query 'eta': a read query must be a single query",
    }];
    const error = await kg.read({ order: E, eta: { iql: '?f(A, C)\n?f(A, C)' } }).catch((e: unknown) => e);
    expect(error).toBeInstanceOf(QueryError);
    expect(error).toMatchObject({ code: 'validation', message: "Query 'eta': a read query must be a single query" });
    expect((error as QueryError).iql).toBe('?e(A, B)\n?f(A, C)\n?f(A, C)');
  });

  it('an error after a snapshot_start discards the rows received', async () => {
    const kg = await graph();
    engine!.read = (msg) => [
      ...snapshotFrames(msg.queries as NamedQuery[], [{ columns: ['a', 'b'], rows: [[1, 1], [2, 2]] }], 3, true).slice(0, 2),
      { type: 'error', code: 'internal', message: 'Internal server error' },
    ];
    const error = await kg.read({ x: E }).catch((e: unknown) => e);
    expect(error).toBeInstanceOf(QueryError);
    expect(error).toMatchObject({ code: 'internal' });
  });

  const header = (rowCounts: number[]) => ({
    type: 'snapshot_start', knowledge_graph: 'kg', revision: 3, execution_time_ms: 0,
    results: rowCounts.map((n, i) => ({ name: ['x', 'y'][i], columns: ['a', 'b'], row_count: n, total_count: n, truncated: false })),
  });
  const chunk = (chunkIndex: number, result: number, rows: unknown[][]) =>
    ({ type: 'snapshot_chunk', result, chunk_index: chunkIndex, rows });

  it.each([
    ['a missing chunk', [header([2, 0]), chunk(0, 0, [[1, 1]]), chunk(2, 0, [[2, 2]]), { type: 'snapshot_end', chunk_count: 3 }]],
    ['a result streamed after a later one', [header([1, 1]), chunk(0, 1, [[1, 1]]), chunk(1, 0, [[2, 2]]), { type: 'snapshot_end', chunk_count: 2 }]],
    ['a chunk for no result', [header([1, 0]), chunk(0, 2, [[1, 1]]), { type: 'snapshot_end', chunk_count: 1 }]],
    ['a chunk past its result\'s rows', [header([1, 0]), chunk(0, 0, [[1, 1], [2, 2]]), { type: 'snapshot_end', chunk_count: 1 }]],
    ['an empty chunk', [header([0, 0]), chunk(0, 0, []), { type: 'snapshot_end', chunk_count: 1 }]],
    ['rows that do not add up', [header([2, 0]), chunk(0, 0, [[1, 1]]), { type: 'snapshot_end', chunk_count: 1 }]],
    ['an end announcing more chunks', [header([1, 0]), chunk(0, 0, [[1, 1]]), { type: 'snapshot_end', chunk_count: 2 }]],
    ['an end without a start', [{ type: 'snapshot_end', chunk_count: 0 }]],
    ['a second start', [header([1, 0]), header([1, 0]), chunk(0, 0, [[1, 1]]), { type: 'snapshot_end', chunk_count: 1 }]],
    ['a snapshot inside a stream', [header([1, 0]), {
      type: 'snapshot', knowledge_graph: 'kg', revision: 3, execution_time_ms: 0,
      results: [{ name: 'x', columns: ['a', 'b'], rows: [], total_count: 0, truncated: false },
        { name: 'y', columns: ['a', 'b'], rows: [], total_count: 0, truncated: false }],
    }]],
    ['results naming other queries', [{
      type: 'snapshot', knowledge_graph: 'kg', revision: 3, execution_time_ms: 0,
      results: [{ name: 'y', columns: ['a', 'b'], rows: [], total_count: 0, truncated: false },
        { name: 'x', columns: ['a', 'b'], rows: [], total_count: 0, truncated: false }],
    }]],
    ['a missing result', [{
      type: 'snapshot', knowledge_graph: 'kg', revision: 3, execution_time_ms: 0,
      results: [{ name: 'x', columns: ['a', 'b'], rows: [], total_count: 0, truncated: false }],
    }]],
    ['a program\'s result', [{ type: 'result', columns: [], rows: [], row_count: 0, total_count: 0, truncated: false, execution_time_ms: 0 }]],
    ['a program\'s streamed result', [
      { type: 'result_start', columns: ['a'], total_count: 0, truncated: false, execution_time_ms: 0 },
      { type: 'result_end', row_count: 0, chunk_count: 0 },
    ]],
  ])('a broken snapshot (%s) fails the read typed; the connection reads on', async (_name, frames) => {
    const kg = await graph();
    engine!.read = () => frames as Frame[];
    const error = await kg.read({ x: E, y: E }).catch((e: unknown) => e);
    expect(error).toBeInstanceOf(InternalError);
    engine!.read = (msg) => snapshotFrames(msg.queries as NamedQuery[], [{ columns: ['a', 'b'], rows: [[4, 4]] }], 8, false);
    expect(await kg.read({ z: E })).toEqual({ revision: 8, results: { z: [{ a: 4, b: 4 }] }, truncated: [] });
    expect(kg.connection.stats.staleReplies).toBe(0);
  });

  it('cancels at its deadline and fails with the engine\'s deadline_exceeded', async () => {
    const kg = await graph({ timeoutGraceMs: 2000 });
    engine!.read = () => [];
    const reading = kg.read({ x: E }, { timeoutMs: 50 }).catch((e: unknown) => e);
    await waitFor(() => engine!.frames.some((f) => f.type === 'cancel'));
    const read = engine!.frames.find((f) => f.type === 'read')!;
    const cancel = engine!.frames.find((f) => f.type === 'cancel')!;
    expect(cancel.target).toBe(read.id);
    engine!.sockets[0].send(JSON.stringify({
      type: 'error', id: read.id, code: 'deadline_exceeded',
      message: 'Request deadline exceeded before it began committing; nothing was applied',
    }));
    engine!.sockets[0].send(JSON.stringify({ type: 'cancel_ack', id: cancel.id, target: read.id, outcome: 'cancelled' }));
    expect(await reading).toBeInstanceOf(DeadlineExceededError);
  });

  it('a read with no reply past its deadline fails locally, not as an unknown outcome', async () => {
    const kg = await graph({ timeoutGraceMs: 30 });
    engine!.read = () => [];
    const error = await kg.read({ x: E }, { timeoutMs: 30 }).catch((e: unknown) => e);
    expect(error).toBeInstanceOf(DeadlineExceededError);
  });

  it('an aborted signal cancels the read', async () => {
    const kg = await graph();
    engine!.read = () => [];
    const controller = new AbortController();
    const reading = kg.read({ x: E }, { signal: controller.signal }).catch((e: unknown) => e);
    await waitFor(() => engine!.frames.some((f) => f.type === 'read'));
    controller.abort();
    await waitFor(() => engine!.frames.some((f) => f.type === 'cancel'));
    const read = engine!.frames.find((f) => f.type === 'read')!;
    engine!.sockets[0].send(JSON.stringify({
      type: 'error', id: read.id, code: 'cancelled', message: 'Request cancelled before it began committing; nothing was applied',
    }));
    expect(await reading).toBeInstanceOf(CancelledError);
  });

  it('a connection lost during a read fails it with ConnectionLostError, which may not have committed', async () => {
    const kg = await graph();
    engine!.read = () => [];
    const reading = kg.read({ x: E }).catch((e: unknown) => e);
    await waitFor(() => engine!.frames.some((f) => f.type === 'read'));
    engine!.sockets[0].terminate();
    const error = await reading;
    expect(error).toBeInstanceOf(ConnectionLostError);
    expect((error as ConnectionLostError).mayHaveCommitted).toBe(false);
  });
});
