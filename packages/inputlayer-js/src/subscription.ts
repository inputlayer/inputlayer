/**
 * Subscriptions: a standing query's result, kept exact on the client.
 *
 * `kg.subscribe(target)` is an async iterator of `Change` events, all of one
 * shape, so a loop that applies `inserted` and `retracted` is always correct:
 *
 * - `snapshot`: the first event, the exact result at `revision`, every row
 *   inserted;
 * - `delta`: the result changed; `inserted` and `retracted` are the set
 *   differences between consecutive results, at `revision` (`seq` is the
 *   engine's delta number). An unchanged result delivers nothing;
 * - `unverified`: the result is no longer known to be current (`reason`: the
 *   connection was lost, a `seq` gap, a broken streamed delta, a
 *   `subscription_reset` or `subscription_error` from the server, or the
 *   consumer fell `queue` events behind). Both lists are empty and
 *   `verified` is false; the SDK resubscribes with backoff;
 * - `resync`: the fresh snapshot after an `unverified`: the exact difference
 *   between the rows the consumer holds (every event before it applied) and
 *   the fresh result, so an outage never replays rows that did not change.
 *
 * The engine diffs full tuples, so a projected subscription (`select` of
 * some columns) keeps a count of the engine rows behind each projected row
 * and reports a row only when its count moves between 0 and 1: a row still
 * supported by another tuple is never retracted.
 *
 * Coalesced commits produce one delta at the latest revision, so a row that
 * appears and disappears between two evaluations is never seen; durable needs
 * belong in facts. Two subscriptions do not share revisions.
 *
 * `kg.subscribeGroup({ name: target, ... })` keeps several queries current
 * together, as one subscription: each `GroupChange` carries every member's
 * change (`unchanged` when it has none), and after every verified event all
 * members are exact at the event's one `revision`. The engine evaluates the
 * members on one snapshot per refresh, so no event shows one member ahead of
 * another. The same kinds, projection counting, `seq` checks and resync
 * apply to the group as a whole: an `unverified` covers every member, and
 * the `resync` after it is each member's exact difference.
 *
 * `kg.read({ name: target, ... })` is the one-off form: every result exact at
 * one revision.
 *
 * The Python SDK's `kg.subscribe()` yields the same events, with the same
 * kinds, fields and reasons.
 */

import type { Connection, ReadOptions, SubscriptionPushMessage, SubscriptionRoute } from './connection.js';
import { compileQueryPlan, resultColumnIndexes, type QueryOptions, type QueryPlan } from './compiler.js';
import {
  CancelledError,
  ConnectionError,
  ConnectionLostError,
  DeadlineExceededError,
  InputLayerError,
  InternalError,
  OutcomeUnknownError,
  QueryError,
  RateLimitedError,
  StatementFailedError,
  SubscriptionRejectedError,
  type SubscriptionRejectedReason,
} from './errors.js';
import { meta } from './meta.js';
import type {
  Subscribed,
  SubscriptionDeltaStartResponse,
  SubscriptionGroupDeltaStartResponse,
} from './protocol.js';
import { rowKey } from './protocol.js';
import { RelationDef } from './relation.js';

/** A row of a subscribed result, keyed by column name. */
export type Row = Record<string, unknown>;

export type ChangeKind = 'snapshot' | 'delta' | 'unverified' | 'resync';

/** Why a subscription's result stopped being verified. */
export type UnverifiedReason =
  | 'connection_lost'
  | 'seq_gap'
  | 'broken_stream'
  | 'subscription_reset'
  | 'subscription_error'
  | 'slow_consumer';

/** One event of a subscription; see the module documentation. */
export interface Change<T = Row> {
  kind: ChangeKind;
  /** Rows that entered the result. */
  inserted: T[];
  /** Rows that left the result. */
  retracted: T[];
  /**
   * The knowledge graph revision the result is at after this event; an
   * `unverified` event carries the last verified one.
   */
  revision: number;
  /**
   * The engine's delta number for a `delta` (from 1 per server
   * subscription); 0 for `snapshot` and `resync`; the last delta's for
   * `unverified`.
   */
  seq: number;
  /** False only on `unverified`. */
  verified: boolean;
  /** Why the result is unverified (`unverified` only). */
  reason?: UnverifiedReason;
  /** The server's message, for `subscription_reset` and `subscription_error`. */
  message?: string;
}

/** One member's part of a `GroupChange`. */
export interface MemberChange {
  /** Rows that entered the member's result. */
  inserted: Row[];
  /** Rows that left the member's result. */
  retracted: Row[];
  /** Both lists are empty: the member's result is as it was, and exact at the event's revision. */
  unchanged: boolean;
}

/**
 * One event of a subscription group: `Change`'s fields, with one
 * `MemberChange` per member instead of one pair of lists. After every
 * verified event, every member is exact at `revision`.
 */
export interface GroupChange {
  kind: ChangeKind;
  /** Every member, by name, in the group's order. */
  members: Record<string, MemberChange>;
  /** The revision every member is at after this event; an `unverified` event carries the last verified one. */
  revision: number;
  /** The group's delta number for a `delta`; 0 for `snapshot` and `resync`; the last delta's for `unverified`. */
  seq: number;
  /** False only on `unverified`. */
  verified: boolean;
  /** Why the group is unverified (`unverified` only). */
  reason?: UnverifiedReason;
  /** The server's message, for `subscription_reset` and `subscription_error`. */
  message?: string;
}

/** Results of `KnowledgeGraph.read`: every one the exact answer at `revision`. */
export interface ReadResult {
  /** The knowledge graph revision every result is at. */
  revision: number;
  /** Each query's rows, by name, as `kg.query` shapes them (rows keyed by column, in the engine's order). */
  results: Record<string, Row[]>;
  /**
   * Names of the results holding fewer rows than their query's whole
   * answer: cut by the target's `limit`, or by the engine's result cap
   * (`storage.performance.max_result_rows`). Empty when every result is
   * whole.
   */
  truncated: string[];
}

/** The whole current result of a watched query; see `KnowledgeGraph.watch`. */
export interface Live<T = Row> {
  rows: T[];
  revision: number;
  /** False from an `unverified` event until the fresh result arrives. */
  verified: boolean;
  reason?: UnverifiedReason;
}

/** What can be subscribed: a relation or view, a query, or raw IQL. */
export type SubscriptionTarget = RelationDef | QueryOptions | { iql: string };

export interface SubscribeOptions {
  /**
   * Events held for a consumer that has not read them (default 1024). A
   * consumer that falls further behind gets `unverified` (reason
   * `slow_consumer`) and, once it has read what was queued, a `resync`.
   */
  queue?: number;
  /**
   * Deadline of each request that opens the subscription (`.subscribe`, or a
   * group's `subscribe`), snapshot included (the connection's default when unset).
   */
  timeoutMs?: number;
}

export interface SubscriptionStats {
  /** Pushes of an earlier generation, dropped. */
  staleDropped: number;
  /** Times the subscription was opened again after an `unverified`. */
  resubscribes: number;
}

/** A handle on a callback subscription; see `KnowledgeGraph.on`. */
export interface SubscriptionHandle {
  readonly stats: SubscriptionStats;
  close(): Promise<void>;
}

/** First and longest wait between attempts to reopen a subscription, in ms. */
const RESUBSCRIBE_DELAY_MS = 1_000;
const MAX_RESUBSCRIBE_DELAY_MS = 30_000;

let nextSubscription = 0;

/** A subscription id unique in this process, so unique on every connection. */
function subscriptionId(): string {
  nextSubscription += 1;
  return `il_sub_${nextSubscription}`;
}

// ── Query shape ─────────────────────────────────────────────────────

/** @internal The query of a target and how to read its rows. */
export interface Shape {
  /** The `?...` query sent to the engine. */
  query: string;
  /** Relations the query reads, checked against the session's rules. */
  relations: string[];
  /** Result column names; for raw IQL, the frame's own columns. */
  labels(columns: string[]): string[];
  /** The projected values of each engine row, in label order. */
  project(columns: string[], rows: unknown[][]): unknown[][];
}

/** What a target is compiled for: a standing query, or one query of a read. */
type Use = 'subscribe' | 'read';

function isRawIql(target: SubscriptionTarget): target is { iql: string } {
  return typeof (target as { iql?: unknown }).iql === 'string';
}

function rejected(message: string, reason: SubscriptionRejectedReason): SubscriptionRejectedError {
  return new SubscriptionRejectedError(message, reason);
}

/** Compile `target`, refusing what a standing query cannot track. */
export function subscriptionShape(target: SubscriptionTarget): Shape {
  return targetShape(target, 'subscribe');
}

/**
 * Compile `target` to one `?` query, refusing what cannot be one: an OR
 * split, a query evaluated through program-local rules or session facts,
 * and, for a subscription, any page of the result (a read takes `limit`,
 * `offset` with a limit, and `orderBy`, which compile into its query).
 */
function targetShape(target: SubscriptionTarget, use: Use): Shape {
  if (isRawIql(target)) {
    const query = target.iql.trim();
    return {
      query,
      relations: [],
      labels: (columns) => columns,
      project: (_columns, rows) => rows,
    };
  }
  const read = use === 'read';
  const opts: QueryOptions = target instanceof RelationDef ? { select: [target] } : target;
  if (!read && (opts.limit !== undefined || opts.offset !== undefined)) {
    throw rejected(
      'A subscription tracks the whole result: remove limit and offset from the query.',
      'limit_offset',
    );
  }
  if (read && opts.offset !== undefined && opts.limit === undefined) {
    throw rejected(
      'The engine takes an offset only with a limit, and a read sends the query as it is: add a limit.',
      'limit_offset',
    );
  }
  // A subscribed result is a set: its order means nothing to deltas.
  const plan = compileQueryPlan(read ? opts : { ...opts, orderBy: undefined });
  if (plan.programs.length > 1) {
    throw rejected(
      read
        ? 'An OR condition splits the query into several queries, which a read cannot send as one. ' +
            'Define a persistent rule with one clause per branch (kg.defineRules) and read it.'
        : 'An OR condition splits the query into several standing queries, which cannot share one ' +
            'revision. Define a persistent rule with one clause per branch (kg.defineRules) and subscribe to it.',
      'or_branches',
    );
  }
  const seen = read ? 'reads see' : 'subscriptions see';
  if (plan.constFacts !== undefined) {
    throw rejected(
      `A NOT(any()) that binds only constants is evaluated through session facts sent with the query, and ${seen} ` +
        'persistent data only. Bind the negated column to a column of a joined relation.',
      'session_view',
    );
  }
  const query = plan.programs[0];
  if (query.includes('\n') || !query.startsWith('?')) {
    throw rejected(
      `An aggregate query is evaluated through a rule that lives only for one request, and ${seen} ` +
        'persistent rules only. Define the aggregate as a persistent rule (kg.defineRules) and ' +
        (read ? 'read that relation.' : 'subscribe to that relation.'),
      'session_view',
    );
  }
  const relations = [
    ...opts.select.filter((s): s is RelationDef => s instanceof RelationDef),
    ...(opts.join ?? []),
  ].map((r) => r.relationName);
  return planShape(query, plan, relations);
}

function planShape(query: string, plan: QueryPlan, relations: string[]): Shape {
  const labels = plan.outputs.map((o) => o.label);
  const variables = plan.outputs.map((o) => o.variable);
  let cached: { key: string; indexes: number[] } | undefined;
  return {
    query,
    relations,
    labels: () => labels,
    project: (columns, rows) => {
      if (rows.length === 0) return rows;
      const key = columns.join('\u0000');
      if (cached?.key !== key) cached = { key, indexes: resultColumnIndexes(plan, columns, variables) };
      const { indexes } = cached;
      return rows.map((row) => indexes.map((i) => row[i]));
    },
  };
}

/**
 * The shape of each named target, in the record's order (the order the
 * engine answers in), refusing an empty record or name. A refused target's
 * message names it.
 */
function namedShapes(targets: Record<string, SubscriptionTarget>, use: Use): Array<{ name: string; shape: Shape }> {
  const what = use === 'read' ? 'A read' : 'A subscription group';
  const entries = Object.entries(targets ?? {});
  if (entries.length === 0) throw rejected(`${what} needs at least one query.`, 'rejected');
  return entries.map(([name, target]) => {
    if (name === '') throw rejected(`${what} needs a name for every query.`, 'rejected');
    try {
      return { name, shape: targetShape(target, use) };
    } catch (e) {
      if (!(e instanceof SubscriptionRejectedError)) throw e;
      throw rejected(`Query '${name}': ${e.message}`, e.reason);
    }
  });
}

/**
 * Refuse a query reading a session rule, given the session's rule clauses:
 * reads and subscriptions see persistent data only, so the engine would
 * answer with nothing.
 */
function checkPersistent(shapes: Shape[], sessionRules: string[], use: Use): void {
  const heads = new Set(sessionRules.map((rule) => rule.split('(', 1)[0].trim()));
  const session = shapes.flatMap((shape) => shape.relations).find((r) => heads.has(r));
  if (session === undefined) return;
  throw rejected(
    use === 'read'
      ? `'${session}' is a session rule, and reads see persistent data only. ` +
          'Define it as a persistent rule (kg.defineRules) to read it.'
      : `'${session}' is a session rule, and subscriptions see persistent data only. ` +
          'Define it as a persistent rule (kg.defineRules) to subscribe to it.',
    'session_view',
  );
}

// ── Held result ─────────────────────────────────────────────────────

interface Held {
  row: Row;
  /** Engine rows behind this projected row. */
  count: number;
}

/** A projected result as a multiset of engine rows: the client's copy of it. */
type Result = Map<string, Held>;

function toRow(labels: string[], values: unknown[]): Row {
  const row: Row = {};
  labels.forEach((label, i) => {
    row[label] = values[i];
  });
  return row;
}

function resultOf(shape: Shape, columns: string[], rows: unknown[][]): Result {
  const result: Result = new Map();
  const labels = shape.labels(columns);
  for (const values of shape.project(columns, rows)) {
    const key = rowKey(values);
    const held = result.get(key);
    if (held) held.count += 1;
    else result.set(key, { row: toRow(labels, values), count: 1 });
  }
  return result;
}

function rowsOf(result: Result): Row[] {
  return [...result.values()].map((held) => held.row);
}

/** Rows to insert and retract to turn `before` into `after`. */
function difference(before: Result, after: Result): { inserted: Row[]; retracted: Row[] } {
  const inserted: Row[] = [];
  const retracted: Row[] = [];
  for (const [key, held] of before) if (!after.has(key)) retracted.push(held.row);
  for (const [key, held] of after) if (!before.has(key)) inserted.push(held.row);
  return { inserted, retracted };
}

/** @internal One member's part of a delta: engine rows, as pushed. */
export interface MemberDelta {
  columns: string[];
  inserted: unknown[][];
  retracted: unknown[][];
}

/** A projected row a delta touches: its engine-row count before and after. */
interface Touched {
  row: Row;
  before: number;
  after: number;
}

/**
 * What `delta` does to `held`, without changing it; undefined when it
 * retracts a row `held` does not hold (or its rows do not fit the query),
 * which no delta of this result can.
 */
function planDelta(shape: Shape, held: Result, delta: MemberDelta): Map<string, Touched> | undefined {
  const { columns, inserted, retracted } = delta;
  const touched = new Map<string, Touched>();
  try {
    const labels = shape.labels(columns);
    const touch = (values: unknown[]): Touched => {
      const key = rowKey(values);
      let t = touched.get(key);
      if (!t) {
        const h = held.get(key);
        const count = h?.count ?? 0;
        t = { row: h?.row ?? toRow(labels, values), before: count, after: count };
        touched.set(key, t);
      }
      return t;
    };
    // Count before and after, so a row whose support only moved is no change.
    for (const values of shape.project(columns, retracted)) {
      const t = touch(values);
      if (t.after === 0) return undefined;
      t.after -= 1;
    }
    for (const values of shape.project(columns, inserted)) touch(values).after += 1;
  } catch {
    return undefined;
  }
  return touched;
}

/** Apply a planned delta to `held`: the rows whose count moved between 0 and more. */
function commitDelta(held: Result, touched: Map<string, Touched>): { inserted: Row[]; retracted: Row[] } {
  const inserted: Row[] = [];
  const retracted: Row[] = [];
  for (const [key, { row, before, after }] of touched) {
    if (after === 0) held.delete(key);
    else held.set(key, { row, count: after });
    if (before === 0 && after > 0) inserted.push(row);
    if (before > 0 && after === 0) retracted.push(row);
  }
  return { inserted, retracted };
}

// ── Snapshot read ───────────────────────────────────────────────────

/**
 * Run `targets` on one snapshot; see `KnowledgeGraph.read`. The session's
 * rules are listed beside the read, only when a target names relations.
 */
export async function snapshotRead(
  conn: Connection,
  targets: Record<string, SubscriptionTarget>,
  opts: ReadOptions = {},
  sessionRules: () => Promise<string[]> = async () => [],
): Promise<ReadResult> {
  const named = namedShapes(targets, 'read');
  const shapes = named.map((n) => n.shape);
  const [reply, rules] = await Promise.all([
    conn.read(
      named.map(({ name, shape }) => ({ name, query: shape.query })),
      opts,
    ),
    shapes.some((shape) => shape.relations.length > 0) ? sessionRules() : [],
  ]);
  checkPersistent(shapes, rules, 'read');
  // The connection checked the reply holds one result per query, in order.
  const results = Object.fromEntries(
    reply.results.map((result, i) => {
      const { name, shape } = named[i];
      const labels = shape.labels(result.columns);
      return [name, shape.project(result.columns, result.rows).map((values) => toRow(labels, values))];
    }),
  );
  return {
    revision: reply.revision,
    results,
    truncated: reply.results.filter((r) => r.truncated).map((r) => r.name),
  };
}

// ── Subscriptions ───────────────────────────────────────────────────

type State = 'idle' | 'opening' | 'live' | 'unverified' | 'closed';

/** @internal What opening a subscription returns: its registration and each member's snapshot, in order. */
export interface Opened {
  subscribed: Subscribed;
  results: Array<{ columns: string[]; rows: unknown[][] }>;
}

/** @internal An event before it takes its public shape: one change per member, in order. */
export interface RawChange {
  kind: ChangeKind;
  revision: number;
  seq: number;
  verified: boolean;
  reason?: UnverifiedReason;
  message?: string;
  members: Array<{ inserted: Row[]; retracted: Row[] }>;
}

/**
 * @internal The life of a subscription of one query or a group: opening
 * with backoff, the held results, deltas applied whole, `unverified` and
 * the `resync` after it, the bounded queue, and closing. A subclass says how
 * to open it, reads its pushes, and shapes its events.
 */
export abstract class StandingSubscription<E, S> implements AsyncIterableIterator<E> {
  /** The id the subscription has on the server. */
  readonly id: string;
  protected readonly conn: Connection;
  /** One per member, in the order the engine answers in. */
  protected readonly shapes: Shape[];
  protected readonly timeoutMs?: number;
  private readonly capacity: number;
  private readonly sessionRules: () => Promise<string[]>;

  private state: State = 'idle';
  private route?: SubscriptionRoute;
  /** The server holds the id: `.unsubscribe` before opening it again. */
  private registered = false;
  private held: Result[];
  private revision = 0;
  /** The last delta's `seq`. */
  protected seq = 0;
  /** A streamed delta being assembled. */
  protected stream?: S;
  private readonly queue: E[] = [];
  private readonly waiters: Array<() => void> = [];
  /** Why the iterator ended, until a `next()` has thrown it. */
  private error?: Error;
  /** Reopen once the consumer has read everything queued (after `slow_consumer`). */
  private reopenWhenDrained = false;
  private reopening = false;
  private unsubscribing?: Promise<void>;
  private wake?: () => void;
  private readonly _stats: SubscriptionStats = { staleDropped: 0, resubscribes: 0 };
  private readonly onDisconnected = () => this.unverified('connection_lost', undefined, false);
  private readonly onClosed = (event: Event) => {
    // Reconnecting gave up (an error), or the client closed the connection.
    const error = (event as CustomEvent<{ error?: Error }>).detail?.error;
    if (error) this.fail(error);
    else void this.close();
  };

  protected constructor(
    conn: Connection,
    shapes: Shape[],
    opts: SubscribeOptions,
    sessionRules: () => Promise<string[]>,
  ) {
    this.id = subscriptionId();
    this.conn = conn;
    this.shapes = shapes;
    this.held = shapes.map(() => new Map());
    this.capacity = Math.max(1, opts.queue ?? 1024);
    this.timeoutMs = opts.timeoutMs;
    this.sessionRules = sessionRules;
  }

  /** Register the subscription on the server: its generation and every member's snapshot. */
  protected abstract request(): Promise<Opened>;
  /** A push of the current generation, while live. */
  protected abstract onPush(push: SubscriptionPushMessage): void;
  /** The public form of an event. */
  protected abstract change(event: RawChange): E;

  get stats(): Readonly<SubscriptionStats> {
    return { ...this._stats };
  }

  [Symbol.asyncIterator](): this {
    return this;
  }

  async next(): Promise<IteratorResult<E>> {
    if (this.state === 'idle') await this.start();
    for (;;) {
      const change = this.queue.shift();
      if (change) {
        if (this.queue.length === 0 && this.reopenWhenDrained) {
          this.reopenWhenDrained = false;
          this.reopen();
        }
        return { value: change, done: false };
      }
      if (this.state === 'closed') {
        const error = this.error;
        this.error = undefined;
        if (error) throw error;
        return { value: undefined, done: true };
      }
      await new Promise<void>((resolve) => this.waiters.push(resolve));
    }
  }

  async return(): Promise<IteratorResult<E>> {
    await this.close();
    return { value: undefined, done: true };
  }

  /** End the subscription here and on the server. Queued events are dropped. */
  async close(): Promise<void> {
    if (this.state === 'closed') return;
    this.end();
    this.queue.length = 0;
    this.error = undefined;
  }

  // ── Opening ───────────────────────────────────────────────────────

  private async start(): Promise<void> {
    this.state = 'opening';
    this.conn.events.addEventListener('disconnected', this.onDisconnected);
    this.conn.events.addEventListener('closed', this.onClosed);
    await this.openWithBackoff('snapshot');
  }

  /** Session rules are invisible to subscriptions: the engine would never push. */
  private async checkPersistent(): Promise<void> {
    if (this.shapes.every((shape) => shape.relations.length === 0)) return;
    checkPersistent(this.shapes, await this.sessionRules(), 'subscribe');
  }

  /** Register on the server and take the snapshot as the first event or as a resync. */
  private async open(kind: 'snapshot' | 'resync'): Promise<void> {
    const route = this.conn.routeSubscription(
      this.id,
      (push) => this.receive(route, push),
      () => {
        this._stats.staleDropped += 1;
      },
    );
    this.route = route;
    let opened: Opened;
    try {
      opened = await this.request();
    } catch (e) {
      route.close();
      if (transient(e) && !(e instanceof ConnectionLostError)) {
        this.registered = true;
        if (this.state === 'closed') this.unsubscribe();
      }
      throw refusal(e as Error);
    }
    if (this.state === 'closed') {
      // Closed while opening: the server registered it anyway.
      route.close();
      this.registered = true;
      this.unsubscribe();
      return;
    }
    this.registered = true;
    const { subscribed } = opened;
    const fresh = opened.results.map((result, i) => resultOf(this.shapes[i], result.columns, result.rows));
    const members = fresh.map((result, i) =>
      kind === 'snapshot' ? { inserted: rowsOf(result), retracted: [] } : difference(this.held[i], result),
    );
    this.push({ kind, members, revision: subscribed.revision, seq: 0, verified: true });
    this.held = fresh;
    this.revision = subscribed.revision;
    this.seq = 0;
    this.stream = undefined;
    this.state = 'live';
    // Pushes that arrived before the reply are delivered now, in order.
    route.setGeneration(subscribed.generation);
  }

  /** Reopen with backoff after the result stopped being verified. */
  private reopen(): void {
    if (this.reopening || this.state !== 'unverified') return;
    this.reopening = true;
    void this.openWithBackoff('resync').finally(() => {
      this.reopening = false;
    });
  }

  /** Open with backoff until it works, is refused, or the subscription ends. */
  private async openWithBackoff(kind: 'snapshot' | 'resync'): Promise<void> {
    const state: State = kind === 'snapshot' ? 'opening' : 'unverified';
    let delay = RESUBSCRIBE_DELAY_MS;
    for (;;) {
      if (this.state !== state) return;
      try {
        if (kind === 'snapshot') await this.checkPersistent();
        await this.unsubscribing;
        if (this.registered) {
          await this.conn.execute(meta.unsubscribe(this.id), { timeoutMs: this.timeoutMs }).catch((e) => {
            if (transient(e)) throw e;
            // Already gone on the server (reset, or a new connection).
          });
          this.registered = false;
        }
        await this.open(kind);
        if (kind === 'resync' && (this.state as State) === 'live') this._stats.resubscribes += 1;
        return;
      } catch (e) {
        if (this.state !== state) return;
        if (e instanceof ConnectionError && !(e instanceof ConnectionLostError)) {
          // Closed for good (reconnecting is off, or gave up), or refused by an open one.
          this.fail(this.conn.connected ? e : new ConnectionLostError(`Connection lost: ${e.message}`, 'closed'));
          return;
        }
        if (!transient(e)) {
          this.fail(refusal(e as Error));
          return;
        }
      }
      // Full jitter in [delay/2, delay], ended early by close().
      await new Promise<void>((resolve) => {
        const timer = setTimeout(resolve, delay * (0.5 + Math.random() / 2));
        this.wake = () => {
          clearTimeout(timer);
          resolve();
        };
      });
      this.wake = undefined;
      delay = Math.min(delay * 2, MAX_RESUBSCRIBE_DELAY_MS);
    }
  }

  // ── Pushes ────────────────────────────────────────────────────────

  private receive(route: SubscriptionRoute, push: SubscriptionPushMessage): void {
    if (route !== this.route || this.state !== 'live') return;
    try {
      this.onPush(push);
    } catch {
      // A malformed frame (a field missing): never a delta to apply.
      this.unverified('broken_stream');
    }
  }

  /**
   * Apply one whole delta, a part per member, to the held results: every
   * member's part or none (a part that cannot apply leaves them all as
   * they were). One event, unless no member's projected result changed.
   */
  protected apply(seq: number, revision: number, parts: MemberDelta[]): void {
    if (this.queue.length >= this.capacity) return this.unverified('slow_consumer');
    if (parts.length !== this.shapes.length) return this.unverified('broken_stream');
    const plans: Array<Map<string, Touched>> = [];
    for (const [i, part] of parts.entries()) {
      const plan = planDelta(this.shapes[i], this.held[i], part);
      if (!plan) return this.unverified('broken_stream');
      plans.push(plan);
    }
    const members = plans.map((plan, i) => commitDelta(this.held[i], plan));
    this.seq = seq;
    this.revision = revision;
    if (members.some((m) => m.inserted.length > 0 || m.retracted.length > 0)) {
      this.push({ kind: 'delta', members, revision, seq, verified: true });
    }
  }

  /**
   * The held result is no longer known to be current: say so at once, then
   * reopen (after the consumer catches up, for `slow_consumer`).
   */
  protected unverified(reason: UnverifiedReason, message?: string, registered = this.registered): void {
    if (this.state !== 'live') {
      // Already unverified, or still opening (a failed open is retried or reported).
      if (reason === 'connection_lost' && this.state !== 'closed') this.registered = false;
      return;
    }
    this.state = 'unverified';
    this.route?.close();
    this.route = undefined;
    this.stream = undefined;
    this.registered = registered;
    this.push({
      kind: 'unverified',
      members: this.shapes.map(() => ({ inserted: [], retracted: [] })),
      revision: this.revision,
      seq: this.seq,
      verified: false,
      reason,
      ...(message !== undefined ? { message } : {}),
    });
    if (reason === 'slow_consumer') {
      // Stop the server pushing what would be dropped.
      if (this.registered) this.unsubscribe();
      this.reopenWhenDrained = true;
      return;
    }
    this.reopen();
  }

  // ── Delivery and ending ───────────────────────────────────────────

  private push(event: RawChange): void {
    this.queue.push(this.change(event));
    for (const waiter of this.waiters.splice(0)) waiter();
  }

  /** End with `error`, thrown by the next `next()` once the queue is read. */
  private fail(error: Error): void {
    if (this.state === 'closed') return;
    this.end();
    this.error = error;
  }

  private end(): void {
    const registered = this.registered;
    this.state = 'closed';
    this.route?.close();
    this.route = undefined;
    this.reopenWhenDrained = false;
    this.wake?.();
    this.conn.events.removeEventListener('disconnected', this.onDisconnected);
    this.conn.events.removeEventListener('closed', this.onClosed);
    if (registered) this.unsubscribe();
    for (const waiter of this.waiters.splice(0)) waiter();
  }

  /**
   * Best effort: the server also drops the subscription with the connection.
   * A reopen waits for it, so the id is free when it is registered again.
   */
  private unsubscribe(): void {
    this.registered = false;
    if (!this.conn.connected) return;
    this.unsubscribing = this.conn
      .execute(meta.unsubscribe(this.id), { timeoutMs: this.timeoutMs })
      .then(
        () => undefined,
        () => undefined, // Gone already, or the connection is ending.
      );
  }
}

/** The optional fields of an event, present only when set. */
function why(event: RawChange): { reason?: UnverifiedReason; message?: string } {
  return {
    ...(event.reason !== undefined ? { reason: event.reason } : {}),
    ...(event.message !== undefined ? { message: event.message } : {}),
  };
}

interface DeltaStream {
  start: SubscriptionDeltaStartResponse;
  inserted: unknown[][];
  retracted: unknown[][];
  chunks: number;
}

/**
 * One standing query, as an async iterator of `Change` events. It opens on
 * the first `next()`; `close()` (or leaving a `for await` loop) ends it on
 * the server. A refusal ends the iterator with `SubscriptionRejectedError`;
 * losing the connection for good ends it with `ConnectionLostError`.
 */
export class Subscription<T = Row> extends StandingSubscription<Change<T>, DeltaStream> {
  /** @internal Use `KnowledgeGraph.subscribe`. */
  constructor(
    conn: Connection,
    target: SubscriptionTarget,
    opts: SubscribeOptions = {},
    sessionRules: () => Promise<string[]> = async () => [],
  ) {
    super(conn, [subscriptionShape(target)], opts, sessionRules);
  }

  /** The `?...` query the subscription stands on. */
  get query(): string {
    return this.shapes[0].query;
  }

  protected async request(): Promise<Opened> {
    const reply = await this.conn.execute(meta.subscribe(this.id, this.query), { timeoutMs: this.timeoutMs });
    if (!reply.subscribed) {
      throw new InternalError(`The .subscribe reply names no subscription: ${JSON.stringify(reply)}`);
    }
    return { subscribed: reply.subscribed, results: [{ columns: reply.columns, rows: reply.rows }] };
  }

  protected onPush(push: SubscriptionPushMessage): void {
    switch (push.type) {
      case 'subscription_delta':
        if (this.stream) return this.unverified('broken_stream');
        if (push.seq !== this.seq + 1) return this.unverified('seq_gap');
        return this.apply(push.seq, push.revision, [push]);
      case 'subscription_delta_start':
        if (this.stream) return this.unverified('broken_stream');
        if (push.seq !== this.seq + 1) return this.unverified('seq_gap');
        this.stream = { start: push, inserted: [], retracted: [], chunks: 0 };
        return;
      case 'subscription_delta_chunk': {
        const stream = this.stream;
        if (!stream || push.seq !== stream.start.seq || push.chunk_index !== stream.chunks) {
          return this.unverified('broken_stream');
        }
        stream.chunks += 1;
        stream.inserted.push(...push.inserted);
        stream.retracted.push(...push.retracted);
        return;
      }
      case 'subscription_delta_end': {
        const stream = this.stream;
        this.stream = undefined;
        if (
          !stream ||
          push.seq !== stream.start.seq ||
          push.chunk_count !== stream.chunks ||
          push.inserted_count !== stream.inserted.length ||
          push.retracted_count !== stream.retracted.length
        ) {
          return this.unverified('broken_stream');
        }
        const { start } = stream;
        return this.apply(start.seq, start.revision, [
          { columns: start.columns, inserted: stream.inserted, retracted: stream.retracted },
        ]);
      }
      case 'subscription_reset':
        // The server removed the subscription; the id is free.
        return this.unverified('subscription_reset', push.message, false);
      case 'subscription_error':
        return this.unverified('subscription_error', push.message);
      default:
        // A group's push: not a frame this subscription can get.
        return this.unverified('broken_stream');
    }
  }

  protected change(event: RawChange): Change<T> {
    const [member] = event.members;
    return {
      kind: event.kind,
      inserted: member.inserted as T[],
      retracted: member.retracted as T[],
      revision: event.revision,
      seq: event.seq,
      verified: event.verified,
      ...why(event),
    };
  }
}

interface GroupDeltaStream {
  start: SubscriptionGroupDeltaStartResponse;
  /** Rows per member, in order. */
  inserted: unknown[][][];
  retracted: unknown[][][];
  /** The member the last chunk belonged to. */
  member: number;
  chunks: number;
}

/**
 * A subscription group: several standing queries kept current together, as
 * an async iterator of `GroupChange` events, every one exact at one
 * revision for every member. It opens on the first `next()`; `close()` (or
 * leaving a `for await` loop) ends it on the server. A refusal ends the
 * iterator with `SubscriptionRejectedError`; losing the connection for good
 * ends it with `ConnectionLostError`.
 */
export class GroupSubscription extends StandingSubscription<GroupChange, GroupDeltaStream> {
  /** Member names, in the order the engine answers in. */
  private readonly names: string[];

  /** @internal Use `KnowledgeGraph.subscribeGroup`. */
  constructor(
    conn: Connection,
    members: Record<string, SubscriptionTarget>,
    opts: SubscribeOptions = {},
    sessionRules: () => Promise<string[]> = async () => [],
  ) {
    const named = namedShapes(members, 'subscribe');
    super(conn, named.map((n) => n.shape), opts, sessionRules);
    this.names = named.map((n) => n.name);
  }

  /** The `?...` query each member stands on, by name. */
  get queries(): Record<string, string> {
    return Object.fromEntries(this.names.map((name, i) => [name, this.shapes[i].query]));
  }

  protected async request(): Promise<Opened> {
    const queries = this.names.map((name, i) => ({ name, query: this.shapes[i].query }));
    // The connection checked the snapshot holds one result per member, in order.
    const reply = await this.conn.subscribeGroup(this.id, queries, { timeoutMs: this.timeoutMs });
    if (!reply.subscribed) {
      throw new InternalError(`The subscribe reply names no subscription (revision ${reply.revision})`);
    }
    return { subscribed: reply.subscribed, results: reply.results };
  }

  /** Whether `members` lists every member, by name, in order. */
  private lists(members: Array<{ name: string }>): boolean {
    return members.length === this.names.length && members.every((m, i) => m.name === this.names[i]);
  }

  protected onPush(push: SubscriptionPushMessage): void {
    switch (push.type) {
      case 'subscription_group_delta':
        if (this.stream) return this.unverified('broken_stream');
        if (push.seq !== this.seq + 1) return this.unverified('seq_gap');
        if (
          !this.lists(push.members) ||
          push.members.some((m) => m.unchanged !== (m.inserted.length === 0 && m.retracted.length === 0))
        ) {
          return this.unverified('broken_stream');
        }
        return this.apply(push.seq, push.revision, push.members);
      case 'subscription_group_delta_start':
        if (this.stream) return this.unverified('broken_stream');
        if (push.seq !== this.seq + 1) return this.unverified('seq_gap');
        if (
          !this.lists(push.members) ||
          push.members.some((m) => m.unchanged !== (m.inserted_count === 0 && m.retracted_count === 0))
        ) {
          return this.unverified('broken_stream');
        }
        this.stream = {
          start: push,
          inserted: push.members.map(() => []),
          retracted: push.members.map(() => []),
          member: 0,
          chunks: 0,
        };
        return;
      case 'subscription_group_delta_chunk': {
        const stream = this.stream;
        if (!stream || push.seq !== stream.start.seq || push.chunk_index !== stream.chunks) {
          return this.unverified('broken_stream');
        }
        const header = stream.start.members[push.member];
        // Members stream in order, each chunk holding rows of one, never past its counts.
        if (!Number.isInteger(push.member) || push.member < stream.member || header === undefined) {
          return this.unverified('broken_stream');
        }
        const inserted = stream.inserted[push.member];
        const retracted = stream.retracted[push.member];
        if (
          push.inserted.length + push.retracted.length === 0 ||
          inserted.length + push.inserted.length > header.inserted_count ||
          retracted.length + push.retracted.length > header.retracted_count
        ) {
          return this.unverified('broken_stream');
        }
        stream.chunks += 1;
        stream.member = push.member;
        inserted.push(...push.inserted);
        retracted.push(...push.retracted);
        return;
      }
      case 'subscription_group_delta_end': {
        const stream = this.stream;
        this.stream = undefined;
        if (
          !stream ||
          push.seq !== stream.start.seq ||
          push.chunk_count !== stream.chunks ||
          stream.start.members.some(
            (m, i) => m.inserted_count !== stream.inserted[i].length || m.retracted_count !== stream.retracted[i].length,
          )
        ) {
          return this.unverified('broken_stream');
        }
        const { start } = stream;
        return this.apply(
          start.seq,
          start.revision,
          start.members.map((m, i) => ({ columns: m.columns, inserted: stream.inserted[i], retracted: stream.retracted[i] })),
        );
      }
      case 'subscription_reset':
        // The server removed the group; the id is free.
        return this.unverified('subscription_reset', push.message, false);
      case 'subscription_error':
        // The refresh failed as a whole: every member keeps its last result.
        return this.unverified('subscription_error', push.message);
      default:
        // A single query's push: not a frame a group can get.
        return this.unverified('broken_stream');
    }
  }

  protected change(event: RawChange): GroupChange {
    return {
      kind: event.kind,
      members: Object.fromEntries(
        event.members.map(({ inserted, retracted }, i) => [
          this.names[i],
          { inserted, retracted, unchanged: inserted.length === 0 && retracted.length === 0 },
        ]),
      ),
      revision: event.revision,
      seq: event.seq,
      verified: event.verified,
      ...why(event),
    };
  }
}

/** Errors after which opening again may work. */
function transient(error: unknown): boolean {
  return (
    error instanceof ConnectionLostError ||
    error instanceof DeadlineExceededError ||
    error instanceof OutcomeUnknownError ||
    error instanceof CancelledError ||
    error instanceof RateLimitedError ||
    error instanceof InternalError
  );
}

/** The engine's refusal of a subscription as a typed rejection; other errors as they are. */
function refusal(error: Error): Error {
  if (error instanceof SubscriptionRejectedError) return error;
  if (!(error instanceof QueryError || error instanceof StatementFailedError) || transient(error)) {
    return error;
  }
  const message = error.message;
  let reason: SubscriptionRejectedReason = 'rejected';
  if (/limit\/offset/.test(message)) reason = 'limit_offset';
  else if (/max_result_rows/.test(message)) reason = 'result_cap';
  else if (/access denied|permission/i.test(message)) reason = 'access_denied';
  else if (/already exists on this connection/.test(message)) reason = 'id_taken';
  else if (/Subscription limit reached/.test(message)) reason = 'subscription_limit';
  const out = new SubscriptionRejectedError(message, reason);
  out.iql = (error as InputLayerError).iql;
  return out;
}

// ── Levels and callbacks ────────────────────────────────────────────

/**
 * The whole current result each time it changes; see `KnowledgeGraph.watch`.
 * Not an async generator: its `return()` would wait for the next change.
 */
export function watchChanges<T>(sub: Subscription<T>): AsyncIterableIterator<Live<T>> {
  const rows = new Map<string, T>();
  return {
    [Symbol.asyncIterator]() {
      return this;
    },
    async next(): Promise<IteratorResult<Live<T>>> {
      const { value: change, done } = await sub.next();
      if (done) return { value: undefined, done: true };
      if (change.kind === 'unverified') {
        return {
          value: { rows: [...rows.values()], revision: change.revision, verified: false, reason: change.reason },
          done: false,
        };
      }
      for (const row of change.retracted) rows.delete(rowKey(row));
      for (const row of change.inserted) rows.set(rowKey(row), row);
      return { value: { rows: [...rows.values()], revision: change.revision, verified: true }, done: false };
    },
    async return(): Promise<IteratorResult<Live<T>>> {
      await sub.close();
      return { value: undefined, done: true };
    },
  };
}

/** Feed every change of `sub` to `callback`, one at a time. */
export function runCallback<T>(
  sub: Subscription<T>,
  callback: (change: Change<T>) => unknown,
  onError: (error: unknown) => void,
): SubscriptionHandle {
  void (async () => {
    try {
      for await (const change of sub) {
        try {
          await callback(change);
        } catch (e) {
          onError(e);
        }
      }
    } catch (e) {
      onError(e);
    }
  })();
  return {
    get stats() {
      return sub.stats;
    },
    close: () => sub.close(),
  };
}
