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
 * The Python SDK's `kg.subscribe()` yields the same events, with the same
 * kinds, fields and reasons.
 */

import type { Connection, SubscriptionPushMessage, SubscriptionRoute } from './connection.js';
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
import type { SubscriptionDeltaStartResponse } from './protocol.js';
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
  /** Deadline of each `.subscribe` request, snapshot included (the connection's default when unset). */
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

/** The standing query of a target and how to read its rows. */
interface Shape {
  /** The `?...` query sent with `.subscribe`. */
  query: string;
  /** Relations the query reads, checked against the session's rules. */
  relations: string[];
  /** Result column names; for raw IQL, the frame's own columns. */
  labels(columns: string[]): string[];
  /** The projected values of each engine row, in label order. */
  project(columns: string[], rows: unknown[][]): unknown[][];
}

function isRawIql(target: SubscriptionTarget): target is { iql: string } {
  return typeof (target as { iql?: unknown }).iql === 'string';
}

function rejected(message: string, reason: SubscriptionRejectedReason): SubscriptionRejectedError {
  return new SubscriptionRejectedError(message, reason);
}

/** Compile `target`, refusing what a standing query cannot track. */
export function subscriptionShape(target: SubscriptionTarget): Shape {
  if (isRawIql(target)) {
    const query = target.iql.trim();
    return {
      query,
      relations: [],
      labels: (columns) => columns,
      project: (_columns, rows) => rows,
    };
  }
  const opts: QueryOptions = target instanceof RelationDef ? { select: [target] } : target;
  if (opts.limit !== undefined || opts.offset !== undefined) {
    throw rejected(
      'A subscription tracks the whole result: remove limit and offset from the query.',
      'limit_offset',
    );
  }
  // A result is a set: its order means nothing to deltas.
  const plan = compileQueryPlan({ ...opts, orderBy: undefined });
  if (plan.programs.length > 1) {
    throw rejected(
      'An OR condition splits the query into several standing queries, which cannot share one ' +
        'revision. Define a persistent rule with one clause per branch (kg.defineRules) and subscribe to it.',
      'or_branches',
    );
  }
  const query = plan.programs[0];
  if (query.includes('\n') || !query.startsWith('?')) {
    throw rejected(
      'An aggregate query is evaluated through a rule that lives only for one request, and ' +
        'subscriptions see persistent rules only. Define the aggregate as a persistent rule ' +
        '(kg.defineRules) and subscribe to that relation.',
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
    const key = JSON.stringify(values);
    const held = result.get(key);
    if (held) held.count += 1;
    else result.set(key, { row: toRow(labels, values), count: 1 });
  }
  return result;
}

// ── Subscription ────────────────────────────────────────────────────

type State = 'idle' | 'opening' | 'live' | 'unverified' | 'closed';

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
export class Subscription<T = Row> implements AsyncIterableIterator<Change<T>> {
  /** The id the subscription has on the server. */
  readonly id: string;
  private readonly conn: Connection;
  private readonly shape: Shape;
  private readonly capacity: number;
  private readonly timeoutMs?: number;
  private readonly sessionRules: () => Promise<string[]>;

  private state: State = 'idle';
  private route?: SubscriptionRoute;
  /** The server holds the id: `.unsubscribe` before opening it again. */
  private registered = false;
  private held: Result = new Map();
  private revision = 0;
  private seq = 0;
  private stream?: DeltaStream;
  private readonly queue: Change<T>[] = [];
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

  /** @internal Use `KnowledgeGraph.subscribe`. */
  constructor(
    conn: Connection,
    target: SubscriptionTarget,
    opts: SubscribeOptions = {},
    sessionRules: () => Promise<string[]> = async () => [],
  ) {
    this.id = subscriptionId();
    this.conn = conn;
    this.shape = subscriptionShape(target);
    this.capacity = Math.max(1, opts.queue ?? 1024);
    this.timeoutMs = opts.timeoutMs;
    this.sessionRules = sessionRules;
  }

  /** The `?...` query the subscription stands on. */
  get query(): string {
    return this.shape.query;
  }

  get stats(): Readonly<SubscriptionStats> {
    return { ...this._stats };
  }

  [Symbol.asyncIterator](): this {
    return this;
  }

  async next(): Promise<IteratorResult<Change<T>>> {
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

  async return(): Promise<IteratorResult<Change<T>>> {
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
    try {
      await this.checkPersistent();
    } catch (e) {
      this.fail(e as Error);
      return;
    }
    await this.openWithBackoff('snapshot');
  }

  /** Session rules are invisible to subscriptions: the engine would never push. */
  private async checkPersistent(): Promise<void> {
    if (this.shape.relations.length === 0) return;
    const heads = new Set((await this.sessionRules()).map((rule) => rule.split('(', 1)[0].trim()));
    const session = this.shape.relations.find((r) => heads.has(r));
    if (session !== undefined) {
      throw rejected(
        `'${session}' is a session rule, and subscriptions see persistent data only. ` +
          'Define it as a persistent rule (kg.defineRules) to subscribe to it.',
        'session_view',
      );
    }
  }

  /** Send `.subscribe` and take its snapshot as the first event or as a resync. */
  private async open(kind: 'snapshot' | 'resync'): Promise<void> {
    const route = this.conn.routeSubscription(
      this.id,
      (push) => this.onPush(route, push),
      () => {
        this._stats.staleDropped += 1;
      },
    );
    this.route = route;
    let reply;
    try {
      reply = await this.conn.execute(meta.subscribe(this.id, this.shape.query), { timeoutMs: this.timeoutMs });
    } catch (e) {
      route.close();
      if (transient(e) && !(e instanceof ConnectionLostError)) this.registered = true;
      throw refusal(e as Error);
    }
    if (this.state === 'closed') {
      // Closed while opening: the server registered it anyway.
      route.close();
      this.registered = true;
      this.unsubscribe();
      return;
    }
    const subscribed = reply.subscribed;
    if (!subscribed) {
      route.close();
      throw new InternalError(`The .subscribe reply names no subscription: ${JSON.stringify(reply)}`);
    }
    this.registered = true;
    const fresh = resultOf(this.shape, reply.columns, reply.rows);
    if (kind === 'snapshot') {
      this.push({ kind, inserted: rowsOf(fresh), retracted: [], revision: subscribed.revision, seq: 0, verified: true });
    } else {
      const { inserted, retracted } = difference<T>(this.held, fresh);
      this.push({ kind, inserted, retracted, revision: subscribed.revision, seq: 0, verified: true });
    }
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
          // Closed for good (reconnecting is off, or gave up).
          this.fail(new ConnectionLostError(`Connection lost: ${e.message}`, 'closed'));
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

  private onPush(route: SubscriptionRoute, push: SubscriptionPushMessage): void {
    if (route !== this.route || this.state !== 'live') return;
    switch (push.type) {
      case 'subscription_delta':
        if (this.stream) return this.unverified('broken_stream');
        if (push.seq !== this.seq + 1) return this.unverified('seq_gap');
        return this.apply(push.seq, push.revision, push.columns, push.inserted, push.retracted);
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
        return this.apply(start.seq, start.revision, start.columns, stream.inserted, stream.retracted);
      }
      case 'subscription_reset':
        // The server removed the subscription; the id is free.
        return this.unverified('subscription_reset', push.message, false);
      case 'subscription_error':
        return this.unverified('subscription_error', push.message);
    }
  }

  /** Apply one whole delta of engine rows to the held result. */
  private apply(seq: number, revision: number, columns: string[], inserted: unknown[][], retracted: unknown[][]): void {
    if (this.queue.length >= this.capacity) return this.unverified('slow_consumer');
    const labels = this.shape.labels(columns);
    // Count before and after, so a row whose support only moved is no change.
    const touched = new Map<string, { before: number; held: Held }>();
    const visit = (key: string, held: Held) => {
      if (!touched.has(key)) touched.set(key, { before: held.count, held });
    };
    for (const values of this.shape.project(columns, retracted)) {
      const key = JSON.stringify(values);
      const held = this.held.get(key);
      if (!held || held.count === 0) return this.unverified('broken_stream');
      visit(key, held);
      held.count -= 1;
    }
    for (const values of this.shape.project(columns, inserted)) {
      const key = JSON.stringify(values);
      let held = this.held.get(key);
      if (!held) {
        held = { row: toRow(labels, values), count: 0 };
        this.held.set(key, held);
      }
      visit(key, held);
      held.count += 1;
    }
    const change: Change<T> = { kind: 'delta', inserted: [], retracted: [], revision, seq, verified: true };
    for (const [key, { before, held }] of touched) {
      if (held.count === 0) this.held.delete(key);
      if (before === 0 && held.count > 0) change.inserted.push(held.row as T);
      if (before > 0 && held.count === 0) change.retracted.push(held.row as T);
    }
    this.seq = seq;
    this.revision = revision;
    if (change.inserted.length > 0 || change.retracted.length > 0) this.push(change);
  }

  /**
   * The held result is no longer known to be current: say so at once, then
   * reopen (after the consumer catches up, for `slow_consumer`).
   */
  private unverified(reason: UnverifiedReason, message?: string, registered = this.registered): void {
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
      inserted: [],
      retracted: [],
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

  private push(change: Change<T>): void {
    this.queue.push(change);
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
   * A reopen waits for it, so the id is free when `.subscribe` reuses it.
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

function rowsOf<T>(result: Result): T[] {
  return [...result.values()].map((held) => held.row as T);
}

/** Rows to insert and retract to turn `before` into `after`. */
function difference<T>(before: Result, after: Result): { inserted: T[]; retracted: T[] } {
  const inserted: T[] = [];
  const retracted: T[] = [];
  for (const [key, held] of before) if (!after.has(key)) retracted.push(held.row as T);
  for (const [key, held] of after) if (!before.has(key)) inserted.push(held.row as T);
  return { inserted, retracted };
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

/** The engine's refusal of a `.subscribe` as a typed rejection; other errors as they are. */
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

/** The whole current result each time it changes; see `KnowledgeGraph.watch`. */
export async function* watchChanges<T>(sub: Subscription<T>): AsyncGenerator<Live<T>, void, undefined> {
  const rows = new Map<string, T>();
  try {
    for await (const change of sub) {
      if (change.kind === 'unverified') {
        yield { rows: [...rows.values()], revision: change.revision, verified: false, reason: change.reason };
        continue;
      }
      for (const row of change.retracted) rows.delete(JSON.stringify(row));
      for (const row of change.inserted) rows.set(JSON.stringify(row), row);
      yield { rows: [...rows.values()], revision: change.revision, verified: true };
    }
  } finally {
    await sub.close();
  }
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
