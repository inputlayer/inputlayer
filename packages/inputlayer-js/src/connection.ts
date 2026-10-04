/**
 * WebSocket connection: one routed reader for every frame.
 *
 * Every request carries an `id` and its reply frames are routed back to the
 * call that sent it, so calls run concurrently on one connection (the engine
 * overlaps queries and runs everything else alone, in order). Pushes never
 * carry an `id`: notifications go to the dispatcher, subscription pushes to
 * the route registered for their subscription and generation, and notices
 * end the pending calls when they announce a close. The reader runs while no
 * call is in flight, so pushes are delivered to an idle client.
 *
 * Calls are programs (`execute`), snapshot reads (`read`, several queries at
 * one revision) and subscription groups (`subscribeGroup`). A read's or a
 * group's reply is a `snapshot`, streamed as `snapshot_start`,
 * `snapshot_chunk`s and `snapshot_end` when large; it is checked whole (chunk
 * order, each result's rows against its header, one result per query, in
 * order) before the call resolves.
 *
 * Deadlines: a call's `timeoutMs` runs from `execute()`; what is left of it
 * when the call is sent goes as `timeout_ms`, and a call still waiting to be
 * sent when it passes fails with `DeadlineExceededError`. If a sent call has
 * no reply at its deadline, the connection sends `cancel` and probes the
 * transport with a WebSocket ping (no answer within `timeoutGraceMs` drops
 * the connection). The engine's reply normally follows
 * (typed, e.g. `DeadlineExceededError`, or the committed result of a write
 * that was already committing); without one (or, while a reply streams, its
 * next part) within `timeoutGraceMs`, the call fails locally: `DeadlineExceededError` for a query, and
 * `OutcomeUnknownError` for a program that may write. A later reply is
 * dropped and counted.
 *
 * Keepalive: whenever nothing was sent for `keepaliveMs`, an application ping
 * resets the server's idle timer (only with no call in flight, as its pong
 * waits behind earlier replies), and the transport is probed, calls in flight
 * or not: no pong and no frame within `timeoutGraceMs` means the socket is
 * half-open, so it is dropped and the connection reconnects. No verdict is
 * taken while the request window is full (the engine then reads nothing):
 * each call's deadline bounds it, and freed slots let the probe run.
 *
 * Reconnect: exponential backoff with jitter, re-opened on the same knowledge
 * graph (`?kg=`) with the notification cursor (`last_seq` and `epoch`), then
 * `reconnected` and `session_reset` events. Calls not yet sent wait for the
 * reconnect; calls in flight fail with `ConnectionLostError`.
 */

import WebSocket from 'ws';
import {
  type ClientMessage,
  type ErrorResponse,
  type NamedQuery,
  type NoticeResponse,
  type NotificationResponse,
  type PushMessage,
  type ResultResponse,
  type ResultStartResponse,
  type ServerMessage,
  type SnapshotChunkResponse,
  type SnapshotResponse,
  type SnapshotStartResponse,
  serializeMessage,
  deserializeMessage,
  isPush,
} from './protocol.js';
import {
  AuthenticationError,
  CancelledError,
  ConnectionError,
  ConnectionLostError,
  DeadlineExceededError,
  InputLayerError,
  InternalError,
  OutcomeUnknownError,
  ProtocolError,
  QueryError,
  RateLimitedError,
  StatementFailedError,
  StoreReadOnlyError,
} from './errors.js';
import { type NotificationEvent, NotificationDispatcher } from './notifications.js';

export interface ConnectionOptions {
  url: string;
  username?: string;
  password?: string;
  apiKey?: string;
  /** Reconnect after an unexpected close (default true). */
  autoReconnect?: boolean;
  /** First reconnect backoff in seconds, doubled per attempt (default 1). */
  reconnectDelay?: number;
  /** Longest reconnect backoff in seconds (default 30). */
  maxReconnectDelay?: number;
  /** Reconnect attempts before giving up (default 10). */
  maxReconnectAttempts?: number;
  /** Knowledge graph the connection is bound to (`?kg=`); the server's default when unset. */
  initialKg?: string;
  /** Notification cursor to resume from: the last `seq` seen... */
  lastSeq?: number;
  /** ...and the `stream_epoch` it belongs to. */
  epoch?: string;
  /** Deadline of a call without its own `timeoutMs` (default 30 000; 0 for none). */
  defaultTimeoutMs?: number;
  /** Wait past a deadline, after cancelling, before failing the call locally (default 2 000). */
  timeoutGraceMs?: number;
  /**
   * Ping when nothing was sent for this long, so the server's idle timeout
   * never ends a connection that only listens, and probe the transport: no
   * WebSocket pong and no frame within `timeoutGraceMs` drops the connection,
   * which then reconnects; no verdict is taken while the request window is
   * full, when each call's deadline bounds it (default 20 000; 0 disables).
   */
  keepaliveMs?: number;
  /**
   * Requests in flight at most (default 15). Keep it below the server's
   * `ws_max_in_flight_requests` (default 16) so a `cancel` can always be read.
   */
  maxInFlight?: number;
  /** Dispatcher for notifications; several connections may share one. */
  dispatcher?: NotificationDispatcher;
  /**
   * Called once when the first open is refused at authentication: return
   * true after creating the missing knowledge graph to retry the open.
   */
  createKg?: (name: string) => Promise<boolean>;
  /** Open on the first call instead of requiring `connect()` (default false). */
  lazy?: boolean;
}

/** Per-call options. */
export interface ExecuteOptions {
  /** Deadline in milliseconds; overrides `defaultTimeoutMs` (0 for none). */
  timeoutMs?: number;
  /** Abort to cancel the call: `CancelledError` unless it was already committing. */
  signal?: AbortSignal;
}

/** Subscription pushes: deltas, their streamed parts, errors and resets. */
export type SubscriptionPushMessage = Exclude<
  PushMessage,
  NoticeResponse | NotificationResponse
>;

/** Where a subscription's pushes go; see `Connection.routeSubscription`. */
export interface SubscriptionRoute {
  /** Deliver only pushes of this generation (the one `.subscribe` replied with). */
  setGeneration(generation: number): void;
  /** Stop routing; later pushes for the subscription are dropped and counted. */
  close(): void;
}

/** Counters of frames the connection dropped. */
export interface ConnectionStats {
  /** Subscription pushes of a stale generation, or for no route. */
  stalePushes: number;
  /** Replies for no waiting call: for an unknown id, or to a call failed locally past its deadline. */
  staleReplies: number;
  /** Frames that were not JSON or had an unknown type. */
  malformedFrames: number;
  /** Successful reconnects. */
  reconnects: number;
}

/**
 * Events on `Connection.events` (an `EventTarget` of `CustomEvent`s):
 * - `disconnected` `{ code }`: the connection closed unexpectedly;
 * - `reconnected` `{ attempt }`: it is open again;
 * - `session_reset`: session facts, session rules and subscriptions were
 *   lost with the old connection: re-create what you hold;
 * - `notification_gap` `{ code, message }`: notifications were missed
 *   (`replay_gap`, `notifications_missed`, or `no_cursor` after a reconnect
 *   with no notification seen before the drop): re-read the state you track;
 * - `closed` `{ error }`: reconnecting gave up; the connection is closed.
 *   `close()` emits it too, without an `error`.
 */
export type ConnectionEventType =
  | 'disconnected'
  | 'reconnected'
  | 'session_reset'
  | 'notification_gap'
  | 'closed';

type State = 'idle' | 'connecting' | 'open' | 'reconnecting' | 'closed';

interface Stream {
  start: ResultStartResponse;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  rows: any[][];
  provenance: string[];
  chunks: number;
}

/** A streamed snapshot being assembled: rows per result, in request order. */
interface SnapshotStream {
  start: SnapshotStartResponse;
  rows: unknown[][][];
  /** The result the last chunk belonged to. */
  result: number;
  chunks: number;
}

/** What a call asks for. */
type Request =
  | { type: 'execute'; program: string }
  | { type: 'read'; queries: NamedQuery[] }
  | { type: 'subscribe'; subscription: string; queries: NamedQuery[] };

interface Call {
  request: Request;
  /** What its errors carry as `iql`: the program, or the queries one per line. */
  program: string;
  /** Whether it may write, so a lost reply leaves its outcome open. */
  mayWrite: boolean;
  /** `performance.now()` at the call's deadline. */
  expiresAt?: number;
  signal?: AbortSignal;
  onAbort?: () => void;
  resolve: (reply: ResultResponse | SnapshotResponse) => void;
  reject: (error: Error) => void;
  id?: string;
  stream?: Stream;
  snapshot?: SnapshotStream;
  deadline?: ReturnType<typeof setTimeout>;
  cancelId?: string;
  /** Its deadline passed: it is in the grace period for the reply. */
  graced?: boolean;
  /** What sent the call's `cancel`: its deadline or its signal. */
  cancelledBy?: 'deadline' | 'signal';
  retried: boolean;
  settled: boolean;
}

interface Route {
  sink: (push: SubscriptionPushMessage) => void;
  onStale?: () => void;
  generation?: number;
  early: SubscriptionPushMessage[];
}

/** Notices after which the server keeps the connection open. */
const OPEN_NOTICES: ReadonlySet<string> = new Set(['notifications_missed', 'replay_gap']);

/** Reply frames of a program, and of a read or a subscription group. */
const RESULT_FRAMES: ReadonlySet<string> = new Set(['result', 'result_start', 'result_chunk', 'result_end']);
const SNAPSHOT_FRAMES: ReadonlySet<string> = new Set(['snapshot', 'snapshot_start', 'snapshot_chunk', 'snapshot_end']);

/** The connection's message rate window is one second. */
const RATE_WINDOW_MS = 1000;

/**
 * Manages one WebSocket connection to an InputLayer server.
 */
export class Connection {
  private readonly url: string;
  private readonly username?: string;
  private readonly password?: string;
  private readonly apiKey?: string;
  private readonly autoReconnect: boolean;
  private readonly reconnectDelay: number;
  private readonly maxReconnectDelay: number;
  private readonly maxReconnectAttempts: number;
  private readonly defaultTimeoutMs: number;
  private readonly timeoutGraceMs: number;
  private readonly keepaliveMs: number;
  private readonly maxInFlight: number;
  private readonly createKg?: (name: string) => Promise<boolean>;
  private readonly lazy: boolean;

  private ws: WebSocket | null = null;
  private state: State = 'idle';
  private opening?: Promise<void>;
  private reconnecting?: Promise<void>;
  /** Ends the reconnect backoff early, when the connection is closed. */
  private wake?: () => void;
  private _sessionId?: string;
  private _serverVersion?: string;
  private _role?: string;
  private _currentKg?: string;
  private _boundKg?: string;
  private _epoch?: string;
  private _lastSeq?: number;
  private lastNotice?: NoticeResponse;
  private nextId = 0;
  private lastSentAt = 0;
  private keepalive?: ReturnType<typeof setInterval>;
  private probe?: { timer: ReturnType<typeof setTimeout>; ws: WebSocket };

  private readonly queue: Call[] = [];
  private readonly inFlight = new Map<string, Call>();
  /** Replies to `cancel`, `ping` and authentication, by id; an error when the connection ends first. */
  private readonly control = new Map<string, (reply: ServerMessage | Error) => void>();
  /**
   * Ids of calls settled before their reply ended (a broken stream, or no
   * reply past the deadline), by whether that reply counts as stale. Their
   * late frames are dropped; they take no slot of `maxInFlight`.
   */
  private readonly abandoned = new Map<string, boolean>();
  private readonly routes = new Map<string, Route>();
  private readonly _dispatcher: NotificationDispatcher;
  private readonly _stats: ConnectionStats = {
    stalePushes: 0,
    staleReplies: 0,
    malformedFrames: 0,
    reconnects: 0,
  };

  /** Connection events; see `ConnectionEventType`. */
  readonly events = new EventTarget();

  constructor(opts: ConnectionOptions) {
    this.url = opts.url;
    this.username = opts.username;
    this.password = opts.password;
    this.apiKey = opts.apiKey;
    this.autoReconnect = opts.autoReconnect ?? true;
    this.reconnectDelay = opts.reconnectDelay ?? 1.0;
    this.maxReconnectDelay = opts.maxReconnectDelay ?? 30;
    this.maxReconnectAttempts = opts.maxReconnectAttempts ?? 10;
    this.defaultTimeoutMs = opts.defaultTimeoutMs ?? 30_000;
    this.timeoutGraceMs = opts.timeoutGraceMs ?? 2_000;
    this.keepaliveMs = opts.keepaliveMs ?? 20_000;
    this.maxInFlight = Math.max(1, opts.maxInFlight ?? 15);
    this.createKg = opts.createKg;
    this.lazy = opts.lazy ?? false;
    this._currentKg = opts.initialKg;
    this._lastSeq = opts.lastSeq;
    this._epoch = opts.epoch;
    this._dispatcher = opts.dispatcher ?? new NotificationDispatcher();
  }

  // ── Properties ──────────────────────────────────────────────────

  get connected(): boolean {
    return this.state === 'open';
  }

  get sessionId(): string | undefined {
    return this._sessionId;
  }

  get serverVersion(): string | undefined {
    return this._serverVersion;
  }

  get role(): string | undefined {
    return this._role;
  }

  /** The knowledge graph the connection is on; a reconnect re-opens it there. */
  get currentKg(): string | undefined {
    return this._currentKg;
  }

  /** The knowledge graph the connection was opened on, before any `.kg use`. */
  get boundKg(): string | undefined {
    return this._boundKg;
  }

  get dispatcher(): NotificationDispatcher {
    return this._dispatcher;
  }

  /** The last notification `seq` this connection received (0 before any). */
  get lastSeq(): number {
    return this._lastSeq ?? 0;
  }

  /** The `stream_epoch` the notification cursor belongs to. */
  get epoch(): string | undefined {
    return this._epoch;
  }

  get stats(): Readonly<ConnectionStats> {
    return { ...this._stats };
  }

  // ── Connection lifecycle ────────────────────────────────────────

  /** Open and authenticate. Concurrent callers share one attempt. */
  async connect(): Promise<void> {
    if (this.state === 'open') return;
    if (this.state === 'closed') throw new ConnectionError('Connection closed');
    if (this.reconnecting) {
      await this.reconnecting;
      return this.connect();
    }
    if (this.opening) return this.opening;
    this.state = 'connecting';
    this.opening = this.openFirst().finally(() => {
      this.opening = undefined;
    });
    return this.opening;
  }

  private async openFirst(): Promise<void> {
    try {
      try {
        await this.open();
      } catch (e) {
        const kg = this._currentKg;
        if (!(e instanceof AuthenticationError) || !kg || !this.createKg) throw e;
        if (!(await this.createKg(kg))) throw e;
        await this.open();
      }
    } catch (e) {
      if (this.state !== 'closed') this.state = 'idle';
      this.failQueued(e as Error);
      throw e;
    }
    this.state = 'open';
    this.startKeepalive();
    this.pump();
  }

  /** Close the connection; pending calls fail with `ConnectionLostError`. */
  async close(): Promise<void> {
    const ws = this.ws;
    const wasClosed = this.state === 'closed';
    this.state = 'closed';
    this.ws = null;
    this.stopKeepalive();
    this.clearProbe();
    this.wake?.();
    this.failControl(new ConnectionError('Connection closed'));
    this.failInFlight('closed', 'Connection closed by the client');
    this.failQueued(new ConnectionError('Connection closed'));
    if (ws) drop(ws);
    if (!wasClosed) this.emit('closed', {});
  }

  /**
   * One socket: open it, read every frame from it, authenticate on it.
   * Resolves once authenticated; rejects (and drops the socket) otherwise.
   */
  private async open(): Promise<void> {
    const wsUrl = this.connectUrl();
    let ws: WebSocket;
    try {
      ws = await createWebSocket(wsUrl);
    } catch (e) {
      throw new ConnectionError(`Failed to connect to ${wsUrl}: ${e}`);
    }
    if (this.state === 'closed') {
      drop(ws);
      throw new ConnectionError('Connection closed');
    }
    this.ws = ws;
    this.lastNotice = undefined;
    ws.on('message', (data: WebSocket.Data) => {
      if (this.ws === ws) this.onFrame(String(data));
    });
    ws.on('close', () => {
      if (this.ws === ws) this.onClose();
    });
    ws.on('error', () => {
      // A failed socket also emits close
    });
    try {
      await this.authenticate();
      if ((this.state as State) === 'closed') throw new ConnectionError('Connection closed');
    } catch (e) {
      if (this.ws === ws) this.ws = null;
      drop(ws);
      throw e;
    }
  }

  private connectUrl(): string {
    const params: string[] = [];
    if (this._currentKg) params.push(`kg=${encodeURIComponent(this._currentKg)}`);
    if (this._lastSeq !== undefined) {
      params.push(`last_seq=${this._lastSeq}`);
      if (this._epoch) params.push(`epoch=${encodeURIComponent(this._epoch)}`);
    }
    if (params.length === 0) return this.url;
    const separator = this.url.includes('?') ? '&' : '?';
    return `${this.url}${separator}${params.join('&')}`;
  }

  // ── Authentication ──────────────────────────────────────────────

  private async authenticate(): Promise<void> {
    let msg: ClientMessage;
    const id = this.newId('a');
    if (this.apiKey) {
      msg = { type: 'authenticate', id, api_key: this.apiKey };
    } else if (this.username && this.password) {
      msg = { type: 'login', id, username: this.username, password: this.password };
    } else {
      throw new AuthenticationError(
        'No credentials provided (need username/password or apiKey)',
      );
    }

    const response = await this.request(msg, id, this.defaultTimeoutMs || 30_000);
    if (response.type === 'auth_error') {
      throw new AuthenticationError(response.message);
    }
    if (response.type === 'authenticated') {
      this._sessionId = response.session_id;
      this._serverVersion = response.version;
      this._role = response.role;
      this._currentKg = response.knowledge_graph;
      this._boundKg = response.knowledge_graph;
      if (response.stream_epoch !== this._epoch) {
        // A cursor from another engine run means nothing in this one.
        this._epoch = response.stream_epoch;
        this._lastSeq = undefined;
      }
      return;
    }
    throw new AuthenticationError(`Unexpected auth response: ${JSON.stringify(response)}`);
  }

  /** Send a control frame and wait for its reply (or the connection's end). */
  private request(msg: ClientMessage, id: string, timeoutMs: number): Promise<ServerMessage> {
    return new Promise<ServerMessage>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.control.delete(id);
        reject(new ConnectionError(`No reply to ${msg.type} within ${timeoutMs} ms`));
      }, timeoutMs);
      this.control.set(id, (reply) => {
        clearTimeout(timer);
        if (reply instanceof Error) {
          reject(reply);
        } else {
          resolve(reply);
        }
      });
      try {
        this.send(msg);
      } catch (e) {
        clearTimeout(timer);
        this.control.delete(id);
        reject(e);
      }
    });
  }

  // ── Command execution ───────────────────────────────────────────

  /**
   * Send a program/command and wait for its result. Calls may overlap: each
   * reply is routed to its call by id, and streamed results are assembled.
   *
   * Rejects with `QueryError` (or a subclass such as `DeadlineExceededError`)
   * for an `error` frame and `StatementFailedError` for a result whose
   * `errors` is not empty, so no caller can read a failed program as data.
   */
  execute(program: string, opts: ExecuteOptions = {}): Promise<ResultResponse> {
    return this.call({ type: 'execute', program }, program, mayWrite(program), opts);
  }

  /**
   * Run `queries` on one snapshot of the knowledge graph: every result is
   * exact at the reply's `revision`, in request order. Deadline and
   * cancellation stop the whole read, as for `execute`; an `error` frame
   * (naming the failing query) rejects it as a whole.
   */
  read(queries: NamedQuery[], opts: ExecuteOptions = {}): Promise<SnapshotResponse> {
    return this.call({ type: 'read', queries }, queryLines(queries), false, opts);
  }

  /**
   * Subscribe to the group `queries` as `subscription`; resolves with the
   * group's snapshot (`subscribed` names the generation its pushes carry).
   * Route the pushes with `routeSubscription` before calling. `timeoutMs`
   * bounds the call locally: the engine takes no deadline for it.
   */
  subscribeGroup(subscription: string, queries: NamedQuery[], opts: ExecuteOptions = {}): Promise<SnapshotResponse> {
    return this.call({ type: 'subscribe', subscription, queries }, queryLines(queries), false, opts);
  }

  private call<R extends ResultResponse | SnapshotResponse>(
    request: Request,
    program: string,
    writes: boolean,
    opts: ExecuteOptions,
  ): Promise<R> {
    if (this.state === 'closed' || (this.state === 'idle' && !this.lazy)) {
      return Promise.reject(withIql(new ConnectionError('Not connected'), program));
    }
    if (this.state === 'idle') {
      // A lazy connection opens on its first call; the call waits in the queue.
      this.connect().catch(() => {
        // The open failure rejects the queued calls
      });
    }
    if (opts.signal?.aborted) {
      return Promise.reject(withIql(new CancelledError('Cancelled before it was sent'), program));
    }
    return new Promise<R>((resolve, reject) => {
      const timeoutMs = opts.timeoutMs ?? this.defaultTimeoutMs;
      const call: Call = {
        request,
        program,
        mayWrite: writes,
        signal: opts.signal,
        resolve: resolve as (reply: ResultResponse | SnapshotResponse) => void,
        reject: (error) => reject(withIql(error, program)),
        retried: false,
        settled: false,
      };
      if (timeoutMs > 0) {
        call.expiresAt = performance.now() + timeoutMs;
        call.deadline = setTimeout(() => this.expire(call), timeoutMs);
      }
      if (call.signal) {
        call.onAbort = () => this.abort(call);
        call.signal.addEventListener('abort', call.onAbort, { once: true });
      }
      this.queue.push(call);
      this.pump();
    });
  }

  /** Send queued calls while the in-flight bound allows. */
  private pump(): void {
    while (this.state === 'open' && this.queue.length > 0 && this.inFlight.size < this.maxInFlight) {
      const call = this.queue.shift()!;
      const id = this.newId('r');
      call.id = id;
      call.stream = undefined;
      call.snapshot = undefined;
      this.inFlight.set(id, call);
      const timeoutMs =
        call.expiresAt === undefined ? undefined : Math.max(1, Math.ceil(call.expiresAt - performance.now()));
      const { request } = call;
      let msg: ClientMessage;
      if (request.type === 'subscribe') {
        // The engine takes no deadline for a subscribe: it is bounded here.
        msg = { type: 'subscribe', id, subscription: request.subscription, queries: request.queries };
      } else if (request.type === 'read') {
        msg = { type: 'read', id, queries: request.queries };
        if (timeoutMs !== undefined) msg.timeout_ms = timeoutMs;
      } else {
        msg = { type: 'execute', id, program: request.program };
        if (timeoutMs !== undefined) msg.timeout_ms = timeoutMs;
      }
      try {
        this.send(msg);
      } catch {
        // The socket is closing: its close handler fails the call.
        return;
      }
    }
  }

  /** The call's deadline passed. */
  private expire(call: Call): void {
    call.deadline = undefined;
    if (call.id && this.inFlight.get(call.id) === call) {
      // The engine's own deadline answers now; cancel in case it does not.
      this.sendCancel(call, 'deadline');
      this.probeServer();
      call.graced = true;
      this.armOverdue(call);
      return;
    }
    // Queued, or waiting to be resent after `rate_limited`: nothing ran.
    const queued = this.queue.indexOf(call);
    if (queued >= 0) this.queue.splice(queued, 1);
    this.settle(call);
    call.reject(new DeadlineExceededError('The deadline passed before the request was sent; nothing was applied'));
  }

  /** Past the deadline, wait a grace period for the reply (or its next streamed part). */
  private armOverdue(call: Call): void {
    if (call.deadline) clearTimeout(call.deadline);
    call.deadline = setTimeout(() => this.overdue(call), this.timeoutGraceMs);
  }

  /** A grace period past the deadline ended with nothing more of the reply: fail locally. */
  private overdue(call: Call): void {
    call.deadline = undefined;
    // The reply and the cancel's ack, if they come, are dropped and counted.
    if (call.cancelId) this.control.delete(call.cancelId);
    this.abandoned.set(call.id!, true);
    const waited = `No reply ${this.timeoutGraceMs} ms past the deadline`;
    this.finish(call, () => {
      throw call.mayWrite
        ? new OutcomeUnknownError(`${waited}: the program may have committed; read the state back before retrying it`)
        : new DeadlineExceededError(waited);
    });
  }

  /** The caller aborted the call. */
  private abort(call: Call): void {
    const queued = this.queue.indexOf(call);
    if (queued >= 0) {
      this.queue.splice(queued, 1);
      this.settle(call);
      call.reject(new CancelledError('Cancelled before it was sent'));
      return;
    }
    if (call.id && this.inFlight.get(call.id) === call) this.sendCancel(call, 'signal');
  }

  private sendCancel(call: Call, by: 'deadline' | 'signal'): void {
    if (call.cancelledBy || !call.id) return;
    call.cancelledBy = by;
    const id = this.newId('c');
    call.cancelId = id;
    // The ack follows the target's own reply, which settles the call.
    this.control.set(id, () => {});
    try {
      this.send({ type: 'cancel', id, target: call.id });
    } catch {
      this.control.delete(id);
    }
  }

  /**
   * Ping the transport: a live server answers at once, even while computing,
   * unless its request window is full (it then reads nothing more). Any frame
   * from it answers the probe too. No answer within `timeoutGraceMs` while
   * the window has room means the connection is dead: drop it, so it
   * reconnects. While the window is full no verdict is taken: each call's
   * deadline bounds it, and freed slots let the next probe judge.
   */
  private probeServer(): void {
    const ws = this.ws;
    if (!ws || this.probe) return;
    const timer = setTimeout(() => {
      this.probe = undefined;
      const outstanding = this.inFlight.size + this.control.size;
      if (this.ws === ws && outstanding < this.maxInFlight) ws.terminate();
    }, Math.max(1, this.timeoutGraceMs));
    this.probe = { timer, ws };
    ws.once('pong', () => {
      if (this.probe?.ws === ws) this.clearProbe();
    });
    try {
      ws.ping();
    } catch {
      // close follows
    }
  }

  private clearProbe(): void {
    if (this.probe) {
      clearTimeout(this.probe.timer);
      this.probe = undefined;
    }
  }

  // ── Frame routing ───────────────────────────────────────────────

  private onFrame(data: string): void {
    this.clearProbe();
    let msg: ServerMessage;
    try {
      msg = deserializeMessage(data);
    } catch {
      this._stats.malformedFrames += 1;
      return;
    }
    if (isPush(msg)) {
      this.onPush(msg);
      return;
    }
    const id = msg.id;
    if (id === undefined) {
      this._stats.staleReplies += 1;
      return;
    }
    const control = this.control.get(id);
    if (control) {
      this.control.delete(id);
      control(msg);
      return;
    }
    const call = this.inFlight.get(id);
    if (!call) {
      const stale = this.abandoned.get(id);
      if (stale !== undefined) {
        if (
          msg.type === 'result' ||
          msg.type === 'result_end' ||
          msg.type === 'snapshot' ||
          msg.type === 'snapshot_end' ||
          msg.type === 'error'
        ) {
          this.abandoned.delete(id);
          if (stale) this._stats.staleReplies += 1;
        }
      } else {
        this._stats.staleReplies += 1;
      }
      return;
    }
    this.onReply(call, id, msg);
  }

  private onReply(call: Call, id: string, msg: ServerMessage): void {
    if (msg.type === 'error') {
      if (msg.code === 'rate_limited' && !call.retried && !call.cancelledBy) {
        this.retryLater(call, id);
        return;
      }
      this.finish(call, () => {
        // The deadline's own cancel can reach the engine before its deadline does.
        if (msg.code === 'cancelled' && call.cancelledBy === 'deadline') {
          throw new DeadlineExceededError(msg.message);
        }
        throw queryError(msg);
      });
      return;
    }
    const expected = call.request.type === 'execute' ? RESULT_FRAMES : SNAPSHOT_FRAMES;
    if (!expected.has(msg.type)) {
      const error = new InternalError(`Unexpected ${msg.type} frame in reply to ${call.request.type}`);
      if (msg.type.endsWith('_start') || msg.type.endsWith('_chunk')) {
        // Parts of a reply announce more frames: drop them too.
        this.abandon(call, id, error);
      } else {
        this.finish(call, () => {
          throw error;
        });
      }
      return;
    }
    switch (msg.type) {
      case 'result':
        this.finish(call, () => this.accept(msg));
        return;
      case 'result_start':
        if (call.stream) {
          this.abandon(call, id, new InternalError('A second result_start arrived inside a streamed result'));
          return;
        }
        call.stream = { start: msg, rows: [], provenance: [], chunks: 0 };
        if (call.graced) this.armOverdue(call);
        return;
      case 'result_chunk': {
        const stream = call.stream;
        if (!stream || msg.chunk_index !== stream.chunks) {
          this.abandon(
            call,
            id,
            new InternalError(
              `Streamed result chunk ${msg.chunk_index} arrived, expected ${stream?.chunks ?? 'result_start'}`,
            ),
          );
          return;
        }
        stream.chunks += 1;
        stream.rows.push(...msg.rows);
        if (msg.row_provenance) stream.provenance.push(...msg.row_provenance);
        if (call.graced) this.armOverdue(call);
        return;
      }
      case 'result_end': {
        const stream = call.stream;
        if (!stream) {
          this.finish(call, () => {
            throw new InternalError('result_end arrived without result_start');
          });
          return;
        }
        this.finish(call, () => {
          if (msg.chunk_count !== stream.chunks || msg.row_count !== stream.rows.length) {
            throw new InternalError(
              `Incomplete streamed result: ${stream.chunks} chunk(s) and ${stream.rows.length} row(s) ` +
                `arrived, end announces ${msg.chunk_count} and ${msg.row_count}`,
            );
          }
          return this.accept(assemble(stream, msg.row_count));
        });
        return;
      }
      case 'snapshot':
        this.finish(call, () => {
          if (call.snapshot) throw new InternalError('A snapshot arrived inside a streamed snapshot');
          return answers(call, msg);
        });
        return;
      case 'snapshot_start':
        if (call.snapshot) {
          this.abandon(call, id, new InternalError('A second snapshot_start arrived inside a streamed snapshot'));
          return;
        }
        call.snapshot = { start: msg, rows: msg.results.map(() => []), result: 0, chunks: 0 };
        if (call.graced) this.armOverdue(call);
        return;
      case 'snapshot_chunk': {
        const snapshot = call.snapshot;
        const broken = brokenChunk(snapshot, msg);
        if (!snapshot || broken) {
          this.abandon(call, id, new InternalError(broken ?? 'snapshot_chunk arrived without snapshot_start'));
          return;
        }
        snapshot.chunks += 1;
        snapshot.result = msg.result;
        snapshot.rows[msg.result].push(...msg.rows);
        if (call.graced) this.armOverdue(call);
        return;
      }
      case 'snapshot_end': {
        const snapshot = call.snapshot;
        this.finish(call, () => {
          if (!snapshot) throw new InternalError('snapshot_end arrived without snapshot_start');
          const short = snapshot.start.results.findIndex((r, i) => r.row_count !== snapshot.rows[i].length);
          if (msg.chunk_count !== snapshot.chunks || short >= 0) {
            throw new InternalError(
              `Incomplete streamed snapshot: ${snapshot.chunks} chunk(s) arrived, end announces ${msg.chunk_count}` +
                (short >= 0
                  ? `; result ${short} has ${snapshot.rows[short].length} row(s), its header announces ` +
                    `${snapshot.start.results[short].row_count}`
                  : ''),
            );
          }
          return answers(call, assembleSnapshot(snapshot));
        });
        return;
      }
    }
  }

  /** Settle a call with the outcome of `outcome`, and send what waits. */
  private finish(call: Call, outcome: () => ResultResponse | SnapshotResponse): void {
    if (call.id) this.inFlight.delete(call.id);
    this.settle(call);
    try {
      call.resolve(outcome());
    } catch (e) {
      call.reject(e as Error);
    }
    this.pump();
  }

  /** Fail a call whose stream broke; drop the rest of its frames. */
  private abandon(call: Call, id: string, error: Error): void {
    this.abandoned.set(id, false);
    this.finish(call, () => {
      throw error;
    });
  }

  /** `rate_limited`: nothing ran. Resend once after the rate window. */
  private retryLater(call: Call, id: string): void {
    this.inFlight.delete(id);
    call.retried = true;
    call.id = undefined;
    setTimeout(() => {
      if (call.settled) return;
      if (call.signal?.aborted) {
        this.settle(call);
        call.reject(new CancelledError('Cancelled before it was resent'));
        return;
      }
      this.queue.unshift(call);
      this.pump();
    }, RATE_WINDOW_MS);
    this.pump();
  }

  private settle(call: Call): void {
    call.settled = true;
    if (call.deadline) clearTimeout(call.deadline);
    call.deadline = undefined;
    if (call.signal && call.onAbort) call.signal.removeEventListener('abort', call.onAbort);
  }

  /** Track a KG switch, then throw if any statement failed. */
  private accept(result: ResultResponse): ResultResponse {
    if (result.switched_kg) {
      this._currentKg = result.switched_kg;
    }
    const unknown = result.errors?.find((e) => e.code === 'outcome_unknown');
    if (unknown) throw new OutcomeUnknownError(unknown.message, result);
    const readOnly = result.errors?.find((e) => e.code === 'store_read_only');
    if (readOnly) throw new StoreReadOnlyError(readOnly.message, result);
    if (result.errors && result.errors.length > 0) {
      throw new StatementFailedError(result.errors, result);
    }
    return result;
  }

  // ── Pushes ──────────────────────────────────────────────────────

  private onPush(msg: PushMessage): void {
    switch (msg.type) {
      case 'notice':
        this.onNotice(msg);
        return;
      case 'persistent_update':
      case 'rule_change':
      case 'kg_change':
      case 'schema_change':
        this.onNotification(msg);
        return;
      default:
        this.onSubscriptionPush(msg);
    }
  }

  private onNotice(notice: NoticeResponse): void {
    if (OPEN_NOTICES.has(notice.code)) {
      this.emit('notification_gap', { code: notice.code, message: notice.message });
      return;
    }
    this.lastNotice = notice;
    // The server closes the connection next, which fails what waits with
    // this notice's code.
  }

  private onNotification(notif: NotificationResponse): void {
    this._lastSeq = Math.max(this._lastSeq ?? 0, notif.seq);
    const event: NotificationEvent = {
      type: notif.type,
      seq: notif.seq,
      timestampMs: notif.timestamp_ms,
      sessionId: notif.session_id,
      knowledgeGraph: notif.knowledge_graph,
      relation: notif.relation,
      operation: notif.operation,
      count: notif.count,
      ruleName: notif.rule_name,
      entity: notif.entity,
    };
    this._dispatcher.dispatch(event, this._epoch);
  }

  /**
   * Route a subscription's pushes to `sink`. Register the route before
   * sending `.subscribe`, then call `setGeneration` with the generation its
   * reply names: pushes that arrive before that are held and then filtered,
   * and pushes of any other generation are dropped and counted (also by
   * `onStale`, when given).
   */
  routeSubscription(
    subscription: string,
    sink: (push: SubscriptionPushMessage) => void,
    onStale?: () => void,
  ): SubscriptionRoute {
    const route: Route = { sink, onStale, early: [] };
    this.routes.set(subscription, route);
    return {
      setGeneration: (generation: number) => {
        route.generation = generation;
        const early = route.early.splice(0);
        for (const push of early) this.deliver(route, push);
      },
      close: () => {
        if (this.routes.get(subscription) === route) this.routes.delete(subscription);
      },
    };
  }

  private onSubscriptionPush(push: SubscriptionPushMessage): void {
    const route = this.routes.get(push.subscription);
    if (!route) {
      this._stats.stalePushes += 1;
      return;
    }
    if (route.generation === undefined) {
      route.early.push(push);
      return;
    }
    this.deliver(route, push);
  }

  private deliver(route: Route, push: SubscriptionPushMessage): void {
    if (push.generation !== route.generation) {
      this._stats.stalePushes += 1;
      route.onStale?.();
      return;
    }
    try {
      route.sink(push);
    } catch {
      // A sink must not break the reader
    }
  }

  // ── Close and reconnect ─────────────────────────────────────────

  private onClose(): void {
    this.ws = null;
    this.stopKeepalive();
    this.clearProbe();
    const code = this.lastNotice?.code ?? 'closed';
    const reason = this.lastNotice?.message ?? 'The connection closed';
    // Waiting control requests (an authentication in progress) fail now: a
    // close is transient, so a reconnect keeps trying.
    this.failControl(new ConnectionLostError(`Connection lost: ${reason}`, code));
    this.failInFlight(code, reason);
    if (this.state !== 'open') return; // an open attempt reports its own failure
    this.emit('disconnected', { code });
    if (!this.autoReconnect) {
      this.state = 'closed';
      this.failQueued(new ConnectionLostError(`Connection lost: ${reason}`, code));
      return;
    }
    this.state = 'reconnecting';
    this.reconnecting = this.reconnect(code, reason).finally(() => {
      this.reconnecting = undefined;
    });
  }

  private failControl(error: Error): void {
    for (const [id, waiter] of this.control) {
      this.control.delete(id);
      waiter(error);
    }
  }

  private failInFlight(code: string, reason: string): void {
    const calls = [...this.inFlight.values()];
    this.inFlight.clear();
    this.abandoned.clear();
    for (const call of calls) {
      this.settle(call);
      call.reject(
        new ConnectionLostError(`Connection lost: ${reason}`, code, call.mayWrite),
      );
    }
  }

  private failQueued(error: Error): void {
    const calls = this.queue.splice(0);
    for (const call of calls) {
      this.settle(call);
      call.reject(copyError(error));
    }
  }

  private async reconnect(code: string, reason: string): Promise<void> {
    const hadCursor = this._lastSeq !== undefined;
    let attempts = 0;
    let delay = this.reconnectDelay;
    let lastError: Error = new ConnectionLostError(`Connection lost: ${reason}`, code);
    for (let attempt = 1; attempt <= this.maxReconnectAttempts; attempt++) {
      // Full jitter in [delay/2, delay] so clients do not reconnect in step.
      await new Promise<void>((resolve) => {
        const timer = setTimeout(resolve, delay * 1000 * (0.5 + Math.random() / 2));
        this.wake = () => {
          clearTimeout(timer);
          resolve();
        };
      });
      this.wake = undefined;
      if (this.state !== 'reconnecting') return;
      attempts = attempt;
      try {
        await this.open();
      } catch (e) {
        if (this.state !== 'reconnecting') return;
        lastError = e as Error;
        if (e instanceof AuthenticationError) break;
        delay = Math.min(delay * 2, this.maxReconnectDelay);
        continue;
      }
      this.state = 'open';
      this._stats.reconnects += 1;
      this.startKeepalive();
      this.emit('reconnected', { attempt });
      this.emit('session_reset', {});
      if (!hadCursor) {
        this.emit('notification_gap', {
          code: 'no_cursor',
          message: 'Reconnected with no notification cursor: notifications sent while disconnected were not replayed',
        });
      }
      this.pump();
      return;
    }
    if (this.state !== 'reconnecting') return;
    this.state = 'closed';
    const error = new ConnectionLostError(
      `Reconnecting failed after ${attempts} attempt(s): ${lastError.message}`,
      code,
    );
    this.failQueued(error);
    this.emit('closed', { error });
  }

  // ── Keep-alive ──────────────────────────────────────────────────

  /** Send an application ping and wait for its pong. */
  async ping(): Promise<void> {
    if (this.state !== 'open') throw new ConnectionError('Not connected');
    const id = this.newId('p');
    await this.request({ type: 'ping', id }, id, this.defaultTimeoutMs || 30_000);
  }

  private startKeepalive(): void {
    this.stopKeepalive();
    if (this.keepaliveMs <= 0) return;
    const period = Math.max(10, Math.floor(this.keepaliveMs / 2));
    this.keepalive = setInterval(() => {
      if (this.state !== 'open') return;
      if (Date.now() - this.lastSentAt < this.keepaliveMs) return;
      if (this.inFlight.size === 0) {
        this.ping().catch(() => {
          // Liveness is the transport probe's to judge
        });
      }
      this.probeServer();
    }, period);
    this.keepalive.unref?.();
  }

  private stopKeepalive(): void {
    if (this.keepalive) clearInterval(this.keepalive);
    this.keepalive = undefined;
  }

  // ── Helpers ─────────────────────────────────────────────────────

  private send(msg: ClientMessage): void {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      throw new ConnectionError('Not connected');
    }
    this.ws.send(serializeMessage(msg));
    this.lastSentAt = Date.now();
  }

  private newId(prefix: string): string {
    this.nextId += 1;
    return `${prefix}${this.nextId}`;
  }

  private emit(type: ConnectionEventType, detail: object): void {
    this.events.dispatchEvent(new CustomEvent(type, { detail }));
  }
}

/** Close a socket whose frames no longer matter. */
function drop(ws: WebSocket): void {
  ws.removeAllListeners();
  ws.on('error', () => {});
  ws.close();
}

function createWebSocket(url: string): Promise<WebSocket> {
  return new Promise<WebSocket>((resolve, reject) => {
    const ws = new WebSocket(url);
    ws.once('open', () => {
      ws.removeAllListeners('error');
      resolve(ws);
    });
    ws.once('error', (err) => reject(err));
  });
}

function assemble(stream: Stream, rowCount: number): ResultResponse {
  const start = stream.start;
  return {
    type: 'result',
    id: start.id,
    columns: start.columns,
    rows: stream.rows,
    row_count: rowCount,
    total_count: start.total_count,
    truncated: start.truncated,
    execution_time_ms: start.execution_time_ms,
    row_provenance: stream.provenance.length > 0 ? stream.provenance : undefined,
    metadata: start.metadata,
    switched_kg: start.switched_kg,
    proof_trees: start.proof_trees,
    timing_breakdown: start.timing_breakdown,
    errors: start.errors,
    subscribed: start.subscribed,
  };
}

/**
 * Why `chunk` cannot extend `snapshot`: out of order, for a result before
 * the last chunk's or past the last one, empty, or past its result's
 * announced rows. Undefined when it fits.
 */
function brokenChunk(snapshot: SnapshotStream | undefined, chunk: SnapshotChunkResponse): string | undefined {
  if (!snapshot) return undefined;
  if (chunk.chunk_index !== snapshot.chunks) {
    return `Streamed snapshot chunk ${chunk.chunk_index} arrived, expected ${snapshot.chunks}`;
  }
  const header = snapshot.start.results[chunk.result];
  if (!Number.isInteger(chunk.result) || chunk.result < snapshot.result || header === undefined) {
    return (
      `Streamed snapshot chunk ${chunk.chunk_index} holds rows of result ${chunk.result}, ` +
      `after result ${snapshot.result} of ${snapshot.start.results.length}`
    );
  }
  if (chunk.rows.length === 0 || snapshot.rows[chunk.result].length + chunk.rows.length > header.row_count) {
    return (
      `Streamed snapshot chunk ${chunk.chunk_index} brings result ${chunk.result} to ` +
      `${snapshot.rows[chunk.result].length + chunk.rows.length} row(s), its header announces ${header.row_count}`
    );
  }
  return undefined;
}

function assembleSnapshot(stream: SnapshotStream): SnapshotResponse {
  const { start } = stream;
  return {
    type: 'snapshot',
    id: start.id,
    knowledge_graph: start.knowledge_graph,
    revision: start.revision,
    results: start.results.map((header, i) => ({
      name: header.name,
      columns: header.columns,
      rows: stream.rows[i],
      total_count: header.total_count,
      truncated: header.truncated,
    })),
    execution_time_ms: start.execution_time_ms,
    subscribed: start.subscribed,
  };
}

/** `snapshot`, if it holds one result per query of the call, in order. */
function answers(call: Call, snapshot: SnapshotResponse): SnapshotResponse {
  const queries = call.request.type === 'execute' ? [] : call.request.queries;
  const names = snapshot.results.map((r) => r.name);
  if (names.length !== queries.length || queries.some((q, i) => q.name !== names[i])) {
    throw new InternalError(
      `The snapshot's results (${names.join(', ')}) do not answer the queries (${queries.map((q) => q.name).join(', ')})`,
    );
  }
  return snapshot;
}

/** The queries of a read or a group, one per line, as its errors carry them. */
function queryLines(queries: NamedQuery[]): string {
  return queries.map((q) => q.query).join('\n');
}

function queryError(response: ErrorResponse): QueryError {
  switch (response.code) {
    case 'outcome_unknown':
      return new OutcomeUnknownError(response.message);
    case 'store_read_only':
      return new StoreReadOnlyError(response.message);
    case 'deadline_exceeded':
      return new DeadlineExceededError(response.message);
    case 'cancelled':
      return new CancelledError(response.message);
    case 'invalid_request':
      return new ProtocolError(response.message);
    case 'rate_limited':
      return new RateLimitedError(response.message);
    default:
      return new QueryError(response.message, {
        code: response.code,
        validationErrors: response.validation_errors,
      });
  }
}

function withIql(error: Error, program: string): Error {
  if (error instanceof InputLayerError && error.iql === undefined) error.iql = program;
  return error;
}

/** A copy of `error` for one of several calls, so each carries its own program. */
function copyError(error: Error): Error {
  const copy = Object.create(Object.getPrototypeOf(error)) as Error;
  return Object.assign(copy, error, { message: error.message, stack: error.stack });
}

/**
 * Whether a program may write: anything but queries (`?...`). A lost
 * connection leaves such a call's outcome open.
 */
function mayWrite(program: string): boolean {
  return program
    .split('\n')
    .map((line) => line.trim())
    .some((line) => line !== '' && !line.startsWith('?') && !line.startsWith('//'));
}
