/**
 * InputLayer - top-level async client.
 */

import { Connection, type ConnectionEventType } from './connection.js';
import { KnowledgeGraph } from './knowledge-graph.js';
import { QueryError } from './errors.js';
import {
  NotificationDispatcher,
  type NotificationEvent,
  type NotificationCallback,
} from './notifications.js';
import {
  type UserInfo,
  type ApiKeyInfo,
  compileCreateUser,
  compileDropUser,
  compileSetPassword,
  compileSetRole,
  compileListUsers,
  compileCreateApiKey,
  compileExpireApiKey,
  compileListApiKeys,
  compileRevokeApiKey,
  parseApiKeys,
} from './auth.js';

export interface InputLayerOptions {
  /** WebSocket URL (e.g. "ws://localhost:8080/ws") */
  url: string;
  /** Username for login auth */
  username?: string;
  /** Password for login auth */
  password?: string;
  /** API key for token auth */
  apiKey?: string;
  /** Enable auto-reconnect on connection loss (default: true) */
  autoReconnect?: boolean;
  /** First reconnect backoff in seconds, doubled per attempt with jitter (default: 1.0) */
  reconnectDelay?: number;
  /** Longest reconnect backoff in seconds (default: 30) */
  maxReconnectDelay?: number;
  /** Max reconnect attempts before giving up (default: 10) */
  maxReconnectAttempts?: number;
  /** Knowledge graph of the client's own connection (default: the server's "default") */
  initialKg?: string;
  /** Last notification sequence seen, to replay what followed it... */
  lastSeq?: number;
  /** ...and the `stream_epoch` it belongs to (`InputLayer.epoch`). */
  epoch?: string;
  /** Deadline of a call without its own `timeoutMs` (default: 30 000; 0 for none) */
  defaultTimeoutMs?: number;
  /** Wait past a deadline, after cancelling, before failing the call locally (default: 2 000) */
  timeoutGraceMs?: number;
  /** Ping an otherwise silent connection this often so it is never idle-closed, and probe the transport: no pong and no frame within `timeoutGraceMs` drops and reconnects it, except while the request window is full, when each call's deadline bounds it (default: 20 000; 0 disables) */
  keepaliveMs?: number;
  /** Requests in flight per connection (default: 15, one below the server's bound so a cancel can always be read) */
  maxInFlight?: number;
}

/**
 * Async client for InputLayer knowledge graph engine.
 *
 * @example
 * ```typescript
 * import { InputLayer, relation } from 'inputlayer';
 *
 * const Employee = relation("Employee", {
 *   id: "int",
 *   name: "string",
 *   department: "string",
 *   salary: "float",
 *   active: "bool",
 * });
 *
 * const il = new InputLayer({ url: "ws://localhost:8080/ws", username: "admin", password: "admin" });
 * await il.connect();
 *
 * const kg = il.knowledgeGraph("default");
 * await kg.define(Employee);
 * await kg.insert(Employee, { id: 1, name: "Alice", department: "eng", salary: 120000, active: true });
 * const result = await kg.query({ select: [Employee] });
 *
 * await il.close();
 * ```
 */
export class InputLayer {
  private readonly opts: InputLayerOptions;
  private readonly conn: Connection;
  private readonly dispatcher = new NotificationDispatcher();
  private creating: Promise<unknown> = Promise.resolve();
  private readonly kgs = new Map<string, KnowledgeGraph>();

  /**
   * Events of every connection the client holds (see `ConnectionEventType`),
   * each `CustomEvent`'s `detail` naming its `knowledgeGraph` handle (absent
   * for the client's own connection).
   */
  readonly events = new EventTarget();

  constructor(opts: InputLayerOptions) {
    this.opts = opts;
    this.conn = this.newConnection(opts.initialKg, {
      lastSeq: opts.lastSeq,
      epoch: opts.epoch,
    });
  }

  private newConnection(
    kg: string | undefined,
    extra: { lastSeq?: number; epoch?: string; handle?: string } = {},
  ): Connection {
    const opts = this.opts;
    const conn = new Connection({
      url: opts.url,
      username: opts.username,
      password: opts.password,
      apiKey: opts.apiKey,
      autoReconnect: opts.autoReconnect,
      reconnectDelay: opts.reconnectDelay,
      maxReconnectDelay: opts.maxReconnectDelay,
      maxReconnectAttempts: opts.maxReconnectAttempts,
      defaultTimeoutMs: opts.defaultTimeoutMs,
      timeoutGraceMs: opts.timeoutGraceMs,
      keepaliveMs: opts.keepaliveMs,
      maxInFlight: opts.maxInFlight,
      initialKg: kg,
      lastSeq: extra.lastSeq,
      epoch: extra.epoch,
      dispatcher: this.dispatcher,
      lazy: extra.handle !== undefined,
      createKg: extra.handle !== undefined ? (name) => this.createIfMissing(name) : undefined,
    });
    const types: ConnectionEventType[] = [
      'disconnected',
      'reconnected',
      'session_reset',
      'notification_gap',
      'closed',
    ];
    for (const type of types) {
      conn.events.addEventListener(type, (event) => {
        const detail = (event as CustomEvent).detail ?? {};
        this.events.dispatchEvent(
          new CustomEvent(type, { detail: { ...detail, knowledgeGraph: extra.handle } }),
        );
      });
    }
    return conn;
  }

  /**
   * A handle's graph refused the open: create it if it does not exist. The
   * client's own connection creates it (`conflict` means it exists, so the
   * refusal stands) and switches back, because `.kg create` also moves the
   * creating session onto the new graph, which could then not be dropped.
   * Creations run one at a time so none reads another's switch as home.
   */
  private createIfMissing(name: string): Promise<boolean> {
    const run = this.creating.then(() => this.createKg(name));
    this.creating = run.catch(() => undefined);
    return run;
  }

  private async createKg(name: string): Promise<boolean> {
    await this.conn.connect();
    const home = this.conn.boundKg;
    try {
      await this.conn.execute(`.kg create ${name}`);
    } catch (e) {
      if (e instanceof QueryError && e.code === 'conflict') return false;
      throw e;
    }
    if (home && this.conn.currentKg !== home) await this.conn.execute(`.kg use ${home}`);
    return true;
  }

  // ── Connection lifecycle ────────────────────────────────────────

  /** Connect and authenticate the client's own connection. */
  async connect(): Promise<void> {
    await this.conn.connect();
  }

  /** Close every connection: the client's own and each knowledge graph handle's. */
  async close(): Promise<void> {
    await Promise.all([
      this.conn.close(),
      ...[...this.kgs.values()].map((kg) => kg.connection.close()),
    ]);
  }

  // ── Properties ──────────────────────────────────────────────────

  get connected(): boolean {
    return this.conn.connected;
  }

  get sessionId(): string | undefined {
    return this.conn.sessionId;
  }

  get serverVersion(): string | undefined {
    return this.conn.serverVersion;
  }

  get role(): string | undefined {
    return this.conn.role;
  }

  /** The highest notification `seq` dispatched; pass it back with `epoch` as `lastSeq`. */
  get lastSeq(): number {
    return this.dispatcher.lastSeq;
  }

  /** The engine run (`stream_epoch`) notification `seq` numbers belong to. */
  get epoch(): string | undefined {
    return this.conn.epoch;
  }

  // ── KG management ───────────────────────────────────────────────

  /**
   * Get a KnowledgeGraph handle. Each handle has its own connection, bound to
   * its graph when it first opens (creating the graph if it is missing), so
   * handles never switch graphs under one another.
   */
  knowledgeGraph(name: string): KnowledgeGraph {
    let kg = this.kgs.get(name);
    if (!kg) {
      kg = new KnowledgeGraph(name, this.newConnection(name, { handle: name }));
      this.kgs.set(name, kg);
    }
    return kg;
  }

  /** List all knowledge graphs. */
  async listKnowledgeGraphs(): Promise<string[]> {
    const result = await this.conn.execute('.kg list');
    return result.rows.length > 0 ? result.rows.map((row) => String(row[0])) : [];
  }

  async dropKnowledgeGraph(name: string): Promise<void> {
    await this.conn.execute(`.kg drop ${name}`);
    const kg = this.kgs.get(name);
    this.kgs.delete(name);
    await kg?.connection.close();
  }

  // ── User management ─────────────────────────────────────────────

  async createUser(username: string, password: string, role = 'viewer'): Promise<void> {
    await this.conn.execute(compileCreateUser(username, password, role));
  }

  async dropUser(username: string): Promise<void> {
    await this.conn.execute(compileDropUser(username));
  }

  async setPassword(username: string, newPassword: string): Promise<void> {
    await this.conn.execute(compileSetPassword(username, newPassword));
  }

  async setRole(username: string, role: string): Promise<void> {
    await this.conn.execute(compileSetRole(username, role));
  }

  async listUsers(): Promise<UserInfo[]> {
    const result = await this.conn.execute(compileListUsers());
    return result.rows
      .filter((row) => row.length >= 2)
      .map((row) => ({
        username: String(row[0]),
        role: String(row[1]),
      }));
  }

  // ── API key management ──────────────────────────────────────────

  /**
   * Create an API key, expiring `ttl` from now (e.g. `"90d"`) or never.
   * Returns the key, which the server shows only once.
   */
  async createApiKey(label: string, ttl?: string): Promise<string> {
    const result = await this.conn.execute(compileCreateApiKey(label, ttl));
    return String(result.rows[0][1]);
  }

  async listApiKeys(): Promise<ApiKeyInfo[]> {
    const result = await this.conn.execute(compileListApiKeys());
    return parseApiKeys(result.columns, result.rows);
  }

  /**
   * Bring a key's expiry forward to `ttl` from now, e.g. the grace period of
   * a rotation. An expiry can only be brought forward.
   */
  async expireApiKey(label: string, ttl: string): Promise<void> {
    await this.conn.execute(compileExpireApiKey(label, ttl));
  }

  async revokeApiKey(label: string): Promise<void> {
    await this.conn.execute(compileRevokeApiKey(label));
  }

  // ── Notifications ───────────────────────────────────────────────

  /**
   * Register a notification callback. Notifications arrive on every open
   * connection of the client: its own (on `initialKg`) and each knowledge
   * graph handle's once that handle has made a call. One seen on several
   * connections is delivered once.
   *
   * @param eventType - Filter by event type (e.g. "persistent_update")
   * @param callback - Function to call when event arrives
   * @param opts - Additional filters
   */
  on(
    eventType: string,
    callback: NotificationCallback,
    opts?: { relation?: string; knowledgeGraph?: string },
  ): void {
    this.dispatcher.on(eventType, opts ?? {}, callback);
  }

  /** Remove a notification callback. */
  off(callback: NotificationCallback): void {
    this.dispatcher.off(callback);
  }

  /** Async iterator yielding notification events. */
  async *notifications(): AsyncIterableIterator<NotificationEvent> {
    yield* this.dispatcher.events();
  }
}
