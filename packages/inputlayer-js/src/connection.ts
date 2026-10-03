/**
 * WebSocket connection management with authentication and streaming support.
 */

import WebSocket from 'ws';
import {
  type ClientMessage,
  type ErrorResponse,
  type ResultResponse,
  type ServerMessage,
  type NotificationResponse,
  type ResultStartResponse,
  serializeMessage,
  deserializeMessage,
  isPush,
} from './protocol.js';
import {
  AuthenticationError,
  ConnectionError,
  InternalError,
  QueryError,
  StatementFailedError,
  OutcomeUnknownError,
  StoreReadOnlyError,
} from './errors.js';
import { type NotificationEvent, NotificationDispatcher } from './notifications.js';

export interface ConnectionOptions {
  url: string;
  username?: string;
  password?: string;
  apiKey?: string;
  autoReconnect?: boolean;
  reconnectDelay?: number;
  maxReconnectAttempts?: number;
  initialKg?: string;
  lastSeq?: number;
}

/**
 * Manages the WebSocket connection to an InputLayer server.
 */
export class Connection {
  private readonly url: string;
  private readonly username?: string;
  private readonly password?: string;
  private readonly apiKey?: string;
  private readonly autoReconnect: boolean;
  private readonly reconnectDelay: number;
  private readonly maxReconnectAttempts: number;
  private readonly initialKg?: string;

  private ws: WebSocket | null = null;
  private _sessionId?: string;
  private _serverVersion?: string;
  private _role?: string;
  private _currentKg?: string;
  private _connected = false;
  private _lastSeq?: number;

  private readonly _dispatcher = new NotificationDispatcher();

  // Reply frames of the call in flight, in arrival order, and the reader
  // waiting for the next one. A reply can arrive in one burst (result_start,
  // chunks, result_end), so frames queue until the reader takes them.
  private inFlight = false;
  private readonly replyFrames: ServerMessage[] = [];
  private nextFrame?: (msg: ServerMessage) => void;

  constructor(opts: ConnectionOptions) {
    this.url = opts.url;
    this.username = opts.username;
    this.password = opts.password;
    this.apiKey = opts.apiKey;
    this.autoReconnect = opts.autoReconnect ?? true;
    this.reconnectDelay = opts.reconnectDelay ?? 1.0;
    this.maxReconnectAttempts = opts.maxReconnectAttempts ?? 10;
    this.initialKg = opts.initialKg;
    this._lastSeq = opts.lastSeq;
  }

  // ── Properties ──────────────────────────────────────────────────

  get connected(): boolean {
    return this._connected;
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

  get currentKg(): string | undefined {
    return this._currentKg;
  }

  /** Force-set the current KG (used by KnowledgeGraph after .kg use). */
  setCurrentKg(name: string): void {
    this._currentKg = name;
  }

  get dispatcher(): NotificationDispatcher {
    return this._dispatcher;
  }

  get lastSeq(): number {
    return this._dispatcher.lastSeq;
  }

  // ── Connection lifecycle ────────────────────────────────────────

  async connect(): Promise<void> {
    let wsUrl = this.url;
    const params: string[] = [];
    if (this.initialKg) {
      params.push(`kg=${this.initialKg}`);
    }
    if (this._lastSeq !== undefined) {
      params.push(`last_seq=${this._lastSeq}`);
    }
    if (params.length > 0) {
      const separator = wsUrl.includes('?') ? '&' : '?';
      wsUrl = `${wsUrl}${separator}${params.join('&')}`;
    }

    try {
      this.ws = await this.createWebSocket(wsUrl);
    } catch (e) {
      throw new ConnectionError(`Failed to connect to ${wsUrl}: ${e}`);
    }

    await this.authenticate();
    this._connected = true;

    // Notifications dispatch at once; other frames belong to the call in flight.
    this.ws.on('message', (data: WebSocket.Data) => {
      let msg: ServerMessage;
      try {
        msg = deserializeMessage(String(data));
      } catch {
        return; // Ignore parse errors in background
      }
      if (isPush(msg)) {
        // Subscription pushes have no consumer in this SDK yet; a closing
        // notice is followed by `close`, which fails the call in flight.
        if (this.isNotification(msg)) {
          this.dispatchNotification(msg as NotificationResponse);
        }
      } else if (this.nextFrame) {
        const deliver = this.nextFrame;
        this.nextFrame = undefined;
        deliver(msg);
      } else if (this.inFlight) {
        this.replyFrames.push(msg);
      }
    });

    this.ws.on('close', () => {
      this._connected = false;
      if (this.autoReconnect) {
        this.reconnect().catch(() => {
          // Reconnection failed
        });
      }
    });

    this.ws.on('error', () => {
      // Errors will trigger close
    });
  }

  async close(): Promise<void> {
    this._connected = false;
    if (this.ws) {
      this.ws.removeAllListeners();
      this.ws.close();
      this.ws = null;
    }
  }

  // ── Authentication ──────────────────────────────────────────────

  private async authenticate(): Promise<void> {
    if (!this.ws) throw new ConnectionError('Not connected');

    let msg: ClientMessage;
    if (this.apiKey) {
      msg = { type: 'authenticate', api_key: this.apiKey };
    } else if (this.username && this.password) {
      msg = { type: 'login', username: this.username, password: this.password };
    } else {
      throw new AuthenticationError(
        'No credentials provided (need username/password or apiKey)',
      );
    }

    this.ws.send(serializeMessage(msg));
    const response = await this.receiveOne();

    if (response.type === 'auth_error' || response.type === 'notice') {
      throw new AuthenticationError(response.message);
    }
    if (response.type === 'authenticated') {
      this._sessionId = response.session_id;
      this._serverVersion = response.version;
      this._role = response.role;
      this._currentKg = response.knowledge_graph;
      return;
    }

    throw new AuthenticationError(`Unexpected auth response: ${JSON.stringify(response)}`);
  }

  // ── Command execution ───────────────────────────────────────────

  /**
   * Send a program/command and wait for the result.
   * Transparently assembles streamed results (result_start -> chunks -> result_end).
   *
   * Rejects with `QueryError` for an `error` frame and `StatementFailedError`
   * for a result whose `errors` is not empty, so no caller can read a failed
   * program as data.
   */
  async execute(program: string): Promise<ResultResponse> {
    if (!this._connected || !this.ws) {
      throw new ConnectionError('Not connected');
    }

    const msg: ClientMessage = { type: 'execute', program };
    this.inFlight = true;
    try {
      this.ws.send(serializeMessage(msg));
      return await this.readResult();
    } finally {
      this.inFlight = false;
      this.replyFrames.length = 0;
    }
  }

  private async readResult(): Promise<ResultResponse> {
    while (true) {
      const response = await this.receiveMessage();

      if (response.type === 'pong') {
        continue;
      }

      if (response.type === 'result') {
        return this.accept(response);
      }

      if (response.type === 'error') {
        throw queryError(response);
      }

      if (response.type === 'result_start') {
        return this.accept(await this.assembleStream(response));
      }

      throw new InternalError(
        `Unexpected message during result read: ${JSON.stringify(response)}`,
      );
    }
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

  private async assembleStream(start: ResultStartResponse): Promise<ResultResponse> {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const allRows: any[][] = [];
    const allProvenance: string[] = [];

    while (true) {
      const response = await this.receiveMessage();

      if (response.type === 'result_chunk') {
        allRows.push(...response.rows);
        if (response.row_provenance) {
          allProvenance.push(...response.row_provenance);
        }
        continue;
      }

      if (response.type === 'result_end') {
        return {
          type: 'result',
          columns: start.columns,
          rows: allRows,
          row_count: response.row_count,
          total_count: start.total_count,
          truncated: start.truncated,
          execution_time_ms: start.execution_time_ms,
          row_provenance: allProvenance.length > 0 ? allProvenance : undefined,
          metadata: start.metadata,
          switched_kg: start.switched_kg,
          proof_trees: start.proof_trees,
          timing_breakdown: start.timing_breakdown,
          errors: start.errors,
        };
      }

      if (response.type === 'error') {
        throw queryError(response);
      }

      throw new InternalError(
        `Unexpected message during streaming: ${JSON.stringify(response)}`,
      );
    }
  }

  // ── Notification handling ───────────────────────────────────────

  private isNotification(msg: ServerMessage): boolean {
    return (
      msg.type === 'persistent_update' ||
      msg.type === 'rule_change' ||
      msg.type === 'kg_change' ||
      msg.type === 'schema_change'
    );
  }

  private dispatchNotification(notif: NotificationResponse): void {
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
    this._dispatcher.dispatch(event);
  }

  // ── Reconnection ────────────────────────────────────────────────

  private async reconnect(): Promise<void> {
    let delay = this.reconnectDelay;
    for (let attempt = 0; attempt < this.maxReconnectAttempts; attempt++) {
      await sleep(delay * 1000);
      try {
        this._lastSeq = this._dispatcher.lastSeq;
        await this.connect();
        return;
      } catch {
        delay = Math.min(delay * 2, 60);
      }
    }
    throw new ConnectionError(
      `Failed to reconnect after ${this.maxReconnectAttempts} attempts`,
    );
  }

  // ── Keep-alive ──────────────────────────────────────────────────

  async ping(): Promise<void> {
    if (!this.ws) throw new ConnectionError('Not connected');
    this.ws.send(serializeMessage({ type: 'ping' }));
  }

  // ── WebSocket helpers ───────────────────────────────────────────

  private createWebSocket(url: string): Promise<WebSocket> {
    return new Promise<WebSocket>((resolve, reject) => {
      const ws = new WebSocket(url);
      ws.once('open', () => resolve(ws));
      ws.once('error', (err) => reject(err));
    });
  }

  /** Receive exactly one message (used during auth before handler is set up). */
  private receiveOne(): Promise<ServerMessage> {
    return new Promise<ServerMessage>((resolve, reject) => {
      if (!this.ws) return reject(new ConnectionError('Not connected'));
      const handler = (data: WebSocket.Data) => {
        this.ws?.removeListener('message', handler);
        try {
          resolve(deserializeMessage(String(data)));
        } catch (e) {
          reject(e);
        }
      };
      this.ws.on('message', handler);
    });
  }

  /** The next reply frame of the call in flight. */
  private receiveMessage(): Promise<ServerMessage> {
    const queued = this.replyFrames.shift();
    if (queued) return Promise.resolve(queued);
    return new Promise<ServerMessage>((resolve) => {
      this.nextFrame = resolve;
    });
  }
}

function queryError(response: ErrorResponse): QueryError {
  if (response.code === 'outcome_unknown') return new OutcomeUnknownError(response.message);
  if (response.code === 'store_read_only') return new StoreReadOnlyError(response.message);
  return new QueryError(response.message, {
    code: response.code,
    validationErrors: response.validation_errors,
  });
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
