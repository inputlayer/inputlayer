/**
 * WebSocket connection management with authentication and streaming support.
 */

import WebSocket from 'ws';
import {
  type ClientMessage,
  type ResultResponse,
  type ServerMessage,
  type NotificationResponse,
  type ResultStartResponse,
  serializeMessage,
  deserializeMessage,
} from './protocol.js';
import { AuthenticationError, ConnectionError, InternalError } from './errors.js';
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

  // Queue for message routing during execute()
  private pendingResolve?: (msg: ServerMessage) => void;

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

    // Set up message handler for notifications when idle
    this.ws.on('message', (data: WebSocket.Data) => {
      try {
        const msg = deserializeMessage(String(data));
        if (this.pendingResolve) {
          this.pendingResolve(msg);
          this.pendingResolve = undefined;
        } else if (this.isNotification(msg)) {
          this.dispatchNotification(msg as NotificationResponse);
        }
      } catch {
        // Ignore parse errors in background
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

    if (response.type === 'auth_error') {
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
   */
  async execute(program: string): Promise<ResultResponse> {
    if (!this._connected || !this.ws) {
      throw new ConnectionError('Not connected');
    }

    const msg: ClientMessage = { type: 'execute', program };
    this.ws.send(serializeMessage(msg));

    return this.readResult();
  }

  private async readResult(): Promise<ResultResponse> {
    while (true) {
      const response = await this.receiveMessage();

      if (this.isNotification(response)) {
        this.dispatchNotification(response as NotificationResponse);
        continue;
      }

      if (response.type === 'pong') {
        continue;
      }

      if (response.type === 'result') {
        if (response.switched_kg) {
          this._currentKg = response.switched_kg;
        }
        return response;
      }

      if (response.type === 'error') {
        return {
          type: 'result',
          columns: ['error'],
          rows: [[response.message]],
          row_count: 1,
          total_count: 1,
          truncated: false,
          execution_time_ms: 0,
        };
      }

      if (response.type === 'result_start') {
        return this.assembleStream(response);
      }

      throw new InternalError(
        `Unexpected message during result read: ${JSON.stringify(response)}`,
      );
    }
  }

  private async assembleStream(start: ResultStartResponse): Promise<ResultResponse> {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const allRows: any[][] = [];
    const allProvenance: string[] = [];

    while (true) {
      const response = await this.receiveMessage();

      if (this.isNotification(response)) {
        this.dispatchNotification(response as NotificationResponse);
        continue;
      }

      if (response.type === 'result_chunk') {
        allRows.push(...response.rows);
        if (response.row_provenance) {
          allProvenance.push(...response.row_provenance);
        }
        continue;
      }

      if (response.type === 'result_end') {
        if (start.switched_kg) {
          this._currentKg = start.switched_kg;
        }
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
        };
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

  /** Receive the next message via the pendingResolve mechanism. */
  private receiveMessage(): Promise<ServerMessage> {
    return new Promise<ServerMessage>((resolve) => {
      this.pendingResolve = resolve;
    });
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
