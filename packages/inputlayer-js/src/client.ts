/**
 * InputLayer - top-level async client.
 */

import { Connection, type ConnectionOptions } from './connection.js';
import { KnowledgeGraph } from './knowledge-graph.js';
import type { NotificationEvent, NotificationCallback } from './notifications.js';
import {
  type UserInfo,
  type ApiKeyInfo,
  compileCreateUser,
  compileDropUser,
  compileSetPassword,
  compileSetRole,
  compileListUsers,
  compileCreateApiKey,
  compileListApiKeys,
  compileRevokeApiKey,
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
  /** Delay between reconnect attempts in seconds (default: 1.0) */
  reconnectDelay?: number;
  /** Max reconnect attempts before giving up (default: 10) */
  maxReconnectAttempts?: number;
  /** Initial knowledge graph to use (default: "default") */
  initialKg?: string;
  /** Last notification sequence for replay on reconnect */
  lastSeq?: number;
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
  private readonly conn: Connection;
  private readonly kgs = new Map<string, KnowledgeGraph>();

  constructor(opts: InputLayerOptions) {
    this.conn = new Connection({
      url: opts.url,
      username: opts.username,
      password: opts.password,
      apiKey: opts.apiKey,
      autoReconnect: opts.autoReconnect,
      reconnectDelay: opts.reconnectDelay,
      maxReconnectAttempts: opts.maxReconnectAttempts,
      initialKg: opts.initialKg,
      lastSeq: opts.lastSeq,
    });
  }

  // ── Connection lifecycle ────────────────────────────────────────

  /** Connect and authenticate. */
  async connect(): Promise<void> {
    await this.conn.connect();
  }

  /** Close the connection. */
  async close(): Promise<void> {
    await this.conn.close();
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

  get lastSeq(): number {
    return this.conn.lastSeq;
  }

  // ── KG management ───────────────────────────────────────────────

  /** Get a KnowledgeGraph handle. Switches the session's active KG. */
  knowledgeGraph(name: string): KnowledgeGraph {
    let kg = this.kgs.get(name);
    if (!kg) {
      kg = new KnowledgeGraph(name, this.conn);
      this.kgs.set(name, kg);
    }
    return kg;
  }

  /** List all knowledge graphs. */
  async listKnowledgeGraphs(): Promise<string[]> {
    const result = await this.conn.execute('.kg list');
    return result.rows.length > 0 ? result.rows.map((row) => String(row[0])) : [];
  }

  /** Drop a knowledge graph. */
  async dropKnowledgeGraph(name: string): Promise<void> {
    await this.conn.execute(`.kg drop ${name}`);
    this.kgs.delete(name);
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

  /** Create an API key. Returns the key string. */
  async createApiKey(label: string): Promise<string> {
    const result = await this.conn.execute(compileCreateApiKey(label));
    if (result.rows.length > 0 && result.rows[0].length > 0) {
      return String(result.rows[0][0]);
    }
    return '';
  }

  async listApiKeys(): Promise<ApiKeyInfo[]> {
    const result = await this.conn.execute(compileListApiKeys());
    return result.rows.map((row) => ({
      label: String(row[0]),
      createdAt: row.length > 1 ? String(row[1]) : '',
    }));
  }

  async revokeApiKey(label: string): Promise<void> {
    await this.conn.execute(compileRevokeApiKey(label));
  }

  // ── Notifications ───────────────────────────────────────────────

  /**
   * Register a notification callback.
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
    this.conn.dispatcher.on(eventType, opts ?? {}, callback);
  }

  /** Remove a notification callback. */
  off(callback: NotificationCallback): void {
    this.conn.dispatcher.off(callback);
  }

  /** Async iterator yielding notification events. */
  async *notifications(): AsyncIterableIterator<NotificationEvent> {
    yield* this.conn.dispatcher.events();
  }
}
