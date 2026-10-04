/**
 * The connection core against a scripted server: the adversarial frame
 * fixtures of `tests/fixtures/connection/` (see its README), then routing,
 * idle delivery, keepalive, cancellation and reconnect.
 */

import { readdirSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import { WebSocketServer, type WebSocket as ServerSocket } from 'ws';
import * as sdk from '../src/index';
import {
  AuthenticationError,
  CancelledError,
  Connection,
  ConnectionError,
  ConnectionLostError,
  type ConnectionOptions,
  type NotificationEvent,
  type SubscriptionPushMessage,
} from '../src/index';

type Frame = Record<string, unknown>;

interface Connected {
  socket: ServerSocket;
  url: string;
  received: Frame[];
}

/** A WebSocket server that authenticates and records every frame. */
class MockServer {
  readonly connections: Connected[] = [];
  epoch = 'e1';
  refuseAuth = false;
  /** Answer authentication with a closing notice and close, as on `auth_timeout`. */
  closeOnAuth = false;
  private readonly server: WebSocketServer;
  private readonly waiters: Array<() => void> = [];

  static async start(): Promise<MockServer> {
    const started = new MockServer();
    await new Promise((resolve) => started.server.once('listening', resolve));
    return started;
  }

  private constructor() {
    this.server = new WebSocketServer({ port: 0, host: '127.0.0.1' });
    this.server.on('connection', (socket, request) => {
      const conn: Connected = { socket, url: request.url ?? '/', received: [] };
      this.connections.push(conn);
      socket.on('message', (data) => {
        const msg = JSON.parse(String(data)) as Frame;
        if ((msg.type === 'login' || msg.type === 'authenticate') && this.closeOnAuth) {
          socket.send(JSON.stringify({ type: 'notice', code: 'auth_timeout', message: 'Authentication timeout' }));
          socket.close();
          return;
        }
        if (msg.type === 'login' || msg.type === 'authenticate') {
          const kg = new URL(conn.url, 'ws://x').searchParams.get('kg') ?? 'default';
          socket.send(
            JSON.stringify(
              this.refuseAuth
                ? { type: 'auth_error', id: msg.id, message: 'Access denied' }
                : {
                    type: 'authenticated',
                    id: msg.id,
                    session_id: `s${this.connections.length}`,
                    knowledge_graph: kg,
                    version: 'test',
                    role: 'admin',
                    protocol_version: 3,
                    stream_epoch: this.epoch,
                  },
            ),
          );
          return;
        }
        conn.received.push(msg);
        if (msg.type === 'ping') socket.send(JSON.stringify({ type: 'pong', id: msg.id }));
        for (const waiter of this.waiters.splice(0)) waiter();
      });
    });
  }

  get url(): string {
    const address = this.server.address();
    if (typeof address !== 'object' || address === null) throw new Error('not listening');
    return `ws://127.0.0.1:${address.port}/ws`;
  }

  get last(): Connected {
    return this.connections[this.connections.length - 1];
  }

  /** Resolve once `predicate` holds, checked after every received frame. */
  async until(predicate: () => boolean, timeoutMs = 5000): Promise<void> {
    const deadline = Date.now() + timeoutMs;
    while (!predicate()) {
      if (Date.now() > deadline) throw new Error('mock server: condition not met in time');
      await new Promise<void>((resolve) => {
        this.waiters.push(resolve);
        setTimeout(resolve, 20);
      });
    }
  }

  async close(): Promise<void> {
    for (const { socket } of this.connections) socket.terminate();
    await new Promise((resolve) => this.server.close(() => resolve(undefined)));
  }
}

let server: MockServer | undefined;
let conn: Connection | undefined;

afterEach(async () => {
  await conn?.close();
  await server?.close();
  conn = undefined;
  server = undefined;
});

async function open(opts: Partial<ConnectionOptions> = {}): Promise<Connection> {
  server = await MockServer.start();
  conn = new Connection({
    url: server.url,
    username: 'u',
    password: 'p',
    autoReconnect: false,
    keepaliveMs: 0,
    ...opts,
  });
  await conn.connect();
  return conn;
}

function executes(c: Connected): Frame[] {
  return c.received.filter((f) => f.type === 'execute');
}

// ── Fixtures ────────────────────────────────────────────────────────

interface Fixture {
  name: string;
  about: string;
  options?: { maxInFlight?: number; timeoutMs?: number; timeoutGraceMs?: number };
  calls: string[];
  server: Frame[];
  expect: {
    calls: Array<{ rows?: unknown[][]; error?: string; code?: string; mayHaveCommitted?: boolean }>;
    notifications?: number[];
    lastSeq?: number;
    events?: string[];
    stats?: Record<string, number>;
    sentTimeoutMs?: number;
  };
}

const FIXTURES = join(__dirname, 'fixtures', 'connection');
const fixtures: Fixture[] = readdirSync(FIXTURES)
  .filter((f) => f.endsWith('.json'))
  .map((f) => JSON.parse(readFileSync(join(FIXTURES, f), 'utf8')));

/** Replace `$r<i>` / `$c<i>` with the ids the client used for call `i`. */
function resolveIds(value: unknown, calls: string[], c: Connected): unknown {
  if (typeof value === 'string') {
    const m = /^\$([rc])(\d+)$/.exec(value);
    if (!m) return value;
    const requests = executes(c).filter((f) => f.program === calls[Number(m[2])]);
    const request = requests[requests.length - 1];
    if (m[1] === 'r') return request?.id;
    return c.received.find((f) => f.type === 'cancel' && f.target === request?.id)?.id;
  }
  if (Array.isArray(value)) return value.map((v) => resolveIds(v, calls, c));
  if (value && typeof value === 'object') {
    return Object.fromEntries(
      Object.entries(value).map(([k, v]) => [k, resolveIds(v, calls, c)]),
    );
  }
  return value;
}

async function play(fixture: Fixture, srv: MockServer): Promise<void> {
  const c = srv.last;
  for (const step of fixture.server) {
    if (step.await) {
      const target = step.await as { executes?: number; cancel?: number };
      if (target.executes !== undefined) {
        await srv.until(() => executes(c).length >= target.executes!);
      } else {
        await srv.until(() => resolveIds(`$c${target.cancel}`, fixture.calls, c) !== undefined);
      }
    } else if (step.send) {
      c.socket.send(JSON.stringify(resolveIds(step.send, fixture.calls, c)));
    } else if (step.raw !== undefined) {
      c.socket.send(String(step.raw));
    } else if (step.assert) {
      expect(executes(c).length).toBe((step.assert as { executes: number }).executes);
    } else if (step.sleep !== undefined) {
      await new Promise((resolve) => setTimeout(resolve, Number(step.sleep)));
    } else if (step.close) {
      c.socket.close();
    } else if (step.stall) {
      // Stop reading: no request is answered and no transport ping either.
      (c.socket as unknown as { _socket: { pause(): void } })._socket.pause();
    }
  }
}

describe('connection fixtures', () => {
  it('cover the adversarial cases', () => {
    expect(fixtures.map((f) => f.name).sort()).toEqual(
      expect.arrayContaining([
        'close_mid_stream',
        'dead_server',
        'deadline_cancel',
        'duplicate_id_rejection',
        'in_flight_bound',
        'interleaved_streams',
        'missing_chunk',
        'out_of_order_replies',
      ]),
    );
  });

  it.each(fixtures.map((f) => [f.name, f] as const))('%s', async (_name, fixture) => {
    const c = await open({
      maxInFlight: fixture.options?.maxInFlight,
      defaultTimeoutMs: fixture.options?.timeoutMs ?? 0,
      timeoutGraceMs: fixture.options?.timeoutGraceMs,
    });
    const notified: NotificationEvent[] = [];
    c.dispatcher.on(undefined, {}, (e) => {
      notified.push(e);
    });
    const events: string[] = [];
    for (const type of ['disconnected', 'reconnected', 'session_reset', 'notification_gap', 'closed']) {
      c.events.addEventListener(type, () => events.push(type));
    }

    const outcomes = fixture.calls.map((program) =>
      c.execute(program).then(
        (result) => ({ rows: result.rows }) as Record<string, unknown>,
        (error: Error & { code?: string; mayHaveCommitted?: boolean }) => ({
          error: error.name,
          code: error.code,
          mayHaveCommitted: error.mayHaveCommitted,
          instance: error,
        }),
      ),
    );
    await play(fixture, server!);
    const settled = await Promise.all(outcomes);

    fixture.expect.calls.forEach((want, i) => {
      const got = settled[i];
      if (want.rows) {
        expect(got, `call ${i}`).toEqual({ rows: want.rows });
        return;
      }
      expect(got.error, `call ${i}`).toBe(want.error);
      const cls = (sdk as Record<string, unknown>)[want.error!] as new (...a: never[]) => Error;
      expect(got.instance).toBeInstanceOf(cls);
      if (want.code !== undefined) expect(got.code).toBe(want.code);
      if (want.mayHaveCommitted !== undefined) expect(got.mayHaveCommitted).toBe(want.mayHaveCommitted);
    });
    if (fixture.expect.notifications) {
      expect(notified.map((e) => e.seq)).toEqual(fixture.expect.notifications);
    }
    if (fixture.expect.lastSeq !== undefined) expect(c.lastSeq).toBe(fixture.expect.lastSeq);
    if (fixture.expect.events) expect(events).toEqual(fixture.expect.events);
    if (fixture.expect.stats) {
      for (const [key, value] of Object.entries(fixture.expect.stats)) {
        expect(c.stats[key as keyof typeof c.stats], key).toBe(value);
      }
    }
    if (fixture.expect.sentTimeoutMs !== undefined) {
      for (const frame of executes(server!.connections[0])) {
        expect(frame.timeout_ms).toBe(fixture.expect.sentTimeoutMs);
      }
    }
  });
});

// ── Routing ─────────────────────────────────────────────────────────

describe('routing', () => {
  it('every request carries a distinct id', async () => {
    const c = await open();
    const calls = Array.from({ length: 40 }, (_, i) => c.execute(`?q${i}(X)`));
    await server!.until(() => executes(server!.last).length === 15);
    const answered = new Set<unknown>();
    // Answer in reverse order as requests arrive, until all 40 are done.
    while (answered.size < 40) {
      const open = executes(server!.last).filter((f) => !answered.has(f.id));
      for (const frame of open.reverse()) {
        answered.add(frame.id);
        server!.last.socket.send(
          JSON.stringify({ type: 'result', id: frame.id, columns: ['p'], rows: [[frame.program]],
            row_count: 1, total_count: 1, truncated: false, execution_time_ms: 0, errors: [] }),
        );
      }
      await server!.until(() => executes(server!.last).length > answered.size || answered.size === 40);
    }
    const results = await Promise.all(calls);
    results.forEach((r, i) => expect(r.rows).toEqual([[`?q${i}(X)`]]));
    const ids = executes(server!.last).map((f) => f.id);
    expect(new Set(ids).size).toBe(40);
  });

  it('subscription pushes reach their route only for the current generation', async () => {
    const c = await open();
    const got: SubscriptionPushMessage[] = [];
    const route = c.routeSubscription('s', (p) => got.push(p));
    const push = (generation: number, seq: number) =>
      server!.last.socket.send(JSON.stringify({ type: 'subscription_delta', subscription: 's',
        generation, knowledge_graph: 'default', seq, revision: seq + 1, columns: ['x'],
        inserted: [[seq]], retracted: [] }));
    // Pushes before the generation is known are held, then filtered.
    push(1, 1);
    push(2, 1);
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(got).toEqual([]);
    route.setGeneration(2);
    push(1, 2);
    push(2, 2);
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(got.map((p) => [p.generation, (p as { seq: number }).seq])).toEqual([[2, 1], [2, 2]]);
    expect(c.stats.stalePushes).toBe(2);
    route.close();
    push(2, 3);
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(got).toHaveLength(2);
    expect(c.stats.stalePushes).toBe(3);
  });

  it('delivers notifications while no call is in flight', async () => {
    const c = await open();
    const seen: number[] = [];
    const iterator = c.dispatcher.events();
    for (const seq of [1, 2, 3]) {
      server!.last.socket.send(JSON.stringify({ type: 'persistent_update', seq, timestamp_ms: 0,
        knowledge_graph: 'default', relation: 'a', operation: 'insert', count: 1 }));
    }
    // The iterator buffers what arrives while its consumer is busy.
    for (let i = 0; i < 3; i++) seen.push((await iterator.next()).value.seq);
    await iterator.return?.(undefined);
    expect(seen).toEqual([1, 2, 3]);
    expect(c.lastSeq).toBe(3);
  });
});

// ── Keepalive, cancellation, errors ─────────────────────────────────

describe('keepalive', () => {
  it('pings a connection that sends nothing', async () => {
    await open({ keepaliveMs: 40 });
    await server!.until(() => server!.last.received.some((f) => f.type === 'ping'), 2000);
  });

  it('does not ping while requests are in flight', async () => {
    const c = await open({ keepaliveMs: 40 });
    const pending = c.execute('?slow(X)').catch(() => undefined);
    await new Promise((resolve) => setTimeout(resolve, 200));
    expect(server!.last.received.filter((f) => f.type === 'ping')).toEqual([]);
    await c.close();
    await pending;
  });
});

describe('cancellation', () => {
  it('aborting a queued call rejects it without sending it', async () => {
    const c = await open({ maxInFlight: 1 });
    const first = c.execute('?a(X)');
    const controller = new AbortController();
    const second = c.execute('?b(X)', { signal: controller.signal });
    controller.abort();
    await expect(second).rejects.toBeInstanceOf(CancelledError);
    await server!.until(() => executes(server!.last).length === 1);
    const id = executes(server!.last)[0].id;
    server!.last.socket.send(JSON.stringify({ type: 'result', id, columns: [], rows: [],
      row_count: 0, total_count: 0, truncated: false, execution_time_ms: 0, errors: [] }));
    await first;
    expect(executes(server!.last).map((f) => f.program)).toEqual(['?a(X)']);
  });

  it('aborting a sent call sends cancel and fails with the engine reply', async () => {
    const c = await open();
    const controller = new AbortController();
    const call = c.execute('?a(X)', { signal: controller.signal });
    await server!.until(() => executes(server!.last).length === 1);
    controller.abort();
    await server!.until(() => server!.last.received.some((f) => f.type === 'cancel'));
    const id = executes(server!.last)[0].id;
    const cancel = server!.last.received.find((f) => f.type === 'cancel')!;
    expect(cancel.target).toBe(id);
    server!.last.socket.send(JSON.stringify({ type: 'error', id, code: 'cancelled',
      message: 'Request cancelled before it began committing; nothing was applied' }));
    server!.last.socket.send(JSON.stringify({ type: 'cancel_ack', id: cancel.id, target: id,
      outcome: 'cancelled' }));
    const error = await call.catch((e: unknown) => e);
    expect(error).toBeInstanceOf(CancelledError);
    expect((error as CancelledError).iql).toBe('?a(X)');
  });

  it('a call on a closed connection is refused', async () => {
    const c = await open();
    await c.close();
    await expect(c.execute('?a(X)')).rejects.toBeInstanceOf(ConnectionError);
  });

  it('close fails calls in flight with ConnectionLostError', async () => {
    const c = await open();
    const call = c.execute('+a(1)');
    await server!.until(() => executes(server!.last).length === 1);
    await c.close();
    const error = await call.catch((e: unknown) => e);
    expect(error).toBeInstanceOf(ConnectionLostError);
    expect((error as ConnectionLostError).mayHaveCommitted).toBe(true);
  });

  it('an auth_error fails the connect', async () => {
    server = await MockServer.start();
    server.refuseAuth = true;
    conn = new Connection({ url: server.url, username: 'u', password: 'p', autoReconnect: false });
    await expect(conn.connect()).rejects.toBeInstanceOf(AuthenticationError);
  });

  it('a close during authentication is transient, not an auth failure', async () => {
    server = await MockServer.start();
    server.closeOnAuth = true;
    conn = new Connection({ url: server.url, username: 'u', password: 'p', autoReconnect: false });
    const error = await conn.connect().catch((e: unknown) => e);
    expect(error).toBeInstanceOf(ConnectionLostError);
    expect((error as ConnectionLostError).code).toBe('auth_timeout');
  });
});

// ── Reconnect ───────────────────────────────────────────────────────

describe('reconnect', () => {
  it('re-opens on the same graph with the notification cursor and resends queued calls', async () => {
    const c = await open({ autoReconnect: true, reconnectDelay: 0.01, initialKg: 'shop', maxInFlight: 1 });
    const events: string[] = [];
    for (const type of ['disconnected', 'reconnected', 'session_reset']) {
      c.events.addEventListener(type, () => events.push(type));
    }
    server!.last.socket.send(JSON.stringify({ type: 'persistent_update', seq: 41, timestamp_ms: 0,
      knowledge_graph: 'shop', relation: 'a', operation: 'insert', count: 1 }));
    await new Promise((resolve) => setTimeout(resolve, 30));
    const inFlight = c.execute('?a(X)');
    const queued = c.execute('?b(X)');
    await server!.until(() => executes(server!.last).length === 1);
    const first = server!.last;
    first.socket.close();

    const lost = await inFlight.catch((e: unknown) => e);
    expect(lost).toBeInstanceOf(ConnectionLostError);
    expect((lost as ConnectionLostError).mayHaveCommitted).toBe(false);

    await server!.until(() => server!.connections.length === 2 && executes(server!.last).length === 1);
    const url = new URL(server!.last.url, 'ws://x');
    expect(url.searchParams.get('kg')).toBe('shop');
    expect(url.searchParams.get('last_seq')).toBe('41');
    expect(url.searchParams.get('epoch')).toBe('e1');
    const id = executes(server!.last)[0].id;
    expect(executes(server!.last)[0].program).toBe('?b(X)');
    server!.last.socket.send(JSON.stringify({ type: 'result', id, columns: ['x'], rows: [[2]],
      row_count: 1, total_count: 1, truncated: false, execution_time_ms: 0, errors: [] }));
    expect((await queued).rows).toEqual([[2]]);
    expect(events).toEqual(['disconnected', 'reconnected', 'session_reset']);
    expect(c.stats.reconnects).toBe(1);
  });

  it('a new engine run drops the old cursor', async () => {
    const c = await open({ autoReconnect: true, reconnectDelay: 0.01 });
    server!.last.socket.send(JSON.stringify({ type: 'persistent_update', seq: 9, timestamp_ms: 0,
      knowledge_graph: 'default', relation: 'a', operation: 'insert', count: 1 }));
    await new Promise((resolve) => setTimeout(resolve, 30));
    server!.epoch = 'e2';
    server!.last.socket.close();
    await server!.until(() => server!.connections.length === 2);
    await new Promise((resolve) => setTimeout(resolve, 30));
    expect(c.epoch).toBe('e2');
    expect(c.lastSeq).toBe(0);
  });

  it('gives up after maxReconnectAttempts and fails what waits', async () => {
    const c = await open({ autoReconnect: true, reconnectDelay: 0.01, maxReconnectAttempts: 2 });
    const closed = new Promise((resolve) => c.events.addEventListener('closed', resolve));
    server!.refuseAuth = true;
    server!.last.socket.close();
    await closed;
    await expect(c.execute('?a(X)')).rejects.toBeInstanceOf(ConnectionError);
    // An authentication refusal is final: no second attempt.
    expect(server!.connections).toHaveLength(2);
  });

  it('a lazy connection opens on its first call', async () => {
    server = await MockServer.start();
    conn = new Connection({ url: server.url, username: 'u', password: 'p', lazy: true, keepaliveMs: 0 });
    const call = conn.execute('?a(X)');
    await server.until(() => server!.connections.length === 1 && executes(server!.last).length === 1);
    const id = executes(server.last)[0].id;
    server.last.socket.send(JSON.stringify({ type: 'result', id, columns: [], rows: [],
      row_count: 0, total_count: 0, truncated: false, execution_time_ms: 0, errors: [] }));
    expect((await call).rows).toEqual([]);
  });
});
