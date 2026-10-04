/**
 * Live tests of the connection core against a real engine: pipelined calls
 * routed by id, deadlines and cancel, notifications delivered to an idle
 * client, reconnect restoring the graph and the notification cursor, and one
 * connection per knowledge graph handle. Set INPUTLAYER_TEST_SERVER (and
 * INPUTLAYER_TEST_USER / INPUTLAYER_TEST_PASSWORD) to enable; `make
 * js-test-live` starts a server and runs them.
 */

import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import {
  CancelledError,
  DeadlineExceededError,
  InputLayer,
  type NotificationEvent,
} from '../src/index';

const SERVER_URL = process.env.INPUTLAYER_TEST_SERVER ?? '';
const USERNAME = process.env.INPUTLAYER_TEST_USER ?? 'admin';
const PASSWORD = process.env.INPUTLAYER_TEST_PASSWORD ?? 'admin';
const SKIP = !SERVER_URL;

const KG_A = 'test_connection_js_a';
const KG_B = 'test_connection_js_b';

/** A chain this long makes its transitive closure take well over 50 ms. */
const CHAIN = 1500;

function client(): InputLayer {
  return new InputLayer({
    url: SERVER_URL,
    username: USERNAME,
    password: PASSWORD,
    reconnectDelay: 0.05,
  });
}

async function waitFor(predicate: () => boolean, timeoutMs = 10_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error('condition not met in time');
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
}

describe.skipIf(SKIP)('Live: connection core', () => {
  let il: InputLayer;

  beforeAll(async () => {
    il = client();
    await il.connect();
    for (const name of [KG_A, KG_B]) {
      await il.dropKnowledgeGraph(name).catch(() => undefined);
    }
    const a = il.knowledgeGraph(KG_A);
    const edges = Array.from({ length: CHAIN }, (_, i) => `(${i}, ${i + 1})`).join(', ');
    await a.execute(`+edge[${edges}]`);
    await a.execute('+reach(X, Y) <- edge(X, Y)\n+reach(X, Z) <- reach(X, Y), edge(Y, Z)');
  });

  afterAll(async () => {
    for (const name of [KG_A, KG_B]) {
      await il?.dropKnowledgeGraph(name).catch(() => undefined);
    }
    await il?.close();
  });

  it('pipelined calls on one handle each get their own reply', async () => {
    const a = il.knowledgeGraph(KG_A);
    const results = await Promise.all(
      Array.from({ length: 40 }, (_, i) => a.execute(`?edge(${i}, Y)`)),
    );
    results.forEach((r, i) => expect(r.toTuples()).toEqual([[i, i + 1]]));
  });

  it('a deadline fails typed and leaves the connection usable', async () => {
    const a = il.knowledgeGraph(KG_A);
    const error = await a
      .execute('?reach(X, Y), X < 5000', { timeoutMs: 50 })
      .catch((e: unknown) => e);
    expect(error).toBeInstanceOf(DeadlineExceededError);
    expect((error as DeadlineExceededError).iql).toBe('?reach(X, Y), X < 5000');
    expect((await a.execute('?edge(0, Y)')).toTuples()).toEqual([[0, 1]]);
  });

  it('aborting a running call cancels it on the server', async () => {
    const a = il.knowledgeGraph(KG_A);
    const controller = new AbortController();
    const call = a.execute('?reach(X, Y), X < 5000', { signal: controller.signal });
    setTimeout(() => controller.abort(), 30);
    expect(await call.catch((e: unknown) => e)).toBeInstanceOf(CancelledError);
    expect((await a.execute('?edge(1, Y)')).toTuples()).toEqual([[1, 2]]);
  });

  it('two handles on different graphs never cross, even interleaved', async () => {
    const a = il.knowledgeGraph(KG_A);
    const b = il.knowledgeGraph(KG_B);
    await b.execute('+edge(100, 200)');
    const reads = await Promise.all(
      Array.from({ length: 20 }, (_, i) => (i % 2 === 0 ? a : b).execute('?edge(X, Y), X >= 100')),
    );
    reads.forEach((r, i) => {
      // KG_A's chain has edges from 100 up; KG_B has exactly one.
      if (i % 2 === 0) expect(r.rowCount).toBe(CHAIN - 100);
      else expect(r.toTuples()).toEqual([[100, 200]]);
    });
    expect(a.connection.currentKg).toBe(KG_A);
    expect(b.connection.currentKg).toBe(KG_B);
  });

  it('a graph a handle created can be dropped while its handle is open', async () => {
    const name = 'test_connection_js_drop';
    await il.dropKnowledgeGraph(name).catch(() => undefined);
    await il.knowledgeGraph(name).execute('+x(1)');
    await il.dropKnowledgeGraph(name);
    const listing = (await il.listKnowledgeGraphs()).map((row) => row.trim());
    expect(listing).not.toContain(name);
  });

  it('delivers notifications to a client with no call in flight', async () => {
    const listener = client();
    await listener.connect();
    const writer = client();
    await writer.connect();
    try {
      const seen: NotificationEvent[] = [];
      listener.on('persistent_update', (e) => {
        seen.push(e);
      }, { knowledgeGraph: KG_B });
      // Open the listener's handle on KG_B, then stay idle.
      await listener.knowledgeGraph(KG_B).execute('?edge(X, Y)');
      await writer.knowledgeGraph(KG_B).execute('+edge(1, 2)');
      await waitFor(() => seen.some((e) => e.relation === 'edge'));
    } finally {
      await listener.close();
      await writer.close();
    }
  });

  it('reconnects to the same graph and replays notifications missed meanwhile', async () => {
    const watcher = client();
    await watcher.connect();
    const writer = client();
    await writer.connect();
    try {
      const kg = watcher.knowledgeGraph(KG_B);
      const seen: NotificationEvent[] = [];
      watcher.on('persistent_update', (e) => {
        seen.push(e);
      }, { knowledgeGraph: KG_B });
      await kg.execute('?edge(X, Y)');
      // A notification sets the cursor.
      await writer.knowledgeGraph(KG_B).execute('+edge(2, 3)');
      await waitFor(() => seen.length >= 1);
      const reconnected = new Promise((resolve) => watcher.events.addEventListener('reconnected', resolve));

      // Drop the socket without a close handshake, as a network failure would,
      // and write while the watcher is away.
      const socket = (kg.connection as unknown as { ws: { terminate(): void } }).ws;
      const before = seen.length;
      socket.terminate();
      await writer.knowledgeGraph(KG_B).execute('+edge(3, 4)');
      await reconnected;

      expect(kg.connection.currentKg).toBe(KG_B);
      expect((await kg.execute('?edge(3, Y)')).toTuples()).toEqual([[3, 4]]);
      // The write made during the outage is replayed from the cursor.
      await waitFor(() => seen.length > before);
      expect(seen.slice(before).some((e) => e.relation === 'edge')).toBe(true);
    } finally {
      await watcher.close();
      await writer.close();
    }
  });
});
