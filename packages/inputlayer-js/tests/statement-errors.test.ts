/**
 * Engine failures through the public API: no failure reads as data.
 *
 * A local WebSocket server answers each `execute` with frames shaped exactly
 * as the engine sends them (`src/protocol/rest/handlers/ws.rs`): an `error`
 * frame for a failed one-statement program and `errors[]` on
 * `result`/`result_start` for failed statements of a longer one.
 */

import { mkdtempSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import { WebSocketServer } from 'ws';
import {
  InputLayer,
  InternalError,
  QueryError,
  StatementFailedError,
  relation,
  type KnowledgeGraph,
} from '../src/index';

type Frame = Record<string, unknown>;

const Demo = relation('Demo', { x: 'int' });

const ARITY = "Insert rejected for 'demo': arity mismatch";
const WAL_FAILURE: Frame = {
  type: 'error',
  message: 'WAL append failed: No such file or directory',
  code: 'internal',
};

function messages(rows: string[], errors: Frame[] = [], extra: Frame = {}): Frame {
  return {
    type: 'result',
    columns: ['message'],
    rows: rows.map((r) => [r]),
    row_count: rows.length,
    total_count: rows.length,
    truncated: false,
    execution_time_ms: 0,
    errors,
    ...extra,
  };
}

const ON_DEFAULT = messages(['Switched to knowledge graph: default'], [], {
  switched_kg: 'default',
});

/** A server whose replies to successive `execute`s are scripted. */
class ScriptedEngine {
  readonly sent: string[] = [];
  private readonly server: WebSocketServer;
  private replies: Frame[][] = [];

  static async start(): Promise<ScriptedEngine> {
    const started = new ScriptedEngine();
    await new Promise((resolve) => started.server.once('listening', resolve));
    return started;
  }

  private constructor() {
    this.server = new WebSocketServer({ port: 0, host: '127.0.0.1' });
    this.server.on('connection', (socket) => {
      socket.on('message', (data) => {
        const msg = JSON.parse(String(data));
        if (msg.type === 'login') {
          socket.send(
            JSON.stringify({
              type: 'authenticated',
              session_id: 's',
              knowledge_graph: 'other',
              version: 'test',
              role: 'admin',
            }),
          );
          return;
        }
        this.sent.push(msg.program);
        for (const frame of this.replies.shift() ?? []) {
          socket.send(JSON.stringify(frame));
        }
      });
    });
  }

  /** Each argument is the full list of frames answering one `execute`. */
  script(...replies: Frame[][]): void {
    this.replies = replies;
  }

  get url(): string {
    const address = this.server.address();
    if (typeof address !== 'object' || address === null) throw new Error('not listening');
    return `ws://127.0.0.1:${address.port}`;
  }

  close(): Promise<void> {
    return new Promise((resolve) => this.server.close(() => resolve()));
  }
}

let engine: ScriptedEngine | undefined;
let client: InputLayer | undefined;

async function kgOn(...replies: Frame[][]): Promise<KnowledgeGraph> {
  engine = await ScriptedEngine.start();
  client = new InputLayer({
    url: engine.url,
    username: 'admin',
    password: 'admin',
    autoReconnect: false,
  });
  await client.connect();
  // The session starts on "other": the first call switches to "default".
  engine.script([ON_DEFAULT], ...replies);
  return client.knowledgeGraph('default');
}

afterEach(async () => {
  await client?.close();
  await engine?.close();
  client = undefined;
  engine = undefined;
});

describe('error frames', () => {
  it('a failed insert rejects instead of counting a row', async () => {
    // https://github.com/inputlayer/inputlayer/issues/93
    const kg = await kgOn([WAL_FAILURE]);
    const err = await kg.insert(Demo, { x: 1 }).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(QueryError);
    expect(err).not.toBeInstanceOf(StatementFailedError);
    expect((err as QueryError).code).toBe('internal');
    expect((err as QueryError).message).toMatch(/^WAL append failed/);
  });

  it('keeps parse error details', async () => {
    const details = [{ line: 1, statement_index: 0, error: 'unexpected token' }];
    const kg = await kgOn([
      {
        type: 'error',
        message: 'Program has 1 parse error(s)',
        validation_errors: details,
        code: 'validation',
      },
    ]);
    const err = (await kg.execute('+demo(').catch((e: unknown) => e)) as QueryError;
    expect(err.code).toBe('validation');
    expect(err.validationErrors).toEqual(details);
  });

  it('an error with no statement cause has no code', async () => {
    const kg = await kgOn([{ type: 'error', message: 'Server shutting down' }]);
    const err = (await kg.execute('?demo(X)').catch((e: unknown) => e)) as QueryError;
    expect(err).toBeInstanceOf(QueryError);
    expect(err.code).toBeUndefined();
  });

  it('a relation named error is data', async () => {
    const kg = await kgOn([
      {
        type: 'result',
        columns: ['error'],
        rows: [['disk full']],
        row_count: 1,
        total_count: 1,
        truncated: false,
        execution_time_ms: 0,
        errors: [],
      },
    ]);
    expect((await kg.execute('?error(E)')).toTuples()).toEqual([['disk full']]);
  });
});

describe('statement errors', () => {
  it('lists every failed statement of a program', async () => {
    const errors = [
      { index: 1, code: 'not_found', message: "Relation 'x' not found." },
      { index: 2, code: 'validation', message: ARITY },
    ];
    const kg = await kgOn([
      messages(["Inserted 1 fact(s) into 'demo'.", "Relation 'x' not found.", ARITY], errors),
    ]);
    const err = (await kg
      .execute('+demo(1)\n.rel drop x\n+demo(1, 2)')
      .catch((e: unknown) => e)) as StatementFailedError;
    expect(err).toBeInstanceOf(StatementFailedError);
    expect(err).toBeInstanceOf(QueryError);
    expect(err.errors).toEqual(errors);
    expect(err.code).toBe('not_found');
    expect(err.result.rows).toHaveLength(3);
  });

  it('still tracks a KG switch made by a failing program', async () => {
    const kg = await kgOn(
      [
        messages(
          ['Switched to knowledge graph: elsewhere', "Relation 'x' not found."],
          [{ index: 1, code: 'not_found', message: "Relation 'x' not found." }],
          { switched_kg: 'elsewhere' },
        ),
      ],
      [ON_DEFAULT],
      [messages(['ok'])],
    );
    await expect(kg.execute('.kg use elsewhere\n.rel drop x')).rejects.toBeInstanceOf(
      StatementFailedError,
    );
    // The session is on "elsewhere" now, so the next call switches back.
    await kg.execute('.status');
    expect(engine?.sent.slice(-2)).toEqual(['.kg use default', '.status']);
  });

  it('load sends the local file and rejects on a failed statement', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'il-load-'));
    const path = join(dir, 'seed.iql');
    writeFileSync(path, '+demo(1)\n+demo(1, 2)\n');
    const kg = await kgOn([
      messages(
        ["Inserted 1 fact(s) into 'demo'.", ARITY],
        [{ index: 1, code: 'validation', message: ARITY }],
      ),
    ]);
    const err = (await kg.load(path).catch((e: unknown) => e)) as StatementFailedError;
    expect(err).toBeInstanceOf(StatementFailedError);
    expect(err.errors.map((e) => e.index)).toEqual([1]);
    expect(engine?.sent.at(-1)).toBe('+demo(1)\n+demo(1, 2)\n');
  });
});

describe('chunked results', () => {
  const stream = (errors: Frame[]): Frame[] => [
    {
      type: 'result_start',
      columns: ['x'],
      total_count: 3,
      truncated: false,
      execution_time_ms: 4,
      timing_breakdown: { total_us: 7 },
      errors,
    },
    { type: 'result_chunk', rows: [[1], [2]], chunk_index: 0 },
    {
      type: 'persistent_update',
      seq: 1,
      timestamp_ms: 0,
      knowledge_graph: 'default',
      relation: 'demo',
      operation: 'insert',
      count: 1,
    },
    { type: 'result_chunk', rows: [[3]], chunk_index: 1 },
    { type: 'result_end', row_count: 3, chunk_count: 2 },
  ];

  it('errors on result_start survive assembly', async () => {
    const errors = [{ index: 0, code: 'validation', message: ARITY }];
    const kg = await kgOn(stream(errors), [messages(['ok'])]);
    const err = (await kg
      .execute('+demo(1, 2)\n?demo(X)')
      .catch((e: unknown) => e)) as StatementFailedError;
    expect(err).toBeInstanceOf(StatementFailedError);
    expect(err.errors).toEqual(errors);
    expect(err.result.rows).toEqual([[1], [2], [3]]);
    // The whole stream was consumed: the next call reads its own reply.
    expect((await kg.execute('.status')).toTuples()).toEqual([['ok']]);
  });

  it('a successful stream keeps its timing', async () => {
    const kg = await kgOn(stream([]));
    const result = await kg.execute('?demo(X)');
    expect(result.toTuples()).toEqual([[1], [2], [3]]);
    expect(result.timingBreakdown).toEqual({ total_us: 7 });
  });

  it('an error frame mid-stream rejects typed', async () => {
    const [start, chunk] = stream([]);
    const kg = await kgOn([start, chunk, { type: 'error', message: 'Server shutting down' }]);
    await expect(kg.execute('?demo(X)')).rejects.toThrow(QueryError);
  });
});

describe('insert count', () => {
  it("is the engine's stored count", async () => {
    const kg = await kgOn([messages(["Inserted 2 fact(s) into 'demo'."])]);
    expect(await kg.insert(Demo, [{ x: 1 }, { x: 2 }, { x: 2 }])).toEqual({ count: 2 });
  });

  it('an unrecognised reply is not success', async () => {
    const kg = await kgOn([messages(['Something else entirely'])]);
    await expect(kg.insert(Demo, { x: 1 })).rejects.toBeInstanceOf(InternalError);
  });
});

describe('KG switch', () => {
  it('creates a missing KG on not_found', async () => {
    engine = await ScriptedEngine.start();
    client = new InputLayer({ url: engine.url, username: 'a', password: 'b', autoReconnect: false });
    await client.connect();
    engine.script(
      [{ type: 'error', message: 'Knowledge graph not found', code: 'not_found' }],
      [messages(["Knowledge graph 'default' created."])],
      [ON_DEFAULT],
      [messages(["Inserted 1 fact(s) into 'demo'."])],
    );
    const kg = client.knowledgeGraph('default');
    expect(await kg.insert(Demo, { x: 1 })).toEqual({ count: 1 });
    expect(engine.sent.slice(0, 3)).toEqual([
      '.kg use default',
      '.kg create default',
      '.kg use default',
    ]);
  });

  it('other switch failures reject', async () => {
    engine = await ScriptedEngine.start();
    client = new InputLayer({ url: engine.url, username: 'a', password: 'b', autoReconnect: false });
    await client.connect();
    engine.script([{ type: 'error', message: 'Permission denied', code: 'validation' }]);
    await expect(client.knowledgeGraph('default').insert(Demo, { x: 1 })).rejects.toBeInstanceOf(
      QueryError,
    );
    expect(engine.sent).toEqual(['.kg use default']);
  });
});
