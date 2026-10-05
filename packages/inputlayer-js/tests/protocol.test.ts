import { describe, it, expect, vi } from 'vitest';
import { PROTOCOL_VERSION, serializeMessage, deserializeMessage, isPush } from '../src/protocol';

describe('serializeMessage', () => {
  it('serializes login message', () => {
    const json = serializeMessage({
      type: 'login',
      username: 'admin',
      password: 'secret',
    });
    const parsed = JSON.parse(json);
    expect(parsed.type).toBe('login');
    expect(parsed.username).toBe('admin');
    expect(parsed.password).toBe('secret');
  });

  it('serializes authenticate message', () => {
    const json = serializeMessage({
      type: 'authenticate',
      api_key: 'il_key_123',
    });
    const parsed = JSON.parse(json);
    expect(parsed.type).toBe('authenticate');
    expect(parsed.api_key).toBe('il_key_123');
  });

  it('serializes execute message', () => {
    const json = serializeMessage({
      type: 'execute',
      program: '?edge(X, Y)',
    });
    const parsed = JSON.parse(json);
    expect(parsed.type).toBe('execute');
    expect(parsed.program).toBe('?edge(X, Y)');
  });

  it('serializes ping message', () => {
    const json = serializeMessage({ type: 'ping' });
    expect(JSON.parse(json).type).toBe('ping');
  });
});

describe('deserializeMessage', () => {
  it('deserializes authenticated response', () => {
    const msg = deserializeMessage(
      JSON.stringify({
        type: 'authenticated',
        session_id: '42',
        knowledge_graph: 'default',
        version: '0.1.0',
        role: 'admin',
      }),
    );
    expect(msg.type).toBe('authenticated');
    if (msg.type === 'authenticated') {
      expect(msg.session_id).toBe('42');
      expect(msg.role).toBe('admin');
    }
  });

  it('deserializes result response', () => {
    const msg = deserializeMessage(
      JSON.stringify({
        type: 'result',
        columns: ['x', 'y'],
        rows: [[1, 2], [3, 4]],
        row_count: 2,
        total_count: 2,
        truncated: false,
        execution_time_ms: 5,
      }),
    );
    expect(msg.type).toBe('result');
    if (msg.type === 'result') {
      expect(msg.columns).toEqual(['x', 'y']);
      expect(msg.rows).toHaveLength(2);
    }
  });

  it('deserializes error response', () => {
    const msg = deserializeMessage(
      JSON.stringify({
        type: 'error',
        message: 'Invalid query',
      }),
    );
    expect(msg.type).toBe('error');
  });

  it('deserializes notification response', () => {
    const msg = deserializeMessage(
      JSON.stringify({
        type: 'persistent_update',
        seq: 42,
        timestamp_ms: 1708732800000,
        knowledge_graph: 'default',
        relation: 'edge',
        operation: 'insert',
        count: 5,
      }),
    );
    expect(msg.type).toBe('persistent_update');
  });

  it('deserializes streaming messages', () => {
    const start = deserializeMessage(
      JSON.stringify({
        type: 'result_start',
        columns: ['x'],
        total_count: 100,
        truncated: false,
        execution_time_ms: 50,
      }),
    );
    expect(start.type).toBe('result_start');

    const chunk = deserializeMessage(
      JSON.stringify({
        type: 'result_chunk',
        rows: [[1], [2]],
        chunk_index: 0,
      }),
    );
    expect(chunk.type).toBe('result_chunk');

    const end = deserializeMessage(
      JSON.stringify({
        type: 'result_end',
        row_count: 100,
        chunk_count: 2,
      }),
    );
    expect(end.type).toBe('result_end');
  });

  it('throws on unknown message type', () => {
    expect(() => deserializeMessage('{"type":"unknown"}')).toThrow('Unknown message type');
  });
});

describe('integers past 2^53', () => {
  it('decode to an exact BigInt; other numbers and digit strings stay as they are', () => {
    const msg = deserializeMessage(
      '{"type":"result","columns":["a","b","c","d","e"],"rows":[[9007199254740993,-9223372036854775808,1e300,42,"12345678901234567"]]}',
    );
    expect(msg.type === 'result' && msg.rows).toEqual([[9007199254740993n, -(2n ** 63n), 1e300, 42, '12345678901234567']]);
  });

  it('leave a frame of full-precision floats on the plain JSON.parse path', () => {
    const parse = vi.spyOn(JSON, 'parse');
    try {
      const frame = '{"type":"result","columns":["a","b","c"],"rows":[[0.30000000000000004,-1.2345678901234567e-300,1.2345678901234567e+300]]}';
      const msg = deserializeMessage(frame);
      expect(msg.type === 'result' && msg.rows).toEqual([[0.30000000000000004, -1.2345678901234567e-300, 1.2345678901234567e300]]);
      expect(parse).toHaveBeenCalledTimes(1);
      expect(parse.mock.calls[0]).toEqual([frame]);
    } finally {
      parse.mockRestore();
    }
  });
});

describe('request ids and pushes', () => {
  it('serializes an optional request id', () => {
    expect(JSON.parse(serializeMessage({ type: 'ping', id: 'p' }))).toEqual({
      type: 'ping',
      id: 'p',
    });
    expect(JSON.parse(serializeMessage({ type: 'execute', program: '?a(X)' }))).toEqual({
      type: 'execute',
      program: '?a(X)',
    });
  });

  it('keeps the id a reply echoes', () => {
    const pong = deserializeMessage('{"type":"pong","id":"p"}');
    expect(pong.type === 'pong' && pong.id).toBe('p');
    expect(isPush(pong)).toBe(false);
    const error = deserializeMessage(
      '{"type":"error","message":"bad","code":"invalid_request"}',
    );
    expect(error.type === 'error' && error.code).toBe('invalid_request');
    expect(isPush(error)).toBe(false);
  });

  it('classifies notices and subscription frames as pushes', () => {
    const frames = [
      '{"type":"notice","code":"idle_timeout","message":"Idle timeout"}',
      '{"type":"notice","code":"replay_gap","message":"re-read state"}',
      '{"type":"subscription_delta","subscription":"s","generation":2,"knowledge_graph":"kg","seq":1,"revision":5,"columns":["x"],"inserted":[[1]],"retracted":[]}',
      '{"type":"subscription_error","subscription":"s","generation":2,"message":"boom"}',
      '{"type":"subscription_delta_start","subscription":"s","generation":2,"knowledge_graph":"kg","seq":2,"revision":6,"columns":["x"]}',
      '{"type":"subscription_delta_chunk","subscription":"s","generation":2,"seq":2,"chunk_index":0,"inserted":[[2]],"retracted":[]}',
      '{"type":"subscription_delta_end","subscription":"s","generation":2,"seq":2,"chunk_count":1,"inserted_count":1,"retracted_count":0}',
      '{"type":"subscription_reset","subscription":"s","generation":2,"message":"gone"}',
      '{"type":"kg_change","knowledge_graph":"kg","operation":"created","timestamp_ms":1,"seq":1}',
      '{"type":"subscription_group_delta","subscription":"g","generation":3,"knowledge_graph":"kg","seq":1,"revision":7,"members":[{"name":"a","unchanged":true,"columns":["x"],"inserted":[],"retracted":[]}]}',
      '{"type":"subscription_group_delta_start","subscription":"g","generation":3,"knowledge_graph":"kg","seq":2,"revision":8,"members":[{"name":"a","unchanged":false,"columns":["x"],"inserted_count":1,"retracted_count":0}]}',
      '{"type":"subscription_group_delta_chunk","subscription":"g","generation":3,"seq":2,"chunk_index":0,"member":0,"inserted":[[1]],"retracted":[]}',
      '{"type":"subscription_group_delta_end","subscription":"g","generation":3,"seq":2,"chunk_count":1}',
    ];
    for (const frame of frames) {
      expect(isPush(deserializeMessage(frame))).toBe(true);
    }
  });
});

describe('snapshot reads and subscription groups (protocol 5)', () => {
  it('speaks protocol version 5', () => {
    expect(PROTOCOL_VERSION).toBe(5);
  });

  it('serializes read and subscribe requests', () => {
    const queries = [{ name: 'orders', query: '?order(S, O)' }, { name: 'eta', query: '?eta(O, T)' }];
    expect(JSON.parse(serializeMessage({ type: 'read', id: 'r1', queries, timeout_ms: 500 }))).toEqual({
      type: 'read', id: 'r1', queries, timeout_ms: 500,
    });
    expect(JSON.parse(serializeMessage({ type: 'subscribe', id: 's1', subscription: 'win', queries }))).toEqual({
      type: 'subscribe', id: 's1', subscription: 'win', queries,
    });
  });

  it('deserializes snapshot replies, which are never pushes', () => {
    const frames = [
      '{"type":"snapshot","id":"s1","knowledge_graph":"default","revision":5,"results":[{"name":"orders","columns":["S","O"],"rows":[[1,2]],"total_count":1,"truncated":false}],"execution_time_ms":2,"subscribed":{"subscription":"win","generation":1,"revision":5}}',
      '{"type":"snapshot_start","id":"s1","knowledge_graph":"default","revision":5,"results":[{"name":"orders","columns":["S","O"],"row_count":3000,"total_count":3000,"truncated":false}],"execution_time_ms":2}',
      '{"type":"snapshot_chunk","id":"s1","result":0,"chunk_index":0,"rows":[[1,2]]}',
      '{"type":"snapshot_end","id":"s1","chunk_count":1}',
    ];
    for (const frame of frames) {
      const msg = deserializeMessage(frame);
      expect(msg.type).toBe(JSON.parse(frame).type);
      expect(isPush(msg)).toBe(false);
      expect((msg as { id?: string }).id).toBe('s1');
    }
  });
});
