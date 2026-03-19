import { describe, it, expect } from 'vitest';
import { serializeMessage, deserializeMessage } from '../src/protocol';

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
