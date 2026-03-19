import { describe, it, expect } from 'vitest';
import { NotificationDispatcher, type NotificationEvent } from '../src/notifications';

describe('NotificationDispatcher', () => {
  it('dispatches to matching callbacks', () => {
    const dispatcher = new NotificationDispatcher();
    const received: NotificationEvent[] = [];

    dispatcher.on('persistent_update', {}, (event) => {
      received.push(event);
    });

    dispatcher.dispatch({
      type: 'persistent_update',
      seq: 1,
      timestampMs: Date.now(),
      relation: 'employee',
      operation: 'insert',
      count: 5,
    });

    expect(received).toHaveLength(1);
    expect(received[0].relation).toBe('employee');
  });

  it('filters by event type', () => {
    const dispatcher = new NotificationDispatcher();
    let called = false;

    dispatcher.on('rule_change', {}, () => {
      called = true;
    });

    dispatcher.dispatch({
      type: 'persistent_update',
      seq: 1,
      timestampMs: Date.now(),
    });

    expect(called).toBe(false);
  });

  it('filters by relation', () => {
    const dispatcher = new NotificationDispatcher();
    const received: NotificationEvent[] = [];

    dispatcher.on('persistent_update', { relation: 'employee' }, (event) => {
      received.push(event);
    });

    dispatcher.dispatch({
      type: 'persistent_update',
      seq: 1,
      timestampMs: Date.now(),
      relation: 'department',
    });

    dispatcher.dispatch({
      type: 'persistent_update',
      seq: 2,
      timestampMs: Date.now(),
      relation: 'employee',
    });

    expect(received).toHaveLength(1);
    expect(received[0].seq).toBe(2);
  });

  it('tracks lastSeq', () => {
    const dispatcher = new NotificationDispatcher();
    expect(dispatcher.lastSeq).toBe(0);

    dispatcher.dispatch({ type: 'persistent_update', seq: 5, timestampMs: Date.now() });
    expect(dispatcher.lastSeq).toBe(5);

    dispatcher.dispatch({ type: 'persistent_update', seq: 3, timestampMs: Date.now() });
    expect(dispatcher.lastSeq).toBe(5); // Doesn't go backwards
  });

  it('removes callbacks with off()', () => {
    const dispatcher = new NotificationDispatcher();
    let count = 0;

    const cb = () => { count++; };
    dispatcher.on(undefined, {}, cb);
    dispatcher.dispatch({ type: 'persistent_update', seq: 1, timestampMs: Date.now() });
    expect(count).toBe(1);

    dispatcher.off(cb);
    dispatcher.dispatch({ type: 'persistent_update', seq: 2, timestampMs: Date.now() });
    expect(count).toBe(1); // Not called again
  });
});
