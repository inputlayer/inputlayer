/**
 * Notification dispatcher for push events from the server.
 */

/** A single notification event from the server. */
export interface NotificationEvent {
  type: string;
  seq: number;
  timestampMs: number;
  sessionId?: string;
  knowledgeGraph?: string;
  relation?: string;
  operation?: string;
  count?: number;
  ruleName?: string;
  entity?: string;
}

export type NotificationCallback = (event: NotificationEvent) => void | Promise<void>;

interface CallbackEntry {
  eventType?: string;
  relation?: string;
  knowledgeGraph?: string;
  callback: NotificationCallback;
}

/** Notification `seq` numbers remembered per epoch to drop duplicates. */
const SEEN_LIMIT = 4096;

/**
 * Routes notification events to registered callbacks and iterators.
 *
 * One dispatcher may be fed by several connections (a client opens one per
 * knowledge graph). Notification `seq` numbers belong to the engine's single
 * stream within a `stream_epoch`, so a notification two connections both see
 * (a `kg_change` for an admin) has the same `seq` and is dispatched once.
 */
export class NotificationDispatcher {
  private callbacks: CallbackEntry[] = [];
  private _lastSeq = 0;
  private readonly iterators = new Set<EventQueue>();
  private epoch?: string;
  private readonly seen = new Set<number>();

  get lastSeq(): number {
    return this._lastSeq;
  }

  /**
   * Register a callback for notifications.
   *
   * @param eventType - Filter by event type (e.g. "persistent_update")
   * @param opts - Additional filters
   * @param callback - Function to call when a matching event arrives
   */
  on(
    eventType: string | undefined,
    opts: { relation?: string; knowledgeGraph?: string },
    callback: NotificationCallback,
  ): void {
    this.callbacks.push({
      eventType,
      relation: opts.relation,
      knowledgeGraph: opts.knowledgeGraph,
      callback,
    });
  }

  /** Remove a previously registered callback. */
  off(callback: NotificationCallback): void {
    this.callbacks = this.callbacks.filter((e) => e.callback !== callback);
  }

  /**
   * Dispatch a notification to matching callbacks and every iterator.
   * `epoch` is the `stream_epoch` of the connection it arrived on; a
   * notification already dispatched for that epoch is dropped.
   */
  dispatch(event: NotificationEvent, epoch?: string): void {
    if (epoch !== undefined) {
      if (epoch !== this.epoch) {
        this.epoch = epoch;
        this.seen.clear();
      }
      if (this.seen.has(event.seq)) return;
      this.seen.add(event.seq);
      if (this.seen.size > SEEN_LIMIT) {
        this.seen.delete(this.seen.values().next().value as number);
      }
    }
    this._lastSeq = Math.max(this._lastSeq, event.seq);

    for (const queue of this.iterators) queue.push(event);

    // Call matching callbacks
    for (const entry of this.callbacks) {
      if (entry.eventType !== undefined && event.type !== entry.eventType) continue;
      if (entry.relation !== undefined && event.relation !== entry.relation) continue;
      if (entry.knowledgeGraph !== undefined && event.knowledgeGraph !== entry.knowledgeGraph) continue;
      try {
        entry.callback(event);
      } catch {
        // Callbacks should not break the dispatcher
      }
    }
  }

  /** Wait for the next notification event. */
  async next(): Promise<NotificationEvent> {
    const queue = new EventQueue();
    this.iterators.add(queue);
    try {
      return await queue.take();
    } finally {
      this.iterators.delete(queue);
    }
  }

  /**
   * Create an async iterable of notification events. Events that arrive
   * while the consumer is busy are buffered, not dropped.
   *
   * Usage:
   *   for await (const event of dispatcher.events()) { ... }
   */
  async *events(): AsyncIterableIterator<NotificationEvent> {
    const queue = new EventQueue();
    this.iterators.add(queue);
    try {
      while (true) {
        yield await queue.take();
      }
    } finally {
      this.iterators.delete(queue);
    }
  }
}

/** Events buffered for one iterator. */
class EventQueue {
  private readonly buffered: NotificationEvent[] = [];
  private waiter?: (event: NotificationEvent) => void;

  push(event: NotificationEvent): void {
    const waiter = this.waiter;
    if (waiter) {
      this.waiter = undefined;
      waiter(event);
    } else {
      this.buffered.push(event);
    }
  }

  take(): Promise<NotificationEvent> {
    const event = this.buffered.shift();
    if (event) return Promise.resolve(event);
    return new Promise((resolve) => {
      this.waiter = resolve;
    });
  }
}
