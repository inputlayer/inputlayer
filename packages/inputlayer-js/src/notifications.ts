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

/**
 * Routes notification events to registered callbacks.
 */
export class NotificationDispatcher {
  private callbacks: CallbackEntry[] = [];
  private _lastSeq = 0;
  private waiters: Array<(event: NotificationEvent) => void> = [];

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

  /** Dispatch a notification to matching callbacks. */
  dispatch(event: NotificationEvent): void {
    this._lastSeq = Math.max(this._lastSeq, event.seq);

    // Notify any async iterators
    for (const waiter of this.waiters) {
      waiter(event);
    }
    this.waiters = [];

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
  next(): Promise<NotificationEvent> {
    return new Promise<NotificationEvent>((resolve) => {
      this.waiters.push(resolve);
    });
  }

  /**
   * Create an async iterable of notification events.
   *
   * Usage:
   *   for await (const event of dispatcher.events()) { ... }
   */
  async *events(): AsyncIterableIterator<NotificationEvent> {
    while (true) {
      yield await this.next();
    }
  }
}
