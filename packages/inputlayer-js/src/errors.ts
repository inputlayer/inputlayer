/**
 * Exception hierarchy for the InputLayer SDK.
 */

import type { ErrorCode, ResultResponse, StatementError } from './protocol.js';

export class InputLayerError extends Error {
  /** The program that was sent, set by the connection on a call's error. */
  iql?: string;

  constructor(message: string) {
    super(message);
    this.name = 'InputLayerError';
  }
}

export class ConnectionError extends InputLayerError {
  constructor(message: string) {
    super(message);
    this.name = 'ConnectionError';
  }
}

/**
 * The connection closed while a call was pending, or reconnecting gave up.
 *
 * `code` is the server's closing notice (`idle_timeout`, `server_shutdown`,
 * ...) when it sent one, else `closed`. `mayHaveCommitted` is set when the
 * call was sent and may write: read the state back before resending it
 * (fact writes are idempotent, so resending one is safe).
 */
export class ConnectionLostError extends ConnectionError {
  constructor(
    message: string,
    readonly code: string,
    readonly mayHaveCommitted = false,
  ) {
    super(message);
    this.name = 'ConnectionLostError';
  }
}

export class AuthenticationError extends InputLayerError {
  constructor(message: string) {
    super(message);
    this.name = 'AuthenticationError';
  }
}

/**
 * The engine rejected a program: it answered with an `error` frame.
 *
 * `code` says why (an `ErrorCode` such as `validation` or `not_found`). It is
 * undefined when the failure has no statement cause, such
 * as an overloaded or shutting-down server or a result too large to send.
 * `validationErrors` lists parse errors.
 */
export class QueryError extends InputLayerError {
  readonly code?: ErrorCode;
  readonly validationErrors: Array<Record<string, unknown>>;

  constructor(
    message: string,
    opts?: { code?: ErrorCode; validationErrors?: Array<Record<string, unknown>> },
  ) {
    super(message);
    this.name = 'QueryError';
    this.code = opts?.code;
    this.validationErrors = opts?.validationErrors ?? [];
  }
}

/**
 * Write outcome unknown. The store may be read-only until restart recovery;
 * if so, a following write raises `StoreReadOnlyError`.
 *
 * The transaction may or may not survive restart; read the recovered data
 * before retrying it. `result` holds the server's response when it has one.
 */
export class OutcomeUnknownError extends QueryError {
  constructor(message: string, readonly result?: ResultResponse) {
    super(message, { code: 'outcome_unknown' });
    this.name = 'OutcomeUnknownError';
  }
}

/**
 * A write was refused because an earlier outcome is unknown.
 *
 * Every write fails until the server restarts and runs recovery.
 */
export class StoreReadOnlyError extends QueryError {
  constructor(message: string, readonly result?: ResultResponse) {
    super(message, { code: 'store_read_only' });
    this.name = 'StoreReadOnlyError';
  }
}

/**
 * The request's deadline (`timeoutMs`) passed before it began committing:
 * nothing was applied. Resend it if it is still wanted.
 */
export class DeadlineExceededError extends QueryError {
  constructor(message: string) {
    super(message, { code: 'deadline_exceeded' });
    this.name = 'DeadlineExceededError';
  }
}

/**
 * The request was cancelled (its `signal` aborted) before it began
 * committing: nothing was applied.
 */
export class CancelledError extends QueryError {
  constructor(message: string) {
    super(message, { code: 'cancelled' });
    this.name = 'CancelledError';
  }
}

/** The server refused the request as malformed (`invalid_request`): an SDK bug. */
export class ProtocolError extends QueryError {
  constructor(message: string) {
    super(`${message} (invalid_request: this is an SDK bug, please report it)`, {
      code: 'invalid_request',
    });
    this.name = 'ProtocolError';
  }
}

/** The connection's message rate limit refused the request; nothing ran. */
export class RateLimitedError extends QueryError {
  constructor(message: string) {
    super(message, { code: 'rate_limited' });
    this.name = 'RateLimitedError';
  }
}

/**
 * Statements of a multi-statement program failed.
 *
 * The engine runs every statement of a program, so the statements not in
 * `errors` took effect. `result` is the whole program's result. `code` is
 * the first failure's code.
 */
export class StatementFailedError extends QueryError {
  readonly errors: StatementError[];
  readonly result: ResultResponse;

  constructor(errors: StatementError[], result: ResultResponse) {
    const first = errors[0];
    super(
      `${errors.length} statement(s) failed; statement ${first.index}: ${first.message}`,
      { code: first.code },
    );
    this.name = 'StatementFailedError';
    this.errors = errors;
    this.result = result;
  }
}

export class SchemaConflictError extends InputLayerError {
  existingSchema?: Record<string, unknown>;
  proposedSchema?: Record<string, unknown>;
  conflicts: string[];

  constructor(
    message: string,
    opts?: {
      existingSchema?: Record<string, unknown>;
      proposedSchema?: Record<string, unknown>;
      conflicts?: string[];
    },
  ) {
    super(message);
    this.name = 'SchemaConflictError';
    this.existingSchema = opts?.existingSchema;
    this.proposedSchema = opts?.proposedSchema;
    this.conflicts = opts?.conflicts ?? [];
  }
}

export class ValidationError extends InputLayerError {
  details: Array<Record<string, unknown>>;

  constructor(
    message: string,
    opts?: { details?: Array<Record<string, unknown>> },
  ) {
    super(message);
    this.name = 'ValidationError';
    this.details = opts?.details ?? [];
  }
}

export class QueryTimeoutError extends InputLayerError {
  constructor(message: string) {
    super(message);
    this.name = 'QueryTimeoutError';
  }
}

export class PermissionError extends InputLayerError {
  constructor(message: string) {
    super(message);
    this.name = 'PermissionError';
  }
}

export class KnowledgeGraphNotFoundError extends InputLayerError {
  constructor(message: string) {
    super(message);
    this.name = 'KnowledgeGraphNotFoundError';
  }
}

export class KnowledgeGraphExistsError extends InputLayerError {
  constructor(message: string) {
    super(message);
    this.name = 'KnowledgeGraphExistsError';
  }
}

export class CannotDropError extends InputLayerError {
  constructor(message: string) {
    super(message);
    this.name = 'CannotDropError';
  }
}

export class RelationNotFoundError extends InputLayerError {
  constructor(message: string) {
    super(message);
    this.name = 'RelationNotFoundError';
  }
}

export class RuleNotFoundError extends InputLayerError {
  constructor(message: string) {
    super(message);
    this.name = 'RuleNotFoundError';
  }
}

export class IndexNotFoundError extends InputLayerError {
  constructor(message: string) {
    super(message);
    this.name = 'IndexNotFoundError';
  }
}

export class InternalError extends InputLayerError {
  constructor(message: string) {
    super(message);
    this.name = 'InternalError';
  }
}

/**
 * The SDK refused to compile a call; nothing was sent. `hint` says how to
 * write it so it compiles.
 */
export class CompileError extends InputLayerError {
  readonly hint?: string;

  constructor(message: string, hint?: string) {
    super(hint ? `${message}. ${hint}` : message);
    this.name = 'CompileError';
    this.hint = hint;
  }
}

/**
 * A guarded program's guard did not hold at commit, so nothing was applied.
 * Re-read the state before retrying: the intent is stale.
 */
export class PreconditionFailed extends QueryError {
  /** The program that was sent. */
  readonly iql: string;
  readonly result: ResultResponse;

  constructor(iql: string, result: ResultResponse) {
    super('Precondition failed: the program guard did not hold, nothing was applied', {
      code: 'validation',
    });
    this.name = 'PreconditionFailed';
    this.iql = iql;
    this.result = result;
  }
}

/**
 * The engine could not evaluate a conditional write against a stable state
 * (concurrent commits kept changing what its guard reads). Nothing was
 * applied; retry with backoff.
 */
export class ConflictError extends QueryError {
  /** The program that was sent. */
  readonly iql: string;

  constructor(message: string, iql: string) {
    super(message, { code: 'conflict' });
    this.name = 'ConflictError';
    this.iql = iql;
  }
}

/**
 * Why a subscription was refused: by the SDK before anything was sent
 * (`limit_offset`, `or_branches`, `session_view`), or by the engine
 * (`result_cap`, `access_denied`, `id_taken`, `subscription_limit`, and
 * `rejected` for any other refusal, such as an invalid query).
 */
export type SubscriptionRejectedReason =
  | 'limit_offset'
  | 'or_branches'
  | 'session_view'
  | 'result_cap'
  | 'access_denied'
  | 'id_taken'
  | 'subscription_limit'
  | 'rejected';

/**
 * A subscription could not be opened, or could not be re-opened after it
 * lost its verified state. Fix or narrow the query; retrying it as it is
 * fails the same way.
 */
export class SubscriptionRejectedError extends InputLayerError {
  constructor(
    message: string,
    readonly reason: SubscriptionRejectedReason,
  ) {
    super(message);
    this.name = 'SubscriptionRejectedError';
  }
}
