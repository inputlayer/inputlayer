/**
 * Exception hierarchy for the InputLayer SDK.
 */

import type { ErrorCode, ResultResponse, StatementError } from './protocol.js';

export class InputLayerError extends Error {
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
 * Write outcome unknown, store read-only until restart recovery.
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
