/**
 * Exception hierarchy for the InputLayer SDK.
 */

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
