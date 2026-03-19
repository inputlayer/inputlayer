/**
 * InputLayer type system - TypeScript types that map to IQL storage types.
 */

/** Supported IQL column types. */
export type IQLType =
  | 'int'
  | 'float'
  | 'string'
  | 'bool'
  | 'timestamp'
  | 'vector'
  | 'vector_int8'
  | `vector[${number}]`
  | `vector_int8[${number}]`;

/** A float32 vector (plain number array). */
export type Vector = number[];

/** An int8 quantized vector (plain number array with values in [-128, 127]). */
export type VectorInt8 = number[];

/** Timestamp as Unix milliseconds since epoch. */
export class Timestamp {
  readonly ms: number;

  constructor(ms: number) {
    this.ms = Math.floor(ms);
  }

  static now(): Timestamp {
    return new Timestamp(Date.now());
  }

  static fromDate(date: Date): Timestamp {
    return new Timestamp(date.getTime());
  }

  toDate(): Date {
    return new Date(this.ms);
  }

  valueOf(): number {
    return this.ms;
  }

  toString(): string {
    return String(this.ms);
  }
}

/** Column definition for a relation schema. */
export interface ColumnDef {
  name: string;
  type: IQLType;
}

/** A relation schema definition describing column names and types. */
export interface RelationSchema {
  /** Override relation name (defaults to camelToSnake of the key). */
  name?: string;
  columns: ColumnDef[];
}

/**
 * Map a TypeScript-friendly type name to its IQL type string.
 *
 * Supported: 'int', 'float', 'string', 'bool', 'timestamp',
 *            'vector', 'vector_int8', or 'vector[N]', 'vector_int8[N]'.
 */
export function toIQLType(type: IQLType): string {
  return type;
}

/** Any value that can appear in a relation fact. */
export type FieldValue =
  | number
  | string
  | boolean
  | null
  | Timestamp
  | Vector
  | VectorInt8;

/** A record of field name -> value, representing one fact/row. */
export type Fact = Record<string, FieldValue>;
