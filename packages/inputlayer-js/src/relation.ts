/**
 * Relation definitions - schema-first approach for TypeScript.
 *
 * Unlike Python's class-based approach with Pydantic metaclasses, TypeScript
 * uses plain objects with a schema descriptor. Relations are defined as:
 *
 *   const Employee = relation("Employee", {
 *     id: "int",
 *     name: "string",
 *     department: "string",
 *     salary: "float",
 *     active: "bool",
 *   });
 *
 * This returns a RelationDef with column accessors, insert helpers, etc.
 */

import type { IQLType, Fact, FieldValue, Timestamp as TimestampValue } from './types.js';
import { Timestamp } from './types.js';
import { camelToSnake } from './naming.js';
import { ColumnProxy, wrap } from './proxy.js';
import { RelationRef } from './proxy.js';
import { anyExpr, type AnyExpr, type Expr } from './ast.js';
import { CompileError } from './errors.js';

/** Column type shorthand map for the schema definition DSL. */
export type ColumnTypes = Record<string, IQLType>;

/** The TypeScript value of a column of IQL type `T`. */
export type ValueOf<T extends IQLType> = T extends 'string'
  ? string
  : T extends 'int'
    ? number | bigint
    : T extends 'float'
    ? number
    : T extends 'bool'
      ? boolean
      : T extends 'timestamp'
        ? number | Date | TimestampValue
        : number[];

/**
 * The row type of a relation definition:
 * `RowOf<typeof Attempt>` is `{ order: string; tool: string; attempt: string }`.
 */
export type RowOf<R> = R extends RelationDef<infer T> ? { [K in keyof T]: ValueOf<T[K]> } : never;

/** What a column of `any()` can be bound to: a value, or a column of another atom. */
export type Binding = FieldValue | ColumnProxy | Expr;

/** A relation definition created by `relation()`. */
export class RelationDef<T extends ColumnTypes = ColumnTypes> {
  /** The IQL relation name (snake_case). */
  readonly relationName: string;
  /** Original class-style name. */
  readonly className: string;
  /** Ordered column names. */
  readonly columns: string[];
  /** Column name -> IQL type. */
  readonly columnTypes: T;

  constructor(className: string, columnTypes: T, name?: string) {
    this.className = className;
    this.relationName = name ?? camelToSnake(className);
    this.columns = Object.keys(columnTypes);
    this.columnTypes = { ...columnTypes };
  }

  /** Get a ColumnProxy for a column (for query building). */
  col(name: string): ColumnProxy {
    if (!(name in this.columnTypes)) {
      throw new Error(
        `Column '${name}' does not exist on relation '${this.relationName}'. ` +
          `Available: ${this.columns.join(', ')}`,
      );
    }
    return new ColumnProxy(this.relationName, name, undefined, this.columns, this.columnTypes[name]);
  }

  /** A row of this relation exists with the given column values: see `any()`. */
  any(bindings: Partial<Record<keyof T & string, Binding>> = {}): AnyExpr {
    return any(this, bindings);
  }

  /**
   * Create multiple independent references for self-joins.
   *
   * Usage:
   *   const [r1, r2] = Follow.refs(2);
   *   kg.query({ select: [r1.col("follower"), r2.col("followee")], join: [r1, r2], ... });
   */
  refs(n: number): RelationRef[] {
    const refs: RelationRef[] = [];
    for (let i = 1; i <= n; i++) {
      refs.push(
        new RelationRef(
          { name: this.relationName, columns: this.columns.map((c) => ({ name: c, type: this.columnTypes[c] })) },
          `${this.relationName}_${i}`,
        ),
      );
    }
    return refs;
  }
}

/**
 * Define a relation schema.
 *
 * @param className - CamelCase name (converted to snake_case for IQL)
 * @param columnTypes - Column name -> type mapping
 * @param opts - Optional overrides
 * @returns A RelationDef for use with KnowledgeGraph operations
 *
 * @example
 * const Employee = relation("Employee", {
 *   id: "int",
 *   name: "string",
 *   department: "string",
 *   salary: "float",
 *   active: "bool",
 * });
 */
export function relation<T extends ColumnTypes>(
  className: string,
  columnTypes: T,
  opts?: { name?: string },
): RelationDef<T> {
  return new RelationDef(className, columnTypes, opts?.name);
}

/**
 * A row of `rel` exists whose columns equal `bindings`; unbound columns match
 * anything. A binding is a value or a column of another atom in the same body.
 * `NOT(any(...))` is true when no such row exists.
 *
 * @example
 * from(ToolPolicy).where((t) => AND(t.col("mode").eq("auto"), NOT(any(KillSwitch, { tool: t.col("tool") }))))
 * kg.claim(Attempt, row, { when: [any(CheckNeeded, { order: "ORD-1" })], unless: any(Attempt, { order: "ORD-1" }) })
 */
export function any<T extends ColumnTypes>(
  rel: RelationDef<T>,
  bindings: Partial<Record<keyof T & string, Binding>> = {},
): AnyExpr {
  const bound: Record<string, Expr> = {};
  for (const [col, value] of Object.entries(bindings)) {
    if (!(col in rel.columnTypes)) {
      throw new CompileError(
        `any(): column '${col}' does not exist on relation '${rel.relationName}'`,
        `Available: ${rel.columns.join(', ')}`,
      );
    }
    if (value === undefined) continue;
    bound[col] = wrap(value);
  }
  return anyExpr(rel.relationName, rel.columns, rel.columnTypes, bound);
}

const I64_MIN = -(2n ** 63n);
const I64_MAX = 2n ** 63n - 1n;

/**
 * Compile a value to its IQL literal representation. `type` is the column's
 * type when known: an `int` or `timestamp` column refuses a number past
 * Number.MAX_SAFE_INTEGER, which has already lost digits (pass a BigInt).
 */
export function compileValue(value: unknown, type?: string): string {
  if (value === null || value === undefined) {
    return 'null';
  }
  if (typeof value === 'boolean') {
    return value ? 'true' : 'false';
  }
  if (value instanceof Timestamp) {
    return compileValue(value.ms, 'int');
  }
  if (value instanceof Date) {
    // Timestamps are stored as int Unix milliseconds.
    return compileValue(value.getTime(), 'int');
  }
  if (typeof value === 'bigint') {
    if (value < I64_MIN || value > I64_MAX) {
      throw new CompileError(
        `Integer ${value} does not fit the engine's 64-bit integers`,
        `keep integers within [${I64_MIN}, ${I64_MAX}], or store it as a string`,
      );
    }
    return value.toString();
  }
  if (typeof value === 'number') {
    if (Number.isSafeInteger(value)) {
      return String(value);
    }
    if (Number.isInteger(value) && (type === 'int' || type === 'timestamp')) {
      throw new CompileError(
        `Integer ${value} is past Number.MAX_SAFE_INTEGER, so it may already have lost digits`,
        'pass the integer as a BigInt',
      );
    }
    return compileFloat(value);
  }
  if (typeof value === 'string') {
    const escaped = value
      .replace(/\\/g, '\\\\')
      .replace(/"/g, '\\"')
      .replace(/\n/g, '\\n')
      .replace(/\r/g, '\\r')
      .replace(/\t/g, '\\t');
    return `"${escaped}"`;
  }
  if (Array.isArray(value)) {
    return `[${value.map(compileVectorItem).join(', ')}]`;
  }
  throw new TypeError(
    `Cannot compile value of type ${typeof value}: ${String(value)}`,
  );
}

/** A float literal the engine parses back as the same f64; refuses NaN and the infinities. */
function compileFloat(value: number): string {
  // IQL has no literal for NaN or the infinities: written bare they parse
  // as variables, so a retract keyed on NaN would match every row.
  if (!Number.isFinite(value)) {
    throw new CompileError(
      `IQL has no literal for ${value}: it does not support infinity or NaN`,
      'use a finite number, or leave the value out',
    );
  }
  if (!Number.isInteger(value)) return String(value);
  // A whole number written as digits would parse as an integer.
  return Number.isSafeInteger(value) ? `${value}.0` : value.toExponential();
}

function compileVectorItem(value: unknown): string {
  if (typeof value !== 'number') {
    throw new CompileError(
      `A vector holds numbers only, got ${typeof value}: ${String(value)}`,
      'pass an array of numbers',
    );
  }
  return compileFloat(value);
}

/** Resolve a RelationDef or string to its IQL relation name. */
export function resolveRelationName(r: RelationDef | string): string {
  if (typeof r === 'string') return r;
  return r.relationName;
}

/** Get ordered column names from a RelationDef. */
export function getColumns(r: RelationDef): string[] {
  return r.columns;
}

/** Get column types from a RelationDef. */
export function getColumnTypes(r: RelationDef): Record<string, IQLType> {
  return r.columnTypes;
}
