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

import type { IQLType, Fact } from './types.js';
import { Timestamp } from './types.js';
import { camelToSnake } from './naming.js';
import { ColumnProxy } from './proxy.js';
import { RelationRef } from './proxy.js';

/** Column type shorthand map for the schema definition DSL. */
export type ColumnTypes = Record<string, IQLType>;

/** A relation definition created by `relation()`. */
export class RelationDef {
  /** The IQL relation name (snake_case). */
  readonly relationName: string;
  /** Original class-style name. */
  readonly className: string;
  /** Ordered column names. */
  readonly columns: string[];
  /** Column name -> IQL type. */
  readonly columnTypes: Record<string, IQLType>;

  constructor(className: string, columnTypes: ColumnTypes, name?: string) {
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
    return new ColumnProxy(this.relationName, name);
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
export function relation(
  className: string,
  columnTypes: ColumnTypes,
  opts?: { name?: string },
): RelationDef {
  return new RelationDef(className, columnTypes, opts?.name);
}

/** Compile a value to its IQL literal representation. */
export function compileValue(value: unknown): string {
  if (value === null || value === undefined) {
    return 'null';
  }
  if (typeof value === 'boolean') {
    return value ? 'true' : 'false';
  }
  if (value instanceof Timestamp) {
    return String(value.ms);
  }
  if (typeof value === 'number') {
    if (Number.isInteger(value)) {
      return String(value);
    }
    return String(value);
  }
  if (typeof value === 'string') {
    const escaped = value.replace(/\\/g, '\\\\').replace(/"/g, '\\"');
    return `"${escaped}"`;
  }
  if (Array.isArray(value)) {
    const inner = value.map(compileValue).join(', ');
    return `[${inner}]`;
  }
  throw new TypeError(
    `Cannot compile value of type ${typeof value}: ${String(value)}`,
  );
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
