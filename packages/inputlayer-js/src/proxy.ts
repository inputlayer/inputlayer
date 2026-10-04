/**
 * Column proxy objects for building expression ASTs via method chaining.
 *
 * In TypeScript we can't overload operators, so we use method names instead.
 */

import {
  type Expr,
  type BoolExpr,
  type Column,
  type OrderedColumn,
  type Comparison,
  type Arithmetic,
  type And,
  type Or,
  type Not,
  type InExpr,
  type NegatedIn,
  type MatchExpr,
  column,
  literal,
  comparison,
  arithmetic,
  orderedColumn,
  and as astAnd,
  or as astOr,
  not as astNot,
  inExpr,
  negatedIn,
  matchExpr,
} from './ast.js';
import type { RelationSchema } from './types.js';
import { camelToSnake } from './naming.js';

// ── Helpers ───────────────────────────────────────────────────────────

/** Wrap a raw value or proxy into an AST Expr. */
export function wrap(value: unknown): Expr {
  if (value instanceof ColumnProxy) {
    return value.toAst();
  }
  if (
    value !== null &&
    typeof value === 'object' &&
    '_tag' in (value as Record<string, unknown>)
  ) {
    return value as Expr;
  }
  return literal(value);
}

// ── ColumnProxy ─────────────────────────────────────────────────────

/**
 * Proxy returned by relation column accessors.
 * Builds AST nodes via method calls (TypeScript has no operator overloading).
 */
export class ColumnProxy {
  readonly relation: string;
  readonly name: string;
  readonly refAlias?: string;
  /** All columns of the relation, in order, when the proxy came from its definition. */
  readonly relationColumns?: readonly string[];
  /** The column's IQL type, when the proxy came from a typed definition. */
  readonly type?: string;

  constructor(relation: string, name: string, refAlias?: string, relationColumns?: readonly string[], type?: string) {
    this.relation = relation;
    this.name = name;
    this.refAlias = refAlias;
    this.relationColumns = relationColumns;
    this.type = type;
  }

  toAst(): Column {
    return column(this.relation, this.name, this.refAlias, this.type);
  }

  // ── Comparison operators -> BoolExpr ────────────────────────────

  eq(other: ColumnProxy | Expr | number | bigint | string | boolean | null): Comparison {
    return comparison('=', this.toAst(), wrap(other));
  }

  ne(other: ColumnProxy | Expr | number | bigint | string | boolean | null): Comparison {
    return comparison('!=', this.toAst(), wrap(other));
  }

  lt(other: ColumnProxy | Expr | number | bigint | string): Comparison {
    return comparison('<', this.toAst(), wrap(other));
  }

  le(other: ColumnProxy | Expr | number | bigint | string): Comparison {
    return comparison('<=', this.toAst(), wrap(other));
  }

  gt(other: ColumnProxy | Expr | number | bigint | string): Comparison {
    return comparison('>', this.toAst(), wrap(other));
  }

  ge(other: ColumnProxy | Expr | number | bigint | string): Comparison {
    return comparison('>=', this.toAst(), wrap(other));
  }

  // ── Arithmetic operators -> Expr ────────────────────────────────

  add(other: ColumnProxy | Expr | number): Arithmetic {
    return arithmetic('+', this.toAst(), wrap(other));
  }

  sub(other: ColumnProxy | Expr | number): Arithmetic {
    return arithmetic('-', this.toAst(), wrap(other));
  }

  mul(other: ColumnProxy | Expr | number): Arithmetic {
    return arithmetic('*', this.toAst(), wrap(other));
  }

  div(other: ColumnProxy | Expr | number): Arithmetic {
    return arithmetic('/', this.toAst(), wrap(other));
  }

  mod(other: ColumnProxy | Expr | number): Arithmetic {
    return arithmetic('%', this.toAst(), wrap(other));
  }

  // ── Membership ──────────────────────────────────────────────────

  in(other: ColumnProxy): InExpr {
    return inExpr(this.toAst(), other.toAst(), other.relationColumns);
  }

  notIn(other: ColumnProxy): NegatedIn {
    return negatedIn(this.toAst(), other.toAst(), other.relationColumns);
  }

  // ── Ordering ────────────────────────────────────────────────────

  asc(): OrderedColumn {
    return orderedColumn(this.toAst(), false);
  }

  desc(): OrderedColumn {
    return orderedColumn(this.toAst(), true);
  }

  // ── Multi-column match ──────────────────────────────────────────

  matches(
    relationName: string,
    on: Record<string, string>,
  ): MatchExpr {
    const bindings: Record<string, Expr> = {};
    for (const [targetCol, sourceColName] of Object.entries(on)) {
      bindings[targetCol] = column(this.relation, sourceColName, this.refAlias);
    }
    return matchExpr(relationName, bindings, false);
  }

  notMatches(
    relationName: string,
    on: Record<string, string>,
  ): MatchExpr {
    const bindings: Record<string, Expr> = {};
    for (const [targetCol, sourceColName] of Object.entries(on)) {
      bindings[targetCol] = column(this.relation, sourceColName, this.refAlias);
    }
    return matchExpr(relationName, bindings, true);
  }
}

// ── BoolExpr combinators ────────────────────────────────────────────

/** Combine boolean expressions with AND. */
export function AND(first: BoolExpr, second: BoolExpr, ...rest: BoolExpr[]): And {
  return rest.reduce<And>((acc, e) => astAnd(acc, e), astAnd(first, second));
}

/** Combine boolean expressions with OR. */
export function OR(first: BoolExpr, second: BoolExpr, ...rest: BoolExpr[]): Or {
  return rest.reduce<Or>((acc, e) => astOr(acc, e), astOr(first, second));
}

/** Negate a boolean expression. */
export function NOT(operand: BoolExpr): Not {
  return astNot(operand);
}

// ── RelationProxy ───────────────────────────────────────────────────

/**
 * Proxy object passed to where/on callbacks.
 * Property access returns a ColumnProxy for the given column name.
 *
 * Usage:
 *   where: (e) => e.col("department").eq("eng")
 */
export class RelationProxy {
  readonly relationName: string;
  readonly refAlias?: string;
  readonly columns?: readonly string[];

  constructor(relationName: string, refAlias?: string, columns?: readonly string[]) {
    this.relationName = relationName;
    this.refAlias = refAlias;
    this.columns = columns;
  }

  /** Get a ColumnProxy for the named column. */
  col(name: string): ColumnProxy {
    return new ColumnProxy(this.relationName, name, this.refAlias, this.columns);
  }
}

// ── RelationRef ─────────────────────────────────────────────────────

/** Independent reference to a relation for self-joins. */
export class RelationRef {
  readonly schema: RelationSchema;
  readonly alias: string;
  readonly relationName: string;

  constructor(schema: RelationSchema, alias: string) {
    this.schema = schema;
    this.alias = alias;
    this.relationName = schema.name ?? camelToSnake(alias);
  }

  /** Get a ColumnProxy for the named column. */
  col(name: string): ColumnProxy {
    return new ColumnProxy(
      this.relationName,
      name,
      this.alias,
      this.schema.columns.map((c) => c.name),
      this.schema.columns.find((c) => c.name === name)?.type,
    );
  }
}
