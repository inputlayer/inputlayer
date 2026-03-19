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

  constructor(relation: string, name: string, refAlias?: string) {
    this.relation = relation;
    this.name = name;
    this.refAlias = refAlias;
  }

  toAst(): Column {
    return column(this.relation, this.name, this.refAlias);
  }

  // ── Comparison operators -> BoolExpr ────────────────────────────

  eq(other: ColumnProxy | Expr | number | string | boolean | null): Comparison {
    return comparison('=', this.toAst(), wrap(other));
  }

  ne(other: ColumnProxy | Expr | number | string | boolean | null): Comparison {
    return comparison('!=', this.toAst(), wrap(other));
  }

  lt(other: ColumnProxy | Expr | number | string): Comparison {
    return comparison('<', this.toAst(), wrap(other));
  }

  le(other: ColumnProxy | Expr | number | string): Comparison {
    return comparison('<=', this.toAst(), wrap(other));
  }

  gt(other: ColumnProxy | Expr | number | string): Comparison {
    return comparison('>', this.toAst(), wrap(other));
  }

  ge(other: ColumnProxy | Expr | number | string): Comparison {
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
    return inExpr(this.toAst(), other.toAst());
  }

  notIn(other: ColumnProxy): NegatedIn {
    return negatedIn(this.toAst(), other.toAst());
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

/** Combine two boolean expressions with AND. */
export function AND(left: BoolExpr, right: BoolExpr): And {
  return astAnd(left, right);
}

/** Combine two boolean expressions with OR. */
export function OR(left: BoolExpr, right: BoolExpr): Or {
  return astOr(left, right);
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

  constructor(relationName: string, refAlias?: string) {
    this.relationName = relationName;
    this.refAlias = refAlias;
  }

  /** Get a ColumnProxy for the named column. */
  col(name: string): ColumnProxy {
    return new ColumnProxy(this.relationName, name, this.refAlias);
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
    return new ColumnProxy(this.relationName, name, this.alias);
  }
}
