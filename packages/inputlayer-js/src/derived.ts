/**
 * Derived relations and the From/Where/Select rule builder.
 *
 * Provides a fluent API for defining rules that mirrors the Python SDK.
 *
 * @example
 * ```typescript
 * const reachableRules = [
 *   from(Edge).select({ src: Edge.col("x"), dst: Edge.col("y") }),
 *   from(Reachable, Edge)
 *     .where((r, e) => r.col("dst").eq(e.col("x")))
 *     .select({ src: Reachable.col("src"), dst: Edge.col("y") }),
 * ];
 *
 * await kg.defineRules("reachable", ["src", "dst"], reachableRules);
 * ```
 */

import type { Expr, BoolExpr } from './ast.js';
import { RelationDef } from './relation.js';
import { ColumnProxy, RelationProxy, type RelationRef } from './proxy.js';
import type { RuleClause } from './compiler.js';

// ── FromWhere ───────────────────────────────────────────────────────

class FromWhere {
  private readonly relations: Array<{ name: string; def: RelationDef; alias?: string }>;
  private readonly condition: BoolExpr;

  constructor(
    relations: Array<{ name: string; def: RelationDef; alias?: string }>,
    condition: BoolExpr,
  ) {
    this.relations = relations;
    this.condition = condition;
  }

  /**
   * Map derived columns to body expressions.
   * Keys must match the derived relation's column names.
   */
  select(columns: Record<string, ColumnProxy | Expr>): RuleClause {
    const selectMap: Record<string, Expr> = {};
    for (const [name, val] of Object.entries(columns)) {
      if (val instanceof ColumnProxy) {
        selectMap[name] = val.toAst();
      } else {
        selectMap[name] = val;
      }
    }
    return {
      relations: this.relations,
      selectMap,
      condition: this.condition,
    };
  }
}

// ── From ────────────────────────────────────────────────────────────

class FromBuilder {
  private readonly relations: Array<{ name: string; def: RelationDef; alias?: string }>;

  constructor(rels: Array<RelationDef | RelationRef>) {
    this.relations = rels.map((r) => {
      if (r instanceof RelationDef) {
        return { name: r.relationName, def: r, alias: undefined };
      }
      // RelationRef
      return {
        name: r.relationName,
        def: {
          relationName: r.relationName,
          columns: r.schema.columns.map((c) => c.name),
          columnTypes: {},
        } as unknown as RelationDef,
        alias: r.alias,
      };
    });
  }

  /**
   * Add a filter/join condition.
   * Accepts a BoolExpr or a callback receiving RelationProxy objects.
   */
  where(condition: BoolExpr | ((...proxies: RelationProxy[]) => BoolExpr)): FromWhere {
    let resolved: BoolExpr;
    if (typeof condition === 'function') {
      const proxies = this.relations.map(
        (r) => new RelationProxy(r.name, r.alias),
      );
      resolved = condition(...proxies);
    } else {
      resolved = condition;
    }
    return new FromWhere(this.relations, resolved);
  }

  /**
   * Map derived columns to body expressions (no filter).
   * Keys must match the derived relation's column names.
   */
  select(columns: Record<string, ColumnProxy | Expr>): RuleClause {
    const selectMap: Record<string, Expr> = {};
    for (const [name, val] of Object.entries(columns)) {
      if (val instanceof ColumnProxy) {
        selectMap[name] = val.toAst();
      } else {
        selectMap[name] = val;
      }
    }
    return {
      relations: this.relations,
      selectMap,
      condition: undefined,
    };
  }
}

/**
 * Start building a rule clause.
 *
 * @param relations - The body relations for this rule clause
 * @returns A FromBuilder with .where() and .select() methods
 *
 * @example
 * from(Edge).select({ src: Edge.col("x"), dst: Edge.col("y") })
 */
export function from(...relations: Array<RelationDef | RelationRef>): FromBuilder {
  return new FromBuilder(relations);
}
