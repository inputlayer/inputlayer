/**
 * Compiler: TypeScript objects and AST nodes -> IQL text.
 *
 * Every function is pure (no I/O), taking TypeScript objects and returning
 * IQL strings.
 */

import {
  type Expr,
  type BoolExpr,
  type Column,
  type AggExpr,
  type OrderedColumn,
  isColumn,
  isLiteral,
  isArithmetic,
  isFuncCall,
  isAggExpr,
  isOrderedColumn,
  isComparison,
  isAnd,
  isOr,
  isNot,
  isInExpr,
  isNegatedIn,
  isMatchExpr,
  column as astColumn,
} from './ast.js';
import { columnToVariable } from './naming.js';
import { RelationDef, compileValue, resolveRelationName, getColumns, getColumnTypes } from './relation.js';
import { RelationRef } from './proxy.js';
import type { ColumnProxy } from './proxy.js';
import type { Fact } from './types.js';

// ── Variable environment ────────────────────────────────────────────

/**
 * Variable environment for tracking column->variable mappings with union-find.
 * Ensures that join conditions produce shared IQL variables.
 */
class VarEnv {
  private map: Map<string, string> = new Map();
  private counter = 0;
  private parent: Map<string, string> = new Map();

  private find(key: string): string {
    let current = key;
    while (true) {
      const p = this.parent.get(current) ?? current;
      if (p === current) return current;
      const gp = this.parent.get(p) ?? p;
      this.parent.set(current, gp);
      current = gp;
    }
  }

  private union(a: string, b: string): void {
    const ra = this.find(a);
    const rb = this.find(b);
    if (ra !== rb) {
      this.parent.set(rb, ra);
    }
  }

  getVar(col: Column): string {
    const key = `${col.refAlias ?? col.relation}.${col.name}`;
    const root = this.find(key);
    const existing = this.map.get(root);
    if (existing !== undefined) return existing;

    let varName = columnToVariable(col.name);
    const usedVars = new Set(this.map.values());
    if (usedVars.has(varName)) {
      this.counter++;
      varName = `${varName}_${this.counter}`;
    }
    this.map.set(root, varName);
    return varName;
  }

  unify(colA: Column, colB: Column): string {
    const keyA = `${colA.refAlias ?? colA.relation}.${colA.name}`;
    const keyB = `${colB.refAlias ?? colB.relation}.${colB.name}`;
    this.union(keyA, keyB);
    const root = this.find(keyA);
    const existing = this.map.get(root);
    if (existing !== undefined) return existing;

    let varName = columnToVariable(colA.name);
    const usedVars = new Set(this.map.values());
    if (usedVars.has(varName)) {
      this.counter++;
      varName = `${varName}_${this.counter}`;
    }
    this.map.set(root, varName);
    return varName;
  }

  lookup(col: Column): string | undefined {
    const key = `${col.refAlias ?? col.relation}.${col.name}`;
    const root = this.find(key);
    return this.map.get(root);
  }

  /** Direct-set a variable for conditional delete setup. */
  set(key: string, varName: string): void {
    this.map.set(key, varName);
  }
}

// ── Expression compilation ──────────────────────────────────────────

export function compileExpr(expr: Expr, env: VarEnv): string {
  if (isColumn(expr)) {
    return env.getVar(expr);
  }
  if (isLiteral(expr)) {
    return compileValue(expr.value);
  }
  if (isArithmetic(expr)) {
    const left = compileExpr(expr.left, env);
    const right = compileExpr(expr.right, env);
    return `${left} ${expr.op} ${right}`;
  }
  if (isFuncCall(expr)) {
    const args = expr.args.map((a) => compileExpr(a, env)).join(', ');
    return `${expr.name}(${args})`;
  }
  if (isOrderedColumn(expr)) {
    const varStr = compileExpr(expr.column, env);
    const suffix = expr.descending ? ':desc' : ':asc';
    return `${varStr}${suffix}`;
  }
  if (isAggExpr(expr)) {
    return compileAggExpr(expr, env);
  }
  throw new TypeError(`Cannot compile expression: ${JSON.stringify(expr)}`);
}

function compileAggExpr(agg: AggExpr, env: VarEnv): string {
  const parts: string[] = [];

  for (const p of agg.params) {
    parts.push(compileValue(p));
  }

  for (const pt of agg.passthrough) {
    parts.push(compileExpr(pt, env));
  }

  if (agg.orderColumn !== undefined) {
    const orderVar = compileExpr(agg.orderColumn, env);
    const suffix = agg.desc ? ':desc' : ':asc';
    parts.push(`${orderVar}${suffix}`);
  } else if (agg.column !== undefined) {
    parts.push(compileExpr(agg.column, env));
  }

  const inner = parts.join(', ');
  return `${agg.func}<${inner}>`;
}

// ── Boolean expression compilation ──────────────────────────────────

export function compileBoolExpr(expr: BoolExpr, env: VarEnv): string[] {
  if (isComparison(expr)) {
    return [compileComparison(expr, env)];
  }
  if (isAnd(expr)) {
    return [...compileBoolExpr(expr.left, env), ...compileBoolExpr(expr.right, env)];
  }
  if (isOr(expr)) {
    throw new Error(
      'OR conditions require query splitting. Use compileOrBranches() instead.',
    );
  }
  if (isNot(expr)) {
    const innerParts = compileBoolExpr(expr.operand, env);
    return [`!(${innerParts.join(', ')})`];
  }
  if (isInExpr(expr)) {
    return [compileIn(expr.column, expr.targetColumn, false, env)];
  }
  if (isNegatedIn(expr)) {
    return [compileIn(expr.column, expr.targetColumn, true, env)];
  }
  if (isMatchExpr(expr)) {
    return [compileMatch(expr, env)];
  }
  throw new TypeError(`Cannot compile boolean expression: ${JSON.stringify(expr)}`);
}

function compileComparison(
  comp: { op: string; left: Expr; right: Expr },
  env: VarEnv,
): string {
  if (comp.op === '=' && isColumn(comp.left) && isColumn(comp.right)) {
    env.unify(comp.left, comp.right);
    return '';
  }
  const left = compileExpr(comp.left, env);
  const right = compileExpr(comp.right, env);
  return `${left} ${comp.op} ${right}`;
}

function compileIn(
  col: Expr,
  target: Expr,
  negated: boolean,
  env: VarEnv,
): string {
  if (isColumn(col) && isColumn(target)) {
    env.unify(col, target);
    const tgtVar = env.getVar(target);
    const prefix = negated ? '!' : '';
    return `${prefix}${target.relation}(..., ${tgtVar}, ...)`;
  }
  const srcVar = compileExpr(col, env);
  const prefix = negated ? '!' : '';
  return `${prefix}(..., ${srcVar}, ...)`;
}

function compileMatch(
  match: { relation: string; bindings: Record<string, Expr>; negated: boolean },
  env: VarEnv,
): string {
  const parts: string[] = [];
  for (const [, sourceExpr] of Object.entries(match.bindings)) {
    parts.push(compileExpr(sourceExpr, env));
  }
  const prefix = match.negated ? '!' : '';
  return `${prefix}${match.relation}(${parts.join(', ')})`;
}

export function compileOrBranches(expr: BoolExpr, env: VarEnv): string[][] {
  if (isOr(expr)) {
    return [
      ...compileOrBranches(expr.left, env),
      ...compileOrBranches(expr.right, env),
    ];
  }
  return [compileBoolExpr(expr, env)];
}

// ── Schema compilation ──────────────────────────────────────────────

/** Compile a relation definition to a schema statement: +employee(id: int, name: string, ...) */
export function compileSchema(rel: RelationDef): string {
  const name = rel.relationName;
  const cols = rel.columns;
  const colTypes = rel.columnTypes;
  const parts = cols.map((c) => {
    // Convert type notation: vector[3] -> vector(3), vector_int8[3] -> vector_int8(3)
    const type = colTypes[c].replace(/\[(\d+)\]/, '($1)');
    return `${c}: ${type}`;
  });
  return `+${name}(${parts.join(', ')})`;
}

// ── Insert compilation ──────────────────────────────────────────────

/** Compile a single fact insert: +employee(1, "Alice", ...) */
export function compileInsert(
  rel: RelationDef,
  fact: Fact,
  persistent = true,
): string {
  const name = rel.relationName;
  const values = rel.columns.map((c) => compileValue(fact[c]));
  const prefix = persistent ? '+' : '';
  return `${prefix}${name}(${values.join(', ')})`;
}

/** Compile a bulk insert: +employee[(1, "Alice", ...), (2, "Bob", ...)] */
export function compileBulkInsert(
  rel: RelationDef,
  facts: Fact[],
  persistent = true,
): string {
  const name = rel.relationName;
  const tuples = facts.map((fact) => {
    const values = rel.columns.map((c) => compileValue(fact[c]));
    return `(${values.join(', ')})`;
  });
  const prefix = persistent ? '+' : '';
  return `${prefix}${name}[${tuples.join(', ')}]`;
}

// ── Delete compilation ──────────────────────────────────────────────

/** Compile a single fact deletion: -employee(1, "Alice", ...) */
export function compileDelete(rel: RelationDef, fact: Fact): string {
  const name = rel.relationName;
  const values = rel.columns.map((c) => compileValue(fact[c]));
  return `-${name}(${values.join(', ')})`;
}

/** Compile a conditional delete: -employee(X0, X1, ...) <- employee(X0, X1, ...), X2 = "sales" */
export function compileConditionalDelete(
  rel: RelationDef,
  condition: BoolExpr,
): string {
  const name = rel.relationName;
  const cols = rel.columns;
  const vars = cols.map((_, i) => `X${i}`);
  const head = `-${name}(${vars.join(', ')})`;

  const env = new VarEnv();
  for (let i = 0; i < cols.length; i++) {
    env.set(`${name}.${cols[i]}`, vars[i]);
  }

  const bodyRel = `${name}(${vars.join(', ')})`;
  const condParts = compileBoolExpr(condition, env).filter((p) => p !== '');
  const allBody = [bodyRel, ...condParts];
  return `${head} <- ${allBody.join(', ')}`;
}

// ── Query compilation ───────────────────────────────────────────────

/** Resolved relation info for the compiler. */
interface ResolvedRelation {
  name: string;
  def: RelationDef;
  alias?: string;
}

function resolveRelations(
  rels: Array<RelationDef | RelationRef>,
): ResolvedRelation[] {
  return rels.map((r) => {
    if (r instanceof RelationDef) {
      return { name: r.relationName, def: r, alias: undefined };
    }
    // RelationRef - need to find the def
    return {
      name: r.relationName,
      def: { relationName: r.relationName, columns: r.schema.columns.map((c) => c.name), columnTypes: {} } as unknown as RelationDef,
      alias: r.alias,
    };
  });
}

function hasOr(expr: BoolExpr): boolean {
  if (isOr(expr)) return true;
  if (isAnd(expr)) return hasOr(expr.left) || hasOr(expr.right);
  if (isNot(expr)) return hasOr(expr.operand);
  return false;
}

function processJoinCondition(condition: BoolExpr, env: VarEnv): void {
  if (isComparison(condition) && condition.op === '=') {
    if (isColumn(condition.left) && isColumn(condition.right)) {
      env.unify(condition.left, condition.right);
      return;
    }
  }
  if (isAnd(condition)) {
    processJoinCondition(condition.left, env);
    processJoinCondition(condition.right, env);
  }
}

export interface QueryOptions {
  /** Columns/relations to select. */
  select: Array<RelationDef | Expr>;
  /** Relations to join. */
  join?: Array<RelationDef | RelationRef>;
  /** Join condition (BoolExpr). */
  on?: BoolExpr;
  /** Where filter (BoolExpr). */
  where?: BoolExpr;
  /** Order by expression. */
  orderBy?: Expr;
  /** Limit number of rows. */
  limit?: number;
  /** Offset for pagination. */
  offset?: number;
  /** Computed columns: alias -> Expr. */
  computed?: Record<string, Expr>;
}

/**
 * Compile a query to IQL.
 * Returns a single string, or an array of strings if OR conditions require splitting.
 */
export function compileQuery(opts: QueryOptions): string | string[] {
  const env = new VarEnv();

  const allRelations: ResolvedRelation[] = [];
  if (opts.join) {
    for (const r of opts.join) {
      if (r instanceof RelationDef) {
        allRelations.push({ name: r.relationName, def: r, alias: undefined });
      } else {
        allRelations.push({
          name: r.relationName,
          def: {
            relationName: r.relationName,
            columns: r.schema.columns.map((c) => c.name),
            columnTypes: {},
          } as unknown as RelationDef,
          alias: r.alias,
        });
      }
    }
  }

  // Process join conditions first
  if (opts.on) {
    processJoinCondition(opts.on, env);
  }

  // Process where conditions
  let whereParts: string[] = [];
  let orBranches: string[][] | undefined;
  if (opts.where) {
    if (hasOr(opts.where)) {
      orBranches = compileOrBranches(opts.where, env);
    } else {
      whereParts = compileBoolExpr(opts.where, env).filter((p) => p !== '');
    }
  }

  const hasAgg = opts.select.some((s) => isAggExpr(s as Expr));
  const computed = opts.computed ?? {};
  const hasComputedAgg = Object.values(computed).some((v) => isAggExpr(v as Expr));

  if (hasAgg || hasComputedAgg) {
    return compileAggQuery(
      opts.select,
      env,
      allRelations,
      whereParts,
      orBranches,
      opts.orderBy,
      opts.limit,
      opts.offset,
      computed,
    );
  }

  // Simple query (no aggregations)
  const headParts: string[] = [];
  const bodyAtoms: string[] = [];

  // Handle full relation selects
  const fullRelations: ResolvedRelation[] = [];
  for (const s of opts.select) {
    if (s instanceof RelationDef) {
      fullRelations.push({ name: s.relationName, def: s, alias: undefined });
    }
  }

  if (fullRelations.length > 0) {
    for (const { name, def, alias } of fullRelations) {
      for (const col of def.columns) {
        const astCol = astColumn(name, col, alias);
        headParts.push(env.getVar(astCol));
      }
      if (!allRelations.some((r) => r.name === name && r.alias === alias)) {
        allRelations.push({ name, def, alias });
      }
    }
  }

  // Add individual selected columns to head
  for (const s of opts.select) {
    if (!(s instanceof RelationDef) && isColumn(s as Expr)) {
      headParts.push(env.getVar(s as Column));
    } else if (!(s instanceof RelationDef) && !isColumn(s as Expr) && '_tag' in (s as Expr)) {
      headParts.push(compileExpr(s as Expr, env));
    }
  }

  // Add computed columns
  for (const [, expr] of Object.entries(computed)) {
    headParts.push(compileExpr(expr, env));
  }

  // Handle order_by
  if (opts.orderBy !== undefined) {
    if (isOrderedColumn(opts.orderBy)) {
      const orderVar = compileExpr(opts.orderBy.column, env);
      const suffix = opts.orderBy.descending ? ':desc' : ':asc';
      for (let i = 0; i < headParts.length; i++) {
        if (headParts[i] === orderVar) {
          headParts[i] = `${orderVar}${suffix}`;
          break;
        }
      }
    } else if (isColumn(opts.orderBy)) {
      const orderVar = env.getVar(opts.orderBy);
      for (let i = 0; i < headParts.length; i++) {
        if (headParts[i] === orderVar) {
          headParts[i] = `${orderVar}:asc`;
          break;
        }
      }
    }
  }

  // Build body atoms for each relation
  for (const { name, def, alias } of allRelations) {
    const cols = def.columns;
    const atomParts = cols.map((col) => {
      const astCol = astColumn(name, col, alias);
      return env.lookup(astCol) ?? '_';
    });
    bodyAtoms.push(`${name}(${atomParts.join(', ')})`);
  }

  // Combine body
  const allBody = [...bodyAtoms, ...whereParts];
  if (opts.limit !== undefined) {
    if (opts.offset !== undefined) {
      allBody.push(`limit(${opts.limit}, ${opts.offset})`);
    } else {
      allBody.push(`limit(${opts.limit})`);
    }
  }

  const headStr = headParts.join(', ');

  if (orBranches !== undefined) {
    return orBranches.map((branchParts) => {
      const filtered = branchParts.filter((p) => p !== '');
      const branchBody = [...bodyAtoms, ...filtered];
      if (opts.limit !== undefined) {
        if (opts.offset !== undefined) {
          branchBody.push(`limit(${opts.limit}, ${opts.offset})`);
        } else {
          branchBody.push(`limit(${opts.limit})`);
        }
      }
      return `?${headStr} <- ${branchBody.join(', ')}`;
    });
  }

  if (allBody.length > 0) {
    return `?${headStr} <- ${allBody.join(', ')}`;
  }
  return `?${headStr}`;
}

function compileAggQuery(
  select: Array<RelationDef | Expr>,
  env: VarEnv,
  allRelations: ResolvedRelation[],
  whereParts: string[],
  orBranches: string[][] | undefined,
  orderBy: Expr | undefined,
  limit: number | undefined,
  offset: number | undefined,
  computed: Record<string, Expr>,
): string {
  const headParts: string[] = [];
  const aggParts: string[] = [];

  for (const s of select) {
    if (s instanceof RelationDef) {
      for (const col of s.columns) {
        const astCol = astColumn(s.relationName, col);
        headParts.push(env.getVar(astCol));
      }
    } else if (isAggExpr(s as Expr)) {
      aggParts.push(compileExpr(s as Expr, env));
    } else if (isColumn(s as Expr)) {
      headParts.push(env.getVar(s as Column));
    }
  }

  for (const [, expr] of Object.entries(computed)) {
    if (isAggExpr(expr)) {
      aggParts.push(compileExpr(expr, env));
    } else {
      headParts.push(compileExpr(expr, env));
    }
  }

  const bodyAtoms: string[] = [];
  for (const { name, def, alias } of allRelations) {
    const cols = def.columns;
    const atomParts = cols.map((col) => {
      const astCol = astColumn(name, col, alias);
      return env.lookup(astCol) ?? '_';
    });
    bodyAtoms.push(`${name}(${atomParts.join(', ')})`);
  }

  const allBody = [...bodyAtoms, ...whereParts];
  if (limit !== undefined) {
    if (offset !== undefined) {
      allBody.push(`limit(${limit}, ${offset})`);
    } else {
      allBody.push(`limit(${limit})`);
    }
  }

  const allHead = [...headParts, ...aggParts];
  const headStr = allHead.join(', ');

  if (allBody.length > 0) {
    return `?${headStr} <- ${allBody.join(', ')}`;
  }
  return `?${headStr}`;
}

// ── Rule compilation ────────────────────────────────────────────────

export interface RuleClause {
  /** Body relations: [name, RelationDef, alias?] */
  relations: Array<{ name: string; def: RelationDef; alias?: string }>;
  /** Head column -> body Expr mapping. */
  selectMap: Record<string, Expr>;
  /** Optional filter condition. */
  condition?: BoolExpr;
}

/** Compile a rule definition to IQL. */
export function compileRule(
  headName: string,
  headColumns: string[],
  clause: RuleClause,
  persistent = true,
): string {
  const env = new VarEnv();

  if (clause.condition) {
    processJoinCondition(clause.condition, env);
  }

  // Build head
  const headParts = headColumns.map((col) => {
    const expr = clause.selectMap[col];
    if (expr !== undefined) {
      return compileExpr(expr, env);
    }
    return columnToVariable(col);
  });

  // Build body atoms
  const bodyAtoms: string[] = [];
  for (const { name, def, alias } of clause.relations) {
    const cols = def.columns;
    const atomParts = cols.map((col) => {
      const astCol = astColumn(name, col, alias);
      return env.lookup(astCol) ?? '_';
    });
    bodyAtoms.push(`${name}(${atomParts.join(', ')})`);
  }

  // Compile filter conditions
  let condParts: string[] = [];
  if (clause.condition) {
    condParts = compileBoolExpr(clause.condition, env).filter((p) => p !== '');
  }

  const allBody = [...bodyAtoms, ...condParts];
  const prefix = persistent ? '+' : '';
  const headStr = `${prefix}${headName}(${headParts.join(', ')})`;

  return `${headStr} <- ${allBody.join(', ')}`;
}
