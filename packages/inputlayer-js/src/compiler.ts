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
import { columnToVariable, snakeToCamel } from './naming.js';
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

  /** A variable named after `base` that no column uses yet. */
  fresh(base: string): string {
    const used = new Set(this.map.values());
    let varName = base;
    for (let n = 2; used.has(varName); n++) varName = `${base}_${n}`;
    this.map.set(`__fresh__.${varName}`, varName);
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

/** One result column: the name the caller sees and the IQL variable that carries it. */
export interface QueryOutput {
  label: string;
  variable: string;
}

/**
 * A compiled query: the IQL programs to run and how to shape the engine's
 * rows into the result the caller asked for.
 *
 * An IQL query has no head, so the engine returns every variable the query
 * binds. The SDK picks the selected columns out of those rows.
 */
export interface QueryPlan {
  /** IQL programs to execute; several when an OR condition splits the query. */
  programs: string[];
  /** Result columns, in select order. */
  outputs: QueryOutput[];
  /**
   * Variables of the query's first atom, in order. The engine names result
   * columns after that relation's schema when the query binds no other
   * variable, and the columns are then these variables by position. Empty
   * for an aggregate query, whose columns are always named by variable.
   */
  goalVars: string[];
  /** Variables of every relation atom; rows from OR branches are deduplicated on them. */
  rowVars: string[];
  /** Ordering and pagination applied after an OR split merges its branches. */
  merge?: {
    order?: { variable: string; descending: boolean };
    limit?: number;
    offset?: number;
  };
}

/**
 * Name of the program-local rule an aggregate query evaluates. A rule
 * defined in the same program as a query lives only for that request, so
 * nothing is left in the session.
 */
const AGG_RULE = 'il_sdk_agg';
/** Program-local rule holding the union of an OR split, aggregated by AGG_RULE. */
const AGG_SOURCE_RULE = 'il_sdk_agg_src';

/**
 * Compile a query to IQL.
 * Returns a single program, or an array of programs if OR conditions require splitting.
 */
export function compileQuery(opts: QueryOptions): string | string[] {
  const { programs } = compileQueryPlan(opts);
  return programs.length === 1 ? programs[0] : programs;
}

/** Compile a query to the programs to run and the shape of its result. */
export function compileQueryPlan(opts: QueryOptions): QueryPlan {
  const env = new VarEnv();
  const relations = resolveRelations(opts.join ?? []);

  // Selecting a whole relation joins it.
  for (const s of opts.select) {
    if (s instanceof RelationDef && !relations.some((r) => r.name === s.relationName && r.alias === undefined)) {
      relations.push({ name: s.relationName, def: s, alias: undefined });
    }
  }
  if (relations.length === 0) {
    throw new Error('A query needs at least one relation: select a relation or pass it in join.');
  }

  // Join conditions first, so unified columns share a variable. Their
  // other comparisons (e1.id != e2.id) filter like a where condition.
  let onParts: string[] = [];
  if (opts.on) {
    if (hasOr(opts.on)) {
      throw new Error('OR is not supported in a join condition; put it in where');
    }
    processJoinCondition(opts.on, env);
    onParts = compileBoolExpr(opts.on, env).filter((p) => p !== '');
  }

  let whereParts: string[] = [];
  let orBranches: string[][] | undefined;
  if (opts.where) {
    if (hasOr(opts.where)) {
      orBranches = compileOrBranches(opts.where, env).map((b) => b.filter((p) => p !== ''));
    } else {
      whereParts = compileBoolExpr(opts.where, env).filter((p) => p !== '');
    }
  }

  const computed = opts.computed ?? {};
  const isAgg =
    opts.select.some((s) => !(s instanceof RelationDef) && isAggExpr(s)) ||
    Object.values(computed).some((v) => isAggExpr(v));

  const order = resolveOrder(opts.orderBy);
  if (order !== undefined && !isAgg) {
    // The engine sorts on a variable of the query's first atom only.
    const i = relations.findIndex(
      (r) => r.name === order.column.relation && r.alias === order.column.refAlias,
    );
    if (i > 0) relations.unshift(...relations.splice(i, 1));
  }

  // Every column of every atom gets a variable, so the rows the engine
  // returns always line up with the atoms.
  const atomVars = relations.map(({ name, def, alias }) =>
    def.columns.map((col) => env.getVar(astColumn(name, col, alias))),
  );
  const rowVars = [...new Set(atomVars.flat())];
  const atoms = relations.map((r, i) => `${r.name}(${atomVars[i].join(', ')})`);

  const shape = {
    env,
    relations,
    atomVars,
    atoms,
    rowVars,
    whereParts: [...onParts, ...whereParts],
    orBranches: orBranches?.map((branch) => [...onParts, ...branch]),
    order,
    limit: opts.limit,
    offset: opts.offset,
  };
  return isAgg
    ? compileAggPlan(shape, opts.select, computed)
    : compilePlainPlan(shape, opts.select, computed);
}

interface QueryShape {
  env: VarEnv;
  relations: ResolvedRelation[];
  atomVars: string[][];
  atoms: string[];
  rowVars: string[];
  whereParts: string[];
  orBranches: string[][] | undefined;
  order: { column: Column; descending: boolean } | undefined;
  limit: number | undefined;
  offset: number | undefined;
}

function resolveOrder(orderBy: Expr | undefined): { column: Column; descending: boolean } | undefined {
  if (orderBy === undefined) return undefined;
  if (isOrderedColumn(orderBy) && isColumn(orderBy.column)) {
    return { column: orderBy.column, descending: orderBy.descending };
  }
  if (isColumn(orderBy)) {
    return { column: orderBy, descending: false };
  }
  throw new TypeError('orderBy must be a column, optionally with .asc() or .desc()');
}

function limitAtom(limit: number | undefined, offset: number | undefined): string[] {
  if (limit === undefined) return [];
  return [offset !== undefined ? `limit(${limit}, ${offset})` : `limit(${limit})`];
}

/** Give each label a unique name, suffixing repeats with _2, _3, ... */
function uniqueLabels(outputs: QueryOutput[]): QueryOutput[] {
  const seen = new Set<string>();
  return outputs.map((o) => {
    let label = o.label;
    for (let n = 2; seen.has(label); n++) label = `${o.label}_${n}`;
    seen.add(label);
    return { ...o, label };
  });
}

function compilePlainPlan(
  shape: QueryShape,
  select: Array<RelationDef | Expr>,
  computed: Record<string, Expr>,
): QueryPlan {
  const { env, relations, atomVars, atoms, order } = shape;
  const outputs: QueryOutput[] = [];
  const bindings: string[] = [];

  for (const s of select) {
    if (s instanceof RelationDef) {
      for (const col of s.columns) {
        outputs.push({ label: col, variable: env.getVar(astColumn(s.relationName, col)) });
      }
    } else if (isColumn(s)) {
      const v = env.getVar(s);
      outputs.push({ label: v, variable: v });
    } else {
      const v = env.fresh('Expr');
      bindings.push(`${v} = ${compileExpr(s, env)}`);
      outputs.push({ label: v, variable: v });
    }
  }
  for (const [alias, expr] of Object.entries(computed)) {
    const label = columnToVariable(alias);
    const v = env.fresh(label);
    bindings.push(`${v} = ${compileExpr(expr, env)}`);
    outputs.push({ label, variable: v });
  }

  // The ordered column's relation was moved first, so its variable is in
  // the first atom, where the engine reads :asc/:desc annotations.
  let orderVar: string | undefined;
  const goal = [...atomVars[0]];
  if (order !== undefined) {
    orderVar = env.getVar(order.column);
    const i = goal.indexOf(orderVar);
    if (i < 0) {
      throw new Error(`orderBy column ${order.column.name} is not in a joined relation`);
    }
    goal[i] = `${orderVar}${order.descending ? ':desc' : ':asc'}`;
  }
  const head = [`${relations[0].name}(${goal.join(', ')})`, ...atoms.slice(1), ...bindings];

  const plan = {
    outputs: uniqueLabels(outputs),
    goalVars: atomVars[0],
    rowVars: shape.rowVars,
  };

  if (shape.orBranches === undefined) {
    const body = [...head, ...shape.whereParts, ...limitAtom(shape.limit, shape.offset)];
    return { ...plan, programs: [`?${body.join(', ')}`] };
  }

  // Each branch returns its own first limit + offset rows in order; the
  // merged union is then sorted and paginated client-side.
  const branchLimit = shape.limit !== undefined ? shape.limit + (shape.offset ?? 0) : undefined;
  const programs = shape.orBranches.map(
    (branch) => `?${[...head, ...branch, ...limitAtom(branchLimit, undefined)].join(', ')}`,
  );
  return {
    ...plan,
    programs,
    merge: {
      order: orderVar !== undefined ? { variable: orderVar, descending: order!.descending } : undefined,
      limit: shape.limit,
      offset: shape.offset,
    },
  };
}

/** Result variables an aggregate contributes to the rule head, in order. */
function aggOutputVars(agg: AggExpr, env: VarEnv): string[] {
  const varOf = (e: Expr): string => (isColumn(e) ? env.getVar(e) : 'Value');
  if (agg.orderColumn !== undefined) {
    // top_k, top_k_threshold, within_radius: one column per passthrough, then the ordered column.
    return [...agg.passthrough.map(varOf), varOf(agg.orderColumn)];
  }
  const fn = snakeToCamel(agg.func);
  return [agg.column !== undefined ? `${fn}${varOf(agg.column)}` : fn];
}

function compileAggPlan(
  shape: QueryShape,
  select: Array<RelationDef | Expr>,
  computed: Record<string, Expr>,
): QueryPlan {
  const { env, atoms, order } = shape;
  const head: string[] = [];
  const outputs: QueryOutput[] = [];
  const bindings: string[] = [];
  const boundVars: string[] = [];

  for (const s of select) {
    if (s instanceof RelationDef) {
      for (const col of s.columns) {
        const v = env.getVar(astColumn(s.relationName, col));
        head.push(v);
        outputs.push({ label: col, variable: v });
      }
    } else if (isAggExpr(s)) {
      head.push(compileAggExpr(s, env));
      for (const v of aggOutputVars(s, env)) outputs.push({ label: v, variable: v });
    } else if (isColumn(s)) {
      const v = env.getVar(s);
      head.push(v);
      outputs.push({ label: v, variable: v });
    } else {
      const v = env.fresh('Expr');
      bindings.push(`${v} = ${compileExpr(s, env)}`);
      boundVars.push(v);
      head.push(v);
      outputs.push({ label: v, variable: v });
    }
  }
  for (const [alias, expr] of Object.entries(computed)) {
    const label = columnToVariable(alias);
    if (isAggExpr(expr)) {
      head.push(compileAggExpr(expr, env));
      const vars = aggOutputVars(expr, env);
      for (const v of vars) outputs.push({ label: vars.length === 1 ? label : v, variable: v });
    } else {
      const v = env.fresh(label);
      bindings.push(`${v} = ${compileExpr(expr, env)}`);
      boundVars.push(v);
      head.push(v);
      outputs.push({ label, variable: v });
    }
  }

  // The query atom names the rule's columns; they only need to be distinct.
  const used = new Set<string>();
  const queryVars = outputs.map((o) => {
    let v = o.variable;
    for (let n = 2; used.has(v); n++) v = `${o.variable}_${n}`;
    used.add(v);
    return v;
  });
  const queryArgs = [...queryVars];
  if (order !== undefined) {
    const orderVar = env.getVar(order.column);
    const i = outputs.findIndex((o) => o.variable === orderVar);
    if (i < 0) {
      throw new Error(`In an aggregate query, orderBy must be a selected column (${order.column.name} is not)`);
    }
    queryArgs[i] = `${queryVars[i]}${order.descending ? ':desc' : ':asc'}`;
  }

  let rules: string[];
  if (shape.orBranches === undefined) {
    const body = [...atoms, ...bindings, ...shape.whereParts];
    rules = [`${AGG_RULE}(${head.join(', ')}) <- ${body.join(', ')}`];
  } else {
    // Aggregate over the union of the branches: collect it in a source rule first.
    const src = `${AGG_SOURCE_RULE}(${[...shape.rowVars, ...boundVars].join(', ')})`;
    rules = [
      ...shape.orBranches.map((branch) => `${src} <- ${[...atoms, ...bindings, ...branch].join(', ')}`),
      `${AGG_RULE}(${head.join(', ')}) <- ${src}`,
    ];
  }
  const query = `?${[`${AGG_RULE}(${queryArgs.join(', ')})`, ...limitAtom(shape.limit, shape.offset)].join(', ')}`;

  return {
    programs: [[...rules, query].join('\n')],
    outputs: uniqueLabels(outputs.map((o, i) => ({ label: o.label, variable: queryVars[i] }))),
    goalVars: [],
    rowVars: [],
  };
}

/**
 * Indexes of `variables` in an engine result. Columns are named by variable,
 * except that a query binding nothing beyond its first atom gets the
 * relation's schema column names; those columns are the first atom's
 * variables by position.
 */
export function resultColumnIndexes(plan: QueryPlan, columns: string[], variables: string[]): number[] {
  const { goalVars } = plan;
  const byName = goalVars.length === 0 || goalVars.every((v, i) => columns[i] === v);
  return variables.map((v) => {
    const i = byName ? columns.indexOf(v) : goalVars.indexOf(v);
    if (i < 0) {
      throw new Error(`Query result has no column for ${v} (columns: ${columns.join(', ')})`);
    }
    return i;
  });
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
