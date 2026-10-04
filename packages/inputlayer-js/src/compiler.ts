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
  type InExpr,
  type NegatedIn,
  type AnyExpr,
  type Literal,
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
  isAnyExpr,
  column as astColumn,
  comparison,
  literal,
  not as astNot,
  anyExpr,
} from './ast.js';
import { CompileError } from './errors.js';
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
    // Keep a variable either side already has, so text compiled before the
    // unification still names the same variable.
    const before = this.map.get(this.find(keyA)) ?? this.map.get(this.find(keyB));
    this.union(keyA, keyB);
    const root = this.find(keyA);
    const existing = this.map.get(root) ?? before;
    if (existing !== undefined) {
      this.map.set(root, existing);
      return existing;
    }

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

  /** Columns some expression refers to; an any() atom gives them a variable instead of `_`. */
  private referenced = new Set<string>();

  reference(keys: Iterable<string>): void {
    for (const k of keys) this.referenced.add(k);
  }

  isReferenced(col: Column): boolean {
    return this.referenced.has(`${col.refAlias ?? col.relation}.${col.name}`);
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
  if (agg.func === 'count' && agg.column === undefined && agg.orderColumn === undefined) {
    throw new Error(
      'count() without a column has no variable to count; the query compiler ' +
        'counts the first column of the first relation (count<V>)',
    );
  }
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

/**
 * A column-less `count()` counts the first column of the first relation:
 * `count<V>` counts body bindings, and the engine rejects `count<>`.
 */
function withCountColumn(expr: Expr, first: Column): Expr {
  if (isAggExpr(expr) && expr.func === 'count' && expr.column === undefined && expr.orderColumn === undefined) {
    return { ...expr, column: first } as AggExpr;
  }
  return expr;
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
  if (isInExpr(expr) || isNegatedIn(expr)) {
    return [compileIn(expr, isNegatedIn(expr), env)];
  }
  if (isMatchExpr(expr)) {
    return [compileMatch(expr, env)];
  }
  if (isAnyExpr(expr)) {
    const { atom, extra } = compileAny(expr, env);
    return [atom, ...extra];
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
  const left = compileOperand(comp.left, comp.right, env);
  const right = compileOperand(comp.right, comp.left, env);
  return `${left} ${comp.op} ${right}`;
}

/** A literal compared with a typed column is compiled as a value of that column's type. */
function compileOperand(expr: Expr, other: Expr, env: VarEnv): string {
  if (isLiteral(expr) && isColumn(other)) return compileValue(expr.value, other.type);
  return compileExpr(expr, env);
}

/**
 * `a.in(b)` becomes an atom of b's relation with a's variable in b's column
 * and `_` elsewhere: `manager(_, EmployeeId)`. notIn negates the atom.
 */
function compileIn(expr: InExpr | NegatedIn, negated: boolean, env: VarEnv): string {
  const target = expr.targetColumn;
  if (!isColumn(target) || expr.targetColumns === undefined) {
    throw new Error(
      'in()/notIn() needs a column taken from a relation definition, ' +
        'e.g. Employee.col("id").in(Manager.col("employeeId"))',
    );
  }
  if (!expr.targetColumns.includes(target.name)) {
    throw new Error(
      `in()/notIn(): column '${target.name}' does not exist on relation '${target.relation}'. ` +
        `Available: ${expr.targetColumns.join(', ')}`,
    );
  }
  const value = compileExpr(expr.column, env);
  const args = expr.targetColumns.map((c) => (c === target.name ? value : '_'));
  return `${negated ? '!' : ''}${target.relation}(${args.join(', ')})`;
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

// ── Bodies: any(), NOT(any()) and canonical order (R-NEG) ─────────

/**
 * How a body binds a negated constant that shares no variable with a
 * positive atom (R-NEG): through a program-local session fact on the read
 * path, through a persistent row staged around a write program (an update
 * body cannot see session facts), and not at all in a rule.
 */
type BodyMode = 'query' | 'write' | 'rule';

/** A constant row the body reads through `relation(k)`. */
export interface ConstRow {
  relation: string;
  /** The IQL literal. */
  literal: string;
}

/** Relation suffix of an IQL column type, for the typed constant relations. */
const CONST_SUFFIX: Record<string, string> = { string: 's', int: 'i', timestamp: 'i', float: 'f', bool: 'b' };

/** Read path: a session fact in the same program as the query. */
const QUERY_CONST = 'il_const_';
/** Write path: a persistent row inserted before the guard and deleted last. */
const WRITE_CONST = 'il_txn_const_';

class BodyContext {
  private n = 0;
  readonly consts: ConstRow[] = [];

  constructor(readonly mode: BodyMode) {}

  /** A key for an atom's own columns that no relation or other atom uses. */
  alias(): string {
    return `il_any_${++this.n}`;
  }

  addConst(row: ConstRow): void {
    if (!this.consts.some((c) => c.relation === row.relation && c.literal === row.literal)) {
      this.consts.push(row);
    }
  }
}

/** Key of the atom a column belongs to: its alias, else its relation. */
function atomKey(col: Column): string {
  return `${col.refAlias ?? col.relation}`;
}

/** The conjuncts of an AND tree, in order. */
export function flattenAnd(expr: BoolExpr): BoolExpr[] {
  return isAnd(expr) ? [...flattenAnd(expr.left), ...flattenAnd(expr.right)] : [expr];
}

/** The branches of a top-level OR tree, in order. */
function splitOr(expr: BoolExpr): BoolExpr[] {
  return isOr(expr) ? [...splitOr(expr.left), ...splitOr(expr.right)] : [expr];
}

function isNegatedAny(e: BoolExpr): e is BoolExpr & { operand: AnyExpr } {
  return isNot(e) && isAnyExpr(e.operand);
}

/** `relation.column` keys of every column an expression tree refers to. */
function collectColumnKeys(nodes: ReadonlyArray<Expr | BoolExpr>, out = new Set<string>()): Set<string> {
  const visit = (n: Expr | BoolExpr | undefined): void => {
    if (n === undefined) return;
    if (isColumn(n as Expr)) {
      const c = n as Column;
      out.add(`${atomKey(c)}.${c.name}`);
      return;
    }
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const o = n as any;
    if (isAnyExpr(n as BoolExpr)) {
      for (const b of Object.values((n as AnyExpr).bindings)) visit(b);
      return;
    }
    for (const field of ['left', 'right', 'operand', 'column', 'targetColumn', 'orderColumn']) visit(o[field]);
    for (const field of ['args', 'passthrough']) for (const a of o[field] ?? []) visit(a);
    if (o.bindings) for (const b of Object.values(o.bindings)) visit(b as Expr);
  };
  for (const n of nodes) visit(n);
  return out;
}

/**
 * Prepare a body's conjuncts for compilation (R-NEG):
 *
 * - each any() atom gets a key for its own columns; a positive atom of a
 *   relation the body does not otherwise use keeps the relation's name, so
 *   `R.col("x")` refers to its columns;
 * - a negated atom bound only to constants is made to share a variable:
 *   with a positive atom's column already equal to the constant (an
 *   equality in the body, or the same constant in a positive any() atom,
 *   which then binds a variable to it); otherwise through a constant row,
 *   per `ctx.mode`. Each rewrite keeps the body's meaning.
 * - a negated atom bound to a column no positive atom has is refused.
 *
 * `contextKeys` are the keys of the body's relation atoms.
 */
function normalizeBody(conjuncts: BoolExpr[], contextKeys: ReadonlySet<string>, ctx: BodyContext): BoolExpr[] {
  const positiveKeys = new Set(contextKeys);
  const out = conjuncts.map((c) => {
    if (isAnyExpr(c)) {
      const alias = positiveKeys.has(c.relation) ? ctx.alias() : undefined;
      positiveKeys.add(alias ?? c.relation);
      return anyExpr(c.relation, c.columns, c.columnTypes, c.bindings, alias);
    }
    if (isNegatedAny(c)) {
      const a = c.operand;
      return astNot(anyExpr(a.relation, a.columns, a.columnTypes, a.bindings, ctx.alias()));
    }
    return c;
  });

  for (let i = 0; i < out.length; i++) {
    const c = out[i];
    if (!isNegatedAny(c)) continue;
    const neg = c.operand;
    const entries = Object.entries(neg.bindings);
    for (const [col, b] of entries) {
      if (isColumn(b)) {
        if (!positiveKeys.has(atomKey(b))) {
          throw new CompileError(
            `NOT(any(${neg.relation})): column '${col}' is bound to ${b.relation}.${b.name}, which no positive atom of this body has`,
            'Bind it to a column of a relation the body joins, or to a value',
          );
        }
      } else if (!isLiteral(b)) {
        throw new CompileError(
          `NOT(any(${neg.relation})): column '${col}' is bound to an expression`,
          'Bind a negated column to a value or to a column of a positive atom',
        );
      }
    }
    if (entries.some(([, b]) => isColumn(b))) continue;
    const first = entries[0];
    if (first === undefined) {
      throw new CompileError(
        `NOT(any(${neg.relation})) binds no column, so it shares no variable with a positive atom`,
        'Bind at least one column to a value or to a column of a positive atom',
      );
    }
    const [col, lit] = first as [string, Literal];
    const shared = sharedColumn(compileValue(lit.value, neg.columnTypes[col]), lit.value, neg.columnTypes[col], out, positiveKeys, ctx, neg.relation);
    out[i] = astNot(anyExpr(neg.relation, neg.columns, neg.columnTypes, { ...neg.bindings, [col]: shared }, neg.alias));
  }

  if (ctx.mode === 'write') {
    // A guard has no relation atoms of its own: a condition may only use
    // columns of its positive any() atoms.
    for (const c of out) {
      if (isAnyExpr(c) || isNegatedAny(c)) continue;
      if (isOr(c)) {
        throw new CompileError('A guard cannot hold OR', 'Write one guarded program per alternative');
      }
      for (const key of collectColumnKeys([c])) {
        if (!positiveKeys.has(key.slice(0, key.lastIndexOf('.')))) {
          throw new CompileError(
            `Guard condition uses column ${key}, which no positive any() of the guard binds`,
            'Add any(Relation, {...}) for that relation to the guard',
          );
        }
      }
    }
  }
  return out;
}

/**
 * A column of a positive atom equal to the constant `text`, adding to `out`
 * what makes it so.
 */
function sharedColumn(
  text: string,
  value: unknown,
  type: string,
  out: BoolExpr[],
  positiveKeys: Set<string>,
  ctx: BodyContext,
  negated: string,
): Column {
  // An equality already pins a positive column to the constant.
  for (const c of out) {
    if (!isComparison(c) || c.op !== '=') continue;
    for (const [a, b] of [[c.left, c.right], [c.right, c.left]]) {
      if (isColumn(a) && isLiteral(b) && compileValue(b.value, a.type) === text && positiveKeys.has(atomKey(a))) {
        return a;
      }
    }
  }
  // A positive any() atom carries the constant: bind a variable to it there.
  for (let j = 0; j < out.length; j++) {
    const p = out[j];
    if (!isAnyExpr(p)) continue;
    for (const [col, b] of Object.entries(p.bindings)) {
      if (!isLiteral(b) || compileValue(b.value, p.columnTypes[col]) !== text) continue;
      const { [col]: _, ...rest } = p.bindings;
      out[j] = anyExpr(p.relation, p.columns, p.columnTypes, rest, p.alias);
      const own = astColumn(p.relation, col, p.alias);
      out.push(comparison('=', own, b));
      return own;
    }
  }
  if (ctx.mode === 'rule') {
    throw new CompileError(
      `NOT(any(${negated})) binds only constants, so it shares no variable with a positive atom`,
      `Bind the negated column to a column of a joined relation, e.g. NOT(any(${negated}, { col: r.col("col") })) with r.col("col").eq(value)`,
    );
  }
  const suffix = CONST_SUFFIX[type];
  if (suffix === undefined) {
    throw new CompileError(
      `NOT(any(${negated})): a ${type} constant cannot be bound through a constant row`,
      'Bind the negated column to a column of a positive atom',
    );
  }
  const relation = `${ctx.mode === 'query' ? QUERY_CONST : WRITE_CONST}${suffix}`;
  const alias = ctx.alias();
  positiveKeys.add(alias);
  ctx.addConst({ relation, literal: text });
  const own = astColumn(relation, 'k', alias);
  out.push(anyExpr(relation, ['k'], { k: type }, {}, alias));
  out.push(comparison('=', own, literal(value)));
  return own;
}

/** One any() atom: bound columns take their value, referenced ones a variable, the rest `_`. */
function compileAny(expr: AnyExpr, env: VarEnv): { atom: string; extra: string[] } {
  const extra: string[] = [];
  const args = expr.columns.map((col) => {
    const own = astColumn(expr.relation, col, expr.alias);
    const b = expr.bindings[col];
    if (b === undefined) return env.isReferenced(own) ? env.getVar(own) : '_';
    if (isColumn(b)) return env.unify(b, own);
    if (isLiteral(b) && !env.isReferenced(own)) return compileValue(b.value, expr.columnTypes[col]);
    const v = env.getVar(own);
    extra.push(`${v} = ${compileExpr(b, env)}`);
    return v;
  });
  return { atom: `${expr.relation}(${args.join(', ')})`, extra };
}

/**
 * A body's compiled parts, in the canonical order: positive atoms, then
 * negated atoms, then comparisons and equalities.
 */
function compileBody(conjuncts: BoolExpr[], env: VarEnv): string[] {
  const positive: string[] = [];
  const negated: string[] = [];
  const conditions: string[] = [];
  for (const c of conjuncts) {
    if (isAnyExpr(c)) {
      const { atom, extra } = compileAny(c, env);
      positive.push(atom);
      conditions.push(...extra);
    } else if (isNegatedAny(c)) {
      negated.push(`!${compileAny(c.operand, env).atom}`);
    } else if (isInExpr(c)) {
      positive.push(compileIn(c, false, env));
    } else if (isNegatedIn(c)) {
      negated.push(compileIn(c, true, env));
    } else if (isMatchExpr(c)) {
      (c.negated ? negated : positive).push(compileMatch(c, env));
    } else {
      conditions.push(...compileBoolExpr(c, env).filter((p) => p !== ''));
    }
  }
  return [...positive, ...negated, ...conditions];
}

/** A compiled write guard: its body and the constant rows staged around it. */
export interface CompiledGuard {
  /** Body text; empty when there is no condition. */
  body: string;
  /** Rows to insert before the guard and delete after it (`il_txn_const_<type>`). */
  constRows: ConstRow[];
}

/**
 * Compile guard conditions (`when`, `unless`) to an update body. Conditions
 * are any()/NOT(any()) atoms and comparisons over the columns of those atoms.
 */
export function compileGuard(conditions: BoolExpr[]): CompiledGuard {
  const env = new VarEnv();
  const ctx = new BodyContext('write');
  const conjuncts = normalizeBody(conditions.flatMap(flattenAnd), new Set(), ctx);
  env.reference(collectColumnKeys(conjuncts));
  return { body: compileBody(conjuncts, env).join(', '), constRows: ctx.consts };
}

// ── Schema compilation ──────────────────────────────────────────────

/** Compile a relation definition to a schema statement: +employee(id: int, name: string, ...) */
export function compileSchema(rel: RelationDef): string {
  const name = rel.relationName;
  const cols = rel.columns;
  const colTypes = rel.columnTypes;
  const parts = cols.map((c) => {
    // Timestamps are stored as int Unix milliseconds. Convert type notation:
    // vector[3] -> vector(3), vector_int8[3] -> vector_int8(3).
    const type = colTypes[c] === 'timestamp' ? 'int' : colTypes[c].replace(/\[(\d+)\]/, '($1)');
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
  const values = rel.columns.map((c) => compileValue(fact[c], rel.columnTypes[c]));
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
    const values = rel.columns.map((c) => compileValue(fact[c], rel.columnTypes[c]));
    return `(${values.join(', ')})`;
  });
  const prefix = persistent ? '+' : '';
  return `${prefix}${name}[${tuples.join(', ')}]`;
}

// ── Delete compilation ──────────────────────────────────────────────

/** Compile a single fact deletion: -employee(1, "Alice", ...) */
export function compileDelete(rel: RelationDef, fact: Fact): string {
  const name = rel.relationName;
  const values = rel.columns.map((c) => compileValue(fact[c], rel.columnTypes[c]));
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
  const conjuncts = normalizeBody(flattenAnd(condition), new Set([name]), new BodyContext('rule'));
  env.reference(collectColumnKeys(conjuncts));
  const allBody = [bodyRel, ...compileBody(conjuncts, env)];
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
  /**
   * The one statement `.debug` takes: the query of the first program, or an
   * aggregate's rule. `.debug` and `.why` see no rule defined beside it, so
   * an OR split shows its first branch only.
   */
  debug: string;
  /**
   * The rule `.why` takes, and the variable of each of its result columns,
   * by position.
   */
  why: { statement: string; columns: string[] };
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
  /**
   * Ordering and pagination of the result. The engine applies them to a
   * single program; the SDK applies them to merged OR branches and to `.why`.
   */
  page: {
    order?: { variable: string; descending: boolean };
    limit?: number;
    offset?: number;
  };
  /**
   * Session facts the programs state before the query, binding a negated
   * constant (R-NEG). `debug` and `why` statements cannot carry them.
   */
  constFacts?: string[];
}

/**
 * Name of the program-local rule an aggregate query evaluates. A rule
 * defined in the same program as a query lives only for that request, so
 * nothing is left in the session.
 */
const AGG_RULE = 'il_sdk_agg';
/** Program-local rule holding the union of an OR split, aggregated by AGG_RULE. */
const AGG_SOURCE_RULE = 'il_sdk_agg_src';
/** Name of the rule `.why` evaluates for a query without aggregates. */
const WHY_RULE = 'il_sdk_why';

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
  if (opts.on) {
    if (hasOr(opts.on)) {
      throw new Error('OR is not supported in a join condition; put it in where');
    }
    processJoinCondition(opts.on, env);
  }
  const onConjuncts = opts.on ? flattenAnd(opts.on) : [];

  // Each body (one, or one per OR branch) is normalized for any() atoms
  // and compiled in the canonical order.
  const ctx = new BodyContext('query');
  const contextKeys = new Set(relations.map((r) => r.alias ?? r.name));
  const branches = (opts.where && hasOr(opts.where) ? splitOr(opts.where) : [opts.where]).map((b) =>
    normalizeBody([...onConjuncts, ...(b ? flattenAnd(b) : [])], contextKeys, ctx),
  );
  env.reference(
    collectColumnKeys([
      ...branches.flat(),
      ...opts.select.filter((x): x is Expr => !(x instanceof RelationDef)),
      ...Object.values(opts.computed ?? {}),
      ...(opts.orderBy ? [opts.orderBy] : []),
    ]),
  );
  const compiled = branches.map((b) => compileBody(b, env));
  const whereParts = compiled.length === 1 ? compiled[0] : [];
  const orBranches = compiled.length > 1 ? compiled : undefined;

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
    whereParts,
    orBranches,
    order,
    limit: opts.limit,
    offset: opts.offset,
  };
  const plan = isAgg
    ? compileAggPlan(shape, opts.select, computed)
    : compilePlainPlan(shape, opts.select, computed);
  if (ctx.consts.length === 0) return plan;
  // Negated constants bind through session facts that live only for the
  // program that states them.
  const facts = ctx.consts.map((c) => `${c.relation}(${c.literal})`);
  return { ...plan, programs: plan.programs.map((p) => [...facts, p].join('\n')), constFacts: facts };
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

  // The why rule keeps every atom variable, so it derives a row for each
  // binding the query returns.
  const whyColumns = [...new Set([...outputs.map((o) => o.variable), ...shape.rowVars])];
  const whyBody = [...atoms, ...bindings, ...(shape.orBranches?.[0] ?? shape.whereParts)];
  const plan = {
    outputs: uniqueLabels(outputs),
    why: { statement: `${WHY_RULE}(${whyColumns.join(', ')}) <- ${whyBody.join(', ')}`, columns: whyColumns },
    goalVars: atomVars[0],
    rowVars: shape.rowVars,
    page: {
      order: orderVar !== undefined ? { variable: orderVar, descending: order!.descending } : undefined,
      limit: shape.limit,
      offset: shape.offset,
    },
  };

  if (shape.orBranches === undefined) {
    const body = [...head, ...shape.whereParts, ...limitAtom(shape.limit, shape.offset)];
    const program = `?${body.join(', ')}`;
    return { ...plan, programs: [program], debug: program };
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
    debug: programs[0],
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
  const first = shape.relations[0];
  const firstColumn = astColumn(first.name, first.def.columns[0], first.alias);
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
      head.push(compileExpr(withCountColumn(s, firstColumn), env));
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
      head.push(compileExpr(withCountColumn(expr, firstColumn), env));
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
  let pageOrder: QueryPlan['page']['order'];
  if (order !== undefined) {
    const orderVar = env.getVar(order.column);
    const i = outputs.findIndex((o) => o.variable === orderVar);
    if (i < 0) {
      throw new Error(`In an aggregate query, orderBy must be a selected column (${order.column.name} is not)`);
    }
    queryArgs[i] = `${queryVars[i]}${order.descending ? ':desc' : ':asc'}`;
    pageOrder = { variable: queryVars[i], descending: order.descending };
  }

  const aggHead = `${AGG_RULE}(${head.join(', ')})`;
  const explain = `${aggHead} <- ${[...atoms, ...bindings, ...(shape.orBranches?.[0] ?? shape.whereParts)].join(', ')}`;
  let rules: string[];
  if (shape.orBranches === undefined) {
    rules = [explain];
  } else {
    // Aggregate over the union of the branches: collect it in a source rule first.
    const src = `${AGG_SOURCE_RULE}(${[...shape.rowVars, ...boundVars].join(', ')})`;
    rules = [
      ...shape.orBranches.map((branch) => `${src} <- ${[...atoms, ...bindings, ...branch].join(', ')}`),
      `${aggHead} <- ${src}`,
    ];
  }
  const query = `?${[`${AGG_RULE}(${queryArgs.join(', ')})`, ...limitAtom(shape.limit, shape.offset)].join(', ')}`;

  return {
    programs: [[...rules, query].join('\n')],
    debug: explain,
    why: { statement: explain, columns: queryVars },
    outputs: uniqueLabels(outputs.map((o, i) => ({ label: o.label, variable: queryVars[i] }))),
    goalVars: [],
    rowVars: [],
    page: { order: pageOrder, limit: shape.limit, offset: shape.offset },
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
  const contextKeys = new Set(clause.relations.map((r) => r.alias ?? r.name));
  const conjuncts = clause.condition
    ? normalizeBody(flattenAnd(clause.condition), contextKeys, new BodyContext('rule'))
    : [];
  env.reference(collectColumnKeys([...conjuncts, ...Object.values(clause.selectMap)]));
  for (const c of conjuncts) processJoinCondition(c, env);

  // Build head. A column-less count() counts the first body column.
  const first = clause.relations[0];
  const firstColumn = first !== undefined ? astColumn(first.name, first.def.columns[0], first.alias) : undefined;
  const headParts = headColumns.map((col) => {
    const expr = clause.selectMap[col];
    if (expr !== undefined) {
      return compileExpr(firstColumn !== undefined ? withCountColumn(expr, firstColumn) : expr, env);
    }
    return columnToVariable(col);
  });

  // Compile filter conditions before the atoms, so a column a condition
  // uses gets its variable in the atom rather than `_`.
  const condParts = compileBody(conjuncts, env);

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

  const allBody = [...bodyAtoms, ...condParts];
  const prefix = persistent ? '+' : '';
  const headStr = `${prefix}${headName}(${headParts.join(', ')})`;

  return `${headStr} <- ${allBody.join(', ')}`;
}
