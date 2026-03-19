/**
 * Internal AST nodes for expression trees compiled to IQL.
 */

// ── Base types ──────────────────────────────────────────────────────

/** Base interface for all expression AST nodes. */
export interface Expr {
  readonly _tag: string;
}

/** Base interface for boolean expression AST nodes (conditions). */
export interface BoolExpr {
  readonly _tag: string;
}

// ── Leaf nodes ──────────────────────────────────────────────────────

/** Reference to a relation column. */
export interface Column extends Expr {
  readonly _tag: 'Column';
  readonly relation: string;
  readonly name: string;
  readonly refAlias?: string;
}

export function column(relation: string, name: string, refAlias?: string): Column {
  return { _tag: 'Column', relation, name, refAlias };
}

/** A constant value. */
export interface Literal extends Expr {
  readonly _tag: 'Literal';
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  readonly value: any;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function literal(value: any): Literal {
  return { _tag: 'Literal', value };
}

// ── Arithmetic ──────────────────────────────────────────────────────

/** Binary arithmetic: +, -, *, /, %. */
export interface Arithmetic extends Expr {
  readonly _tag: 'Arithmetic';
  readonly op: '+' | '-' | '*' | '/' | '%';
  readonly left: Expr;
  readonly right: Expr;
}

export function arithmetic(
  op: '+' | '-' | '*' | '/' | '%',
  left: Expr,
  right: Expr,
): Arithmetic {
  return { _tag: 'Arithmetic', op, left, right };
}

// ── Function call ───────────────────────────────────────────────────

/** Built-in function call: distance(V1, V2), upper(S), etc. */
export interface FuncCall extends Expr {
  readonly _tag: 'FuncCall';
  readonly name: string;
  readonly args: readonly Expr[];
}

export function funcCall(name: string, args: readonly Expr[]): FuncCall {
  return { _tag: 'FuncCall', name, args };
}

// ── Aggregation ─────────────────────────────────────────────────────

/** Aggregation expression: count<X>, sum<X>, top_k<k, ...>, etc. */
export interface AggExpr extends Expr {
  readonly _tag: 'AggExpr';
  readonly func: string;
  readonly column?: Expr;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  readonly params: readonly any[];
  readonly passthrough: readonly Expr[];
  readonly orderColumn?: Expr;
  readonly desc: boolean;
}

export function aggExpr(opts: {
  func: string;
  column?: Expr;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  params?: readonly any[];
  passthrough?: readonly Expr[];
  orderColumn?: Expr;
  desc?: boolean;
}): AggExpr {
  return {
    _tag: 'AggExpr',
    func: opts.func,
    column: opts.column,
    params: opts.params ?? [],
    passthrough: opts.passthrough ?? [],
    orderColumn: opts.orderColumn,
    desc: opts.desc ?? true,
  };
}

// ── Ordering ────────────────────────────────────────────────────────

/** A column with sort direction. */
export interface OrderedColumn extends Expr {
  readonly _tag: 'OrderedColumn';
  readonly column: Expr;
  readonly descending: boolean;
}

export function orderedColumn(col: Expr, descending: boolean): OrderedColumn {
  return { _tag: 'OrderedColumn', column: col, descending };
}

// ── Boolean expressions ─────────────────────────────────────────────

/** Binary comparison: ==, !=, <, <=, >, >=. */
export interface Comparison extends BoolExpr {
  readonly _tag: 'Comparison';
  readonly op: '=' | '!=' | '<' | '<=' | '>' | '>=';
  readonly left: Expr;
  readonly right: Expr;
}

export function comparison(
  op: '=' | '!=' | '<' | '<=' | '>' | '>=',
  left: Expr,
  right: Expr,
): Comparison {
  return { _tag: 'Comparison', op, left, right };
}

/** Logical AND of two conditions (IQL comma). */
export interface And extends BoolExpr {
  readonly _tag: 'And';
  readonly left: BoolExpr;
  readonly right: BoolExpr;
}

export function and(left: BoolExpr, right: BoolExpr): And {
  return { _tag: 'And', left, right };
}

/** Logical OR - requires splitting into multiple queries. */
export interface Or extends BoolExpr {
  readonly _tag: 'Or';
  readonly left: BoolExpr;
  readonly right: BoolExpr;
}

export function or(left: BoolExpr, right: BoolExpr): Or {
  return { _tag: 'Or', left, right };
}

/** Negation: !relation(X, Y) in IQL. */
export interface Not extends BoolExpr {
  readonly _tag: 'Not';
  readonly operand: BoolExpr;
}

export function not(operand: BoolExpr): Not {
  return { _tag: 'Not', operand };
}

/** Membership test: Column appears in another relation's column. */
export interface InExpr extends BoolExpr {
  readonly _tag: 'InExpr';
  readonly column: Expr;
  readonly targetColumn: Expr;
}

export function inExpr(col: Expr, target: Expr): InExpr {
  return { _tag: 'InExpr', column: col, targetColumn: target };
}

/** Negated membership test. */
export interface NegatedIn extends BoolExpr {
  readonly _tag: 'NegatedIn';
  readonly column: Expr;
  readonly targetColumn: Expr;
}

export function negatedIn(col: Expr, target: Expr): NegatedIn {
  return { _tag: 'NegatedIn', column: col, targetColumn: target };
}

/** Multi-column negation/existence check against a relation. */
export interface MatchExpr extends BoolExpr {
  readonly _tag: 'MatchExpr';
  readonly relation: string;
  readonly bindings: Record<string, Expr>;
  readonly negated: boolean;
}

export function matchExpr(
  relation: string,
  bindings: Record<string, Expr>,
  negated: boolean,
): MatchExpr {
  return { _tag: 'MatchExpr', relation, bindings, negated };
}

// ── Type guards ─────────────────────────────────────────────────────

export function isColumn(e: Expr): e is Column {
  return e._tag === 'Column';
}
export function isLiteral(e: Expr): e is Literal {
  return e._tag === 'Literal';
}
export function isArithmetic(e: Expr): e is Arithmetic {
  return e._tag === 'Arithmetic';
}
export function isFuncCall(e: Expr): e is FuncCall {
  return e._tag === 'FuncCall';
}
export function isAggExpr(e: Expr): e is AggExpr {
  return e._tag === 'AggExpr';
}
export function isOrderedColumn(e: Expr): e is OrderedColumn {
  return e._tag === 'OrderedColumn';
}

export function isComparison(e: BoolExpr): e is Comparison {
  return e._tag === 'Comparison';
}
export function isAnd(e: BoolExpr): e is And {
  return e._tag === 'And';
}
export function isOr(e: BoolExpr): e is Or {
  return e._tag === 'Or';
}
export function isNot(e: BoolExpr): e is Not {
  return e._tag === 'Not';
}
export function isInExpr(e: BoolExpr): e is InExpr {
  return e._tag === 'InExpr';
}
export function isNegatedIn(e: BoolExpr): e is NegatedIn {
  return e._tag === 'NegatedIn';
}
export function isMatchExpr(e: BoolExpr): e is MatchExpr {
  return e._tag === 'MatchExpr';
}
