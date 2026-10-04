/**
 * KnowledgeGraph - the primary workspace for data, queries, and rules.
 */

import type { Connection, ExecuteOptions } from './connection.js';
import type { ResultResponse } from './protocol.js';
import type { Expr, BoolExpr, OrderedColumn } from './ast.js';
import { compileValue, type ColumnTypes, type RelationDef, type RowOf } from './relation.js';
import type { Fact } from './types.js';
import type { ColumnProxy, RelationRef } from './proxy.js';
import type { AclEntry } from './auth.js';
import {
  compileSchema,
  compileInsert,
  compileBulkInsert,
  compileDelete,
  compileConditionalDelete,
  compileQueryPlan,
  resultColumnIndexes,
  compileRule,
  type QueryOptions,
  type QueryPlan,
  type RuleClause,
} from './compiler.js';
import {
  CompileError,
  ConflictError,
  InternalError,
  PreconditionFailed,
  QueryError,
  StatementFailedError,
} from './errors.js';
import {
  GUARD_SCHEMAS,
  Program,
  compileClaim,
  parseWriteMessage,
  type Claim,
  type ClaimOptions,
  type ProgramResult,
} from './program.js';
import { HnswIndex } from './index-def.js';
import { ResultSet } from './result.js';
import { Session } from './session.js';
import { meta, ruleClauses, ruleList } from './meta.js';
import {
  Subscription,
  runCallback,
  watchChanges,
  type Change,
  type Live,
  type Row,
  type SubscribeOptions,
  type SubscriptionHandle,
  type SubscriptionTarget,
} from './subscription.js';

// ── Data types ──────────────────────────────────────────────────────

export interface RelationInfo {
  name: string;
  rowCount: number;
}

export interface ColumnInfo {
  name: string;
  type: string;
}

export interface RelationDescription {
  name: string;
  columns: ColumnInfo[];
  rowCount: number;
  sample: Array<Record<string, unknown>>;
}

export interface RuleInfo {
  name: string;
  clauseCount: number;
}

export interface IndexInfo {
  name: string;
  relation: string;
  column: string;
  metric: string;
  rowCount: number;
}

export interface IndexStats {
  name: string;
  rowCount: number;
  layers: number;
  memoryBytes: number;
}

export interface InsertResult {
  /** Facts the engine newly stored; duplicates of stored facts do not count. */
  count: number;
}

export interface DeleteResult {
  count: number;
}

export interface ClearResult {
  relationsCleared: number;
  factsCleared: number;
  details: Array<[string, number]>;
}

export interface DebugResult {
  iql: string;
  plan: string;
}

export interface ServerStatus {
  version: string;
  knowledgeGraph: string;
}

/** A node in a proof tree explaining why a fact was derived. */
export interface ProofNode {
  kind: 'fact' | 'rule' | 'negation' | 'vector_search' | 'aggregate' | 'truncated' | 'why_not';
  conclusion: { pred: string; args: unknown[] };
  rule_id?: string;
  bindings?: Record<string, unknown>;
  aggregate?: {
    fn: string;
    value_var: string;
    result: unknown;
    contributing_count: number;
    sample_inputs?: unknown[][];
    full_inputs?: unknown[][] | null;
  };
  negation?: { pattern: string };
  vector_search?: {
    index_name: string;
    metric: string;
    query_vector: number[];
    result_id: number;
    distance: number;
    k: number;
    ef_search?: number;
  };
  truncated?: { depth_limit: number };
  why_not?: {
    rule_name: string;
    clause_index: number;
    clause_text: string;
    blocker: Record<string, unknown>;
  };
  children: string[];
}

/** A proof tree - a DAG of proof nodes. */
export interface ProofTree {
  version: number;
  roots: string[];
  nodes: Record<string, ProofNode>;
}

/** Result of a .why query with proof trees. */
export interface WhyResult {
  /** Result data rows */
  results: ResultSet;
  /** Structured proof trees - one per result row */
  proofTrees: ProofTree[];
}

/** Explanation of why a fact was NOT derived. */
export interface WhyNotResult {
  /** Human-readable explanation */
  text: string;
  /** Structured explanation as proof tree */
  explanation: ProofTree | null;
}

/** Blocker details for why-not explanations. */
export interface WhyNotBlocker {
  type: string;
  reason?: string;
  predicateIndex?: number;
  predicateText?: string;
  comparisonText?: string;
  lhsValue?: string;
  rhsValue?: string;
  relation?: string;
  matchingTuple?: unknown[];
  indexName?: string;
  k?: number;
}

// ── KnowledgeGraph ──────────────────────────────────────────────────

/**
 * Primary workspace for interacting with a knowledge graph.
 */
export class KnowledgeGraph {
  private readonly _name: string;
  private readonly conn: Connection;
  private readonly _session: Session;

  /** Whether this handle declared the guard relations (il_txn, il_txn_pending, il_assert). */
  private guardRelationsDeclared = false;

  constructor(name: string, connection: Connection) {
    this._name = name;
    this.conn = connection;
    this._session = new Session(connection);
  }

  get name(): string {
    return this._name;
  }

  get session(): Session {
    return this._session;
  }

  /**
   * The handle's own connection, bound to this knowledge graph at connect
   * time (`?kg=`), so no call ever switches graphs under another.
   */
  get connection(): Connection {
    return this.conn;
  }

  // ── Schema ──────────────────────────────────────────────────────

  /**
   * Deploy schema definitions, with the relations guarded programs use
   * (`il_txn`, `il_txn_pending`, `il_assert`), in one program. Idempotent.
   */
  async define(...relations: RelationDef[]): Promise<void> {
    await this.conn.execute([...relations.map(compileSchema), ...GUARD_SCHEMAS].join('\n'));
    this.guardRelationsDeclared = true;
  }

  /** Declare the guard relations once per handle, for a graph defined elsewhere. */
  private async ensureGuardRelations(): Promise<void> {
    if (this.guardRelationsDeclared) return;
    await this.conn.execute(GUARD_SCHEMAS.join('\n'));
    this.guardRelationsDeclared = true;
  }

  /** List all relations in this KG. */
  async relations(): Promise<RelationInfo[]> {
    const result = await this.conn.execute('.rel');
    return result.rows.map((row) => ({
      name: String(row[0]),
      rowCount: row.length > 1 ? Number(row[1]) : 0,
    }));
  }

  /** Describe a relation's schema. */
  async describe(relation: RelationDef | string): Promise<RelationDescription> {
    const name = typeof relation === 'string' ? relation : relation.relationName;
    const result = await this.conn.execute(`.rel ${name}`);
    const columns = result.rows.map((row) => ({
      name: String(row[0]),
      type: String(row[1]),
    }));
    return { name, columns, rowCount: 0, sample: [] };
  }

  /** Drop a relation and all its data. */
  async dropRelation(relation: RelationDef | string): Promise<void> {
    const name = typeof relation === 'string' ? relation : relation.relationName;
    await this.conn.execute(`.rel drop ${name}`);
  }

  // ── Insert ──────────────────────────────────────────────────────

  /** Insert facts into the knowledge graph. */
  async insert(rel: RelationDef, facts: Fact | Fact[]): Promise<InsertResult> {
    const factList = Array.isArray(facts) ? facts : [facts];
    if (factList.length === 0) return { count: 0 };

    let iql: string;
    if (factList.length === 1) {
      iql = compileInsert(rel, factList[0]);
    } else {
      iql = compileBulkInsert(rel, factList);
    }

    const result = await this.conn.execute(iql);
    return { count: insertedCount(result) };
  }

  // ── Delete ──────────────────────────────────────────────────────

  /**
   * Delete facts from the knowledge graph.
   *
   * @param rel - The relation definition
   * @param factsOrCondition - Either specific facts to delete, or a BoolExpr condition
   */
  async delete(rel: RelationDef, factsOrCondition: Fact | Fact[] | BoolExpr): Promise<DeleteResult> {
    // Check if it's a BoolExpr (has _tag property)
    if (
      typeof factsOrCondition === 'object' &&
      factsOrCondition !== null &&
      '_tag' in factsOrCondition
    ) {
      const iql = compileConditionalDelete(rel, factsOrCondition as BoolExpr);
      const result = await this.conn.execute(iql);
      return { count: result.rows.length };
    }

    const facts = Array.isArray(factsOrCondition) ? factsOrCondition : [factsOrCondition];
    for (const fact of facts) {
      const iql = compileDelete(rel, fact as Fact);
      await this.conn.execute(iql);
    }
    return { count: facts.length };
  }

  /**
   * Retract a row (every column given), or every row matching the given
   * columns, as one program: `retract(Eta, { shipment: "S-77" })`.
   */
  async retract(rel: RelationDef, rowOrKey: Fact): Promise<DeleteResult> {
    const result = await this.program().retract(rel, rowOrKey).commit();
    return { count: result.deleted };
  }

  // ── Programs and claims ─────────────────────────────────────────

  /**
   * Start a program: statements committed as one request and one
   * transaction. `.when()` makes the whole program conditional.
   *
   * @example
   * await kg.program()
   *   .insert(AttemptDone, { attempt: "att-9f3", status: "ok" })
   *   .when(any(Attempt, { attempt: "att-9f3" }), NOT(any(AttemptDone, { attempt: "att-9f3" })))
   *   .commit();
   */
  program(): Program {
    return new Program((program, strict) => this.commitProgram(program, strict));
  }

  private async commitProgram(program: Program, strict: boolean): Promise<ProgramResult> {
    if (program.guarded) await this.ensureGuardRelations();
    const compiled = program.compile(strict);
    const { iql } = compiled;
    let result: ResultResponse;
    try {
      result = await this.conn.execute(iql);
    } catch (e) {
      if (
        e instanceof StatementFailedError &&
        compiled.assertIndex !== undefined &&
        e.errors[0]?.index === compiled.assertIndex
      ) {
        throw new PreconditionFailed(iql, e.result);
      }
      throw asConflict(e, iql);
    }
    const message = (i: number) => String(result.rows[i]?.[0] ?? '');
    let inserted = 0;
    let deleted = 0;
    for (const i of compiled.writeIndexes) {
      const counts = parseWriteMessage(message(i));
      inserted += counts.inserted;
      deleted += counts.deleted;
    }
    const applied = compiled.tokenIndex === undefined || parseWriteMessage(message(compiled.tokenIndex)).inserted === 1;
    if (strict && !applied) throw new PreconditionFailed(iql, result);
    return { applied, inserted, deleted, iql };
  }

  /**
   * Insert `row` only if the `when` conditions hold and no `unless` row
   * exists, deciding at commit, and say who holds the key afterwards. One
   * request: of many concurrent claims on one key, exactly one wins.
   *
   * @example
   * const c = await kg.claim(Attempt, { order: "ORD-1", tool: "carrier_check", attempt: id }, {
   *   when: [any(CheckNeeded, { order: "ORD-1" })],
   *   unless: any(Attempt, { order: "ORD-1", tool: "carrier_check" }),
   * });
   * if (c.won) { ... }
   */
  async claim<T extends ColumnTypes>(
    rel: RelationDef<T>,
    row: RowOf<RelationDef<T>>,
    opts: ClaimOptions<keyof T & string> = {},
  ): Promise<Claim<RowOf<RelationDef<T>>>> {
    const fact = row as unknown as Fact;
    const { iql } = compileClaim(rel, fact, opts);
    let result: ResultResponse;
    try {
      result = await this.conn.execute(iql);
    } catch (e) {
      throw asConflict(e, iql);
    }
    const cols = rel.columns;
    const types = cols.map((c) => rel.columnTypes[c]);
    const ours = cols.map((c, i) => compileValue(fact[c], types[i]));
    const rows = result.rows.map((r) => {
      if (r.length !== cols.length) {
        throw new InternalError(`Unexpected claim reply: ${JSON.stringify(r)} for columns ${cols.join(', ')}`);
      }
      return r;
    });
    const same = (v: unknown, i: number): boolean => {
      try {
        return compileValue(v, types[i]) === ours[i];
      } catch {
        return false;
      }
    };
    if (rows.some((r) => r.every(same))) {
      return { won: true, holder: row };
    }
    const first = rows[0];
    const holder = first === undefined ? null : (Object.fromEntries(cols.map((c, i) => [c, first[i]])) as RowOf<RelationDef<T>>);
    return { won: false, holder };
  }

  // ── Query ───────────────────────────────────────────────────────

  /**
   * Query the knowledge graph.
   *
   * @example
   * // Simple query
   * const result = await kg.query({ select: [Employee] });
   *
   * // Filter
   * const result = await kg.query({
   *   select: [Employee.col("name"), Employee.col("salary")],
   *   join: [Employee],
   *   where: Employee.col("department").eq("eng"),
   * });
   *
   * // Join
   * const result = await kg.query({
   *   select: [Employee.col("name"), Department.col("budget")],
   *   join: [Employee, Department],
   *   on: Employee.col("department").eq(Department.col("name")),
   * });
   */
  async query(opts: QueryOptions): Promise<ResultSet> {
    const plan = compileQueryPlan(opts);
    const columns = plan.outputs.map((o) => o.label);
    const outputVars = plan.outputs.map((o) => o.variable);

    if (plan.programs.length > 1) {
      return new ResultSet({ columns, rows: await this.queryBranches(plan, outputVars) });
    }

    const result = await this.conn.execute(plan.programs[0]);
    // The engine paginates only with a limit; an offset alone is applied here.
    const skip = plan.page.limit === undefined ? (plan.page.offset ?? 0) : 0;
    const rows = projectRows(plan, result.columns, result.rows, outputVars).slice(skip);
    const rs = new ResultSet({
      columns,
      rows,
      rowCount: skip > 0 ? rows.length : result.row_count,
      totalCount: result.total_count,
      truncated: result.truncated,
      executionTimeMs: result.execution_time_ms,
      rowProvenance: result.row_provenance?.slice(skip),
      timingBreakdown: result.timing_breakdown,
    });
    if (result.metadata) {
      rs.hasEphemeral = result.metadata.has_ephemeral ?? false;
      rs.ephemeralSources = result.metadata.ephemeral_sources ?? [];
      rs.warnings = result.metadata.warnings ?? [];
    }
    return rs;
  }

  /** Run each branch of an OR split, then merge: union, order, paginate. */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private async queryBranches(plan: QueryPlan, outputVars: string[]): Promise<any[][]> {
    const { order } = plan.page;
    const orderVars = order ? [order.variable] : [];
    // Each row carries its outputs, then the atom variables that identify
    // it, then the sort key.
    const vars = [...outputVars, ...plan.rowVars, ...orderVars];
    const idEnd = outputVars.length + plan.rowVars.length;
    const seen = new Set<string>();
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const merged: any[][] = [];
    for (const program of plan.programs) {
      const result = await this.conn.execute(program);
      for (const row of projectRows(plan, result.columns, result.rows, vars)) {
        const key = JSON.stringify(row.slice(outputVars.length, idEnd));
        if (seen.has(key)) continue;
        seen.add(key);
        merged.push(row);
      }
    }
    return pageOf(plan, merged, (row) => row[idEnd]).map((row) => row.slice(0, outputVars.length));
  }

  /**
   * Stream query results in batches.
   *
   * Returns an async generator yielding arrays of rows.
   */
  async *queryStream(
    opts: QueryOptions & { batchSize?: number },
  ): AsyncIterableIterator<Array<Record<string, unknown>>> {
    const batchSize = opts.batchSize ?? 1000;
    const result = await this.query(opts);
    for (let i = 0; i < result.rows.length; i += batchSize) {
      const batch = result.rows.slice(i, i + batchSize);
      yield batch.map((row) => {
        const obj: Record<string, unknown> = {};
        for (let j = 0; j < result.columns.length && j < row.length; j++) {
          obj[result.columns[j]] = row[j];
        }
        return obj;
      });
    }
  }

  // ── Subscriptions ───────────────────────────────────────────────

  /**
   * Subscribe to a relation or view, a query, or raw IQL (`{ iql: "?..." }`):
   * an async iterator of `Change` events (`snapshot`, then `delta`s, with
   * `unverified` and `resync` around anything that broke the stream; see
   * `subscription.ts`). It opens on the first `next()`; leaving the loop or
   * `close()` ends it.
   *
   * Refused with `SubscriptionRejectedError` for a query with `limit` or
   * `offset`, an OR condition, an aggregate, or a session rule, which a
   * standing query cannot track; declare a persistent rule instead.
   *
   * @example
   * for await (const change of kg.subscribe(Late)) {
   *   for (const row of change.retracted) cancel(row);
   *   for (const row of change.inserted) start(row);
   *   if (!change.verified) pause();
   * }
   */
  subscribe<T = Row>(target: SubscriptionTarget, opts?: SubscribeOptions): Subscription<T> {
    return new Subscription<T>(this.conn, target, opts, () => this._session.listRules());
  }

  /**
   * The whole current result of `target` each time it changes, with its
   * revision. `verified` is false from a lost connection (or any other
   * `unverified` event) until the fresh result arrives: act on nothing new
   * meanwhile. Coalesced commits are seen as one change.
   */
  watch<T = Row>(target: SubscriptionTarget, opts?: SubscribeOptions): AsyncIterableIterator<Live<T>> {
    return watchChanges(this.subscribe<T>(target, opts));
  }

  /**
   * Call `callback` with every `Change` of `target`, one at a time (an async
   * callback is awaited). Errors it throws, and the error that ends the
   * subscription, go to `onError` (default: `console.error`).
   */
  on<T = Row>(
    target: SubscriptionTarget,
    callback: (change: Change<T>) => unknown,
    opts: SubscribeOptions & { onError?: (error: unknown) => void } = {},
  ): SubscriptionHandle {
    const onError = opts.onError ?? ((error: unknown) => console.error('inputlayer: subscription callback', error));
    return runCallback(this.subscribe<T>(target, opts), callback, onError);
  }

  // ── Vector search ───────────────────────────────────────────────

  /**
   * Perform a vector similarity search.
   */
  async vectorSearch(opts: {
    relation: RelationDef;
    queryVec: number[];
    column?: string;
    k?: number;
    radius?: number;
    metric?: 'cosine' | 'euclidean' | 'manhattan' | 'dot_product';
  }): Promise<ResultSet> {
    const rel = opts.relation;
    const relName = rel.relationName;
    const cols = rel.columns;

    // Find vector column if not specified
    let vecColumn = opts.column;
    if (!vecColumn) {
      for (const [name, type] of Object.entries(rel.columnTypes)) {
        if (type === 'vector' || type.startsWith('vector[')) {
          vecColumn = name;
          break;
        }
      }
      if (!vecColumn) {
        throw new Error(`No vector column found in ${relName}`);
      }
    }

    const vecStr = `[${opts.queryVec.join(', ')}]`;
    const distFn: Record<string, string> = {
      cosine: 'cosine',
      euclidean: 'euclidean',
      manhattan: 'manhattan',
      dot_product: 'dot',
    };
    const fnName = distFn[opts.metric ?? 'cosine'] ?? 'cosine';

    const colVars = cols.map((_, i) => `X${i}`).join(', ');
    const vecVar = `X${cols.indexOf(vecColumn)}`;
    const distAssign = `Dist = ${fnName}(${vecVar}, ${vecStr})`;

    let query: string;
    if (opts.k !== undefined) {
      query = `?top_k<${opts.k}, ${colVars}, Dist:asc> <- ${relName}(${colVars}), ${distAssign}`;
    } else if (opts.radius !== undefined) {
      query = `?within_radius<${opts.radius}, ${colVars}, Dist:asc> <- ${relName}(${colVars}), ${distAssign}`;
    } else {
      throw new Error('Must specify either k or radius');
    }

    const result = await this.conn.execute(query);
    return new ResultSet({
      columns: result.columns,
      rows: result.rows,
      rowCount: result.row_count,
      totalCount: result.total_count,
      truncated: result.truncated,
      executionTimeMs: result.execution_time_ms,
      timingBreakdown: result.timing_breakdown,
    });
  }

  // ── Rules ───────────────────────────────────────────────────────

  /** Deploy persistent rule definitions. */
  async defineRules(
    headName: string,
    headColumns: string[],
    clauses: RuleClause[],
  ): Promise<void> {
    for (const clause of clauses) {
      const iql = compileRule(headName, headColumns, clause, true);
      await this.conn.execute(iql);
    }
  }

  /** List all rules in this KG. */
  async listRules(): Promise<RuleInfo[]> {
    const result = await this.conn.execute(meta.ruleList());
    return ruleList(result.rows);
  }

  /** Get the IQL clauses of a rule, in order. */
  async ruleDefinition(name: string): Promise<string[]> {
    const result = await this.conn.execute(meta.ruleDef(name));
    return ruleClauses(result.rows);
  }

  /** Drop all clauses of a rule. */
  async dropRule(name: string): Promise<void> {
    await this.conn.execute(meta.ruleDrop(name));
  }

  /** Remove a specific clause from a rule (1-based index). */
  async dropRuleClause(name: string, index: number): Promise<void> {
    await this.conn.execute(meta.ruleRemove(name, index));
  }

  /** Replace a specific rule clause (remove + re-add). */
  async editRuleClause(
    name: string,
    index: number,
    headColumns: string[],
    clause: RuleClause,
  ): Promise<void> {
    await this.dropRuleClause(name, index);
    const iql = compileRule(name, headColumns, clause, true);
    await this.conn.execute(iql);
  }

  /** Clear a rule's materialized data. */
  async clearRule(name: string): Promise<void> {
    await this.conn.execute(meta.ruleClear(name));
  }

  /** Drop all rules whose names start with prefix. */
  async dropRulesByPrefix(prefix: string): Promise<void> {
    await this.conn.execute(meta.ruleDropPrefix(prefix));
  }

  // ── Indexes ─────────────────────────────────────────────────────

  /** Create an HNSW vector index. */
  async createIndex(index: HnswIndex): Promise<void> {
    await this.conn.execute(index.toIQL());
  }

  /** List all indexes. */
  async listIndexes(): Promise<IndexInfo[]> {
    const result = await this.conn.execute('.index list');
    return result.rows.map((row) => ({
      name: String(row[0]),
      relation: row.length > 1 ? String(row[1]) : '',
      column: row.length > 2 ? String(row[2]) : '',
      metric: row.length > 3 ? String(row[3]) : '',
      rowCount: row.length > 4 ? Number(row[4]) : 0,
    }));
  }

  /** Get statistics for an index. */
  async indexStats(name: string): Promise<IndexStats> {
    const result = await this.conn.execute(`.index stats ${name}`);
    const row = result.rows[0] ?? [name, 0, 0, 0];
    return {
      name: String(row[0]),
      rowCount: row.length > 1 ? Number(row[1]) : 0,
      layers: row.length > 2 ? Number(row[2]) : 0,
      memoryBytes: row.length > 3 ? Number(row[3]) : 0,
    };
  }

  /** Drop an index. */
  async dropIndex(name: string): Promise<void> {
    await this.conn.execute(`.index drop ${name}`);
  }

  /** Rebuild an index. */
  async rebuildIndex(name: string): Promise<void> {
    await this.conn.execute(`.index rebuild ${name}`);
  }

  // ── ACL ─────────────────────────────────────────────────────────

  /** Grant per-KG access. */
  async grantAccess(username: string, role: string): Promise<void> {
    await this.conn.execute(`.kg acl grant ${this._name} ${username} ${role}`);
  }

  /** Revoke per-KG access. */
  async revokeAccess(username: string): Promise<void> {
    await this.conn.execute(`.kg acl revoke ${this._name} ${username}`);
  }

  /** List ACL entries. */
  async listAcl(): Promise<AclEntry[]> {
    const result = await this.conn.execute(`.kg acl list ${this._name}`);
    return result.rows
      .filter((row) => row.length >= 2)
      .map((row) => ({
        username: String(row[0]),
        role: String(row[1]),
      }));
  }

  // ── Meta ────────────────────────────────────────────────────────

  /** Show the query plan without executing. */
  async debug(opts: QueryOptions): Promise<DebugResult> {
    const iql = explainable(compileQueryPlan(opts), 'debug').debug;
    const result = await this.conn.execute(`.debug ${iql}`);
    const planText = result.rows.map((row) => String(row[0])).join('\n');
    return { iql, plan: planText };
  }

  /** Show proof trees explaining why query results were derived.
   *
   * Returns structured proof trees alongside the result data.
   * Each result row has a corresponding proof tree explaining its derivation.
   */
  async why(opts: QueryOptions & { full?: boolean }): Promise<WhyResult> {
    const plan = explainable(compileQueryPlan(opts), 'why');
    const result = await this.conn.execute(meta.why(plan.why.statement, opts.full));
    // The rule's columns are its head variables by position.
    const at = (v: string) => plan.why.columns.indexOf(v);
    const outputIdx = plan.outputs.map((o) => at(o.variable));
    const orderIdx = plan.page.order ? at(plan.page.order.variable) : -1;
    const proofs = (result.proof_trees ?? []) as ProofTree[];
    const derived = pageOf(
      plan,
      result.rows.map((row, i) => ({ row, proof: proofs[i], provenance: result.row_provenance?.[i] })),
      (d) => d.row[orderIdx],
    );
    const resultSet = new ResultSet({
      columns: plan.outputs.map((o) => o.label),
      rows: derived.map((d) => outputIdx.map((c) => d.row[c])),
      rowCount: derived.length,
      totalCount: result.total_count,
      truncated: result.truncated,
      executionTimeMs: result.execution_time_ms,
      rowProvenance: result.row_provenance ? derived.map((d) => d.provenance as string) : undefined,
      timingBreakdown: result.timing_breakdown,
    });
    return { results: resultSet, proofTrees: derived.map((d) => d.proof).filter((p) => p !== undefined) };
  }

  /** Explain why a specific fact was NOT derived.
   *
   * Returns a structured explanation with the specific blocker for each rule.
   */
  async whyNot(relation: RelationDef, fact: Fact): Promise<WhyNotResult> {
    const relName = relation.relationName;
    const cols = relation.columns;
    const vals = cols.map((col) => compileValue(fact[col], relation.columnTypes[col])).join(', ');
    const result = await this.conn.execute(meta.whyNot(`${relName}(${vals})`));
    const text = result.rows.map((row) => String(row[0])).join('\n');
    const explanation = (result.proof_trees?.[0] ?? null) as ProofTree | null;
    return { text, explanation };
  }

  /** Trigger storage compaction. */
  async compact(): Promise<void> {
    await this.conn.execute('.compact');
  }

  /** Get server status. */
  async status(): Promise<ServerStatus> {
    const result = await this.conn.execute('.status');
    const row = result.rows[0] ?? ['unknown', 'unknown'];
    return {
      version: row.length > 0 ? String(row[0]) : 'unknown',
      knowledgeGraph: row.length > 1 ? String(row[1]) : this._name,
    };
  }

  /**
   * Load data from a file on the server (`.load <path> [mode]`).
   *
   * The engine serves `.load` only to its interactive client today, so over
   * the WebSocket this rejects with `QueryError` (code `unsupported`).
   */
  async load(path: string, mode?: string): Promise<void> {
    let cmd = `.load ${path}`;
    if (mode) cmd += ` ${mode}`;
    await this.conn.execute(cmd);
  }

  /** Clear all relations matching a prefix. */
  async clearPrefix(prefix: string): Promise<ClearResult> {
    const result = await this.conn.execute(`.clear prefix ${prefix}`);
    const details: Array<[string, number]> = result.rows
      .filter((row) => row.length > 1)
      .map((row) => [String(row[0]), Number(row[1])]);
    return {
      relationsCleared: result.rows.length,
      factsCleared: details.reduce((sum, [, count]) => sum + count, 0),
      details,
    };
  }

  /** Execute raw IQL. `timeoutMs` and `signal` bound and cancel the call. */
  async execute(iql: string, opts?: ExecuteOptions): Promise<ResultSet> {
    const result = await this.conn.execute(iql, opts);
    return new ResultSet({
      columns: result.columns,
      rows: result.rows,
      rowCount: result.row_count,
      totalCount: result.total_count,
      truncated: result.truncated,
      executionTimeMs: result.execution_time_ms,
      timingBreakdown: result.timing_breakdown,
    });
  }
}

/** A `conflict` failure of a conditional write as `ConflictError`; anything else unchanged. */
function asConflict(e: unknown, iql: string): unknown {
  if (e instanceof QueryError && e.code === 'conflict') return new ConflictError(e.message, iql);
  return e;
}

/** `.debug` and `.why` take one statement, so they cannot state the session facts a negated constant needs. */
function explainable(plan: QueryPlan, what: string): QueryPlan {
  if (plan.constFacts !== undefined) {
    throw new CompileError(
      `${what}() cannot explain a query whose NOT(any()) binds only constants`,
      'Bind the negated column to a column of a joined relation',
    );
  }
  return plan;
}

/** Facts stored, summed over the engine's per-statement insert replies. */
function insertedCount(result: ResultResponse): number {
  return result.rows.reduce((count, row) => count + parseWriteMessage(String(row[0])).inserted, 0);
}

/** Pick `vars` out of engine rows, in order. */
function projectRows(
  plan: QueryPlan,
  columns: string[],
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  rows: any[][],
  vars: string[],
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
): any[][] {
  if (rows.length === 0) return rows;
  const idx = resultColumnIndexes(plan, columns, vars);
  if (idx.length === rows[0].length && idx.every((c, i) => c === i)) return rows;
  return rows.map((row) => idx.map((c) => row[c]));
}

/** Apply the plan's ordering and pagination to `items`, sorting on `key`. */
function pageOf<T>(plan: QueryPlan, items: T[], key: (item: T) => unknown): T[] {
  const { order, limit, offset } = plan.page;
  if (order) {
    const sign = order.descending ? -1 : 1;
    items = [...items].sort((a, b) => sign * compareValues(key(a), key(b)));
  }
  const start = offset ?? 0;
  return items.slice(start, limit !== undefined ? start + limit : undefined);
}

/** Order engine values: nulls last, numbers and strings by value, anything else by text. */
function compareValues(a: unknown, b: unknown): number {
  if (a === b) return 0;
  if (a === null || a === undefined) return 1;
  if (b === null || b === undefined) return -1;
  if (typeof a === 'number' && typeof b === 'number') return a - b;
  const sa = typeof a === 'string' ? a : JSON.stringify(a);
  const sb = typeof b === 'string' ? b : JSON.stringify(b);
  return sa < sb ? -1 : sa > sb ? 1 : 0;
}
