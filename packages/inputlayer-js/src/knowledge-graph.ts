/**
 * KnowledgeGraph - the primary workspace for data, queries, and rules.
 */

import type { Connection } from './connection.js';
import type { Expr, BoolExpr, OrderedColumn } from './ast.js';
import type { RelationDef } from './relation.js';
import type { Fact } from './types.js';
import type { ColumnProxy, RelationRef } from './proxy.js';
import type { AclEntry } from './auth.js';
import {
  compileSchema,
  compileInsert,
  compileBulkInsert,
  compileDelete,
  compileConditionalDelete,
  compileQuery,
  compileRule,
  type QueryOptions,
  type RuleClause,
} from './compiler.js';
import { HnswIndex } from './index-def.js';
import { ResultSet } from './result.js';
import { Session } from './session.js';

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

export interface ExplainResult {
  iql: string;
  plan: string;
}

export interface ServerStatus {
  version: string;
  knowledgeGraph: string;
}

// ── KnowledgeGraph ──────────────────────────────────────────────────

/**
 * Primary workspace for interacting with a knowledge graph.
 */
export class KnowledgeGraph {
  private readonly _name: string;
  private readonly conn: Connection;
  private readonly _session: Session;

  private kgActive = false;

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

  /** Ensure the connection is using this knowledge graph. */
  private async ensureKg(): Promise<void> {
    if (this.conn.currentKg === this._name) return;
    // .kg create is idempotent (no-ops if KG exists).
    await this.conn.execute(`.kg create ${this._name}`);
    await this.conn.execute(`.kg use ${this._name}`);
    // Force-update currentKg since the server may not set switched_kg
    // on .kg use responses.
    this.conn.setCurrentKg(this._name);
  }

  // ── Schema ──────────────────────────────────────────────────────

  /** Deploy schema definitions. Idempotent. */
  async define(...relations: RelationDef[]): Promise<void> {
    await this.ensureKg();
    for (const rel of relations) {
      const iql = compileSchema(rel);
      await this.conn.execute(iql);
    }
  }

  /** List all relations in this KG. */
  async relations(): Promise<RelationInfo[]> {
    await this.ensureKg();
    const result = await this.conn.execute('.rel');
    return result.rows.map((row) => ({
      name: String(row[0]),
      rowCount: row.length > 1 ? Number(row[1]) : 0,
    }));
  }

  /** Describe a relation's schema. */
  async describe(relation: RelationDef | string): Promise<RelationDescription> {
    await this.ensureKg();
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
    await this.ensureKg();
    const name = typeof relation === 'string' ? relation : relation.relationName;
    await this.conn.execute(`.rel drop ${name}`);
  }

  // ── Insert ──────────────────────────────────────────────────────

  /** Insert facts into the knowledge graph. */
  async insert(rel: RelationDef, facts: Fact | Fact[]): Promise<InsertResult> {
    await this.ensureKg();
    const factList = Array.isArray(facts) ? facts : [facts];
    if (factList.length === 0) return { count: 0 };

    let iql: string;
    if (factList.length === 1) {
      iql = compileInsert(rel, factList[0]);
    } else {
      iql = compileBulkInsert(rel, factList);
    }

    const result = await this.conn.execute(iql);
    return { count: result.rows.length };
  }

  // ── Delete ──────────────────────────────────────────────────────

  /**
   * Delete facts from the knowledge graph.
   *
   * @param rel - The relation definition
   * @param factsOrCondition - Either specific facts to delete, or a BoolExpr condition
   */
  async delete(rel: RelationDef, factsOrCondition: Fact | Fact[] | BoolExpr): Promise<DeleteResult> {
    await this.ensureKg();
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
    await this.ensureKg();
    const iql = compileQuery(opts);

    if (Array.isArray(iql)) {
      // OR split -> execute each and union
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const allRows: any[][] = [];
      let columns: string[] = [];
      for (const q of iql) {
        const result = await this.conn.execute(q);
        if (columns.length === 0) {
          columns = result.columns;
        }
        allRows.push(...result.rows);
      }
      return new ResultSet({ columns, rows: allRows });
    }

    const result = await this.conn.execute(iql);
    const rs = new ResultSet({
      columns: result.columns,
      rows: result.rows,
      rowCount: result.row_count,
      totalCount: result.total_count,
      truncated: result.truncated,
      executionTimeMs: result.execution_time_ms,
      rowProvenance: result.row_provenance,
    });
    if (result.metadata) {
      rs.hasEphemeral = result.metadata.has_ephemeral ?? false;
      rs.ephemeralSources = result.metadata.ephemeral_sources ?? [];
      rs.warnings = result.metadata.warnings ?? [];
    }
    return rs;
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
    await this.ensureKg();
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
    });
  }

  // ── Rules ───────────────────────────────────────────────────────

  /** Deploy persistent rule definitions. */
  async defineRules(
    headName: string,
    headColumns: string[],
    clauses: RuleClause[],
  ): Promise<void> {
    await this.ensureKg();
    for (const clause of clauses) {
      const iql = compileRule(headName, headColumns, clause, true);
      await this.conn.execute(iql);
    }
  }

  /** List all rules in this KG. */
  async listRules(): Promise<RuleInfo[]> {
    await this.ensureKg();
    const result = await this.conn.execute('.rule list');
    return result.rows.map((row) => ({
      name: String(row[0]),
      clauseCount: row.length > 1 ? Number(row[1]) : 1,
    }));
  }

  /** Get the IQL definition of a rule. */
  async ruleDefinition(name: string): Promise<string[]> {
    await this.ensureKg();
    const result = await this.conn.execute(`.rule show ${name}`);
    return result.rows.map((row) => String(row[0]));
  }

  /** Drop all clauses of a rule. */
  async dropRule(name: string): Promise<void> {
    await this.ensureKg();
    await this.conn.execute(`.rule drop ${name}`);
  }

  /** Remove a specific clause from a rule (1-based index). */
  async dropRuleClause(name: string, index: number): Promise<void> {
    await this.ensureKg();
    await this.conn.execute(`.rule remove ${name} ${index}`);
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
    await this.ensureKg();
    await this.conn.execute(`.rule clear ${name}`);
  }

  /** Drop all rules whose names start with prefix. */
  async dropRulesByPrefix(prefix: string): Promise<void> {
    await this.ensureKg();
    await this.conn.execute(`.rule drop prefix ${prefix}`);
  }

  // ── Indexes ─────────────────────────────────────────────────────

  /** Create an HNSW vector index. */
  async createIndex(index: HnswIndex): Promise<void> {
    await this.ensureKg();
    await this.conn.execute(index.toIQL());
  }

  /** List all indexes. */
  async listIndexes(): Promise<IndexInfo[]> {
    await this.ensureKg();
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
    await this.ensureKg();
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
    await this.ensureKg();
    await this.conn.execute(`.index drop ${name}`);
  }

  /** Rebuild an index. */
  async rebuildIndex(name: string): Promise<void> {
    await this.ensureKg();
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
  async explain(opts: QueryOptions): Promise<ExplainResult> {
    await this.ensureKg();
    let iql = compileQuery(opts);
    if (Array.isArray(iql)) {
      iql = iql[0];
    }
    const result = await this.conn.execute(`.explain ${iql}`);
    const planText = result.rows.map((row) => String(row[0])).join('\n');
    return { iql, plan: planText };
  }

  /** Trigger storage compaction. */
  async compact(): Promise<void> {
    await this.ensureKg();
    await this.conn.execute('.compact');
  }

  /** Get server status. */
  async status(): Promise<ServerStatus> {
    await this.ensureKg();
    const result = await this.conn.execute('.status');
    const row = result.rows[0] ?? ['unknown', 'unknown'];
    return {
      version: row.length > 0 ? String(row[0]) : 'unknown',
      knowledgeGraph: row.length > 1 ? String(row[1]) : this._name,
    };
  }

  /** Load data from a file on the server. */
  async load(path: string, mode?: string): Promise<void> {
    await this.ensureKg();
    let cmd = `.load ${path}`;
    if (mode) cmd += ` ${mode}`;
    await this.conn.execute(cmd);
  }

  /** Clear all relations matching a prefix. */
  async clearPrefix(prefix: string): Promise<ClearResult> {
    await this.ensureKg();
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

  /** Execute raw IQL. */
  async execute(iql: string): Promise<ResultSet> {
    await this.ensureKg();
    const result = await this.conn.execute(iql);
    return new ResultSet({
      columns: result.columns,
      rows: result.rows,
      rowCount: result.row_count,
      totalCount: result.total_count,
      truncated: result.truncated,
      executionTimeMs: result.execution_time_ms,
    });
  }
}
