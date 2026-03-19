/**
 * Session - ephemeral facts and rules (no + prefix).
 */

import type { Connection } from './connection.js';
import type { RelationDef } from './relation.js';
import type { Fact } from './types.js';
import type { RuleClause } from './compiler.js';
import { compileInsert, compileBulkInsert, compileRule } from './compiler.js';

/**
 * Manage session-scoped (ephemeral) data.
 *
 * Session inserts and rules omit the + prefix, making them ephemeral
 * (cleared on disconnect or KG switch).
 */
export class Session {
  private readonly conn: Connection;

  constructor(connection: Connection) {
    this.conn = connection;
  }

  /** Insert ephemeral session facts (no + prefix). */
  async insert(rel: RelationDef, facts: Fact | Fact[]): Promise<void> {
    const factList = Array.isArray(facts) ? facts : [facts];
    if (factList.length === 0) return;

    let iql: string;
    if (factList.length === 1) {
      iql = compileInsert(rel, factList[0], false);
    } else {
      iql = compileBulkInsert(rel, factList, false);
    }
    await this.conn.execute(iql);
  }

  /** Define session-scoped rules (no + prefix). */
  async defineRules(
    headName: string,
    headColumns: string[],
    clauses: RuleClause[],
  ): Promise<void> {
    for (const clause of clauses) {
      const iql = compileRule(headName, headColumns, clause, false);
      await this.conn.execute(iql);
    }
  }

  /** List session rules. */
  async listRules(): Promise<string[]> {
    const result = await this.conn.execute('.session list');
    return result.rows.length > 0 ? result.rows.map((row) => String(row[0])) : [];
  }

  /** Drop a session rule by name, or a specific clause by index. */
  async dropRule(name: string, index?: number): Promise<void> {
    if (index !== undefined) {
      await this.conn.execute(`.session remove ${name} ${index}`);
    } else {
      await this.conn.execute(`.session drop ${name}`);
    }
  }

  /** Clear all session facts and rules. */
  async clear(): Promise<void> {
    await this.conn.execute('.session clear');
  }
}
