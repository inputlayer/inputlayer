/**
 * Session - ephemeral facts and rules (no + prefix).
 */

import type { Connection } from './connection.js';
import type { RelationDef } from './relation.js';
import type { Fact } from './types.js';
import type { RuleClause } from './compiler.js';
import { compileInsert, compileBulkInsert, compileRule } from './compiler.js';
import { meta, sessionRules } from './meta.js';

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

  /** List session rules, one clause per entry, in definition order. */
  async listRules(): Promise<string[]> {
    const result = await this.conn.execute(meta.sessionList());
    return sessionRules(result.rows);
  }

  /** Drop a session rule by name, or one of its clauses by index (1-based). */
  async dropRule(name: string, index?: number): Promise<void> {
    if (index === undefined) {
      await this.conn.execute(meta.sessionDrop(name));
      return;
    }
    // The engine drops a clause by its position among all session rules,
    // so find that position.
    const positions: number[] = [];
    (await this.listRules()).forEach((rule, i) => {
      if (rule.split('(', 1)[0].trim() === name) positions.push(i + 1);
    });
    if (!Number.isInteger(index) || index < 1 || index > positions.length) {
      throw new RangeError(
        `Session rule '${name}' has ${positions.length} clause(s); index ${index} is out of range`,
      );
    }
    await this.conn.execute(meta.sessionDrop(positions[index - 1]));
  }

  /** Clear all session facts and rules. */
  async clear(): Promise<void> {
    await this.conn.execute(meta.sessionClear());
  }
}
