/**
 * Meta commands the SDK sends, in one table.
 *
 * The table is pinned by the shared fixture
 * `packages/conformance/meta-commands.json`, which a live test also runs
 * against the engine. The Python SDK is planned to render its own table
 * against the same fixture, so a command spelling cannot be wrong in one SDK
 * only (`.session list`, `.session remove` and `.rule show` were).
 */

export const meta = {
  /** List session facts and rules. The reply is parsed by {@link sessionRules}. */
  sessionList: (): string => '.session',
  /** Drop a session rule by name (every clause), or by 1-based position among all session rules. */
  sessionDrop: (nameOrPosition: string | number): string => `.session drop ${nameOrPosition}`,
  /** Clear every session fact and rule. */
  sessionClear: (): string => '.session clear',
  /** List persistent rules. */
  ruleList: (): string => '.rule list',
  /** Show a persistent rule's clauses. */
  ruleDef: (name: string): string => `.rule def ${name}`,
  /** Drop every clause of a persistent rule. */
  ruleDrop: (name: string): string => `.rule drop ${name}`,
  /** Drop every persistent rule whose name starts with the prefix. */
  ruleDropPrefix: (prefix: string): string => `.rule drop prefix ${prefix}`,
  /** Remove one clause of a persistent rule, by 1-based index within that rule. */
  ruleRemove: (name: string, index: number): string => `.rule remove ${name} ${index}`,
  /** Clear a persistent rule's clauses. */
  ruleClear: (name: string): string => `.rule clear ${name}`,
  /** Open a standing query. */
  subscribe: (id: string, query: string): string => `.subscribe ${id} ${query}`,
  /** Close a standing query. */
  unsubscribe: (id: string): string => `.unsubscribe ${id}`,
  /** Proof trees for a rule's derived rows. */
  why: (statement: string, full = false): string =>
    full ? `.why full ${statement}` : `.why ${statement}`,
  /** Why a fact was not derived. */
  whyNot: (atom: string): string => `.why_not ${atom}`,
} as const;

/** Name of a command in the table. */
export type MetaCommandName = keyof typeof meta;

/**
 * Rule clauses in a `.session` reply, in definition order.
 *
 * The engine answers with message lines: `No session data defined.`, or an
 * optional `Session facts (n):` section, then `Session rules (n):` followed by
 * one `  <position>. <clause>` line per rule.
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function sessionRules(rows: any[][]): string[] {
  const lines = rows.filter((row) => row.length > 0).map((row) => String(row[0]));
  const start = lines.findIndex((line) => line.startsWith('Session rules ('));
  if (start < 0) return [];
  const rules: string[] = [];
  for (const line of lines.slice(start + 1)) {
    const m = /^\s+\d+\. (.*)$/.exec(line);
    if (m) rules.push(m[1]);
  }
  return rules;
}

/**
 * Rules in a `.rule list` reply: a `Rules:` line, then one
 * `  <name> (<n> clause(s))` line per rule.
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function ruleList(rows: any[][]): Array<{ name: string; clauseCount: number }> {
  const rules: Array<{ name: string; clauseCount: number }> = [];
  for (const row of rows) {
    const m = /^\s+(\S+) \((\d+) clause\(s\)\)$/.exec(String(row[0] ?? ''));
    if (m) rules.push({ name: m[1], clauseCount: Number(m[2]) });
  }
  return rules;
}

/**
 * Clauses in a `.rule def` reply, in order. The engine answers with one
 * message: `Rule: <name>`, `Clauses:`, then one `  <i>. <clause>` line each.
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function ruleClauses(rows: any[][]): string[] {
  const clauses: string[] = [];
  for (const row of rows) {
    for (const line of String(row[0] ?? '').split('\n')) {
      const m = /^\s+\d+\. (.*)$/.exec(line);
      if (m) clauses.push(m[1]);
    }
  }
  return clauses;
}
