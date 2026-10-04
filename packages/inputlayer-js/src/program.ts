/**
 * Write programs: batched statements, guards and claims, compiled to IQL.
 *
 * A program is one request and one transaction. `.when()` makes it
 * conditional through a transaction token (R-GUARD): the guard is evaluated
 * once into an `il_txn` row, every fact write is conditioned on that row,
 * and the row is removed last, so the program applies whole or not at all.
 * The single-head `+r(...) <- body` form is never emitted for a fact write:
 * the engine registers it as a rule. A guarded insert is the update form
 * with a ghost delete, `-r(ghost), +r(...) <- body`.
 *
 * Every function here is pure; `KnowledgeGraph` runs what they return.
 */

import type { BoolExpr } from './ast.js';
import { isAnyExpr, not as astNot } from './ast.js';
import {
  compileBulkInsert,
  compileGuard,
  compileInsert,
  compileRule,
  compileSchema,
  type ConstRow,
  type RuleClause,
} from './compiler.js';
import { CompileError, InternalError } from './errors.js';
import { columnToVariable } from './naming.js';
import { any, compileValue, type RelationDef } from './relation.js';
import type { Fact } from './types.js';

/** Token relation: one row while a guarded program runs, none after. */
export const TXN = 'il_txn';
/** Abort form: the token id, staged before the guard. */
export const TXN_PENDING = 'il_txn_pending';
/** Abort form: an int relation the assertion inserts a string into. */
export const ASSERT = 'il_assert';

/** Schemas of the SDK's guard relations, declared by `kg.define()`. */
export const GUARD_SCHEMAS = [`+${TXN}(id: string)`, `+${TXN_PENDING}(id: string)`, `+${ASSERT}(v: int)`];

/** One statement of a program, before guard compilation. */
type Statement =
  | { kind: 'insert'; rel: RelationDef; facts: Fact[] }
  | { kind: 'retract'; rel: RelationDef; fact: Fact }
  | { kind: 'retractKey'; rel: RelationDef; key: Fact }
  | { kind: 'schema'; rel: RelationDef }
  | { kind: 'rule'; text: string }
  | { kind: 'clearRule'; name: string };

/** A compiled program and how to read its reply. */
export interface CompiledProgram {
  /** The program text. */
  iql: string;
  /** Index of the token statement, whose insert count says whether the guard held. */
  tokenIndex?: number;
  /** Index of the abort form's assertion, the statement that fails when the guard does not hold. */
  assertIndex?: number;
  /** Indexes of the caller's fact writes, whose counts make `inserted`/`deleted`. */
  writeIndexes: number[];
}

/** A never-present row of `rel`: the delete anchor of a guarded insert. */
function ghost(rel: RelationDef): string {
  const values = rel.columns.map((c) => {
    const type = rel.columnTypes[c];
    if (type === 'string') return '""';
    if (type === 'float') return '-1.0';
    if (type === 'bool') return 'false';
    if (type.startsWith('vector')) return '[]';
    return '-1';
  });
  return `-${rel.relationName}(${values.join(', ')})`;
}

/** Literal values of a full row, in column order; refuses a missing column. */
function rowValues(rel: RelationDef, fact: Fact, what: string): string[] {
  return rel.columns.map((c) => {
    if (fact[c] === undefined) {
      throw new CompileError(
        `${what} on '${rel.relationName}' is missing column '${c}'`,
        `Give every column: ${rel.columns.join(', ')}`,
      );
    }
    return compileValue(fact[c]);
  });
}

/** Variables for a relation's columns, distinct from each other. */
function columnVars(rel: RelationDef): string[] {
  const used = new Set<string>();
  return rel.columns.map((c) => {
    let v = columnToVariable(c);
    for (let n = 2; used.has(v); n++) v = `${columnToVariable(c)}_${n}`;
    used.add(v);
    return v;
  });
}

/** `-r(k, V..) <- r(k, V..)` for a partial key: every row with those values. */
function keyedDelete(rel: RelationDef, key: Fact, extra: string[]): string {
  if (Object.values(key).every((v) => v === undefined)) {
    throw new CompileError(
      `retract() on '${rel.relationName}' names no column, so it would remove every row`,
      'Give the columns of the rows to remove; kg.delete() with a condition removes by condition',
    );
  }
  for (const k of Object.keys(key)) {
    if (!rel.columns.includes(k)) {
      throw new CompileError(
        `retract(): column '${k}' does not exist on relation '${rel.relationName}'`,
        `Available: ${rel.columns.join(', ')}`,
      );
    }
  }
  const vars = columnVars(rel);
  const args = rel.columns.map((c, i) => (key[c] !== undefined ? compileValue(key[c]) : vars[i]));
  const atom = `${rel.relationName}(${args.join(', ')})`;
  return `-${atom} <- ${[atom, ...extra].join(', ')}`;
}

function isFullRow(rel: RelationDef, fact: Fact): boolean {
  return rel.columns.every((c) => fact[c] !== undefined);
}

/** Fact-write statements of one statement, conditioned on `cond` when given. */
function writeStatements(s: Statement, cond?: string): string[] {
  switch (s.kind) {
    case 'insert':
      if (cond === undefined) {
        for (const f of s.facts) rowValues(s.rel, f, 'insert()');
        return [s.facts.length === 1 ? compileInsert(s.rel, s.facts[0]) : compileBulkInsert(s.rel, s.facts)];
      }
      return s.facts.map(
        (f) => `${ghost(s.rel)}, +${s.rel.relationName}(${rowValues(s.rel, f, 'insert()').join(', ')}) <- ${cond}`,
      );
    case 'retract': {
      const atom = `${s.rel.relationName}(${rowValues(s.rel, s.fact, 'retract()').join(', ')})`;
      return [cond === undefined ? `-${atom}` : `-${atom} <- ${atom}, ${cond}`];
    }
    case 'retractKey':
      return [keyedDelete(s.rel, s.key, cond === undefined ? [] : [cond])];
    default:
      throw new InternalError(`not a fact write: ${s.kind}`);
  }
}

function isFactWrite(s: Statement): boolean {
  return s.kind === 'insert' || s.kind === 'retract' || s.kind === 'retractKey';
}

/** Rule and schema statements: unconditional, so a guard can only stop them by aborting. */
function plainStatement(s: Statement): string {
  switch (s.kind) {
    case 'schema':
      return compileSchema(s.rel);
    case 'rule':
      return s.text;
    case 'clearRule':
      return `.rule clear ${s.name}`;
    default:
      throw new InternalError(`not a plain statement: ${s.kind}`);
  }
}

/** A token id unique to one program. */
export function newTokenId(): string {
  const uuid = globalThis.crypto?.randomUUID?.();
  return `t-${uuid ?? `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 12)}`}`;
}

const constInsert = (c: ConstRow) => `+${c.relation}(${c.literal})`;
const constDelete = (c: ConstRow) => `-${c.relation}(${c.literal})`;

/**
 * A batch of writes committed as one request and one transaction:
 * `kg.program()` builds one. Methods chain.
 *
 * @example
 * const r = await kg.program()
 *   .retract(CarrierNote, { shipment: "S-77" })
 *   .insert(CarrierNote, { shipment: "S-77", reason: "weather_delay" })
 *   .when(any(Attempt, { attempt: "att-9f3" }), NOT(any(AttemptDone, { attempt: "att-9f3" })))
 *   .commit();
 */
export class Program {
  private readonly statements: Statement[] = [];
  private readonly guards: BoolExpr[] = [];

  /** @internal `runner` sends the program; `kg.program()` supplies it. */
  constructor(private readonly runner?: (program: Program, strict: boolean) => Promise<ProgramResult>) {}

  /**
   * Send the program as one request and one transaction.
   *
   * With a guard and `strict: true` (the default), a guard that does not
   * hold rejects with `PreconditionFailed` and nothing is applied, rules and
   * schema included. With `strict: false` it resolves with `applied: false`
   * instead; that form cannot hold rules or schema.
   */
  async commit(opts?: { strict?: boolean }): Promise<ProgramResult> {
    if (this.runner === undefined) {
      throw new InternalError('This program is not bound to a knowledge graph; build it with kg.program()');
    }
    return this.runner(this, opts?.strict ?? true);
  }

  /** Insert facts. */
  insert(rel: RelationDef, facts: Fact | Fact[]): this {
    const list = Array.isArray(facts) ? facts : [facts];
    if (list.length > 0) this.statements.push({ kind: 'insert', rel, facts: list });
    return this;
  }

  /**
   * Retract a row (every column given), or every row matching the given
   * columns: `retract(Eta, { shipment: "S-77" })`.
   */
  retract(rel: RelationDef, rowOrKey: Fact): this {
    this.statements.push(
      isFullRow(rel, rowOrKey) ? { kind: 'retract', rel, fact: rowOrKey } : { kind: 'retractKey', rel, key: rowOrKey },
    );
    return this;
  }

  /** Declare relation schemas in the program (unconditional: needs the abort form). */
  define(...rels: RelationDef[]): this {
    for (const rel of rels) this.statements.push({ kind: 'schema', rel });
    return this;
  }

  /** Add persistent rule clauses in the program (unconditional: needs the abort form). */
  defineRules(headName: string, headColumns: string[], clauses: RuleClause[]): this {
    for (const clause of clauses) {
      this.statements.push({ kind: 'rule', text: compileRule(headName, headColumns, clause, true) });
    }
    return this;
  }

  /** Remove every clause of an existing rule (unconditional: needs the abort form). */
  clearRule(name: string): this {
    this.statements.push({ kind: 'clearRule', name });
    return this;
  }

  /**
   * Make the whole program conditional: it applies only when every
   * condition holds at commit. Conditions are `any()`/`NOT(any())` atoms and
   * comparisons over their columns; repeated calls add conditions.
   */
  when(...conditions: BoolExpr[]): this {
    this.guards.push(...conditions);
    return this;
  }

  /** True when the program holds statements a token cannot condition. */
  get hasRulesOrSchema(): boolean {
    return this.statements.some((s) => !isFactWrite(s));
  }

  get guarded(): boolean {
    return this.guards.length > 0;
  }

  /** The IQL this program sends (with a fresh token id each call). */
  iql(opts?: { strict?: boolean }): string {
    return this.compile(opts?.strict ?? true).iql;
  }

  /** @internal Compile to IQL; `token` fixes the token id for tests. */
  compile(strict: boolean, token = newTokenId()): CompiledProgram {
    if (this.statements.length === 0) {
      throw new CompileError('The program holds no statement', 'Add insert(), retract() or define() calls first');
    }
    if (!this.guarded) {
      const lines: string[] = [];
      const writeIndexes: number[] = [];
      for (const s of this.statements) {
        if (isFactWrite(s)) {
          for (const line of writeStatements(s)) {
            writeIndexes.push(lines.length);
            lines.push(line);
          }
        } else {
          lines.push(plainStatement(s));
        }
      }
      return { iql: lines.join('\n'), writeIndexes };
    }

    if (!strict && this.hasRulesOrSchema) {
      throw new CompileError(
        'strict: false cannot guard a program holding rules or schema: a guard can only stop them by aborting the program',
        'Commit it with strict: true (the default)',
      );
    }
    const guard = compileGuard(this.guards);
    if (guard.body === '') {
      throw new CompileError('The guard has no condition', 'Pass any()/NOT(any()) conditions to when()');
    }
    const tokenAtom = `${TXN}(${compileValue(token)})`;
    const lines: string[] = guard.constRows.map(constInsert);
    const writeIndexes: number[] = [];
    if (strict) lines.push(`+${TXN_PENDING}(${compileValue(token)})`);
    const tokenIndex = lines.length;
    lines.push(`-${TXN}(""), +${tokenAtom} <- ${guard.body}`);
    let assertIndex: number | undefined;
    if (strict) {
      // When the token is absent this inserts a string into an int relation:
      // the engine rejects the statement and rolls the whole program back,
      // rules and schema included.
      assertIndex = lines.length;
      lines.push(
        `-${ASSERT}(0), +${ASSERT}(${compileValue(`precondition_failed:${token}`)}) <- ` +
          `${TXN_PENDING}(K), K = ${compileValue(token)}, !${TXN}(K)`,
      );
    }
    for (const s of this.statements) {
      if (isFactWrite(s)) {
        for (const line of writeStatements(s, tokenAtom)) {
          writeIndexes.push(lines.length);
          lines.push(line);
        }
      } else {
        lines.push(plainStatement(s));
      }
    }
    lines.push(`-${tokenAtom} <- ${tokenAtom}`);
    if (strict) lines.push(`-${TXN_PENDING}(${compileValue(token)})`);
    lines.push(...guard.constRows.map(constDelete));
    return { iql: lines.join('\n'), tokenIndex, assertIndex, writeIndexes };
  }
}

/** What a committed program did. */
export interface ProgramResult {
  /** False only for a guarded `strict: false` program whose guard did not hold. */
  applied: boolean;
  /** Facts newly stored by the program's writes. */
  inserted: number;
  /** Facts removed by the program's writes. */
  deleted: number;
  /** The program that was sent. */
  iql: string;
}

/** Outcome of `kg.claim()`. */
export interface Claim<R> {
  /** True when the claimed row holds the key after the commit. */
  won: boolean;
  /**
   * The row holding the key: the claimed row when won, the row already
   * there when lost, or null when the `when` guard did not hold.
   */
  holder: R | null;
}

/** Options of `kg.claim()`. */
export interface ClaimOptions<T extends string = string> {
  /** Conditions that must hold at commit: any()/NOT(any()) atoms and comparisons over their columns. */
  when?: BoolExpr | BoolExpr[];
  /**
   * The row that, when present, means the claim is taken: usually
   * `any(Rel, { ...key columns })`. Defaults to the `key` columns of the row.
   */
  unless?: BoolExpr;
  /**
   * Columns that identify the claim; the holder is read back by them.
   * Defaults to the columns `unless` binds, else every column.
   */
  key?: T[];
}

/** A compiled claim: one program, ending with the query that reads the holder. */
export interface CompiledClaim {
  iql: string;
}

/**
 * `claim()` (R-CLAIM): the guarded insert of `row` with `unless` as a
 * negated atom in the guard, then a query on the key in the same program.
 * The query sees the staged insert, so its rows say who holds the key.
 */
export function compileClaim(rel: RelationDef, row: Fact, opts: ClaimOptions = {}): CompiledClaim & { key: string[] } {
  const values = rowValues(rel, row, 'claim()');
  for (const k of opts.key ?? []) {
    if (!rel.columns.includes(k)) {
      throw new CompileError(
        `claim(): key column '${k}' does not exist on relation '${rel.relationName}'`,
        `Available: ${rel.columns.join(', ')}`,
      );
    }
  }
  let unless = opts.unless;
  if (unless === undefined && opts.key !== undefined) {
    unless = any(rel, Object.fromEntries(opts.key.map((k) => [k, row[k]])));
  }
  const key =
    opts.key ??
    (unless !== undefined && isAnyExpr(unless) && unless.relation === rel.relationName
      ? Object.keys(unless.bindings)
      : rel.columns);

  const when = opts.when === undefined ? [] : Array.isArray(opts.when) ? opts.when : [opts.when];
  const guard = compileGuard([...when, ...(unless !== undefined ? [astNot(unless)] : [])]);
  const insert = guard.body === ''
    ? `+${rel.relationName}(${values.join(', ')})`
    : `${ghost(rel)}, +${rel.relationName}(${values.join(', ')}) <- ${guard.body}`;

  const vars = columnVars(rel);
  const queryArgs = rel.columns.map((c, i) => (key.includes(c) ? values[i] : vars[i]));
  const lines = [
    ...guard.constRows.map(constInsert),
    insert,
    ...guard.constRows.map(constDelete),
    `?${rel.relationName}(${queryArgs.join(', ')})`,
  ];
  return { iql: lines.join('\n'), key };
}

// ── Reading replies ─────────────────────────────────────────────────

/** Counts of one write statement's reply. */
export interface WriteCounts {
  inserted: number;
  deleted: number;
}

// The engine reports per-statement outcomes only as text, so counts are read
// from these replies. The grammar is pinned by a live test against every
// engine build in CI.
const INSERTED = /^Inserted (\d+) fact\(s\) into '.*'\.$/;
const UPDATED = /^Update: (\d+) deleted, (\d+) inserted\.$/;
const COND_DELETED = /^Conditional delete: (\d+) fact\(s\) deleted from '.*'\.$/;
const DELETED = /^Deleted (\d+) facts from '.*'\.$/;

/** Counts of a write statement's reply message. */
export function parseWriteMessage(message: string): WriteCounts {
  let m = INSERTED.exec(message);
  if (m) return { inserted: Number(m[1]), deleted: 0 };
  m = UPDATED.exec(message);
  if (m) return { inserted: Number(m[2]), deleted: Number(m[1]) };
  m = COND_DELETED.exec(message) ?? DELETED.exec(message);
  if (m) return { inserted: 0, deleted: Number(m[1]) };
  throw new InternalError(`Unexpected write reply from the engine: ${JSON.stringify(message)}`);
}
