import { describe, it, expect } from 'vitest';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { meta as commands, sessionRules, ruleList, ruleClauses } from '../src/meta';
// Fixture names are snake_case; the table's keys are lowerCamel.
const key = (name: string) => name.replace(/_([a-z])/g, (_, c: string) => c.toUpperCase());

// The fixture the command table is rendered against (the Python SDK is to adopt it too).
const fixture = JSON.parse(
  readFileSync(resolve(__dirname, '../../conformance/meta-commands.json'), 'utf8'),
);

describe('meta command table', () => {
  it('renders every fixture command exactly', () => {
    for (const { name, args, text } of fixture.commands) {
      const build = commands[key(name) as keyof typeof commands] as (...a: unknown[]) => string;
      expect(build, name).toBeTypeOf('function');
      expect(build(...args)).toBe(text);
    }
  });

  it('has no command the fixture does not pin', () => {
    const pinned = new Set(fixture.commands.map((c: { name: string }) => key(c.name)));
    expect(Object.keys(commands).filter((k) => !pinned.has(k))).toEqual([]);
  });

  it('never renders a spelling the engine rejects', () => {
    const rendered = fixture.commands.map((c: { text: string }) => c.text);
    for (const { text } of fixture.rejected) expect(rendered).not.toContain(text);
  });
});

describe('meta reply parsers', () => {
  it('reads session rules from a .session reply', () => {
    for (const { rows, rules } of fixture.replies.session_list) {
      expect(sessionRules(rows)).toEqual(rules);
    }
  });

  it('reads rules from a .rule list reply', () => {
    for (const { rows, rules } of fixture.replies.rule_list) {
      expect(ruleList(rows)).toEqual(
        rules.map((r: { name: string; clause_count: number }) => ({ name: r.name, clauseCount: r.clause_count })),
      );
    }
  });

  it('reads clauses from a .rule def reply', () => {
    for (const { rows, clauses } of fixture.replies.rule_def) {
      expect(ruleClauses(rows)).toEqual(clauses);
    }
  });
});
