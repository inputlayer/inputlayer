import { describe, it, expect } from 'vitest';
import { relation } from '../src/relation';
import { from } from '../src/derived';
import { compileRule } from '../src/compiler';

describe('from() builder', () => {
  const Edge = relation('Edge', { src: 'int', dst: 'int' });

  it('builds a simple rule clause', () => {
    const clause = from(Edge).select({
      src: Edge.col('src'),
      dst: Edge.col('dst'),
    });

    expect(clause.relations).toHaveLength(1);
    expect(clause.relations[0].name).toBe('edge');
    expect(clause.condition).toBeUndefined();
    expect(Object.keys(clause.selectMap)).toEqual(['src', 'dst']);
  });

  it('builds a rule clause with where', () => {
    const Reachable = relation('Reachable', { src: 'int', dst: 'int' });

    const clause = from(Reachable, Edge)
      .where((r, e) => r.col('dst').eq(e.col('src')))
      .select({
        src: Reachable.col('src'),
        dst: Edge.col('dst'),
      });

    expect(clause.relations).toHaveLength(2);
    expect(clause.condition).toBeDefined();
    expect(clause.condition?._tag).toBe('Comparison');
  });

  it('integrates with compileRule', () => {
    const clause = from(Edge).select({
      src: Edge.col('src'),
      dst: Edge.col('dst'),
    });

    const datalog = compileRule('reachable', ['src', 'dst'], clause);
    expect(datalog).toBe('+reachable(Src, Dst) <- edge(Src, Dst)');
  });

  it('integrates with recursive rule', () => {
    const Reachable = relation('Reachable', { src: 'int', dst: 'int' });

    const clause = from(Reachable, Edge)
      .where((r, e) => r.col('dst').eq(e.col('src')))
      .select({
        src: Reachable.col('src'),
        dst: Edge.col('dst'),
      });

    const datalog = compileRule('reachable', ['src', 'dst'], clause);
    // Should have both body relations and shared variable for the join
    expect(datalog).toContain('reachable(');
    expect(datalog).toContain('edge(');
    expect(datalog).toContain('<-');
  });
});
