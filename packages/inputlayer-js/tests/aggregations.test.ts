import { describe, it, expect } from 'vitest';
import { relation } from '../src/relation';
import { count, countDistinct, sum, min, max, avg, topK, topKThreshold, withinRadius } from '../src/aggregations';

describe('aggregations', () => {
  const Employee = relation('Employee', {
    id: 'int',
    department: 'string',
    salary: 'float',
  });

  it('creates count without column', () => {
    const expr = count();
    expect(expr._tag).toBe('AggExpr');
    expect(expr.func).toBe('count');
    expect(expr.column).toBeUndefined();
  });

  it('creates count with column', () => {
    const expr = count(Employee.col('id'));
    expect(expr.func).toBe('count');
    expect(expr.column).toBeDefined();
  });

  it('creates count_distinct', () => {
    const expr = countDistinct(Employee.col('department'));
    expect(expr.func).toBe('count_distinct');
  });

  it('creates sum', () => {
    const expr = sum(Employee.col('salary'));
    expect(expr.func).toBe('sum');
  });

  it('creates min', () => {
    const expr = min(Employee.col('salary'));
    expect(expr.func).toBe('min');
  });

  it('creates max', () => {
    const expr = max(Employee.col('salary'));
    expect(expr.func).toBe('max');
  });

  it('creates avg', () => {
    const expr = avg(Employee.col('salary'));
    expect(expr.func).toBe('avg');
  });

  it('creates top_k', () => {
    const expr = topK({
      k: 10,
      orderBy: Employee.col('salary'),
      desc: true,
    });
    expect(expr.func).toBe('top_k');
    expect(expr.params).toEqual([10]);
    expect(expr.desc).toBe(true);
  });

  it('creates top_k with passthrough', () => {
    const expr = topK({
      k: 5,
      passthrough: [Employee.col('id'), Employee.col('department')],
      orderBy: Employee.col('salary'),
    });
    expect(expr.passthrough).toHaveLength(2);
  });

  it('creates top_k_threshold', () => {
    const expr = topKThreshold({
      k: 10,
      threshold: 50000,
      orderBy: Employee.col('salary'),
    });
    expect(expr.func).toBe('top_k_threshold');
    expect(expr.params).toEqual([10, 50000]);
  });

  it('creates within_radius', () => {
    const expr = withinRadius({
      maxDistance: 0.5,
      distance: Employee.col('salary'),
    });
    expect(expr.func).toBe('within_radius');
    expect(expr.params).toEqual([0.5]);
  });
});
