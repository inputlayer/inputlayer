import { describe, it, expect } from 'vitest';
import { ResultSet } from '../src/result';

describe('ResultSet', () => {
  const rs = new ResultSet({
    columns: ['id', 'name', 'salary'],
    rows: [
      [1, 'Alice', 120000],
      [2, 'Bob', 100000],
      [3, 'Charlie', 110000],
    ],
    executionTimeMs: 5,
  });

  it('has correct length', () => {
    expect(rs.length).toBe(3);
  });

  it('is not empty', () => {
    expect(rs.isEmpty).toBe(false);
  });

  it('gets a row by index', () => {
    const row = rs.get(0);
    expect(row.id).toBe(1);
    expect(row.name).toBe('Alice');
    expect(row.salary).toBe(120000);
  });

  it('gets the first row', () => {
    const first = rs.first();
    expect(first?.name).toBe('Alice');
  });

  it('returns undefined for first() on empty set', () => {
    const empty = new ResultSet({ columns: ['x'], rows: [] });
    expect(empty.first()).toBeUndefined();
  });

  it('extracts a scalar', () => {
    const scalar = new ResultSet({ columns: ['count'], rows: [[42]] });
    expect(scalar.scalar()).toBe(42);
  });

  it('throws on scalar from empty set', () => {
    const empty = new ResultSet({ columns: ['x'], rows: [] });
    expect(() => empty.scalar()).toThrow();
  });

  it('converts to dicts', () => {
    const dicts = rs.toDicts();
    expect(dicts).toHaveLength(3);
    expect(dicts[0]).toEqual({ id: 1, name: 'Alice', salary: 120000 });
  });

  it('converts to tuples', () => {
    const tuples = rs.toTuples();
    expect(tuples[0]).toEqual([1, 'Alice', 120000]);
  });

  it('is iterable', () => {
    const names: string[] = [];
    for (const row of rs) {
      names.push(row.name as string);
    }
    expect(names).toEqual(['Alice', 'Bob', 'Charlie']);
  });

  it('auto-computes rowCount and totalCount', () => {
    expect(rs.rowCount).toBe(3);
    expect(rs.totalCount).toBe(3);
  });
});
