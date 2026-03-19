import { describe, it, expect } from 'vitest';
import { relation, compileValue, RelationDef } from '../src/relation';
import { Timestamp } from '../src/types';

describe('relation', () => {
  it('creates a relation def with snake_case name', () => {
    const Employee = relation('Employee', {
      id: 'int',
      name: 'string',
      salary: 'float',
    });

    expect(Employee.relationName).toBe('employee');
    expect(Employee.columns).toEqual(['id', 'name', 'salary']);
    expect(Employee.columnTypes).toEqual({ id: 'int', name: 'string', salary: 'float' });
  });

  it('supports custom name override', () => {
    const MyRel = relation('MyRel', { x: 'int' }, { name: 'custom_rel' });
    expect(MyRel.relationName).toBe('custom_rel');
  });

  it('provides column proxies', () => {
    const Employee = relation('Employee', { id: 'int', name: 'string' });
    const proxy = Employee.col('id');
    expect(proxy.relation).toBe('employee');
    expect(proxy.name).toBe('id');
  });

  it('throws on unknown column', () => {
    const Employee = relation('Employee', { id: 'int' });
    expect(() => Employee.col('nonexistent')).toThrow('does not exist');
  });

  it('creates refs for self-joins', () => {
    const Follow = relation('Follow', { follower: 'int', followee: 'int' });
    const refs = Follow.refs(2);
    expect(refs).toHaveLength(2);
    expect(refs[0].alias).toBe('follow_1');
    expect(refs[1].alias).toBe('follow_2');
  });
});

describe('compileValue', () => {
  it('compiles null', () => {
    expect(compileValue(null)).toBe('null');
  });

  it('compiles booleans', () => {
    expect(compileValue(true)).toBe('true');
    expect(compileValue(false)).toBe('false');
  });

  it('compiles integers', () => {
    expect(compileValue(42)).toBe('42');
  });

  it('compiles floats', () => {
    expect(compileValue(3.14)).toBe('3.14');
  });

  it('compiles strings with escaping', () => {
    expect(compileValue('hello')).toBe('"hello"');
    expect(compileValue('say "hi"')).toBe('"say \\"hi\\""');
    expect(compileValue('back\\slash')).toBe('"back\\\\slash"');
  });

  it('compiles vectors', () => {
    expect(compileValue([1.0, 2.0, 3.0])).toBe('[1, 2, 3]');
  });

  it('compiles timestamps', () => {
    const ts = new Timestamp(1708732800000);
    expect(compileValue(ts)).toBe('1708732800000');
  });
});
