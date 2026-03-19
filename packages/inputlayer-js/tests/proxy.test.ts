import { describe, it, expect } from 'vitest';
import { ColumnProxy, AND, OR, NOT, RelationProxy } from '../src/proxy';
import { relation } from '../src/relation';

describe('ColumnProxy', () => {
  const Employee = relation('Employee', {
    id: 'int',
    name: 'string',
    salary: 'float',
  });

  it('creates comparison BoolExprs', () => {
    const col = Employee.col('salary');
    const expr = col.gt(100000);
    expect(expr._tag).toBe('Comparison');
    expect(expr.op).toBe('>');
  });

  it('creates arithmetic Exprs', () => {
    const col = Employee.col('salary');
    const expr = col.mul(12);
    expect(expr._tag).toBe('Arithmetic');
    expect(expr.op).toBe('*');
  });

  it('creates ordered columns', () => {
    const col = Employee.col('salary');
    const asc = col.asc();
    expect(asc._tag).toBe('OrderedColumn');
    expect(asc.descending).toBe(false);

    const desc = col.desc();
    expect(desc.descending).toBe(true);
  });

  it('creates in expressions', () => {
    const Other = relation('Other', { id: 'int' });
    const expr = Employee.col('id').in(Other.col('id'));
    expect(expr._tag).toBe('InExpr');
  });

  it('creates notIn expressions', () => {
    const Other = relation('Other', { id: 'int' });
    const expr = Employee.col('id').notIn(Other.col('id'));
    expect(expr._tag).toBe('NegatedIn');
  });
});

describe('Boolean combinators', () => {
  const Employee = relation('Employee', {
    id: 'int',
    department: 'string',
    active: 'bool',
  });

  it('AND combines two conditions', () => {
    const cond = AND(
      Employee.col('department').eq('eng'),
      Employee.col('active').eq(true),
    );
    expect(cond._tag).toBe('And');
  });

  it('OR combines two conditions', () => {
    const cond = OR(
      Employee.col('department').eq('eng'),
      Employee.col('department').eq('sales'),
    );
    expect(cond._tag).toBe('Or');
  });

  it('NOT negates a condition', () => {
    const cond = NOT(Employee.col('active').eq(false));
    expect(cond._tag).toBe('Not');
  });
});

describe('RelationProxy', () => {
  it('returns column proxies via col()', () => {
    const proxy = new RelationProxy('employee');
    const col = proxy.col('name');
    expect(col.relation).toBe('employee');
    expect(col.name).toBe('name');
  });
});
