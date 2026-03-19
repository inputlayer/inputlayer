import { describe, it, expect } from 'vitest';
import { relation } from '../src/relation';
import {
  compileSchema,
  compileInsert,
  compileBulkInsert,
  compileDelete,
  compileConditionalDelete,
  compileQuery,
  compileRule,
} from '../src/compiler';
import { count, sum, avg } from '../src/aggregations';

const Employee = relation('Employee', {
  id: 'int',
  name: 'string',
  department: 'string',
  salary: 'float',
  active: 'bool',
});

const Department = relation('Department', {
  name: 'string',
  budget: 'float',
});

describe('compileSchema', () => {
  it('compiles a relation schema', () => {
    expect(compileSchema(Employee)).toBe(
      '+employee(id: int, name: string, department: string, salary: float, active: bool)',
    );
  });
});

describe('compileInsert', () => {
  it('compiles a single insert', () => {
    const result = compileInsert(Employee, {
      id: 1,
      name: 'Alice',
      department: 'eng',
      salary: 120000.0,
      active: true,
    });
    expect(result).toBe('+employee(1, "Alice", "eng", 120000, true)');
  });

  it('compiles a session (ephemeral) insert', () => {
    const result = compileInsert(
      Employee,
      { id: 1, name: 'Alice', department: 'eng', salary: 120000.0, active: true },
      false,
    );
    expect(result).toBe('employee(1, "Alice", "eng", 120000, true)');
  });
});

describe('compileBulkInsert', () => {
  it('compiles a bulk insert', () => {
    const result = compileBulkInsert(Employee, [
      { id: 1, name: 'Alice', department: 'eng', salary: 120000.0, active: true },
      { id: 2, name: 'Bob', department: 'sales', salary: 100000.0, active: false },
    ]);
    expect(result).toBe(
      '+employee[(1, "Alice", "eng", 120000, true), (2, "Bob", "sales", 100000, false)]',
    );
  });
});

describe('compileDelete', () => {
  it('compiles a single delete', () => {
    const result = compileDelete(Employee, {
      id: 1,
      name: 'Alice',
      department: 'eng',
      salary: 120000.0,
      active: true,
    });
    expect(result).toBe('-employee(1, "Alice", "eng", 120000, true)');
  });
});

describe('compileConditionalDelete', () => {
  it('compiles a conditional delete', () => {
    const condition = Employee.col('department').eq('sales');
    const result = compileConditionalDelete(Employee, condition);
    expect(result).toBe(
      '-employee(X0, X1, X2, X3, X4) <- employee(X0, X1, X2, X3, X4), X2 = "sales"',
    );
  });
});

describe('compileQuery', () => {
  it('compiles a simple full-relation query', () => {
    const result = compileQuery({ select: [Employee], join: [Employee] });
    expect(result).toBe(
      '?Id, Name, Department, Salary, Active <- employee(Id, Name, Department, Salary, Active)',
    );
  });

  it('compiles a query with column selection', () => {
    const result = compileQuery({
      select: [Employee.col('name').toAst(), Employee.col('salary').toAst()],
      join: [Employee],
    });
    // Should select just Name, Salary from employee
    expect(result).toContain('Name');
    expect(result).toContain('Salary');
    expect(result).toContain('employee(');
  });

  it('compiles a query with where filter', () => {
    const result = compileQuery({
      select: [Employee],
      join: [Employee],
      where: Employee.col('department').eq('eng'),
    });
    expect(result).toContain('Department = "eng"');
  });

  it('compiles a query with join', () => {
    const result = compileQuery({
      select: [Employee.col('name').toAst(), Department.col('budget').toAst()],
      join: [Employee, Department],
      on: Employee.col('department').eq(Department.col('name')),
    });
    // Should have shared variable for the join
    expect(result).toContain('employee(');
    expect(result).toContain('department(');
  });

  it('compiles a query with limit', () => {
    const result = compileQuery({
      select: [Employee],
      join: [Employee],
      limit: 10,
    });
    expect(result).toContain('limit(10)');
  });

  it('compiles a query with limit and offset', () => {
    const result = compileQuery({
      select: [Employee],
      join: [Employee],
      limit: 10,
      offset: 5,
    });
    expect(result).toContain('limit(10, 5)');
  });

  it('compiles a query with order by', () => {
    const result = compileQuery({
      select: [Employee],
      join: [Employee],
      orderBy: Employee.col('salary').desc(),
    });
    expect(result).toContain('Salary:desc');
  });

  it('compiles an aggregation query', () => {
    const result = compileQuery({
      select: [
        Employee.col('department').toAst(),
        count(Employee.col('id')),
        avg(Employee.col('salary')),
      ],
      join: [Employee],
    });
    expect(result).toContain('count<');
    expect(result).toContain('avg<');
    expect(result).toContain('Department');
  });
});

describe('compileRule', () => {
  const Edge = relation('Edge', { src: 'int', dst: 'int' });

  it('compiles a simple rule', () => {
    const result = compileRule('reachable', ['src', 'dst'], {
      relations: [{ name: 'edge', def: Edge }],
      selectMap: {
        src: Edge.col('src').toAst(),
        dst: Edge.col('dst').toAst(),
      },
    });
    expect(result).toBe('+reachable(Src, Dst) <- edge(Src, Dst)');
  });

  it('compiles a session rule', () => {
    const result = compileRule(
      'reachable',
      ['src', 'dst'],
      {
        relations: [{ name: 'edge', def: Edge }],
        selectMap: {
          src: Edge.col('src').toAst(),
          dst: Edge.col('dst').toAst(),
        },
      },
      false,
    );
    expect(result).toBe('reachable(Src, Dst) <- edge(Src, Dst)');
  });
});
