import { describe, it, expect } from 'vitest';
import { relation } from '../src/relation';
import {
  compileSchema,
  compileInsert,
  compileBulkInsert,
  compileDelete,
  compileConditionalDelete,
  compileQuery,
  compileQueryPlan,
  resultColumnIndexes,
  compileRule,
} from '../src/compiler';
import { count, sum, avg, topK } from '../src/aggregations';
import { AND, OR } from '../src/proxy';
import { from } from '../src/derived';
import { Timestamp } from '../src/types';

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

describe('compileSchema timestamp', () => {
  const Event = relation('Event', { id: 'int', occurredAt: 'timestamp' });

  it('declares a timestamp column as int', () => {
    expect(compileSchema(Event)).toBe('+event(id: int, occurredAt: int)');
  });

  it('writes Timestamp and Date values as Unix milliseconds', () => {
    expect(compileInsert(Event, { id: 1, occurredAt: new Timestamp(1704067200000) })).toBe('+event(1, 1704067200000)');
    expect(compileInsert(Event, { id: 2, occurredAt: new Date(1704153600000) })).toBe('+event(2, 1704153600000)');
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
  // The engine rejects `<-` in a query, so a query is always `?atom, body...`.

  it('compiles a simple full-relation query', () => {
    expect(compileQuery({ select: [Employee] })).toBe(
      '?employee(Id, Name, Department, Salary, Active)',
    );
    expect(compileQuery({ select: [Employee], join: [Employee] })).toBe(
      '?employee(Id, Name, Department, Salary, Active)',
    );
  });

  it('compiles a query with column selection and a where filter', () => {
    const plan = compileQueryPlan({
      select: [Employee.col('name').toAst(), Employee.col('salary').toAst()],
      join: [Employee],
      where: Employee.col('department').eq('eng'),
    });
    expect(plan.programs).toEqual([
      '?employee(Id, Name, Department, Salary, Active), Department = "eng"',
    ]);
    expect(plan.outputs).toEqual([
      { label: 'Name', variable: 'Name' },
      { label: 'Salary', variable: 'Salary' },
    ]);
  });

  it('compiles a query with join', () => {
    expect(
      compileQuery({
        select: [Employee.col('name').toAst(), Department.col('budget').toAst()],
        join: [Employee, Department],
        on: Employee.col('department').eq(Department.col('name')),
      }),
    ).toBe('?employee(Id, Name, Department, Salary, Active), department(Department, Budget)');
  });

  it('keeps non-equality join conditions as filters', () => {
    const [e1, e2] = Employee.refs(2);
    expect(
      compileQuery({
        select: [e1.col('name').toAst(), e2.col('name').toAst()],
        join: [e1, e2],
        on: AND(e1.col('department').eq(e2.col('department')), e1.col('id').ne(e2.col('id'))),
      }),
    ).toBe(
      '?employee(Id, Name, Department, Salary, Active), ' +
        'employee(Id_1, Name_2, Department, Salary_3, Active_4), Id != Id_1',
    );
  });

  it('binds computed columns in the body', () => {
    const plan = compileQueryPlan({
      select: [Employee.col('name').toAst()],
      join: [Employee],
      computed: { bonus: Employee.col('salary').mul(0.1) },
    });
    expect(plan.programs).toEqual(['?employee(Id, Name, Department, Salary, Active), Bonus = Salary * 0.1']);
    expect(plan.outputs.map((o) => o.label)).toEqual(['Name', 'Bonus']);
  });

  it('compiles a query with limit', () => {
    expect(compileQuery({ select: [Employee], join: [Employee], limit: 10 })).toBe(
      '?employee(Id, Name, Department, Salary, Active), limit(10)',
    );
  });

  it('compiles a query with limit and offset', () => {
    expect(compileQuery({ select: [Employee], join: [Employee], limit: 10, offset: 5 })).toBe(
      '?employee(Id, Name, Department, Salary, Active), limit(10, 5)',
    );
  });

  it('compiles a query with order by as a sort annotation in the first atom', () => {
    expect(
      compileQuery({ select: [Employee], join: [Employee], orderBy: Employee.col('salary').desc() }),
    ).toBe('?employee(Id, Name, Department, Salary:desc, Active)');
  });

  it("moves the ordered column's relation first", () => {
    expect(
      compileQuery({
        select: [Employee.col('name').toAst()],
        join: [Employee, Department],
        on: Employee.col('department').eq(Department.col('name')),
        orderBy: Department.col('budget').desc(),
        limit: 2,
      }),
    ).toBe('?department(Department, Budget:desc), employee(Id, Name, Department, Salary, Active), limit(2)');
  });

  it('splits OR into branches merged client-side', () => {
    const plan = compileQueryPlan({
      select: [Employee.col('name').toAst()],
      join: [Employee],
      where: OR(Employee.col('department').eq('hr'), Employee.col('salary').gt(115000)),
      orderBy: Employee.col('salary').desc(),
      limit: 3,
      offset: 1,
    });
    expect(plan.programs).toEqual([
      '?employee(Id, Name, Department, Salary:desc, Active), Department = "hr", limit(4)',
      '?employee(Id, Name, Department, Salary:desc, Active), Salary > 115000, limit(4)',
    ]);
    expect(plan.page).toEqual({ order: { variable: 'Salary', descending: true }, limit: 3, offset: 1 });
  });

  it('compiles an aggregation query to a program-local rule', () => {
    const plan = compileQueryPlan({
      select: [
        Employee.col('department').toAst(),
        count(Employee.col('id')),
        avg(Employee.col('salary')),
      ],
      join: [Employee],
      orderBy: Employee.col('department').asc(),
      limit: 5,
    });
    expect(plan.programs).toEqual([
      'il_sdk_agg(Department, count<Id>, avg<Salary>) <- employee(Id, Name, Department, Salary, Active)\n' +
        '?il_sdk_agg(Department:asc, CountId, AvgSalary), limit(5)',
    ]);
    expect(plan.outputs.map((o) => o.label)).toEqual(['Department', 'CountId', 'AvgSalary']);
    expect(plan.debug).toBe(
      'il_sdk_agg(Department, count<Id>, avg<Salary>) <- employee(Id, Name, Department, Salary, Active)',
    );
  });

  it('gives each aggregate of the same column its own result column', () => {
    const plan = compileQueryPlan({
      select: [avg(Employee.col('salary')), sum(Employee.col('salary'))],
      join: [Employee],
    });
    expect(plan.outputs.map((o) => o.variable)).toEqual(['AvgSalary', 'SumSalary']);
  });

  it('expands topK into its passthrough and ordered columns', () => {
    const plan = compileQueryPlan({
      select: [
        Employee.col('department').toAst(),
        topK({ k: 3, passthrough: [Employee.col('name')], orderBy: Employee.col('salary'), desc: true }),
      ],
      join: [Employee],
    });
    expect(plan.programs).toEqual([
      'il_sdk_agg(Department, top_k<3, Name, Salary:desc>) <- employee(Id, Name, Department, Salary, Active)\n' +
        '?il_sdk_agg(Department, Name, Salary)',
    ]);
  });

  it('aggregates over the union of OR branches', () => {
    expect(
      compileQuery({
        select: [count(Employee.col('id'))],
        join: [Employee],
        where: OR(Employee.col('department').eq('hr'), Employee.col('salary').gt(115000)),
      }),
    ).toBe(
      'il_sdk_agg_src(Id, Name, Department, Salary, Active) <- employee(Id, Name, Department, Salary, Active), Department = "hr"\n' +
        'il_sdk_agg_src(Id, Name, Department, Salary, Active) <- employee(Id, Name, Department, Salary, Active), Salary > 115000\n' +
        'il_sdk_agg(count<Id>) <- il_sdk_agg_src(Id, Name, Department, Salary, Active)\n' +
        '?il_sdk_agg(CountId)',
    );
  });

  it('rejects a query without a relation', () => {
    expect(() => compileQuery({ select: [Employee.col('name').toAst()] })).toThrow(/at least one relation/);
  });
});

describe('count() without a column', () => {
  it('counts the first variable of the first atom', () => {
    const plan = compileQueryPlan({
      select: [Employee.col('department').toAst(), count()],
      join: [Employee],
    });
    expect(plan.programs).toEqual([
      'il_sdk_agg(Department, count<Id>) <- employee(Id, Name, Department, Salary, Active)\n' +
        '?il_sdk_agg(Department, Count)',
    ]);
    expect(plan.outputs.map((o) => o.label)).toEqual(['Department', 'Count']);
  });

  it('counts the first column of the first joined relation', () => {
    const plan = compileQueryPlan({
      select: [count()],
      join: [Department, Employee],
      on: Employee.col('department').eq(Department.col('name')),
    });
    expect(plan.programs[0]).toMatch(/^il_sdk_agg\(count<Department>\) <- department\(Department, Budget\), employee/);
  });

  it('never emits count<> in a rule head', () => {
    const rule = compileRule('dept_size', ['department', 'n'], {
      relations: [{ name: 'employee', def: Employee }],
      selectMap: { department: Employee.col('department').toAst(), n: count() },
    });
    expect(rule).toBe('+dept_size(Department, count<Id>) <- employee(Id, _, Department, _, _)');
  });
});

describe('resultColumnIndexes', () => {
  const plan = compileQueryPlan({
    select: [Employee.col('salary').toAst(), Employee.col('name').toAst()],
    join: [Employee],
  });

  it('maps schema-named columns by position in the first atom', () => {
    expect(
      resultColumnIndexes(plan, ['id', 'name', 'department', 'salary', 'active'], ['Salary', 'Name']),
    ).toEqual([3, 1]);
  });

  it('maps variable-named columns by name', () => {
    expect(
      resultColumnIndexes(plan, ['Id', 'Name', 'Department', 'Salary', 'Active', 'Bonus'], ['Bonus', 'Name']),
    ).toEqual([5, 1]);
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

describe('in / notIn', () => {
  const Manager = relation('Manager', { employeeId: 'int', since: 'int' });

  it('compiles in() to an atom of the target relation', () => {
    const iql = compileConditionalDelete(Employee, Employee.col('id').in(Manager.col('employeeId')));
    expect(iql).toBe(
      '-employee(X0, X1, X2, X3, X4) <- employee(X0, X1, X2, X3, X4), manager(X0, _)',
    );
  });

  it('compiles notIn() to a negated atom', () => {
    const iql = compileConditionalDelete(Employee, Employee.col('id').notIn(Manager.col('employeeId')));
    expect(iql).toBe(
      '-employee(X0, X1, X2, X3, X4) <- employee(X0, X1, X2, X3, X4), !manager(X0, _)',
    );
  });

  it('places the variable at the target column and works through refs()', () => {
    const [m] = Manager.refs(1);
    const iql = compileConditionalDelete(Employee, Employee.col('id').in(m.col('since')));
    expect(iql).toContain('manager(_, X0)');
  });

  it('binds the source column in a rule when it is not selected', () => {
    const rule = compileRule(
      'unmanaged',
      ['name'],
      from(Employee)
        .where((e) => e.col('id').notIn(Manager.col('employeeId')))
        .select({ name: Employee.col('name') }),
    );
    expect(rule).toBe('+unmanaged(Name) <- employee(Id, Name, _, _, _), !manager(Id, _)');
  });

  it('compiles in() in a query to a positive atom', () => {
    expect(
      compileQuery({
        select: [Employee.col('name').toAst()],
        join: [Employee],
        where: Employee.col('id').in(Manager.col('employeeId')),
      }),
    ).toBe('?employee(Id, Name, Department, Salary, Active), manager(Id, _)');
  });

  it('rejects a target column with no relation definition', () => {
    const target = { _tag: 'Column', relation: 'manager', name: 'employeeId' } as const;
    expect(() =>
      compileConditionalDelete(Employee, {
        _tag: 'InExpr',
        column: Employee.col('id').toAst(),
        targetColumn: target,
      } as never),
    ).toThrow(/relation definition/);
  });
});
