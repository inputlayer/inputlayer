/**
 * Live tests for every query example in docs/content/docs/guides/js-sdk.mdx.
 *
 * Each test runs the documented code against a real engine, so a query form
 * the engine rejects fails here. Set INPUTLAYER_TEST_SERVER (and
 * INPUTLAYER_TEST_USER / INPUTLAYER_TEST_PASSWORD) to enable; `make
 * js-test-live` starts a server and runs them.
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import {
  InputLayer,
  KnowledgeGraph,
  relation,
  from,
  AND,
  OR,
  count,
  avg,
  max,
  topK,
} from '../src/index';
import * as fn from '../src/functions';

const SERVER_URL = process.env.INPUTLAYER_TEST_SERVER ?? '';
const USERNAME = process.env.INPUTLAYER_TEST_USER ?? 'admin';
const PASSWORD = process.env.INPUTLAYER_TEST_PASSWORD ?? 'admin';

const KG_NAME = 'test_docs_queries_js';

// Schemas as the guide defines them.
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

// The guide's Document also has `createdAt: "timestamp"`, which no query
// example reads; the engine does not accept that column type in a schema
// declaration yet, so it is left out here.
const Document = relation('Document', {
  id: 'int',
  title: 'string',
  content: 'string',
  embedding: 'vector[3]',
});

const Edge = relation('Edge', { src: 'int', dst: 'int' });
const Reachable = relation('Reachable', { src: 'int', dst: 'int' });

// Timestamps are Unix milliseconds.
const Reading = relation('Reading', { sensorId: 'int', value: 'float', timestamp: 'int' });
const Article = relation('Article', { title: 'string', publishedAt: 'int' });

const NOW = Date.now();

describe.skipIf(!SERVER_URL)('js-sdk.mdx query examples', () => {
  let il: InputLayer;
  let kg: KnowledgeGraph;

  beforeAll(async () => {
    il = new InputLayer({ url: SERVER_URL, username: USERNAME, password: PASSWORD });
    await il.connect();
    try {
      await il.knowledgeGraph('default').execute('.kg use default');
      await il.dropKnowledgeGraph(KG_NAME);
    } catch {
      // not there yet
    }
    kg = il.knowledgeGraph(KG_NAME);
    await kg.define(Employee, Department, Document, Edge, Reading, Article);
    await kg.insert(Employee, [
      { id: 1, name: 'Alice', department: 'eng', salary: 120000.0, active: true },
      { id: 2, name: 'Bob', department: 'hr', salary: 90000.0, active: true },
      { id: 3, name: 'Charlie', department: 'eng', salary: 110000.0, active: false },
      { id: 4, name: 'Dana', department: 'eng', salary: 130000.0, active: true },
      { id: 5, name: 'Eve', department: 'hr', salary: 95000.0, active: true },
    ]);
    await kg.insert(Department, [
      { name: 'eng', budget: 1000000.0 },
      { name: 'hr', budget: 300000.0 },
    ]);
    await kg.insert(Document, [
      { id: 1, title: 'same', content: 'a', embedding: [1, 0, 0] },
      { id: 2, title: 'orthogonal', content: 'b', embedding: [0, 1, 0] },
    ]);
    await kg.insert(Edge, [
      { src: 1, dst: 2 },
      { src: 2, dst: 3 },
      { src: 3, dst: 4 },
    ]);
    await kg.insert(Reading, [
      { sensorId: 1, value: 1.5, timestamp: NOW - 60_000 },
      { sensorId: 2, value: 2.5, timestamp: NOW - 86_400_000 },
    ]);
    await kg.insert(Article, [{ title: 'fresh', publishedAt: NOW }]);
  });

  afterAll(async () => {
    try {
      await il.knowledgeGraph('default').execute('.kg use default');
      await il.dropKnowledgeGraph(KG_NAME);
    } catch {
      // best effort
    }
    await il.close();
  });

  const names = (rows: Iterable<Record<string, unknown>>, key = 'name'): unknown[] =>
    [...rows].map((r) => r[key]);

  it('Basic Queries: all rows', async () => {
    const result = await kg.query({ select: [Employee] });
    const lines: string[] = [];
    for (const emp of result) {
      lines.push(`${emp.name} - ${emp.department}`);
    }
    expect(result.columns).toEqual(['id', 'name', 'department', 'salary', 'active']);
    expect(lines.sort()).toEqual(['Alice - eng', 'Bob - hr', 'Charlie - eng', 'Dana - eng', 'Eve - hr']);
  });

  it('Basic Queries: with a filter', async () => {
    const engineers = await kg.query({
      select: [Employee],
      join: [Employee],
      where: AND(
        Employee.col('department').eq('eng'),
        Employee.col('active').eq(true),
      ),
    });
    expect(names(engineers).sort()).toEqual(['Alice', 'Dana']);
  });

  it('Selecting Specific Columns', async () => {
    const result = await kg.query({
      select: [Employee.col('name').toAst(), Employee.col('salary').toAst()],
      join: [Employee],
      where: Employee.col('department').eq('eng'),
    });
    const lines: string[] = [];
    for (const row of result) {
      lines.push(`${row.Name}: $${row.Salary}`);
    }
    expect(result.columns).toEqual(['Name', 'Salary']);
    expect(lines.sort()).toEqual(['Alice: $120000', 'Charlie: $110000', 'Dana: $130000']);
  });

  it('Joins', async () => {
    const result = await kg.query({
      select: [Employee.col('name').toAst(), Department.col('budget').toAst()],
      join: [Employee, Department],
      on: Employee.col('department').eq(Department.col('name')),
    });
    expect(result.columns).toEqual(['Name', 'Budget']);
    expect(result.toTuples().sort()).toEqual([
      ['Alice', 1000000],
      ['Bob', 300000],
      ['Charlie', 1000000],
      ['Dana', 1000000],
      ['Eve', 300000],
    ]);
  });

  it('Self-Joins', async () => {
    const [e1, e2] = Employee.refs(2);

    const result = await kg.query({
      select: [e1.col('name').toAst(), e2.col('name').toAst()],
      join: [e1, e2],
      on: AND(
        e1.col('department').eq(e2.col('department')),
        e1.col('id').ne(e2.col('id')),
      ),
    });
    // Every ordered pair of distinct colleagues, never a row with itself.
    expect(result.toTuples().map((t) => t.join('-')).sort()).toEqual([
      'Alice-Charlie',
      'Alice-Dana',
      'Bob-Eve',
      'Charlie-Alice',
      'Charlie-Dana',
      'Dana-Alice',
      'Dana-Charlie',
      'Eve-Bob',
    ]);
  });

  it('Computed Columns', async () => {
    const result = await kg.query({
      select: [Employee.col('name').toAst()],
      join: [Employee],
      computed: {
        bonus: Employee.col('salary').mul(0.1),
      },
    });
    expect(result.columns).toEqual(['Name', 'Bonus']);
    const bonus = Object.fromEntries(result.toTuples());
    expect(bonus.Alice).toBeCloseTo(12000);
    expect(bonus.Bob).toBeCloseTo(9000);
    expect(result.length).toBe(5);
  });

  it('Ordering and Pagination', async () => {
    // Top 10 highest paid
    const result = await kg.query({
      select: [Employee],
      join: [Employee],
      orderBy: Employee.col('salary').desc(),
      limit: 10,
    });

    // Second page
    const page2 = await kg.query({
      select: [Employee],
      join: [Employee],
      orderBy: Employee.col('name').asc(),
      limit: 10,
      offset: 10,
    });

    expect(names(result)).toEqual(['Dana', 'Alice', 'Charlie', 'Eve', 'Bob']);
    expect(page2.length).toBe(0);

    // The same pagination on a page that has rows.
    const page = await kg.query({
      select: [Employee],
      join: [Employee],
      orderBy: Employee.col('name').asc(),
      limit: 2,
      offset: 2,
    });
    expect(names(page)).toEqual(['Charlie', 'Dana']);
  });

  it('Aggregations: group by department with stats', async () => {
    // Group by department with stats
    const result = await kg.query({
      select: [
        Employee.col('department').toAst(),
        count(Employee.col('id')),
        avg(Employee.col('salary')),
        max(Employee.col('salary')),
      ],
      join: [Employee],
    });
    expect(result.columns).toEqual(['Department', 'CountId', 'AvgSalary', 'MaxSalary']);
    expect(result.toTuples().sort()).toEqual([
      ['eng', 3, 120000, 130000],
      ['hr', 2, 92500, 95000],
    ]);
  });

  it('Aggregations: topK per group', async () => {
    // Top 3 highest-paid employees per department
    const result = await kg.query({
      select: [
        Employee.col('department').toAst(),
        topK({
          k: 3,
          passthrough: [Employee.col('name')],
          orderBy: Employee.col('salary'),
          desc: true,
        }),
      ],
      join: [Employee],
    });
    expect(result.columns).toEqual(['Department', 'Name', 'Salary']);
    expect(result.toTuples().sort()).toEqual([
      ['eng', 'Alice', 120000],
      ['eng', 'Charlie', 110000],
      ['eng', 'Dana', 130000],
      ['hr', 'Bob', 90000],
      ['hr', 'Eve', 95000],
    ]);

    // k bounds each group.
    const top1 = await kg.query({
      select: [
        Employee.col('department').toAst(),
        topK({ k: 1, passthrough: [Employee.col('name')], orderBy: Employee.col('salary'), desc: true }),
      ],
      join: [Employee],
    });
    expect(top1.toTuples().sort()).toEqual([
      ['eng', 'Dana', 130000],
      ['hr', 'Eve', 95000],
    ]);
  });

  it('Working with Results', async () => {
    const result = await kg.query({ select: [Employee] });

    // Iterate as keyed objects
    expect(names(result).sort()).toEqual(['Alice', 'Bob', 'Charlie', 'Dana', 'Eve']);

    // Check result metadata
    expect(result.length).toBe(5);
    expect(result.totalCount).toBe(5);
    expect(typeof result.executionTimeMs).toBe('number');

    // Get the first row (or undefined if empty)
    const first = result.first();
    expect(first).toHaveProperty('name');

    // Get a single scalar value
    const total = (await kg.query({
      select: [count(Employee.col('id'))],
      join: [Employee],
    })).scalar();
    expect(total).toBe(5);

    // Convert to different formats
    const dicts = result.toDicts();
    const tuples = result.toTuples();
    expect(dicts).toHaveLength(5);
    expect(tuples[0]).toHaveLength(5);
  });

  it('Query Plans', async () => {
    const plan = await kg.debug({
      select: [Employee],
      join: [Employee],
      where: Employee.col('department').eq('eng'),
    });
    expect(plan.iql).toBe(
      '?employee(Id, Name, Department, Salary, Active), Department = "eng"',
    );
    expect(plan.plan).toContain('employee');
  });

  it('debug shows the plan of an aggregate query', async () => {
    const plan = await kg.debug({
      select: [Employee.col('department').toAst(), count(Employee.col('id'))],
      join: [Employee],
    });
    expect(plan.plan).toContain('employee');
  });

  it('why returns the selected columns with a proof per row', async () => {
    const why = await kg.why({
      select: [Employee.col('name').toAst(), Employee.col('salary').toAst()],
      join: [Employee],
      where: Employee.col('department').eq('eng'),
    });
    expect(why.results.columns).toEqual(['Name', 'Salary']);
    expect(why.results.toTuples().sort()).toEqual([
      ['Alice', 120000],
      ['Charlie', 110000],
      ['Dana', 130000],
    ]);
    expect(why.proofTrees).toHaveLength(3);
  });

  it('why explains an aggregate query', async () => {
    const why = await kg.why({
      select: [Employee.col('department').toAst(), count(Employee.col('id'))],
      join: [Employee],
    });
    expect(why.results.columns).toEqual(['Department', 'CountId']);
    expect(why.results.toTuples().sort()).toEqual([
      ['eng', 3],
      ['hr', 2],
    ]);
    expect(why.proofTrees).toHaveLength(2);
  });

  it('Raw IQL', async () => {
    const result = await kg.execute('?employee(Id, Name, _, Salary, _), Salary > 100000');
    expect(result.rows.map((r) => r[1]).sort()).toEqual(['Alice', 'Charlie', 'Dana']);
  });

  it('Derived Relations: querying a recursive rule', async () => {
    const reachableRules = [
      // Base case: direct edges are reachable
      from(Edge).select({ src: Edge.col('src'), dst: Edge.col('dst') }),
      // Recursive case: if A reaches B and B reaches C, then A reaches C
      from(Reachable, Edge)
        .where((r, e) => r.col('dst').eq(e.col('src')))
        .select({ src: Reachable.col('src'), dst: Edge.col('dst') }),
    ];

    // Deploy the rule (persistent - survives restarts)
    await kg.defineRules('reachable', ['src', 'dst'], reachableRules);

    // Query it
    const result = await kg.query({
      select: [Reachable],
      join: [Reachable],
      where: Reachable.col('src').eq(1),
    });
    const reached: unknown[] = [];
    for (const row of result) {
      reached.push(row.dst);
    }
    expect(reached.sort()).toEqual([2, 3, 4]);
  });

  it('Sessions: session facts mix with persistent data', async () => {
    await kg.session.insert(Employee, [
      { id: 999, name: 'Temp', department: 'eng', salary: 0.0, active: true },
    ]);

    // Query as normal - session facts mix with persistent data
    const result = await kg.query({ select: [Employee] });
    expect(names(result)).toContain('Temp');
    expect(result.length).toBe(6);
  });

  it('Distance Functions', async () => {
    const queryVec = [1, 0, 0];
    const result = await kg.query({
      select: [Document.col('title').toAst()],
      join: [Document],
      computed: {
        distance: fn.cosine(Document.col('embedding'), queryVec),
      },
    });
    expect(result.columns).toEqual(['Title', 'Distance']);
    const distance = Object.fromEntries(result.toTuples());
    expect(distance.same).toBeCloseTo(0);
    expect(distance.orthogonal).toBeCloseTo(1);
  });

  it('Temporal Functions', async () => {
    // Rows from the last hour
    const result = await kg.query({
      select: [Reading],
      join: [Reading],
      where: Reading.col('timestamp').gt(fn.timeSub(fn.timeNow(), 3600000)),
    });
    expect(names(result, 'sensorId')).toEqual([1]);

    // Time-decayed scoring
    const scored = await kg.query({
      select: [Article.col('title').toAst()],
      join: [Article],
      computed: {
        score: fn.timeDecay(Article.col('publishedAt'), fn.timeNow(), 86400000),
      },
    });
    expect(scored.columns).toEqual(['Title', 'Score']);
    expect(scored.first()?.Score).toBeGreaterThan(0.99);
  });

  it('OR conditions merge their branches', async () => {
    const result = await kg.query({
      select: [Employee.col('name').toAst()],
      join: [Employee],
      where: OR(Employee.col('department').eq('hr'), Employee.col('salary').gt(115000)),
      orderBy: Employee.col('salary').desc(),
      limit: 3,
    });
    expect(names(result, 'Name')).toEqual(['Dana', 'Alice', 'Eve']);
  });
});
