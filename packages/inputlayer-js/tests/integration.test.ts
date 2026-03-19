/**
 * Integration tests - require a running InputLayer server.
 *
 * Set INPUTLAYER_TEST_SERVER=ws://localhost:8080/ws to enable.
 * Set INPUTLAYER_TEST_USER and INPUTLAYER_TEST_PASSWORD for auth.
 *
 * These tests verify end-to-end behavior against a live server,
 * using raw IQL execution to avoid compiler-level query format issues.
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import {
  InputLayer,
  relation,
  from,
  HnswIndex,
  AND,
  compileSchema,
  compileInsert,
  compileBulkInsert,
  compileDelete,
  compileConditionalDelete,
  compileRule,
} from '../src/index';

const SERVER_URL = process.env.INPUTLAYER_TEST_SERVER ?? '';
const USERNAME = process.env.INPUTLAYER_TEST_USER ?? 'admin';
const PASSWORD = process.env.INPUTLAYER_TEST_PASSWORD ?? 'admin';
const API_KEY = process.env.INPUTLAYER_TEST_API_KEY ?? '';

const SKIP = !SERVER_URL;

// ── Test Relations ──────────────────────────────────────────────────

const Edge = relation('Edge', { src: 'int', dst: 'int' });

const Employee = relation('Employee', {
  id: 'int',
  name: 'string',
  department: 'string',
  salary: 'float',
  active: 'bool',
});

const Document = relation('Document', {
  id: 'int',
  title: 'string',
  embedding: 'vector[3]',
});

// ── Helpers ─────────────────────────────────────────────────────────

function kgName(testName: string): string {
  return `test_${testName}_js`;
}

// ── Connection Tests ────────────────────────────────────────────────

describe.skipIf(SKIP)('Integration: Connection', () => {
  let client: InputLayer;

  beforeAll(async () => {
    client = new InputLayer(API_KEY ? { url: SERVER_URL, apiKey: API_KEY } : { url: SERVER_URL, username: USERNAME, password: PASSWORD });
    await client.connect();
  });

  afterAll(async () => {
    await client.close();
  });

  it('connects and authenticates', () => {
    expect(client.connected).toBe(true);
    expect(client.sessionId).toBeDefined();
    expect(client.serverVersion).toBeDefined();
    expect(client.role).toBeDefined();
  });

  it('lists knowledge graphs', async () => {
    const kg = client.knowledgeGraph('default');
    const result = await kg.execute('.kg list');
    const text = result.rows.map((r) => String(r[0])).join('\n');
    expect(text).toContain('default');
  });
});

// ── Schema & Define Tests ───────────────────────────────────────────

describe.skipIf(SKIP)('Integration: Schema', () => {
  let client: InputLayer;
  const kg_name = kgName('schema');

  beforeAll(async () => {
    client = new InputLayer(API_KEY ? { url: SERVER_URL, apiKey: API_KEY } : { url: SERVER_URL, username: USERNAME, password: PASSWORD });
    await client.connect();
  });

  afterAll(async () => {
    try { await client.dropKnowledgeGraph(kg_name); } catch {}
    await client.close();
  });

  it('compileSchema produces valid IQL', () => {
    expect(compileSchema(Edge)).toBe('+edge(src: int, dst: int)');
    expect(compileSchema(Employee)).toBe(
      '+employee(id: int, name: string, department: string, salary: float, active: bool)',
    );
  });

  it('defines and uses relations via SDK', async () => {
    const kg = client.knowledgeGraph(kg_name);
    await kg.define(Edge);
    await kg.define(Employee);
    // Relations appear in .rel after inserting data
    await kg.insert(Edge, { src: 1, dst: 2 });
    await kg.insert(Employee, { id: 1, name: 'Test', department: 'eng', salary: 100000, active: true });
    const result = await kg.execute('.rel');
    const text = result.rows.map((r) => String(r[0])).join('\n');
    expect(text).toContain('edge');
    expect(text).toContain('employee');
  });

  it('define is idempotent', async () => {
    const kg = client.knowledgeGraph(kg_name);
    await kg.define(Edge);
    await kg.define(Edge); // No error
  });
});

// ── Insert & Query Tests ────────────────────────────────────────────

describe.skipIf(SKIP)('Integration: Insert & Query', () => {
  let client: InputLayer;
  const kg_name = kgName('iq');

  beforeAll(async () => {
    client = new InputLayer(API_KEY ? { url: SERVER_URL, apiKey: API_KEY } : { url: SERVER_URL, username: USERNAME, password: PASSWORD });
    await client.connect();
  });

  afterAll(async () => {
    try { await client.dropKnowledgeGraph(kg_name); } catch {}
    await client.close();
  });

  it('compileInsert produces valid IQL', () => {
    expect(compileInsert(Edge, { src: 1, dst: 2 })).toBe('+edge(1, 2)');
    expect(compileBulkInsert(Edge, [{ src: 1, dst: 2 }, { src: 3, dst: 4 }])).toBe(
      '+edge[(1, 2), (3, 4)]',
    );
  });

  it('inserts and queries batch facts', async () => {
    const kg = client.knowledgeGraph(kg_name);
    await kg.define(Edge);
    await kg.insert(Edge, [
      { src: 1, dst: 2 },
      { src: 2, dst: 3 },
      { src: 3, dst: 4 },
    ]);
    const result = await kg.execute('?edge(X, Y)');
    expect(result.length).toBeGreaterThanOrEqual(3);
  });

  it('inserts a single fact', async () => {
    const kg = client.knowledgeGraph(kg_name);
    const beforeResult = await kg.execute('?edge(X, Y)');
    const beforeCount = beforeResult.length;
    await kg.insert(Edge, { src: 10, dst: 20 });
    const result = await kg.execute('?edge(X, Y)');
    expect(result.length).toBe(beforeCount + 1);
  });

  it('queries with filter via raw IQL', async () => {
    const kg = client.knowledgeGraph(kg_name);
    await kg.define(Employee);
    await kg.insert(Employee, [
      { id: 1, name: 'Alice', department: 'eng', salary: 120000, active: true },
      { id: 2, name: 'Bob', department: 'hr', salary: 90000, active: true },
      { id: 3, name: 'Charlie', department: 'eng', salary: 110000, active: false },
    ]);
    const result = await kg.execute(
      '?employee(Id, Name, Dept, Salary, Active), Dept = "eng", Active = true',
    );
    expect(result.length).toBeGreaterThanOrEqual(1);
  });

  it('queries with ordering and limit', async () => {
    const kg = client.knowledgeGraph(kg_name);
    const result = await kg.execute('?employee(Id, Name, Dept, Salary:desc, Active) :limit 2');
    expect(result.length).toBeLessThanOrEqual(2);
  });
});

// ── Join Tests ──────────────────────────────────────────────────────

describe.skipIf(SKIP)('Integration: Joins', () => {
  let client: InputLayer;
  const kg_name = kgName('joins');
  const Department = relation('Department', { name: 'string', budget: 'float' });

  beforeAll(async () => {
    client = new InputLayer(API_KEY ? { url: SERVER_URL, apiKey: API_KEY } : { url: SERVER_URL, username: USERNAME, password: PASSWORD });
    await client.connect();
    const kg = client.knowledgeGraph(kg_name);
    await kg.define(Employee, Department);
    await kg.insert(Employee, [
      { id: 1, name: 'Alice', department: 'eng', salary: 120000, active: true },
      { id: 2, name: 'Bob', department: 'hr', salary: 90000, active: true },
    ]);
    await kg.insert(Department, [
      { name: 'eng', budget: 500000 },
      { name: 'hr', budget: 200000 },
    ]);
  });

  afterAll(async () => {
    try { await client.dropKnowledgeGraph(kg_name); } catch {}
    await client.close();
  });

  it('joins two relations via raw IQL', async () => {
    const kg = client.knowledgeGraph(kg_name);
    const result = await kg.execute(
      '?employee(_, Name, Dept, _, _), department(Dept, Budget)',
    );
    expect(result.length).toBe(2);
  });
});

// ── Aggregation Tests ───────────────────────────────────────────────

describe.skipIf(SKIP)('Integration: Aggregations', () => {
  let client: InputLayer;
  const kg_name = kgName('agg');

  beforeAll(async () => {
    client = new InputLayer(API_KEY ? { url: SERVER_URL, apiKey: API_KEY } : { url: SERVER_URL, username: USERNAME, password: PASSWORD });
    await client.connect();
    const kg = client.knowledgeGraph(kg_name);
    await kg.define(Employee);
    await kg.insert(Employee, [
      { id: 1, name: 'Alice', department: 'eng', salary: 120000, active: true },
      { id: 2, name: 'Bob', department: 'eng', salary: 100000, active: true },
      { id: 3, name: 'Charlie', department: 'hr', salary: 90000, active: true },
    ]);
  });

  afterAll(async () => {
    try { await client.dropKnowledgeGraph(kg_name); } catch {}
    await client.close();
  });

  it('count aggregation via rule', async () => {
    const kg = client.knowledgeGraph(kg_name);
    await kg.execute('+emp_count(count<Id>) <- employee(Id, _, _, _, _)');
    const result = await kg.execute('?emp_count(C)');
    expect(result.rows[0][0]).toBe(3);
  });

  it('sum aggregation via rule', async () => {
    const kg = client.knowledgeGraph(kg_name);
    await kg.execute('+salary_sum(sum<Salary>) <- employee(_, _, _, Salary, _)');
    const result = await kg.execute('?salary_sum(S)');
    expect(result.rows[0][0]).toBe(310000);
  });

  it('group-by count via rule', async () => {
    const kg = client.knowledgeGraph(kg_name);
    await kg.execute('+dept_count(Dept, count<Id>) <- employee(Id, _, Dept, _, _)');
    const result = await kg.execute('?dept_count(D, C)');
    expect(result.length).toBe(2);
  });

  it('min/max aggregation via rule', async () => {
    const kg = client.knowledgeGraph(kg_name);
    await kg.execute('+salary_min(min<Salary>) <- employee(_, _, _, Salary, _)');
    await kg.execute('+salary_max(max<Salary>) <- employee(_, _, _, Salary, _)');
    const minResult = await kg.execute('?salary_min(M)');
    expect(minResult.rows[0][0]).toBe(90000);
    const maxResult = await kg.execute('?salary_max(M)');
    expect(maxResult.rows[0][0]).toBe(120000);
  });
});

// ── Rule Tests ──────────────────────────────────────────────────────

describe.skipIf(SKIP)('Integration: Rules', () => {
  let client: InputLayer;
  const kg_name = kgName('rules');
  const Reachable = relation('Reachable', { src: 'int', dst: 'int' });

  beforeAll(async () => {
    client = new InputLayer(API_KEY ? { url: SERVER_URL, apiKey: API_KEY } : { url: SERVER_URL, username: USERNAME, password: PASSWORD });
    await client.connect();
  });

  afterAll(async () => {
    try { await client.dropKnowledgeGraph(kg_name); } catch {}
    await client.close();
  });

  it('compileRule produces valid IQL', () => {
    const clause = from(Edge).select({ src: Edge.col('src'), dst: Edge.col('dst') });
    const iql = compileRule('reachable', ['src', 'dst'], clause);
    expect(iql).toBe('+reachable(Src, Dst) <- edge(Src, Dst)');
  });

  it('defines and queries a recursive rule', async () => {
    const kg = client.knowledgeGraph(kg_name);
    await kg.define(Edge);
    await kg.insert(Edge, [
      { src: 1, dst: 2 },
      { src: 2, dst: 3 },
      { src: 3, dst: 4 },
    ]);

    const reachableRules = [
      from(Edge).select({ src: Edge.col('src'), dst: Edge.col('dst') }),
      from(Reachable, Edge)
        .where((r, e) => r.col('dst').eq(e.col('src')))
        .select({ src: Reachable.col('src'), dst: Edge.col('dst') }),
    ];
    await kg.defineRules('reachable', ['src', 'dst'], reachableRules);

    const result = await kg.execute('?reachable(X, Y)');
    // Transitive closure: 1->2, 1->3, 1->4, 2->3, 2->4, 3->4
    expect(result.length).toBeGreaterThanOrEqual(6);
  });

  it('lists rules', async () => {
    const kg = client.knowledgeGraph(kg_name);
    const result = await kg.execute('.rule list');
    const text = result.rows.map((r) => String(r[0])).join('\n');
    expect(text).toContain('reachable');
  });

  it('drops a rule', async () => {
    const kg = client.knowledgeGraph(kg_name);
    await kg.execute('+high_edge(Src, Dst) <- edge(Src, Dst), Src > 1');
    let result = await kg.execute('.rule list');
    let text = result.rows.map((r) => String(r[0])).join('\n');
    expect(text).toContain('high_edge');

    await kg.dropRule('high_edge');
    result = await kg.execute('.rule list');
    text = result.rows.map((r) => String(r[0])).join('\n');
    expect(text).not.toContain('high_edge');
  });
});

// ── Delete Tests ────────────────────────────────────────────────────

describe.skipIf(SKIP)('Integration: Delete', () => {
  let client: InputLayer;
  const kg_name = kgName('delete');

  beforeAll(async () => {
    client = new InputLayer(API_KEY ? { url: SERVER_URL, apiKey: API_KEY } : { url: SERVER_URL, username: USERNAME, password: PASSWORD });
    await client.connect();
  });

  afterAll(async () => {
    try { await client.dropKnowledgeGraph(kg_name); } catch {}
    await client.close();
  });

  it('compileDelete produces valid IQL', () => {
    expect(compileDelete(Edge, { src: 1, dst: 2 })).toBe('-edge(1, 2)');
  });

  it('deletes a specific fact via SDK', async () => {
    const kg = client.knowledgeGraph(kg_name);
    await kg.define(Edge);
    await kg.insert(Edge, [
      { src: 1, dst: 2 },
      { src: 2, dst: 3 },
    ]);
    await kg.delete(Edge, { src: 1, dst: 2 });
    const result = await kg.execute('?edge(X, Y)');
    expect(result.length).toBe(1);
  });

  it('deletes by condition via SDK', async () => {
    const kg = client.knowledgeGraph(kg_name);
    await kg.define(Employee);
    await kg.insert(Employee, [
      { id: 1, name: 'Alice', department: 'eng', salary: 120000, active: true },
      { id: 2, name: 'Bob', department: 'hr', salary: 90000, active: true },
      { id: 3, name: 'Charlie', department: 'hr', salary: 80000, active: false },
    ]);
    await kg.delete(Employee, Employee.col('department').eq('hr'));
    const result = await kg.execute('?employee(Id, Name, Dept, Salary, Active)');
    expect(result.length).toBe(1);
  });
});

// ── Session Tests ───────────────────────────────────────────────────

describe.skipIf(SKIP)('Integration: Sessions', () => {
  let client: InputLayer;
  const kg_name = kgName('session');

  beforeAll(async () => {
    client = new InputLayer(API_KEY ? { url: SERVER_URL, apiKey: API_KEY } : { url: SERVER_URL, username: USERNAME, password: PASSWORD });
    await client.connect();
  });

  afterAll(async () => {
    try { await client.dropKnowledgeGraph(kg_name); } catch {}
    await client.close();
  });

  it('inserts ephemeral session facts', async () => {
    const kg = client.knowledgeGraph(kg_name);
    await kg.define(Edge);
    await kg.insert(Edge, { src: 1, dst: 2 });
    await kg.session.insert(Edge, { src: 10, dst: 20 });
    const result = await kg.execute('?edge(X, Y)');
    expect(result.length).toBeGreaterThanOrEqual(2);
  });

  it('clears session data', async () => {
    const kg = client.knowledgeGraph(kg_name);
    await kg.session.clear();
    const result = await kg.execute('?edge(X, Y)');
    expect(result.length).toBeGreaterThanOrEqual(1);
  });
});

// ── Vector Search Tests ─────────────────────────────────────────────

describe.skipIf(SKIP)('Integration: Vector Search', () => {
  let client: InputLayer;
  const kg_name = kgName('vector');

  beforeAll(async () => {
    client = new InputLayer(API_KEY ? { url: SERVER_URL, apiKey: API_KEY } : { url: SERVER_URL, username: USERNAME, password: PASSWORD });
    await client.connect();
    const kg = client.knowledgeGraph(kg_name);
    await kg.define(Document);
    await kg.insert(Document, [
      { id: 1, title: 'doc-a', embedding: [1.0, 0.0, 0.0] },
      { id: 2, title: 'doc-b', embedding: [0.0, 1.0, 0.0] },
      { id: 3, title: 'doc-c', embedding: [0.0, 0.0, 1.0] },
      { id: 4, title: 'doc-d', embedding: [0.7, 0.7, 0.0] },
    ]);
  });

  afterAll(async () => {
    try { await client.dropKnowledgeGraph(kg_name); } catch {}
    await client.close();
  });

  it('top-k vector search', async () => {
    const kg = client.knowledgeGraph(kg_name);
    const result = await kg.vectorSearch({
      relation: Document,
      queryVec: [1.0, 0.0, 0.0],
      k: 3,
      metric: 'cosine',
    });
    expect(result.length).toBeGreaterThanOrEqual(1);
    expect(result.length).toBeLessThanOrEqual(3);
  });

  it('radius vector search', async () => {
    const kg = client.knowledgeGraph(kg_name);
    const result = await kg.vectorSearch({
      relation: Document,
      queryVec: [1.0, 0.0, 0.0],
      radius: 0.5,
      metric: 'cosine',
    });
    expect(result.length).toBeGreaterThanOrEqual(1);
  });
});

// ── Index Tests ─────────────────────────────────────────────────────

describe.skipIf(SKIP)('Integration: Indexes', () => {
  let client: InputLayer;
  const kg_name = kgName('index');

  beforeAll(async () => {
    client = new InputLayer(API_KEY ? { url: SERVER_URL, apiKey: API_KEY } : { url: SERVER_URL, username: USERNAME, password: PASSWORD });
    await client.connect();
    const kg = client.knowledgeGraph(kg_name);
    await kg.define(Document);
    await kg.insert(Document, [
      { id: 1, title: 'doc-a', embedding: [1.0, 0.0, 0.0] },
      { id: 2, title: 'doc-b', embedding: [0.0, 1.0, 0.0] },
    ]);
  });

  afterAll(async () => {
    try { await client.dropKnowledgeGraph(kg_name); } catch {}
    await client.close();
  });

  it('creates and drops an HNSW index', async () => {
    const kg = client.knowledgeGraph(kg_name);
    const index = new HnswIndex({
      name: 'test_doc_idx',
      relation: Document,
      column: 'embedding',
      metric: 'cosine',
    });
    await kg.createIndex(index);

    const result = await kg.execute('.index list');
    const text = result.rows.map((r) => String(r[0])).join('\n');
    expect(text).toContain('test_doc_idx');

    await kg.dropIndex('test_doc_idx');
  });
});

// ── ResultSet Tests ─────────────────────────────────────────────────

describe.skipIf(SKIP)('Integration: ResultSet', () => {
  let client: InputLayer;
  const kg_name = kgName('resultset');

  beforeAll(async () => {
    client = new InputLayer(API_KEY ? { url: SERVER_URL, apiKey: API_KEY } : { url: SERVER_URL, username: USERNAME, password: PASSWORD });
    await client.connect();
    const kg = client.knowledgeGraph(kg_name);
    await kg.define(Employee);
    await kg.insert(Employee, [
      { id: 1, name: 'Alice', department: 'eng', salary: 120000, active: true },
      { id: 2, name: 'Bob', department: 'hr', salary: 90000, active: true },
    ]);
  });

  afterAll(async () => {
    try { await client.dropKnowledgeGraph(kg_name); } catch {}
    await client.close();
  });

  it('result has correct row count', async () => {
    const kg = client.knowledgeGraph(kg_name);
    const result = await kg.execute('?employee(Id, Name, Dept, Salary, Active)');
    expect(result.length).toBe(2);
  });

  it('first() returns first row', async () => {
    const kg = client.knowledgeGraph(kg_name);
    const result = await kg.execute('?employee(Id, Name, Dept, Salary, Active)');
    const first = result.first();
    expect(first).toBeDefined();
  });

  it('toDicts() returns array of objects', async () => {
    const kg = client.knowledgeGraph(kg_name);
    const result = await kg.execute('?employee(Id, Name, Dept, Salary, Active)');
    const dicts = result.toDicts();
    expect(dicts.length).toBe(2);
  });

  it('toTuples() returns array of arrays', async () => {
    const kg = client.knowledgeGraph(kg_name);
    const result = await kg.execute('?employee(Id, Name, Dept, Salary, Active)');
    const tuples = result.toTuples();
    expect(Array.isArray(tuples[0])).toBe(true);
  });

  it('is iterable', async () => {
    const kg = client.knowledgeGraph(kg_name);
    const result = await kg.execute('?employee(Id, Name, Dept, Salary, Active)');
    const rows: Array<Record<string, unknown>> = [];
    for (const row of result) {
      rows.push(row);
    }
    expect(rows.length).toBe(2);
  });

  it('has execution metadata', async () => {
    const kg = client.knowledgeGraph(kg_name);
    const result = await kg.execute('?employee(Id, Name, Dept, Salary, Active)');
    expect(result.executionTimeMs).toBeGreaterThanOrEqual(0);
    expect(result.truncated).toBe(false);
  });
});

// ── Raw IQL Tests ───────────────────────────────────────────────────

describe.skipIf(SKIP)('Integration: Raw IQL', () => {
  let client: InputLayer;
  const kg_name = kgName('raw');

  beforeAll(async () => {
    client = new InputLayer(API_KEY ? { url: SERVER_URL, apiKey: API_KEY } : { url: SERVER_URL, username: USERNAME, password: PASSWORD });
    await client.connect();
  });

  afterAll(async () => {
    try { await client.dropKnowledgeGraph(kg_name); } catch {}
    await client.close();
  });

  it('executes raw IQL statements', async () => {
    const kg = client.knowledgeGraph(kg_name);
    await kg.execute('+edge(src: int, dst: int)');
    await kg.execute('+edge(1, 2)');
    await kg.execute('+edge(2, 3)');
    const result = await kg.execute('?edge(X, Y)');
    expect(result.length).toBe(2);
  });

  it('server status', async () => {
    const kg = client.knowledgeGraph(kg_name);
    const result = await kg.execute('.status');
    expect(result.length).toBeGreaterThan(0);
  });
});

// ── Multi-KG Tests ──────────────────────────────────────────────────

describe.skipIf(SKIP)('Integration: Multi-KG', () => {
  let client: InputLayer;
  const kg_name = kgName('multikg');

  beforeAll(async () => {
    client = new InputLayer(API_KEY ? { url: SERVER_URL, apiKey: API_KEY } : { url: SERVER_URL, username: USERNAME, password: PASSWORD });
    await client.connect();
  });

  afterAll(async () => {
    try { await client.dropKnowledgeGraph(kg_name); } catch {}
    await client.close();
  });

  it('creates and lists a knowledge graph', async () => {
    const kg = client.knowledgeGraph(kg_name);
    await kg.define(Edge);
    await kg.insert(Edge, { src: 1, dst: 2 });

    // Switch to default to list all KGs
    const defaultKg = client.knowledgeGraph('default');
    const result = await defaultKg.execute('.kg list');
    const text = result.rows.map((r) => String(r[0])).join('\n');
    expect(text).toContain(kg_name);
  });
});

// ── Explain Tests ───────────────────────────────────────────────────

describe.skipIf(SKIP)('Integration: Explain', () => {
  let client: InputLayer;
  const kg_name = kgName('explain');

  beforeAll(async () => {
    client = new InputLayer(API_KEY ? { url: SERVER_URL, apiKey: API_KEY } : { url: SERVER_URL, username: USERNAME, password: PASSWORD });
    await client.connect();
    const kg = client.knowledgeGraph(kg_name);
    await kg.define(Employee);
  });

  afterAll(async () => {
    try { await client.dropKnowledgeGraph(kg_name); } catch {}
    await client.close();
  });

  it('explains a query plan via raw IQL', async () => {
    const kg = client.knowledgeGraph(kg_name);
    const result = await kg.execute('.explain ?employee(Id, Name, Dept, Salary, Active)');
    expect(result.length).toBeGreaterThan(0);
  });
});

// ── Drop Relation Tests ─────────────────────────────────────────────

describe.skipIf(SKIP)('Integration: Drop Relation', () => {
  let client: InputLayer;
  const kg_name = kgName('droprel');

  beforeAll(async () => {
    client = new InputLayer(API_KEY ? { url: SERVER_URL, apiKey: API_KEY } : { url: SERVER_URL, username: USERNAME, password: PASSWORD });
    await client.connect();
  });

  afterAll(async () => {
    try { await client.dropKnowledgeGraph(kg_name); } catch {}
    await client.close();
  });

  it('drops a relation', async () => {
    const kg = client.knowledgeGraph(kg_name);
    await kg.define(Edge, Employee);
    let result = await kg.execute('.rel');
    let text = result.rows.map((r) => String(r[0])).join('\n');
    expect(text).toContain('edge');

    await kg.dropRelation('edge');
    result = await kg.execute('.rel');
    text = result.rows.map((r) => String(r[0])).join('\n');
    expect(text).not.toContain(' edge '); // Avoid matching "edge" inside other relation names
  });
});

// ── ACL Tests ───────────────────────────────────────────────────────

describe.skipIf(SKIP)('Integration: ACL', () => {
  let client: InputLayer;
  const kg_name = kgName('acl');

  beforeAll(async () => {
    client = new InputLayer(API_KEY ? { url: SERVER_URL, apiKey: API_KEY } : { url: SERVER_URL, username: USERNAME, password: PASSWORD });
    await client.connect();
    const kg = client.knowledgeGraph(kg_name);
    await kg.define(Edge);
  });

  afterAll(async () => {
    try { await client.dropKnowledgeGraph(kg_name); } catch {}
    await client.close();
  });

  it('lists ACL entries', async () => {
    const kg = client.knowledgeGraph(kg_name);
    const acl = await kg.listAcl();
    expect(Array.isArray(acl)).toBe(true);
  });
});

// ── Server Operations ───────────────────────────────────────────────

describe.skipIf(SKIP)('Integration: Server Operations', () => {
  let client: InputLayer;

  beforeAll(async () => {
    client = new InputLayer(API_KEY ? { url: SERVER_URL, apiKey: API_KEY } : { url: SERVER_URL, username: USERNAME, password: PASSWORD });
    await client.connect();
  });

  afterAll(async () => {
    await client.close();
  });

  it('gets server status', async () => {
    const kg = client.knowledgeGraph('default');
    const status = await kg.status();
    expect(status.version).toBeDefined();
  });

  it('triggers compaction without error', async () => {
    const kg = client.knowledgeGraph('default');
    await kg.compact();
  });
});
