import { describe, it, expect } from 'vitest';
import { relation } from '../src/relation';
import * as fn from '../src/functions';

describe('functions', () => {
  const Doc = relation('Document', { id: 'int', embedding: 'vector[384]', title: 'string' });

  it('creates distance function calls', () => {
    const expr = fn.cosine(Doc.col('embedding'), [1.0, 2.0, 3.0]);
    expect(expr._tag).toBe('FuncCall');
    expect(expr.name).toBe('cosine');
    expect(expr.args).toHaveLength(2);
  });

  it('creates euclidean', () => {
    expect(fn.euclidean(Doc.col('embedding'), Doc.col('embedding')).name).toBe('euclidean');
  });

  it('creates manhattan', () => {
    expect(fn.manhattan(Doc.col('embedding'), Doc.col('embedding')).name).toBe('manhattan');
  });

  it('creates dot', () => {
    expect(fn.dot(Doc.col('embedding'), Doc.col('embedding')).name).toBe('dot');
  });

  it('creates vector ops', () => {
    expect(fn.normalize(Doc.col('embedding')).name).toBe('normalize');
    expect(fn.vecDim(Doc.col('embedding')).name).toBe('vec_dim');
    expect(fn.vecAdd(Doc.col('embedding'), Doc.col('embedding')).name).toBe('vec_add');
    expect(fn.vecScale(Doc.col('embedding'), 2.0).name).toBe('vec_scale');
  });

  it('creates temporal functions', () => {
    expect(fn.timeNow().name).toBe('time_now');
    expect(fn.timeDiff(Doc.col('id'), Doc.col('id')).name).toBe('time_diff');
    expect(fn.timeAdd(Doc.col('id'), 1000).name).toBe('time_add');
    expect(fn.timeSub(Doc.col('id'), 1000).name).toBe('time_sub');
  });

  it('creates math functions', () => {
    expect(fn.abs(Doc.col('id')).name).toBe('abs');
    expect(fn.sqrt(Doc.col('id')).name).toBe('sqrt');
    expect(fn.pow(Doc.col('id'), 2).name).toBe('pow');
    expect(fn.log(Doc.col('id')).name).toBe('log');
    expect(fn.floor(Doc.col('id')).name).toBe('floor');
    expect(fn.ceil(Doc.col('id')).name).toBe('ceil');
    expect(fn.sign(Doc.col('id')).name).toBe('sign');
  });

  it('creates string functions', () => {
    expect(fn.len(Doc.col('title')).name).toBe('len');
    expect(fn.upper(Doc.col('title')).name).toBe('upper');
    expect(fn.lower(Doc.col('title')).name).toBe('lower');
    expect(fn.trim(Doc.col('title')).name).toBe('trim');
    expect(fn.substr(Doc.col('title'), 0, 5).name).toBe('substr');
    expect(fn.replace(Doc.col('title'), 'a', 'b').name).toBe('replace');
    expect(fn.concat(Doc.col('title'), ' suffix').name).toBe('concat');
  });

  it('creates type conversion functions', () => {
    expect(fn.toFloat(Doc.col('id')).name).toBe('to_float');
    expect(fn.toInt(Doc.col('id')).name).toBe('to_int');
  });

  it('creates hnsw_nearest', () => {
    const expr = fn.hnswNearest('my_idx', [1.0, 2.0], 10);
    expect(expr.name).toBe('hnsw_nearest');
    expect(expr.args).toHaveLength(3);
  });

  it('creates hnsw_nearest with ef_search', () => {
    const expr = fn.hnswNearest('my_idx', [1.0, 2.0], 10, { efSearch: 100 });
    expect(expr.args).toHaveLength(4);
  });
});
