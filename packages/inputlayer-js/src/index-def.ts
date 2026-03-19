/**
 * HNSW index definition and compilation.
 */

import type { RelationDef } from './relation.js';
import { resolveRelationName } from './relation.js';

export interface HnswIndexOptions {
  name: string;
  relation: RelationDef;
  column: string;
  metric?: 'cosine' | 'euclidean' | 'manhattan' | 'dot_product';
  m?: number;
  efConstruction?: number;
  efSearch?: number;
}

/**
 * HNSW vector index configuration.
 */
export class HnswIndex {
  readonly name: string;
  readonly relation: RelationDef;
  readonly column: string;
  readonly metric: string;
  readonly m: number;
  readonly efConstruction: number;
  readonly efSearch: number;

  constructor(opts: HnswIndexOptions) {
    this.name = opts.name;
    this.relation = opts.relation;
    this.column = opts.column;
    this.metric = opts.metric ?? 'cosine';
    this.m = opts.m ?? 16;
    this.efConstruction = opts.efConstruction ?? 100;
    this.efSearch = opts.efSearch ?? 50;
  }

  /** Compile this index definition to an IQL meta command. */
  toIQL(): string {
    const relName = resolveRelationName(this.relation);
    return (
      `.index create ${this.name} on ${relName}(${this.column}) ` +
      `type hnsw metric ${this.metric} ` +
      `m ${this.m} ef_construction ${this.efConstruction} ` +
      `ef_search ${this.efSearch}`
    );
  }
}
