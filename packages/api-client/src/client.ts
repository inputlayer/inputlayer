/**
 * InputLayer API Client
 *
 * High-level client wrapper for the InputLayer REST API with runtime validation
 */

import { configureFetch, customFetch, ApiError, type FetchConfig } from './utils/fetcher';

// Re-export ApiError for consumers
export { ApiError };

/**
 * Client configuration options
 */
export interface ClientConfig {
  /** Base URL for API requests (default: '/api/v1') */
  baseUrl?: string;
  /** Additional headers to include in all requests */
  headers?: Record<string, string>;
}

/**
 * Knowledge Graph information
 */
export interface KnowledgeGraph {
  name: string;
  description?: string;
  relationsCount: number;
  viewsCount: number;
}

/**
 * Relation information
 */
export interface Relation {
  name: string;
  arity: number;
  tupleCount: number;
  columns: string[];
  isView: boolean;
}

/**
 * View information
 */
export interface View {
  name: string;
  definition: string;
  arity: number;
  columns: string[];
  dependencies: string[];
}

/**
 * Query execution result
 */
export interface QueryResult {
  columns: string[];
  rows: (string | number | boolean | null)[][];
  rowCount: number;
  executionTimeMs: number;
  status: 'success' | 'error';
  error?: string;
}

/**
 * Health check response
 */
export interface HealthStatus {
  status: string;
  version: string;
  uptimeSecs: number;
}

/**
 * Server statistics
 */
export interface ServerStats {
  knowledgeGraphs: number;
  relations: number;
  views: number;
  memoryUsage: string;
  queryCount: number;
  uptimeSecs: number;
}

/**
 * Knowledge Graph API methods
 */
export class KnowledgeGraphApi {
  /** List all knowledge graphs */
  async list(): Promise<{ knowledgeGraphs: KnowledgeGraph[]; current?: string; warning?: string }> {
    return customFetch('/knowledge-graphs');
  }

  /** Get knowledge graph details */
  async get(name: string): Promise<KnowledgeGraph> {
    return customFetch(`/knowledge-graphs/${encodeURIComponent(name)}`);
  }

  /** Create a new knowledge graph */
  async create(params: { name: string; description?: string }): Promise<KnowledgeGraph> {
    return customFetch('/knowledge-graphs', {
      method: 'POST',
      body: params,
    });
  }

  /** Delete a knowledge graph */
  async delete(name: string): Promise<void> {
    return customFetch(`/knowledge-graphs/${encodeURIComponent(name)}`, {
      method: 'DELETE',
    });
  }
}

/**
 * Query API methods
 */
export class QueryApi {
  /** Execute a Datalog query */
  async execute(params: {
    query: string;
    knowledgeGraph: string;
    timeoutMs?: number;
  }): Promise<QueryResult> {
    return customFetch('/query/execute', {
      method: 'POST',
      body: params,
    });
  }

  /** Explain a query plan */
  async explain(params: {
    query: string;
    knowledgeGraph: string;
  }): Promise<{ plan: string; optimizations: string[] }> {
    return customFetch('/query/explain', {
      method: 'POST',
      body: params,
    });
  }
}

/**
 * Relations API methods
 */
export class RelationsApi {
  /** List all relations in a knowledge graph */
  async list(knowledgeGraph: string): Promise<{ relations: Relation[] }> {
    return customFetch(`/knowledge-graphs/${encodeURIComponent(knowledgeGraph)}/relations`);
  }

  /** Get relation details */
  async get(knowledgeGraph: string, name: string): Promise<Relation> {
    return customFetch(
      `/knowledge-graphs/${encodeURIComponent(knowledgeGraph)}/relations/${encodeURIComponent(name)}`
    );
  }

  /** Get relation data with pagination */
  async getData(
    knowledgeGraph: string,
    name: string,
    params?: { offset?: number; limit?: number }
  ): Promise<{ columns: string[]; rows: unknown[][]; totalCount: number }> {
    const query = new URLSearchParams();
    if (params?.offset) query.set('offset', String(params.offset));
    if (params?.limit) query.set('limit', String(params.limit));
    const queryString = query.toString();
    const url = `/knowledge-graphs/${encodeURIComponent(knowledgeGraph)}/relations/${encodeURIComponent(name)}/data${queryString ? `?${queryString}` : ''}`;
    return customFetch(url);
  }

  /** Insert data into a relation */
  async insertData(
    knowledgeGraph: string,
    name: string,
    data: { rows: unknown[][] }
  ): Promise<{ inserted: number }> {
    return customFetch(
      `/knowledge-graphs/${encodeURIComponent(knowledgeGraph)}/relations/${encodeURIComponent(name)}/data`,
      {
        method: 'POST',
        body: data,
      }
    );
  }

  /** Delete data from a relation */
  async deleteData(
    knowledgeGraph: string,
    name: string,
    data?: { rows?: unknown[][] }
  ): Promise<{ deleted: number }> {
    return customFetch(
      `/knowledge-graphs/${encodeURIComponent(knowledgeGraph)}/relations/${encodeURIComponent(name)}/data`,
      {
        method: 'DELETE',
        body: data,
      }
    );
  }
}

/**
 * Views API methods
 */
export class ViewsApi {
  /** List all views in a knowledge graph */
  async list(knowledgeGraph: string): Promise<{ views: View[] }> {
    return customFetch(`/knowledge-graphs/${encodeURIComponent(knowledgeGraph)}/views`);
  }

  /** Get view details */
  async get(knowledgeGraph: string, name: string): Promise<View> {
    return customFetch(
      `/knowledge-graphs/${encodeURIComponent(knowledgeGraph)}/views/${encodeURIComponent(name)}`
    );
  }

  /** Get view data with pagination */
  async getData(
    knowledgeGraph: string,
    name: string,
    params?: { offset?: number; limit?: number }
  ): Promise<{ columns: string[]; rows: unknown[][]; totalCount: number }> {
    const query = new URLSearchParams();
    if (params?.offset) query.set('offset', String(params.offset));
    if (params?.limit) query.set('limit', String(params.limit));
    const queryString = query.toString();
    const url = `/knowledge-graphs/${encodeURIComponent(knowledgeGraph)}/views/${encodeURIComponent(name)}/data${queryString ? `?${queryString}` : ''}`;
    return customFetch(url);
  }

  /** Create a new view */
  async create(
    knowledgeGraph: string,
    params: { name: string; definition: string }
  ): Promise<View> {
    return customFetch(`/knowledge-graphs/${encodeURIComponent(knowledgeGraph)}/views`, {
      method: 'POST',
      body: params,
    });
  }

  /** Delete a view */
  async delete(knowledgeGraph: string, name: string): Promise<void> {
    return customFetch(
      `/knowledge-graphs/${encodeURIComponent(knowledgeGraph)}/views/${encodeURIComponent(name)}`,
      {
        method: 'DELETE',
      }
    );
  }
}

/**
 * Rules API methods
 */
export class RulesApi {
  /** List all rules in a knowledge graph */
  async list(knowledgeGraph: string): Promise<{ rules: { name: string; definition: string }[] }> {
    return customFetch(`/knowledge-graphs/${encodeURIComponent(knowledgeGraph)}/rules`);
  }

  /** Get rule details */
  async get(knowledgeGraph: string, name: string): Promise<{ name: string; definition: string }> {
    return customFetch(
      `/knowledge-graphs/${encodeURIComponent(knowledgeGraph)}/rules/${encodeURIComponent(name)}`
    );
  }

  /** Delete a rule */
  async delete(knowledgeGraph: string, name: string): Promise<void> {
    return customFetch(
      `/knowledge-graphs/${encodeURIComponent(knowledgeGraph)}/rules/${encodeURIComponent(name)}`,
      {
        method: 'DELETE',
      }
    );
  }
}

/**
 * Admin API methods
 */
export class AdminApi {
  /** Health check */
  async health(): Promise<HealthStatus> {
    return customFetch('/health');
  }

  /** Get server statistics */
  async stats(): Promise<ServerStats> {
    return customFetch('/stats');
  }
}

/**
 * InputLayer API Client
 *
 * @example
 * ```typescript
 * import { InputLayerClient } from '@inputlayer/api-client';
 *
 * const client = new InputLayerClient({ baseUrl: '/api/v1' });
 *
 * // List knowledge graphs
 * const { knowledgeGraphs } = await client.knowledgeGraphs.list();
 *
 * // Execute a query
 * const result = await client.query.execute({
 *   query: 'person(X, Y)?',
 *   knowledgeGraph: 'mykg',
 * });
 * ```
 */
export class InputLayerClient {
  public readonly knowledgeGraphs: KnowledgeGraphApi;
  public readonly query: QueryApi;
  public readonly relations: RelationsApi;
  public readonly views: ViewsApi;
  public readonly rules: RulesApi;
  public readonly admin: AdminApi;

  constructor(config: ClientConfig = {}) {
    const baseUrl = config.baseUrl ?? '/api/v1';

    // Configure global fetch settings
    configureFetch({
      baseUrl,
      headers: config.headers,
    });

    // Initialize API namespaces (they use the global fetch config)
    this.knowledgeGraphs = new KnowledgeGraphApi();
    this.query = new QueryApi();
    this.relations = new RelationsApi();
    this.views = new ViewsApi();
    this.rules = new RulesApi();
    this.admin = new AdminApi();
  }
}
