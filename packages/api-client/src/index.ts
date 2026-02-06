/**
 * @inputlayer/api-client
 *
 * TypeScript API client for InputLayer Knowledge Graph Database with runtime validation
 */

// Main client export
export { InputLayerClient, type ClientConfig } from './client';

// Type exports
export type {
  KnowledgeGraph,
  Relation,
  View,
  QueryResult,
  HealthStatus,
  ServerStats,
} from './client';

// API namespace exports (for advanced usage)
export {
  KnowledgeGraphApi,
  QueryApi,
  RelationsApi,
  ViewsApi,
  RulesApi,
  AdminApi,
} from './client';

// Error handling
export { ApiError } from './utils/fetcher';

// Utility exports (for advanced usage)
export {
  configureFetch,
  getFetchConfig,
  customFetch,
  validatedFetch,
  type FetchConfig,
  type RequestOptions,
  type ApiResponse,
} from './utils/fetcher';

export {
  snakeToCamel,
  camelToSnake,
  transformKeys,
  toApiFormat,
  fromApiFormat,
} from './utils/case-transform';
