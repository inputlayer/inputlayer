/**
 * Custom fetch wrapper with case transformation and error handling
 */

import { z } from 'zod';
import { fromApiFormat, toApiFormat } from './case-transform';

/**
 * API error class for structured error handling
 */
export class ApiError extends Error {
  constructor(
    public code: string,
    message: string,
    public status?: number
  ) {
    super(message);
    this.name = 'ApiError';
  }
}

/**
 * Standard API response wrapper schema
 */
export const apiResponseSchema = <T extends z.ZodTypeAny>(dataSchema: T) =>
  z.object({
    success: z.boolean(),
    data: dataSchema.optional(),
    error: z
      .object({
        code: z.string(),
        message: z.string(),
      })
      .optional(),
  });

export type ApiResponse<T> = {
  success: boolean;
  data?: T;
  error?: {
    code: string;
    message: string;
  };
};

/**
 * Configuration for the custom fetch function
 */
export interface FetchConfig {
  baseUrl?: string;
  headers?: Record<string, string>;
}

/**
 * Extended request options that allow object bodies
 */
export interface RequestOptions extends Omit<RequestInit, 'body'> {
  body?: unknown;
}

let globalConfig: FetchConfig = {
  baseUrl: '/api/v1',
};

/**
 * Configure the global fetch settings
 */
export function configureFetch(config: FetchConfig): void {
  globalConfig = { ...globalConfig, ...config };
}

/**
 * Get the current fetch configuration
 */
export function getFetchConfig(): FetchConfig {
  return { ...globalConfig };
}

/**
 * Custom fetch function used by Orval-generated code
 * Handles case transformation, validation, and error handling
 */
export async function customFetch<TData>(
  url: string,
  options?: RequestOptions
): Promise<TData> {
  const { baseUrl, headers: defaultHeaders } = globalConfig;
  const fullUrl = url.startsWith('http') ? url : `${baseUrl}${url}`;

  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
    ...defaultHeaders,
    ...(options?.headers as Record<string, string>),
  };

  // Extract body separately to handle type conversion
  const { body: requestBody, ...restOptions } = options ?? {};

  const fetchOptions: RequestInit = {
    ...restOptions,
    headers,
  };

  // Transform request body from camelCase to snake_case
  if (requestBody && typeof requestBody === 'object') {
    fetchOptions.body = JSON.stringify(toApiFormat(requestBody));
  } else if (typeof requestBody === 'string') {
    fetchOptions.body = requestBody;
  }

  let response: Response;
  try {
    response = await fetch(fullUrl, fetchOptions);
  } catch (err) {
    // Network errors (server down, CORS, DNS failure, etc.)
    const message = err instanceof Error ? err.message : 'Network request failed';
    throw new ApiError('NETWORK_ERROR', message);
  }

  // Handle non-JSON responses
  const contentType = response.headers.get('content-type');
  if (!contentType?.includes('application/json')) {
    if (!response.ok) {
      throw new ApiError('HTTP_ERROR', `HTTP ${response.status}: ${response.statusText}`, response.status);
    }
    return undefined as TData;
  }

  const json = await response.json();

  // Handle API error responses
  if (!response.ok || (json.success === false && json.error)) {
    const error = json.error || { code: 'UNKNOWN', message: 'Request failed' };
    throw new ApiError(error.code, error.message, response.status);
  }

  // Transform response from snake_case to camelCase
  const data = json.data !== undefined ? json.data : json;
  return fromApiFormat<TData>(data);
}

/**
 * Validated fetch function that also validates the response with a Zod schema
 */
export async function validatedFetch<TData>(
  url: string,
  schema: z.ZodSchema<TData>,
  options?: RequestOptions
): Promise<TData> {
  const data = await customFetch<TData>(url, options);
  return schema.parse(data);
}
