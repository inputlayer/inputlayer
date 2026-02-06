/**
 * Case transformation utilities for converting between snake_case (API) and camelCase (TypeScript)
 */

/**
 * Convert a snake_case string to camelCase
 */
export function snakeToCamel(str: string): string {
  return str.replace(/_([a-z])/g, (_, letter) => letter.toUpperCase());
}

/**
 * Convert a camelCase string to snake_case
 */
export function camelToSnake(str: string): string {
  return str.replace(/[A-Z]/g, (letter) => `_${letter.toLowerCase()}`);
}

/**
 * Recursively transform all keys in an object using the provided transformer function
 */
export function transformKeys<T>(
  obj: unknown,
  transformer: (key: string) => string
): T {
  if (Array.isArray(obj)) {
    return obj.map((item) => transformKeys(item, transformer)) as T;
  }

  if (obj !== null && typeof obj === 'object' && !(obj instanceof Date)) {
    return Object.fromEntries(
      Object.entries(obj).map(([key, value]) => [
        transformer(key),
        transformKeys(value, transformer),
      ])
    ) as T;
  }

  return obj as T;
}

/**
 * Convert an object with camelCase keys to snake_case keys (for sending to API)
 */
export function toApiFormat<T>(obj: T): unknown {
  return transformKeys(obj, camelToSnake);
}

/**
 * Convert an object with snake_case keys to camelCase keys (for receiving from API)
 */
export function fromApiFormat<T>(obj: unknown): T {
  return transformKeys<T>(obj, snakeToCamel);
}
