/**
 * Naming convention utilities: CamelCase <-> snake_case, column -> IQL variable.
 */

/** Convert CamelCase class name to snake_case relation name. */
export function camelToSnake(name: string): string {
  // Insert underscore between sequences of uppercase and a following lower/digit
  let s = name.replace(/([A-Z]+)([A-Z][a-z])/g, '$1_$2');
  // Insert underscore between lowercase/digit and uppercase
  s = s.replace(/([a-z0-9])([A-Z])/g, '$1_$2');
  return s.toLowerCase();
}

/** Convert snake_case to CamelCase. */
export function snakeToCamel(name: string): string {
  return name
    .split('_')
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join('');
}

/** Convert a snake_case column name to a IQL variable (Capitalized). */
export function columnToVariable(columnName: string): string {
  return snakeToCamel(columnName);
}
