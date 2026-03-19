import { describe, it, expect } from 'vitest';
import { camelToSnake, snakeToCamel, columnToVariable } from '../src/naming';

describe('camelToSnake', () => {
  it('converts simple names', () => {
    expect(camelToSnake('Employee')).toBe('employee');
  });

  it('converts multi-word names', () => {
    expect(camelToSnake('UserProfile')).toBe('user_profile');
  });

  it('handles consecutive uppercase', () => {
    expect(camelToSnake('HTTPRequest')).toBe('http_request');
    expect(camelToSnake('ABCDef')).toBe('abc_def');
  });
});

describe('snakeToCamel', () => {
  it('converts simple names', () => {
    expect(snakeToCamel('employee')).toBe('Employee');
  });

  it('converts multi-word names', () => {
    expect(snakeToCamel('user_profile')).toBe('UserProfile');
  });
});

describe('columnToVariable', () => {
  it('capitalizes single word', () => {
    expect(columnToVariable('id')).toBe('Id');
    expect(columnToVariable('name')).toBe('Name');
  });

  it('converts snake_case to CamelCase', () => {
    expect(columnToVariable('department_name')).toBe('DepartmentName');
  });
});
