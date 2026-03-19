/**
 * Authentication helpers - data types and meta-command compilation for user/key/ACL management.
 */

// ── Data types ──────────────────────────────────────────────────────

export interface UserInfo {
  username: string;
  role: string;
}

export interface ApiKeyInfo {
  label: string;
  createdAt: string;
}

export interface AclEntry {
  username: string;
  role: string;
}

// ── Meta command compilation ────────────────────────────────────────

export function compileCreateUser(
  username: string,
  password: string,
  role = 'viewer',
): string {
  return `.user create ${username} ${password} ${role}`;
}

export function compileDropUser(username: string): string {
  return `.user drop ${username}`;
}

export function compileSetPassword(username: string, newPassword: string): string {
  return `.user password ${username} ${newPassword}`;
}

export function compileSetRole(username: string, role: string): string {
  return `.user role ${username} ${role}`;
}

export function compileListUsers(): string {
  return '.user list';
}

export function compileCreateApiKey(label: string): string {
  return `.apikey create ${label}`;
}

export function compileListApiKeys(): string {
  return '.apikey list';
}

export function compileRevokeApiKey(label: string): string {
  return `.apikey revoke ${label}`;
}
