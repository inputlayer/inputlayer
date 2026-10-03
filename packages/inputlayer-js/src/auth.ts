/**
 * Authentication helpers - data types and meta-command compilation for user/key/ACL management.
 */

// ── Data types ──────────────────────────────────────────────────────

export interface UserInfo {
  username: string;
  role: string;
}

/**
 * An API key as `.apikey list` reports it. Times are Unix milliseconds;
 * `null` is unknown (`createdAt`), never (`expiresAt`) or not yet
 * (`lastUsedAt`).
 */
export interface ApiKeyInfo {
  label: string;
  owner: string;
  createdAt: number | null;
  expiresAt: number | null;
  lastUsedAt: number | null;
  status: 'active' | 'expired';
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

/** A TTL as the engine accepts it: a number and a unit, e.g. `30s`, `90d`. */
const TTL = /^[0-9]+(ms|s|m|h|d)$/;

function validateTtl(ttl: string): string {
  if (!TTL.test(ttl)) {
    throw new Error(`ttl must be a number and a unit (ms, s, m, h or d), e.g. '90d': '${ttl}'`);
  }
  return ttl;
}

export function compileCreateApiKey(label: string, ttl?: string): string {
  return ttl === undefined
    ? `.apikey create ${label}`
    : `.apikey create ${label} ${validateTtl(ttl)}`;
}

export function compileExpireApiKey(label: string, ttl: string): string {
  return `.apikey expire ${label} ${validateTtl(ttl)}`;
}

/** `.apikey list` rows as {@link ApiKeyInfo}. */
export function parseApiKeys(columns: string[], rows: unknown[][]): ApiKeyInfo[] {
  return rows.map((row) => {
    const named = new Map(columns.map((column, i) => [column, row[i]]));
    const time = (column: string) => (named.get(column) as number | null | undefined) ?? null;
    return {
      label: String(named.get('label')),
      owner: String(named.get('owner')),
      createdAt: time('created_at'),
      expiresAt: time('expires_at'),
      lastUsedAt: time('last_used_at'),
      status: named.get('status') === 'expired' ? 'expired' : 'active',
    };
  });
}

export function compileListApiKeys(): string {
  return '.apikey list';
}

export function compileRevokeApiKey(label: string): string {
  return `.apikey revoke ${label}`;
}
