import { describe, it, expect } from 'vitest';
import { compileCreateApiKey, compileExpireApiKey, parseApiKeys } from '../src/auth';

describe('API key commands', () => {
  it('creates a key with and without a TTL', () => {
    expect(compileCreateApiKey('svc')).toBe('.apikey create svc');
    expect(compileCreateApiKey('svc', '90d')).toBe('.apikey create svc 90d');
    expect(compileCreateApiKey('svc', '500ms')).toBe('.apikey create svc 500ms');
  });

  it('expires a key', () => {
    expect(compileExpireApiKey('svc', '24h')).toBe('.apikey expire svc 24h');
    expect(compileExpireApiKey('svc', '0s')).toBe('.apikey expire svc 0s');
  });

  it.each(['', '90', 'd', '1y', '-1d', '1.5h', '1d; .user drop x', '1 d'])(
    'rejects TTL %j before sending',
    (ttl) => {
      expect(() => compileCreateApiKey('svc', ttl)).toThrow();
      expect(() => compileExpireApiKey('svc', ttl)).toThrow();
    },
  );
});

describe('parseApiKeys', () => {
  it('maps .apikey list columns by name', () => {
    const columns = ['label', 'owner', 'created_at', 'expires_at', 'last_used_at', 'status'];
    const rows = [
      ['ci', 'admin', 1_700_000_000_000, 1_800_000_000_000, null, 'active'],
      ['legacy', 'bob', null, null, 1_750_000_000_000, 'expired'],
    ];
    expect(parseApiKeys(columns, rows)).toEqual([
      {
        label: 'ci',
        owner: 'admin',
        createdAt: 1_700_000_000_000,
        expiresAt: 1_800_000_000_000,
        lastUsedAt: null,
        status: 'active',
      },
      {
        label: 'legacy',
        owner: 'bob',
        createdAt: null,
        expiresAt: null,
        lastUsedAt: 1_750_000_000_000,
        status: 'expired',
      },
    ]);
  });
});
