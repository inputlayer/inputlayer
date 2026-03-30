import Database from "better-sqlite3"
import type { Config } from "./config.js"

export interface Invite {
  hash: string
  email: string
  kg: string
  username: string
  password: string
  created_at: string
  expires_at: string
  claimed: number
  claimed_at: string | null
}

export class InviteDb {
  private db: Database.Database

  constructor(config: Config) {
    this.db = new Database(config.db.path)
    this.db.pragma("journal_mode = WAL")
    this.db.pragma("foreign_keys = ON")
    this.migrate()
  }

  private migrate() {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS invites (
        hash TEXT PRIMARY KEY,
        email TEXT NOT NULL,
        kg TEXT NOT NULL,
        username TEXT NOT NULL,
        password TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        expires_at TEXT NOT NULL,
        claimed INTEGER NOT NULL DEFAULT 0,
        claimed_at TEXT
      );
      CREATE INDEX IF NOT EXISTS idx_invites_email ON invites(email);
    `)
  }

  createInvite(invite: {
    hash: string
    email: string
    kg: string
    username: string
    password: string
    expiresAt: string
  }): void {
    this.db
      .prepare(
        `INSERT INTO invites (hash, email, kg, username, password, expires_at)
         VALUES (?, ?, ?, ?, ?, ?)`
      )
      .run(invite.hash, invite.email, invite.kg, invite.username, invite.password, invite.expiresAt)
  }

  claimInvite(hash: string): Invite | null {
    const invite = this.db
      .prepare(
        `SELECT * FROM invites
         WHERE hash = ? AND expires_at > datetime('now')`
      )
      .get(hash) as Invite | undefined

    if (!invite) return null

    // Mark as claimed (allow re-claiming within expiry window for sessionStorage recovery)
    if (!invite.claimed) {
      this.db
        .prepare(`UPDATE invites SET claimed = 1, claimed_at = datetime('now') WHERE hash = ?`)
        .run(hash)
    }

    return invite
  }

  getActiveInviteByEmail(email: string, kg: string): Invite | null {
    return (
      (this.db
        .prepare(
          `SELECT * FROM invites
           WHERE email = ? AND kg = ? AND expires_at > datetime('now')
           ORDER BY created_at DESC LIMIT 1`
        )
        .get(email, kg) as Invite | undefined) ?? null
    )
  }

  cleanupExpired(): number {
    const result = this.db
      .prepare(`DELETE FROM invites WHERE expires_at <= datetime('now')`)
      .run()
    return result.changes
  }

  close() {
    this.db.close()
  }
}
