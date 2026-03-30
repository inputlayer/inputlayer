import { Hono } from "hono"
import crypto from "node:crypto"
import type { Config } from "./config.js"
import type { InviteDb } from "./db.js"
import type { AdminClient } from "./admin-client.js"
import type { EmailSender } from "./email.js"
import { gatewayPage } from "./pages/gateway.js"
import { requestAccessPage } from "./pages/request-access.js"

const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/
const KG_NAME_RE = /^[a-zA-Z_][a-zA-Z0-9_]*$/

/** Only allow access requests for demo KGs that actually exist */
const ALLOWED_KGS = new Set([
  "default", "flights", "rules_vectors", "retraction", "incremental", "provenance",
])

/** Simple in-memory rate limiter: max requests per IP per window */
class RateLimiter {
  private hits = new Map<string, { count: number; resetAt: number }>()

  constructor(
    private maxRequests: number,
    private windowMs: number,
  ) {}

  check(key: string): boolean {
    const now = Date.now()
    const entry = this.hits.get(key)
    if (!entry || now > entry.resetAt) {
      this.hits.set(key, { count: 1, resetAt: now + this.windowMs })
      return true
    }
    if (entry.count >= this.maxRequests) return false
    entry.count++
    return true
  }
}

interface Deps {
  config: Config
  db: InviteDb
  adminClient: AdminClient
  emailSender: EmailSender
}

export function createRoutes(deps: Deps): Hono {
  const { config, db, adminClient, emailSender } = deps
  const app = new Hono()

  // 5 access requests per IP per 10 minutes
  const accessLimiter = new RateLimiter(5, 10 * 60 * 1000)

  // Gateway page at root
  app.get("/", (c) => {
    return c.html(gatewayPage())
  })

  // Request access page
  app.get("/demo/request-access", (c) => {
    const raw = c.req.query("kg") || config.invite.defaultKg
    const kg = ALLOWED_KGS.has(raw) ? raw : config.invite.defaultKg
    return c.html(requestAccessPage(kg))
  })

  // API: Request demo access (email capture + user provisioning)
  app.post("/demo/api/request-access", async (c) => {
    try {
      // Rate limit by IP
      const ip = c.req.header("x-forwarded-for")?.split(",")[0]?.trim() || "unknown"
      if (!accessLimiter.check(ip)) {
        return c.json({ error: "Too many requests. Please try again in a few minutes." }, 429)
      }

      const body = await c.req.json<{ email?: string; kg?: string }>()
      const email = body.email?.trim().toLowerCase()
      const kg = body.kg?.trim() || config.invite.defaultKg

      if (!email || !EMAIL_RE.test(email)) {
        return c.json({ error: "Please enter a valid email address." }, 400)
      }

      if (!KG_NAME_RE.test(kg) || !ALLOWED_KGS.has(kg)) {
        return c.json({ error: "Invalid knowledge graph." }, 400)
      }

      // Check for existing active invite for this email + kg
      const existing = db.getActiveInviteByEmail(email, kg)
      if (existing) {
        // Resend the same invite
        const inviteUrl = `${config.baseUrl}/?invite=${existing.hash}`
        await emailSender.sendInvite(email, inviteUrl, kg)
        return c.json({ success: true })
      }

      // Generate credentials
      const username = `demo_${crypto.randomBytes(4).toString("hex")}`
      const password = crypto.randomBytes(12).toString("base64url").slice(0, 16)

      // Create user on InputLayer
      await adminClient.createDemoUser(username, password, kg)

      // Generate invite
      const hash = crypto.randomBytes(32).toString("hex")
      const expiresAt = new Date(
        Date.now() + config.invite.expiryHours * 60 * 60 * 1000
      ).toISOString()

      db.createInvite({ hash, email, kg, username, password, expiresAt })

      // Send email
      const inviteUrl = `${config.baseUrl}/?invite=${hash}`
      await emailSender.sendInvite(email, inviteUrl, kg)

      console.log(`[routes] access requested: email=${email} kg=${kg} user=${username}`)
      return c.json({ success: true })
    } catch (err) {
      console.error("[routes] request-access error:", err)
      return c.json({ error: "Failed to create demo access. Please try again." }, 500)
    }
  })

  // API: Claim an invite (returns credentials for browser storage)
  app.post("/demo/api/invites/:hash/claim", async (c) => {
    try {
      const hash = c.req.param("hash")
      const invite = db.claimInvite(hash)

      if (!invite) {
        return c.json(
          { error: "This invite link has expired or is invalid. Please request a new one." },
          404
        )
      }

      return c.json({
        kg: invite.kg,
        username: invite.username,
        password: invite.password,
      })
    } catch (err) {
      console.error("[routes] claim error:", err)
      return c.json({ error: "Failed to claim invite." }, 500)
    }
  })

  // API: Health check
  app.get("/demo/api/health", (c) => {
    return c.json({ status: "ok" })
  })

  return app
}
