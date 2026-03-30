import http from "node:http"
import path from "node:path"
import { fileURLToPath } from "node:url"
import { loadConfig } from "./config.js"
import { InviteDb } from "./db.js"
import { AdminClient } from "./admin-client.js"
import { createEmailSender } from "./email.js"
import { createRoutes } from "./routes.js"
import { createProxy } from "./proxy.js"
import { seedAll } from "./seeder.js"

// Load .env file if present (for local development)
try {
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  const dotenv = await import("dotenv" as string)
  dotenv.config()
} catch {
  // dotenv not installed or .env not present - use process.env as-is
}

const config = loadConfig()

// Initialize components
const db = new InviteDb(config)
const adminClient = new AdminClient(config)
const emailSender = createEmailSender(config)

// Connect admin client to InputLayer
console.log(
  `[demo] connecting to InputLayer at ${config.inputlayer.host}:${config.inputlayer.port}...`
)
try {
  await adminClient.connect()

  // Seed demo knowledge graphs on first boot
  const __dirname = path.dirname(fileURLToPath(import.meta.url))
  const seedsDir = process.env.DEMO_SEEDS_DIR || path.resolve(__dirname, "../seeds")
  await seedAll(adminClient, seedsDir)
} catch (err) {
  console.error("[demo] failed to connect admin client:", err)
  console.error("[demo] will retry on first request")
}

// Create Hono app with demo routes
const honoApp = createRoutes({ config, db, adminClient, emailSender })

// Create reverse proxy to InputLayer
const proxyHandler = createProxy(config)

/** Check if a request should be handled by the demo service (not proxied) */
function isDemoRoute(pathname: string, hasInviteParam: boolean): boolean {
  if (pathname === "/" && hasInviteParam) return true // invite claim
  if (pathname === "/") return true // gateway
  if (pathname.startsWith("/demo/")) return true
  return false
}

// Create raw HTTP server
const server = http.createServer(async (req, res) => {
  const url = new URL(req.url || "/", `http://${req.headers.host || "localhost"}`)
  const hasInviteParam = url.searchParams.has("invite")

  if (isDemoRoute(url.pathname, hasInviteParam)) {
    // Convert Node.js request to Web Request for Hono
    const headers = new Headers()
    for (const [key, val] of Object.entries(req.headers)) {
      if (val) headers.set(key, Array.isArray(val) ? val.join(", ") : val)
    }

    const webUrl = `${url.protocol}//${url.host}${url.pathname}${url.search}`

    // Collect request body for non-GET/HEAD methods
    let body: Buffer | undefined
    if (req.method !== "GET" && req.method !== "HEAD") {
      const chunks: Buffer[] = []
      for await (const chunk of req) {
        chunks.push(typeof chunk === "string" ? Buffer.from(chunk) : chunk)
      }
      body = Buffer.concat(chunks)
    }

    const webReq = new Request(webUrl, {
      method: req.method,
      headers,
      body: body ? new Uint8Array(body) : undefined,
    })

    try {
      const response = await honoApp.fetch(webReq)

      // Write Hono response back to Node.js response
      res.writeHead(response.status, Object.fromEntries(response.headers.entries()))
      if (response.body) {
        const reader = response.body.getReader()
        const pump = async () => {
          while (true) {
            const { done, value } = await reader.read()
            if (done) break
            res.write(value)
          }
          res.end()
        }
        await pump()
      } else {
        res.end()
      }
    } catch (err) {
      console.error("[demo] hono error:", err)
      if (!res.headersSent) {
        res.writeHead(500, { "Content-Type": "text/plain" })
        res.end("Internal Server Error")
      }
    }
    return
  }

  // Proxy to InputLayer
  proxyHandler.web(req, res)
})

// WebSocket upgrades - always proxy to InputLayer
server.on("upgrade", (req, socket, head) => {
  proxyHandler.upgrade(req, socket, head)
})

server.listen(config.port, () => {
  console.log(`[demo] gateway listening on http://localhost:${config.port}`)
  console.log(`[demo] proxying to InputLayer at ${config.inputlayer.host}:${config.inputlayer.port}`)
})

// Periodic cleanup of expired invites
setInterval(() => {
  const cleaned = db.cleanupExpired()
  if (cleaned > 0) {
    console.log(`[demo] cleaned up ${cleaned} expired invites`)
  }
}, 60 * 60 * 1000)

// Graceful shutdown
function shutdown() {
  console.log("[demo] shutting down...")
  adminClient.disconnect()
  db.close()
  server.close()
  process.exit(0)
}

process.on("SIGINT", shutdown)
process.on("SIGTERM", shutdown)
