import httpProxy from "http-proxy"
import type { IncomingMessage, ServerResponse } from "node:http"
import type { Duplex } from "node:stream"
import type { Config } from "./config.js"

/**
 * Creates an HTTP proxy that forwards requests to the InputLayer backend.
 * Returns the proxy instance and handlers for both HTTP and WebSocket.
 */
export function createProxy(config: Config) {
  const target = `http://${config.inputlayer.host}:${config.inputlayer.port}`

  const proxy = httpProxy.createProxyServer({
    target,
    ws: true,
    changeOrigin: true,
    // Don't add x-forwarded headers to avoid confusing the backend
    xfwd: false,
  })

  proxy.on("error", (err, _req, res) => {
    console.error("[proxy] error:", err.message)
    if (res && "writeHead" in res && typeof res.writeHead === "function") {
      const httpRes = res as ServerResponse
      if (!httpRes.headersSent) {
        httpRes.writeHead(502, { "Content-Type": "text/plain" })
        httpRes.end("Bad Gateway - InputLayer backend unavailable")
      }
    }
  })

  return {
    proxy,

    /** Proxy an HTTP request to the InputLayer backend */
    web(req: IncomingMessage, res: ServerResponse) {
      proxy.web(req, res)
    },

    /** Proxy a WebSocket upgrade to the InputLayer backend */
    upgrade(req: IncomingMessage, socket: Duplex, head: Buffer) {
      proxy.ws(req, socket, head)
    },
  }
}
