export interface Config {
  port: number
  baseUrl: string
  inputlayer: {
    host: string
    port: number
    adminUser: string
    adminPassword: string
  }
  invite: {
    expiryHours: number
    defaultKg: string
  }
  db: {
    path: string
  }
  smtp: {
    transport: "smtp" | "console"
    host: string
    port: number
    user: string
    pass: string
    from: string
  }
}

export function loadConfig(): Config {
  return {
    port: parseInt(process.env.DEMO_PORT || "3000", 10),
    baseUrl: process.env.DEMO_BASE_URL || "http://localhost:3000",
    inputlayer: {
      host: process.env.INPUTLAYER_HOST || "127.0.0.1",
      port: parseInt(process.env.INPUTLAYER_PORT || "8080", 10),
      adminUser: process.env.INPUTLAYER_ADMIN_USER || "admin",
      adminPassword: process.env.INPUTLAYER_ADMIN_PASSWORD || "",
    },
    invite: {
      expiryHours: parseInt(process.env.DEMO_INVITE_EXPIRY_HOURS || "168", 10),
      defaultKg: process.env.DEMO_DEFAULT_KG || "default",
    },
    db: {
      path: process.env.DEMO_DB_PATH || "./demo.db",
    },
    smtp: {
      transport: (process.env.SMTP_TRANSPORT || "smtp") as "smtp" | "console",
      host: process.env.SMTP_HOST || "",
      port: parseInt(process.env.SMTP_PORT || "587", 10),
      user: process.env.SMTP_USER || "",
      pass: process.env.SMTP_PASS || "",
      from: process.env.SMTP_FROM || "InputLayer Demo <demo@inputlayer.ai>",
    },
  }
}
