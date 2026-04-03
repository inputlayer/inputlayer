import nodemailer from "nodemailer"
import type { Config } from "./config.js"

export interface EmailSender {
  sendInvite(to: string, inviteUrl: string, kg: string): Promise<void>
}

function buildHtml(inviteUrl: string, kg: string): string {
  return `<!DOCTYPE html>
<html>
<head><meta charset="utf-8"></head>
<body style="margin:0;padding:0;background:#ffffff;color:#09090b;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif">
  <div style="max-width:480px;margin:0 auto;padding:48px 24px">
    <div style="margin-bottom:32px">
      <strong style="font-size:18px;color:#09090b">InputLayer</strong>
    </div>
    <h1 style="font-size:24px;font-weight:600;margin:0 0 16px;color:#09090b">Your demo access is ready</h1>
    <p style="font-size:14px;line-height:1.6;color:#52525b;margin:0 0 24px">
      Click the button below to open the InputLayer Studio with the
      <strong style="color:#09090b">${kg}</strong> knowledge graph loaded.
      Your credentials will be set up automatically.
    </p>
    <a href="${inviteUrl}" style="display:inline-block;background:#2563eb;color:#fff;padding:12px 24px;border-radius:6px;text-decoration:none;font-size:14px;font-weight:500">
      Open in Studio
    </a>
    <p style="font-size:12px;line-height:1.6;color:#71717a;margin:24px 0 0">
      This link expires in 7 days. If it has expired, you can request a new one from the
      <a href="https://inputlayer.ai" style="color:#2563eb;text-decoration:none">InputLayer website</a>.
    </p>
  </div>
</body>
</html>`
}

function buildText(inviteUrl: string, kg: string): string {
  return `Your InputLayer demo access is ready.

Open the link below to access the Studio with the "${kg}" knowledge graph:

${inviteUrl}

This link expires in 7 days.`
}

export function createEmailSender(config: Config): EmailSender {
  if (config.smtp.transport === "console") {
    return {
      async sendInvite(to, inviteUrl, kg) {
        console.log(`[email] Would send invite to ${to}`)
        console.log(`[email] URL: ${inviteUrl}`)
        console.log(`[email] KG: ${kg}`)
      },
    }
  }

  const transporter = nodemailer.createTransport({
    host: config.smtp.host,
    port: config.smtp.port,
    secure: config.smtp.port === 465,
    auth: {
      user: config.smtp.user,
      pass: config.smtp.pass,
    },
  })

  return {
    async sendInvite(to, inviteUrl, kg) {
      await transporter.sendMail({
        from: config.smtp.from,
        to,
        subject: "Your InputLayer Demo Access",
        text: buildText(inviteUrl, kg),
        html: buildHtml(inviteUrl, kg),
      })
      console.log(`[email] invite sent to ${to}`)
    },
  }
}
