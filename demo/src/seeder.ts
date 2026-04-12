import fs from "node:fs"
import path from "node:path"
import type { AdminClient } from "./admin-client.js"

/** List of demo knowledge graphs to seed. Each maps to a seeds/<name>.iql file. */
const DEMO_KGS = ["default", "flights", "rules_vectors", "retraction", "incremental", "provenance"]

/**
 * Parse an .iql file into executable statements.
 * Splits on blank lines (statement separator) and strips comments.
 * Multi-line statements (e.g. +fact[(...),\n  (...)]) are joined.
 */
function parseStatements(content: string): string[] {
  const statements: string[] = []
  let current = ""

  for (const line of content.split("\n")) {
    const trimmed = line.trim()

    // Skip pure comment lines
    if (trimmed.startsWith("//")) continue

    // Strip inline comments (but not inside strings)
    const stripped = trimmed.replace(/\/\/.*$/, "").trimEnd()

    if (stripped === "") {
      // Blank line = statement boundary
      if (current.trim()) {
        statements.push(current.trim())
      }
      current = ""
      continue
    }

    // A new line starting with '+' or '.' that isn't a continuation
    // of a multi-line rule (indicated by the previous line ending with
    // '<-' or ',') is a new statement.
    if ((stripped.startsWith("+") || stripped.startsWith(".")) && current.trim()) {
      const trimmedCurrent = current.trimEnd()
      const isContinuation = trimmedCurrent.endsWith("<-") || trimmedCurrent.endsWith(",")
      if (!isContinuation) {
        statements.push(current.trim())
        current = ""
      }
    }

    // Append to current statement
    current += (current ? "\n" : "") + stripped
  }

  // Don't forget the last statement
  if (current.trim()) {
    statements.push(current.trim())
  }

  return statements
}

/**
 * Check if a knowledge graph already has data (has been seeded before).
 * We check by listing relations - if any exist, consider it seeded.
 */
async function isSeeded(adminClient: AdminClient, kgName: string, seedsDir: string): Promise<boolean> {
  try {
    await adminClient.execute(`.kg use ${kgName}`)
    const rows = await adminClient.execute(".rel")
    if (rows.length === 0) return false

    // Check that at least the first relation from the seed file exists.
    // The "default" KG is auto-created by InputLayer with 0 user relations,
    // so just checking .rel length isn't enough.
    const filePath = path.join(seedsDir, `${kgName}.iql`)
    if (!fs.existsSync(filePath)) return false
    const content = fs.readFileSync(filePath, "utf-8")
    const match = content.match(/^\+(\w+)\(/m)
    if (!match) return rows.length > 0
    const firstRelation = match[1]
    const relNames = rows.map((r: unknown[]) => String(r[0]))
    return relNames.includes(firstRelation)
  } catch {
    return false
  }
}

/**
 * Seed a single knowledge graph from its .iql file.
 */
async function seedKg(
  adminClient: AdminClient,
  kgName: string,
  seedsDir: string
): Promise<void> {
  const filePath = path.join(seedsDir, `${kgName}.iql`)

  if (!fs.existsSync(filePath)) {
    console.warn(`[seeder] seed file not found: ${filePath}`)
    return
  }

  console.log(`[seeder] seeding ${kgName}...`)

  // Create KG (ignore error if it already exists)
  try {
    await adminClient.execute(`.kg create ${kgName}`)
  } catch (err) {
    const msg = String(err)
    if (!msg.includes("already exists")) {
      throw err
    }
  }

  await adminClient.execute(`.kg use ${kgName}`)

  const content = fs.readFileSync(filePath, "utf-8")
  const statements = parseStatements(content)

  let executed = 0
  for (const stmt of statements) {
    // Skip queries (?) and meta commands that aren't insertions
    // We only want to seed data (+) and rules (+...(...) <-)
    // But also allow .kg and other setup commands
    if (stmt.startsWith("?") || stmt.startsWith(".why") || stmt.startsWith(".why_not")) {
      continue
    }

    try {
      // Join multi-line statements into a single line - the engine
      // may not handle newlines in WebSocket execute messages
      const oneLine = stmt.replace(/\n\s*/g, " ")
      await adminClient.execute(oneLine)
      executed++
    } catch (err) {
      console.error(`[seeder] error in ${kgName}: ${err}`)
      console.error(`[seeder] statement: ${stmt.slice(0, 120)}...`)
      // Continue seeding - don't abort on individual statement failures
    }
  }

  console.log(`[seeder] ${kgName}: ${executed} statements executed`)
}

/**
 * Seed all demo knowledge graphs. Skips KGs that already have data.
 */
export async function seedAll(adminClient: AdminClient, seedsDir: string): Promise<void> {
  console.log("[seeder] checking demo knowledge graphs...")

  for (const kgName of DEMO_KGS) {
    const seeded = await isSeeded(adminClient, kgName, seedsDir)
    if (seeded) {
      console.log(`[seeder] ${kgName}: already seeded, skipping`)
      continue
    }

    try {
      await seedKg(adminClient, kgName, seedsDir)
    } catch (err) {
      console.error(`[seeder] failed to seed ${kgName}:`, err)
    }
  }

  // Switch back to default KG
  try {
    await adminClient.execute(".kg use default")
  } catch {
    // ignore
  }

  console.log("[seeder] done")
}
