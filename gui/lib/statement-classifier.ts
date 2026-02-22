export type StatementType =
  | "persistent-rule"
  | "session-rule"
  | "insert"
  | "delete"
  | "query"
  | "meta"
  | "session-fact"
  | "schema"
  | "comment"

/** Classify each line of a Datalog program by statement type. */
export function classifyLines(text: string): (StatementType | null)[] {
  const lines = text.split("\n")
  const result: (StatementType | null)[] = []
  let inBlockComment = false
  let parentType: StatementType | null = null

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i]
    const trimmed = line.trimStart()
    const leadingWhitespace = line.length - trimmed.length

    // Handle block comments
    if (inBlockComment) {
      result.push("comment")
      if (trimmed.includes("*/")) {
        inBlockComment = false
        parentType = null
      }
      continue
    }

    // Empty or whitespace-only line
    if (trimmed.length === 0) {
      result.push(null)
      // Don't reset parentType on blank lines within a statement
      continue
    }

    // Line comment
    if (trimmed.startsWith("//")) {
      result.push("comment")
      continue
    }

    // Block comment start
    if (trimmed.startsWith("/*")) {
      result.push("comment")
      if (!trimmed.includes("*/")) {
        inBlockComment = true
      }
      parentType = null
      continue
    }

    // Continuation line: indented and doesn't start a new statement.
    // Any indented line that doesn't begin with a statement prefix is a continuation.
    if (
      leadingWhitespace > 0 &&
      parentType &&
      !trimmed.startsWith("+") &&
      !trimmed.startsWith("-") &&
      !trimmed.startsWith("?") &&
      !trimmed.startsWith(".") &&
      !trimmed.startsWith("~") &&
      !trimmed.startsWith("//") &&
      !trimmed.startsWith("/*")
    ) {
      result.push(parentType)
      continue
    }

    // Meta command
    if (trimmed.startsWith(".")) {
      parentType = "meta"
      result.push("meta")
      continue
    }

    // Query
    if (trimmed.startsWith("?")) {
      parentType = "query"
      result.push("query")
      continue
    }

    // Persistent rule, schema, or insert (starts with +)
    if (trimmed.startsWith("+")) {
      if (trimmed.includes("<-")) {
        parentType = "persistent-rule"
        result.push("persistent-rule")
      } else if (/^\+\w+\([^)]*:/.test(trimmed)) {
        parentType = "schema"
        result.push("schema")
      } else {
        parentType = "insert"
        result.push("insert")
      }
      continue
    }

    // Delete (starts with -)
    if (trimmed.startsWith("-")) {
      parentType = "delete"
      result.push("delete")
      continue
    }

    // Session rule with ~ prefix
    if (trimmed.startsWith("~")) {
      parentType = "session-rule"
      result.push("session-rule")
      continue
    }

    // Starts lowercase: session rule (has <-) or session fact (has parenthesis)
    if (/^[a-z]/.test(trimmed)) {
      if (trimmed.includes("<-")) {
        parentType = "session-rule"
        result.push("session-rule")
      } else if (trimmed.includes("(")) {
        parentType = "session-fact"
        result.push("session-fact")
      } else {
        parentType = null
        result.push(null)
      }
      continue
    }

    // Unrecognized
    parentType = null
    result.push(null)
  }

  return result
}

const TYPE_LABELS: Record<StatementType, string> = {
  "persistent-rule": "Persistent rule",
  "session-rule": "Session rule",
  "insert": "Insert",
  "delete": "Delete",
  "query": "Query",
  "meta": "Meta command",
  "session-fact": "Session fact",
  "schema": "Schema",
  "comment": "Comment",
}

export function getStatementLabel(type: StatementType): string {
  return TYPE_LABELS[type]
}
