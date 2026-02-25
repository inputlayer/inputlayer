/**
 * Syntax highlighting tokenizer for InputLayer Datalog.
 *
 * Port of the PEG grammar in src/syntax/datalog.pest to a JS regex-based
 * tokenizer.  Produces classified spans that can be rendered as colored
 * HTML in the editor overlay.
 */

// ── Token types ────────────────────────────────────────────────────────

export type TokenKind =
  | "comment"
  | "string"
  | "meta"
  | "query"
  | "arrow"
  | "operator"
  | "negation"
  | "comparison"
  | "number"
  | "aggregate"
  | "builtin"
  | "keyword"
  | "variable"
  | "identifier"
  | "body-identifier"
  | "arith"
  | "punctuation"
  | "whitespace"
  | "unknown"

export interface Token {
  kind: TokenKind
  text: string
}

// ── Keyword sets (from datalog.pest) ───────────────────────────────────

const AGGREGATES = new Set([
  "count_distinct", "count", "sum", "avg",
  "top_k_threshold", "top_k", "within_radius",
  "min", "max",
])

const BUILTINS = new Set([
  // distance
  "euclidean_int8", "euclidean", "cosine_int8", "cosine",
  "dot_int8", "dot", "manhattan_int8", "manhattan",
  // vector
  "normalize", "vec_dim", "vec_add", "vec_scale",
  // quantization
  "quantize_linear", "quantize_symmetric", "dequantize_scaled", "dequantize",
  // LSH
  "lsh_multi_probe", "lsh_bucket", "lsh_probes",
  // HNSW
  "hnsw_nearest",
  // temporal
  "time_decay_linear", "time_decay", "time_diff", "time_add", "time_sub",
  "time_now", "time_before", "time_after", "time_between",
  "within_last", "intervals_overlap", "interval_contains",
  "interval_duration", "point_in_interval",
  // math
  "abs_int64", "abs_float64", "abs", "sqrt", "pow", "log", "exp",
  "sin", "cos", "tan", "floor", "ceil", "sign",
  // conversion
  "to_float", "to_int",
  // string
  "len", "upper", "lower", "trim", "substr", "replace", "concat",
  // scalar
  "min_val", "max_val",
])

const KEYWORDS = new Set([
  "type", "true", "false", "int", "string", "bool", "float", "list",
  "vector", "timestamp",
])

// ── Ordered regex patterns ─────────────────────────────────────────────
// Order matters: most specific first (mirrors pest ordered choice).

const TOKEN_REGEX = new RegExp(
  [
    // Block comment
    /\/\*[\s\S]*?(?:\*\/|$)/.source,
    // Line comment
    /\/\/[^\n]*/.source,
    // String literal (with escapes)
    /"(?:[^"\\]|\\.)*(?:"|$)/.source,
    // Meta commands (dot-prefix) - must precede punctuation
    /\.(?:kg\s+(?:create|list|use|drop)|rel|rule\s+(?:list|drop|remove|def|clear|edit|query)|session\s+(?:clear|drop)|index\s+(?:list|create|drop|stats|rebuild)|user\s+(?:list|create|drop|password|role)|apikey\s+(?:create|list|revoke)|kg|rule|session|index|user|apikey|load|compact|status|help|quit|exit|explain|\?|q)\b/.source,
    // Rule arrow
    /<-/.source,
    // Comparison operators (before < > punctuation)
    />=|<=|!=/.source,
    // Query marker: ? followed by alpha
    /\?(?=[A-Za-z])/.source,
    // Operator prefix: + or - followed by alpha/quote/bracket
    /[+\-](?=[a-z"~\[])/.source,
    // Session rule prefix: ~ followed by alpha
    /~(?=[a-z])/.source,
    // Negation prefix: ! followed by alpha
    /!(?=[A-Za-z])/.source,
    // Float number (before integer)
    /\b[0-9]+\.[0-9]+(?:[eE][+\-]?[0-9]+)?\b/.source,
    // Integer number
    /\b[0-9]+\b/.source,
    // Word boundary tokens (identifiers, variables, keywords, etc.)
    /\b[A-Za-z_][A-Za-z0-9_]*\b/.source,
    // Arithmetic operators
    /[+\-*/%]/.source,
    // Comparison single-char (after multi-char comparisons matched above)
    /[<>=]/.source,
    // Punctuation
    /[()[\]{},.:;]/.source,
    // Whitespace
    /\s+/.source,
    // Catch-all
    /./.source,
  ].join("|"),
  "g"
)

// ── Classify a matched token ───────────────────────────────────────────

function classifyWord(word: string, inBody: boolean): TokenKind {
  if (AGGREGATES.has(word)) return "aggregate"
  if (BUILTINS.has(word)) return "builtin"
  if (KEYWORDS.has(word)) return "keyword"
  // Variable: starts with uppercase or _
  if (/^[A-Z_]/.test(word)) return "variable"
  // Identifier (lowercase start)
  if (/^[a-z]/.test(word)) return inBody ? "body-identifier" : "identifier"
  return "unknown"
}

function classify(match: string, inBody: boolean): TokenKind {
  if (match.startsWith("/*") || match.startsWith("//")) return "comment"
  if (match.startsWith('"')) return "string"
  if (match.startsWith(".")) return "meta"
  if (match === "<-") return "arrow"
  if (match === ">=" || match === "<=" || match === "!=") return "comparison"
  if (match === "?" && match.length === 1) return "query"
  if (/^[+\-]$/.test(match)) return "arith"
  if (match === "~") return "operator"
  if (match === "!") return "negation"
  if (/^[0-9]/.test(match)) return "number"
  if (/^[A-Za-z_]/.test(match)) return classifyWord(match, inBody)
  if (/^[+\-*/%]$/.test(match)) return "arith"
  if (/^[<>=]$/.test(match)) return "comparison"
  if (/^[()[\]{},.:;]$/.test(match)) return "punctuation"
  if (/^\s+$/.test(match)) return "whitespace"
  return "unknown"
}

// ── Public API ─────────────────────────────────────────────────────────

/** Tokenize a string of Datalog input into classified spans. */
export function tokenize(input: string): Token[] {
  const tokens: Token[] = []
  let inBody = false

  TOKEN_REGEX.lastIndex = 0
  let m: RegExpExecArray | null
  while ((m = TOKEN_REGEX.exec(input)) !== null) {
    const text = m[0]
    const kind = classify(text, inBody)

    // Track head vs body: after `<-` we're in the body until a newline
    // that is NOT followed by indentation (continuation).
    if (kind === "arrow") {
      inBody = true
    } else if (kind === "whitespace" && text.includes("\n")) {
      // Only reset to head context if the text after the last newline
      // has no leading indentation - indented lines are continuations.
      const afterLastNL = text.substring(text.lastIndexOf("\n") + 1)
      if (!/[ \t]/.test(afterLastNL)) {
        inBody = false
      }
    }

    tokens.push({ kind, text })
  }

  return tokens
}

// ── CSS class for each token kind ──────────────────────────────────────

// Colors designed for dark backgrounds, matching the ANSI palette from
// src/syntax/mod.rs.  The actual color values are defined as CSS custom
// properties in globals.css so themes can override them.

const KIND_TO_CLASS: Record<TokenKind, string> = {
  "comment":         "syn-comment",
  "string":          "syn-string",
  "meta":            "syn-meta",
  "query":           "syn-query",
  "arrow":           "syn-arrow",
  "operator":        "syn-operator",
  "negation":        "syn-negation",
  "comparison":      "syn-comparison",
  "number":          "syn-number",
  "aggregate":       "syn-aggregate",
  "builtin":         "syn-builtin",
  "keyword":         "syn-keyword",
  "variable":        "syn-variable",
  "identifier":      "syn-identifier",
  "body-identifier": "syn-body-id",
  "arith":           "syn-arith",
  "punctuation":     "syn-punct",
  "whitespace":      "",
  "unknown":         "",
}

/** Render tokens as an HTML string with <span class="syn-*"> wrappers. */
export function tokensToHtml(tokens: Token[]): string {
  let html = ""
  for (const t of tokens) {
    // Escape HTML entities
    const escaped = t.text
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")

    const cls = KIND_TO_CLASS[t.kind]
    if (cls) {
      html += `<span class="${cls}">${escaped}</span>`
    } else {
      html += escaped
    }
  }
  // Ensure trailing newline so the overlay height matches the textarea
  if (!html.endsWith("\n")) html += "\n"
  return html
}

/** Convenience: tokenize + render to HTML in one call. */
export function highlightToHtml(input: string): string {
  return tokensToHtml(tokenize(input))
}
