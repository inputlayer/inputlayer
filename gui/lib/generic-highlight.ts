/**
 * Lightweight syntax highlighting for common languages used in docs.
 * Uses simple regex tokenizers — not a full parser, but good enough
 * for documentation code blocks.
 */

function escapeHtml(text: string): string {
  return text
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
}

interface Rule {
  pattern: RegExp
  className: string
}

function highlightWithRules(code: string, rules: Rule[]): string {
  // Build a combined regex from all rules
  const combined = new RegExp(
    rules.map((r, i) => `(${r.pattern.source})`).join("|"),
    "gm"
  )

  let result = ""
  let lastIndex = 0

  combined.lastIndex = 0
  let m: RegExpExecArray | null
  while ((m = combined.exec(code)) !== null) {
    // Add any unmatched text before this match
    if (m.index > lastIndex) {
      result += escapeHtml(code.slice(lastIndex, m.index))
    }

    // Find which group matched
    let cls = ""
    for (let i = 0; i < rules.length; i++) {
      if (m[i + 1] !== undefined) {
        cls = rules[i].className
        break
      }
    }

    if (cls) {
      result += `<span class="${cls}">${escapeHtml(m[0])}</span>`
    } else {
      result += escapeHtml(m[0])
    }

    lastIndex = m.index + m[0].length
    // Prevent infinite loop on zero-length matches
    if (m[0].length === 0) {
      combined.lastIndex++
      lastIndex++
    }
  }

  // Add remaining text
  if (lastIndex < code.length) {
    result += escapeHtml(code.slice(lastIndex))
  }

  return result
}

// ── Python ──────────────────────────────────────────────────────────────

const PYTHON_RULES: Rule[] = [
  { pattern: /#[^\n]*/, className: "syn-comment" },
  { pattern: /"""[\s\S]*?"""|'''[\s\S]*?'''/, className: "syn-string" },
  { pattern: /f"(?:[^"\\]|\\.)*"|f'(?:[^'\\]|\\.)*'/, className: "syn-string" },
  { pattern: /"(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*'/, className: "syn-string" },
  { pattern: /\b(?:def|class|return|if|elif|else|for|while|with|as|try|except|finally|raise|import|from|yield|async|await|lambda|pass|break|continue|and|or|not|in|is|global|nonlocal|assert|del)\b/, className: "syn-keyword" },
  { pattern: /\b(?:True|False|None)\b/, className: "syn-number" },
  { pattern: /\b(?:print|len|range|type|int|str|float|list|dict|set|tuple|bool|isinstance|enumerate|zip|map|filter|sorted|open|super|property|staticmethod|classmethod)\b/, className: "syn-builtin" },
  { pattern: /\b[0-9]+(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?\b/, className: "syn-number" },
  { pattern: /@\w+/, className: "syn-meta" },
  { pattern: /\b(?:self|cls)\b/, className: "syn-variable" },
]

// ── Bash / Shell ────────────────────────────────────────────────────────

const BASH_RULES: Rule[] = [
  { pattern: /#[^\n]*/, className: "syn-comment" },
  { pattern: /"(?:[^"\\]|\\.)*"|'[^']*'/, className: "syn-string" },
  { pattern: /\$\{[^}]*\}|\$\w+/, className: "syn-variable" },
  { pattern: /\b(?:if|then|else|elif|fi|for|while|do|done|case|esac|function|return|local|export|source|alias|unalias|set|unset|declare|readonly|shift|trap|exit|exec|eval)\b/, className: "syn-keyword" },
  { pattern: /\b(?:echo|cd|ls|cat|grep|sed|awk|find|mkdir|rm|cp|mv|chmod|chown|curl|wget|tar|ssh|sudo|apt|pip|npm|cargo|make|docker|git)\b/, className: "syn-builtin" },
  { pattern: /[|&;><]+/, className: "syn-operator" },
  { pattern: /--?[\w-]+/, className: "syn-meta" },
]

// ── JSON ────────────────────────────────────────────────────────────────

const JSON_RULES: Rule[] = [
  { pattern: /"(?:[^"\\]|\\.)*"\s*(?=:)/, className: "syn-identifier" },
  { pattern: /"(?:[^"\\]|\\.)*"/, className: "syn-string" },
  { pattern: /\b(?:true|false|null)\b/, className: "syn-keyword" },
  { pattern: /-?\b[0-9]+(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?\b/, className: "syn-number" },
]

// ── TOML ────────────────────────────────────────────────────────────────

const TOML_RULES: Rule[] = [
  { pattern: /#[^\n]*/, className: "syn-comment" },
  { pattern: /\[\[?[\w.\-]+\]\]?/, className: "syn-meta" },
  { pattern: /"""[\s\S]*?"""|'''[\s\S]*?'''/, className: "syn-string" },
  { pattern: /"(?:[^"\\]|\\.)*"|'[^']*'/, className: "syn-string" },
  { pattern: /\b(?:true|false)\b/, className: "syn-keyword" },
  { pattern: /\b[0-9]{4}-[0-9]{2}-[0-9]{2}(?:T[0-9:]+)?/, className: "syn-number" },
  { pattern: /-?\b[0-9]+(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?\b/, className: "syn-number" },
  { pattern: /^[\w.-]+(?=\s*=)/m, className: "syn-identifier" },
]

// ── YAML ────────────────────────────────────────────────────────────────

const YAML_RULES: Rule[] = [
  { pattern: /#[^\n]*/, className: "syn-comment" },
  { pattern: /"(?:[^"\\]|\\.)*"|'[^']*'/, className: "syn-string" },
  { pattern: /\b(?:true|false|null|yes|no|on|off)\b/i, className: "syn-keyword" },
  { pattern: /-?\b[0-9]+(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?\b/, className: "syn-number" },
  { pattern: /^[\w.-]+(?=\s*:)/m, className: "syn-identifier" },
  { pattern: /&\w+|\*\w+/, className: "syn-variable" },
]

// ── JavaScript ──────────────────────────────────────────────────────────

const JS_RULES: Rule[] = [
  { pattern: /\/\/[^\n]*/, className: "syn-comment" },
  { pattern: /\/\*[\s\S]*?\*\//, className: "syn-comment" },
  { pattern: /`(?:[^`\\]|\\.)*`/, className: "syn-string" },
  { pattern: /"(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*'/, className: "syn-string" },
  { pattern: /\b(?:const|let|var|function|return|if|else|for|while|do|switch|case|break|continue|class|extends|new|this|import|export|from|default|async|await|yield|throw|try|catch|finally|typeof|instanceof|in|of|delete|void)\b/, className: "syn-keyword" },
  { pattern: /\b(?:true|false|null|undefined|NaN|Infinity)\b/, className: "syn-number" },
  { pattern: /\b(?:console|Math|JSON|Array|Object|String|Number|Boolean|Promise|Map|Set|RegExp|Error|Date|Symbol|parseInt|parseFloat|isNaN|fetch|setTimeout|setInterval)\b/, className: "syn-builtin" },
  { pattern: /\b[0-9]+(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?\b/, className: "syn-number" },
  { pattern: /=>/, className: "syn-arrow" },
]

// ── Rust ────────────────────────────────────────────────────────────────

const RUST_RULES: Rule[] = [
  { pattern: /\/\/[^\n]*/, className: "syn-comment" },
  { pattern: /\/\*[\s\S]*?\*\//, className: "syn-comment" },
  { pattern: /"(?:[^"\\]|\\.)*"/, className: "syn-string" },
  { pattern: /\b(?:fn|let|mut|const|static|struct|enum|impl|trait|type|pub|use|mod|crate|super|self|Self|where|for|loop|while|if|else|match|return|break|continue|move|async|await|unsafe|extern|ref|dyn|as|in)\b/, className: "syn-keyword" },
  { pattern: /\b(?:true|false)\b/, className: "syn-number" },
  { pattern: /\b(?:String|Vec|Option|Result|Box|Rc|Arc|Cell|RefCell|HashMap|HashSet|BTreeMap|BTreeSet|Ok|Err|Some|None|println|eprintln|format|panic|assert|todo|unimplemented|dbg)\b/, className: "syn-builtin" },
  { pattern: /\b(?:i8|i16|i32|i64|i128|isize|u8|u16|u32|u64|u128|usize|f32|f64|bool|char|str)\b/, className: "syn-keyword" },
  { pattern: /\b[0-9]+(?:\.[0-9]+)?(?:_?[0-9]+)*(?:[eE][+-]?[0-9]+)?(?:i32|i64|u32|u64|f32|f64|usize|isize)?\b/, className: "syn-number" },
  { pattern: /#\[[\w:(,\s"')]*\]/, className: "syn-meta" },
  { pattern: /&(?:mut\b)?|'[a-z]\w*/, className: "syn-operator" },
]

// ── HTTP ────────────────────────────────────────────────────────────────

const HTTP_RULES: Rule[] = [
  { pattern: /\b(?:GET|POST|PUT|DELETE|PATCH|HEAD|OPTIONS)\b/, className: "syn-keyword" },
  { pattern: /HTTP\/[0-9.]+/, className: "syn-meta" },
  { pattern: /\b[0-9]{3}\b/, className: "syn-number" },
  { pattern: /^[\w-]+(?=:)/m, className: "syn-identifier" },
  { pattern: /"(?:[^"\\]|\\.)*"/, className: "syn-string" },
]

// ── Dockerfile ──────────────────────────────────────────────────────────

const DOCKERFILE_RULES: Rule[] = [
  { pattern: /#[^\n]*/, className: "syn-comment" },
  { pattern: /"(?:[^"\\]|\\.)*"|'[^']*'/, className: "syn-string" },
  { pattern: /\b(?:FROM|RUN|CMD|LABEL|MAINTAINER|EXPOSE|ENV|ADD|COPY|ENTRYPOINT|VOLUME|USER|WORKDIR|ARG|ONBUILD|STOPSIGNAL|HEALTHCHECK|SHELL|AS)\b/, className: "syn-keyword" },
  { pattern: /\$\{[^}]*\}|\$\w+/, className: "syn-variable" },
]

// ── EBNF ────────────────────────────────────────────────────────────────

const EBNF_RULES: Rule[] = [
  { pattern: /\(\*[\s\S]*?\*\)/, className: "syn-comment" },
  { pattern: /"[^"]*"|'[^']*'/, className: "syn-string" },
  { pattern: /::=|[|;]/, className: "syn-operator" },
  { pattern: /[[\]{}()]/, className: "syn-punct" },
  { pattern: /<[\w-]+>/, className: "syn-variable" },
]

// ── Nginx / Caddyfile / INI (config-style) ──────────────────────────────

const CONFIG_RULES: Rule[] = [
  { pattern: /#[^\n]*/, className: "syn-comment" },
  { pattern: /"(?:[^"\\]|\\.)*"|'[^']*'/, className: "syn-string" },
  { pattern: /\b[0-9]+[smhd]?\b/, className: "syn-number" },
  { pattern: /\$[\w]+/, className: "syn-variable" },
  { pattern: /\b(?:server|location|listen|root|proxy_pass|upstream|error_log|access_log|worker_processes|events|http|include|ssl_certificate|ssl_certificate_key|reverse_proxy|tls|encode|file_server|header|respond|rewrite|handle|route|log)\b/, className: "syn-keyword" },
  { pattern: /^\[[\w.-]+\]/m, className: "syn-meta" },
  { pattern: /^[\w.-]+(?=\s*[={])/m, className: "syn-identifier" },
]

// ── Language dispatch ───────────────────────────────────────────────────

const LANGUAGE_RULES: Record<string, Rule[]> = {
  python: PYTHON_RULES,
  py: PYTHON_RULES,
  bash: BASH_RULES,
  sh: BASH_RULES,
  shell: BASH_RULES,
  zsh: BASH_RULES,
  json: JSON_RULES,
  toml: TOML_RULES,
  yaml: YAML_RULES,
  yml: YAML_RULES,
  javascript: JS_RULES,
  js: JS_RULES,
  typescript: JS_RULES,
  ts: JS_RULES,
  rust: RUST_RULES,
  rs: RUST_RULES,
  http: HTTP_RULES,
  dockerfile: DOCKERFILE_RULES,
  docker: DOCKERFILE_RULES,
  ebnf: EBNF_RULES,
  bnf: EBNF_RULES,
  nginx: CONFIG_RULES,
  caddyfile: CONFIG_RULES,
  ini: CONFIG_RULES,
  conf: CONFIG_RULES,
}

/**
 * Highlight a code block for the given language.
 * Returns HTML string with <span class="syn-*"> wrappers,
 * or null if the language is not supported.
 */
export function highlightGeneric(code: string, language: string): string | null {
  const rules = LANGUAGE_RULES[language.toLowerCase()]
  if (!rules) return null
  return highlightWithRules(code, rules)
}
