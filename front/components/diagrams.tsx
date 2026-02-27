"use client"

import { cn } from "@/lib/utils"

/* ------------------------------------------------------------------ */
/*  Shared utilities                                                   */
/* ------------------------------------------------------------------ */

type NodeTag = "highlight" | "success" | "primary" | "muted" | null

function parseTag(text: string): { text: string; tag: NodeTag } {
  const m = text.match(/\s*\[(highlight|success|primary|muted)\]\s*$/)
  if (m) return { text: text.slice(0, m.index).trim(), tag: m[1] as NodeTag }
  return { text: text.trim(), tag: null }
}

function nodeStyles(tag: NodeTag, conclusion = false) {
  if (conclusion)
    return "bg-primary/10 border-2 border-primary font-medium"
  switch (tag) {
    case "highlight":
      return "bg-red-50 dark:bg-red-950/30 border-red-300 dark:border-red-800 text-red-800 dark:text-red-200 font-medium"
    case "success":
      return "bg-emerald-50 dark:bg-emerald-950/30 border-emerald-300 dark:border-emerald-800 text-emerald-800 dark:text-emerald-200"
    case "primary":
      return "bg-primary/10 border-primary/60"
    case "muted":
      return "bg-muted/50 border-border text-muted-foreground"
    default:
      return "bg-card border-border"
  }
}

/* ------------------------------------------------------------------ */
/*  Chain Diagram – vertical connected nodes                           */
/*                                                                     */
/*  Format:                                                            */
/*    Node text                                                        */
/*    -- edge label                                                    */
/*    Node text [highlight]                                            */
/*    => Conclusion text                                               */
/* ------------------------------------------------------------------ */

type ChainItem =
  | { kind: "node"; text: string; tag: NodeTag; conclusion: boolean }
  | { kind: "edge"; label: string }

function parseChain(src: string): ChainItem[] {
  return src
    .split("\n")
    .filter((l) => l.trim())
    .map((raw) => {
      const t = raw.trim()
      if (t.startsWith("--")) {
        return {
          kind: "edge" as const,
          label: t.replace(/^-+\s*/, "").replace(/\s*-*>?\s*$/, ""),
        }
      }
      if (t.startsWith("=>")) {
        const p = parseTag(t.slice(2))
        return { kind: "node" as const, ...p, conclusion: true }
      }
      const p = parseTag(t)
      return { kind: "node" as const, ...p, conclusion: false }
    })
}

function ChainDiagram({ content }: { content: string }) {
  const items = parseChain(content)
  return (
    <div className="my-8 flex flex-col items-center gap-0">
      {items.map((item, i) =>
        item.kind === "node" ? (
          <div
            key={i}
            className={cn(
              "rounded-lg border px-5 py-2.5 text-sm text-center max-w-xs w-full shadow-sm",
              nodeStyles(item.tag, item.conclusion),
            )}
          >
            {item.text}
          </div>
        ) : (
          <div key={i} className="flex flex-col items-center py-0.5">
            <div className="w-px h-2.5 bg-border" />
            {item.label && (
              <span className="text-[11px] text-muted-foreground leading-tight py-px">
                {item.label}
              </span>
            )}
            <div className="w-px h-2.5 bg-border" />
            {/* arrow tip */}
            <div className="w-0 h-0 border-l-[5px] border-r-[5px] border-t-[6px] border-l-transparent border-r-transparent border-t-border" />
          </div>
        ),
      )}
    </div>
  )
}

/* ------------------------------------------------------------------ */
/*  Tree Diagram – hierarchical indented nodes                         */
/*                                                                     */
/*  Format (2-space indent):                                           */
/*    Root                                                             */
/*      Child A                                                        */
/*        Grandchild                                                   */
/*      Child B [muted]                                                */
/* ------------------------------------------------------------------ */

interface TNode {
  text: string
  tag: NodeTag
  children: TNode[]
}

function parseTree(src: string): TNode[] {
  const lines = src.split("\n").filter((l) => l.trimEnd())
  const roots: TNode[] = []
  const stack: { node: TNode; depth: number }[] = []

  for (const line of lines) {
    const depth = line.search(/\S/)
    const { text, tag } = parseTag(line.trim())
    const node: TNode = { text, tag, children: [] }

    while (stack.length && stack[stack.length - 1].depth >= depth) stack.pop()

    if (!stack.length) roots.push(node)
    else stack[stack.length - 1].node.children.push(node)

    stack.push({ node, depth })
  }
  return roots
}

function TreeNode({ node }: { node: TNode }) {
  return (
    <div>
      <div
        className={cn(
          "rounded-md border px-3 py-1.5 text-sm inline-block shadow-sm",
          nodeStyles(node.tag),
        )}
      >
        {node.text}
      </div>

      {node.children.length > 0 && (
        <div className="ml-3.5 mt-1.5 border-l-2 border-border/50 pl-5 space-y-1.5 pb-0.5">
          {node.children.map((child, i) => (
            <div key={i} className="relative">
              {/* horizontal connector */}
              <div className="absolute left-[-22px] top-[13px] w-[18px] h-px bg-border/50" />
              <TreeNode node={child} />
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

function TreeDiagram({ content }: { content: string }) {
  const roots = parseTree(content)
  return (
    <div className="my-8 space-y-2">
      {roots.map((r, i) => (
        <TreeNode key={i} node={r} />
      ))}
    </div>
  )
}

/* ------------------------------------------------------------------ */
/*  Flow Diagram – horizontal process boxes                            */
/*                                                                     */
/*  Format:                                                            */
/*    Step A -> Step B -> Step C [primary]                              */
/* ------------------------------------------------------------------ */

function parseFlow(src: string) {
  return src
    .split(/\s*-+>\s*/)
    .map((s) => parseTag(s.trim()))
    .filter((n) => n.text)
}

function FlowDiagram({ content }: { content: string }) {
  const nodes = parseFlow(content)
  return (
    <div className="my-8 overflow-x-auto">
      <div className="flex items-center justify-center gap-0 min-w-max px-2">
        {nodes.map((node, i) => (
          <div key={i} className="flex items-center shrink-0">
            <div
              className={cn(
                "rounded-lg border px-4 py-2 text-sm shadow-sm whitespace-nowrap",
                nodeStyles(node.tag),
              )}
            >
              {node.text}
            </div>
            {i < nodes.length - 1 && (
              <div className="flex items-center px-1">
                <div className="w-5 h-px bg-border" />
                <div className="w-0 h-0 border-t-[4px] border-b-[4px] border-l-[6px] border-t-transparent border-b-transparent border-l-border" />
              </div>
            )}
          </div>
        ))}
      </div>
    </div>
  )
}

/* ------------------------------------------------------------------ */
/*  Steps Diagram – numbered timeline                                  */
/*                                                                     */
/*  Format:                                                            */
/*    Step description :: annotation text                               */
/*    Another step :: annotation [success]                              */
/* ------------------------------------------------------------------ */

interface Step {
  desc: string
  note: string | null
  tag: NodeTag
}

function parseSteps(src: string): Step[] {
  return src
    .split("\n")
    .filter((l) => l.trim())
    .map((raw) => {
      const clean = raw.replace(/^\d+\.\s*/, "").trim()
      const [left, ...rest] = clean.split("::")
      const right = rest.length ? rest.join("::").trim() : null
      if (right) {
        const { text, tag } = parseTag(right)
        return { desc: left.trim(), note: text, tag }
      }
      const { text, tag } = parseTag(left)
      return { desc: text, note: null, tag }
    })
}

function stepCircleStyle(tag: NodeTag) {
  switch (tag) {
    case "success":
      return "bg-emerald-100 dark:bg-emerald-900/50 text-emerald-700 dark:text-emerald-300 ring-emerald-300 dark:ring-emerald-700"
    case "highlight":
      return "bg-red-100 dark:bg-red-900/50 text-red-700 dark:text-red-300 ring-red-300 dark:ring-red-700"
    case "primary":
      return "bg-primary/20 text-primary ring-primary/40"
    default:
      return "bg-muted text-muted-foreground ring-border"
  }
}

function StepsDiagram({ content }: { content: string }) {
  const steps = parseSteps(content)
  return (
    <div className="my-8">
      {steps.map((step, i) => (
        <div key={i} className="flex gap-3.5">
          {/* timeline rail */}
          <div className="flex flex-col items-center">
            <div
              className={cn(
                "w-7 h-7 rounded-full flex items-center justify-center text-xs font-bold shrink-0 ring-2",
                stepCircleStyle(step.tag),
              )}
            >
              {i + 1}
            </div>
            {i < steps.length - 1 && (
              <div className="w-px flex-1 min-h-5 bg-border/60" />
            )}
          </div>
          {/* content */}
          <div className="pb-5 pt-1 min-w-0">
            <p className="text-sm font-medium leading-snug">{step.desc}</p>
            {step.note && (
              <p className="text-xs text-muted-foreground mt-0.5 leading-snug">
                {step.note}
              </p>
            )}
          </div>
        </div>
      ))}
    </div>
  )
}

/* ------------------------------------------------------------------ */
/*  Note – styled callout box                                          */
/*                                                                     */
/*  Format:                                                            */
/*    type: info | warning | tip | success                             */
/*    Body text here                                                   */
/* ------------------------------------------------------------------ */

type NoteKind = "info" | "warning" | "tip" | "success"

const noteTheme: Record<NoteKind, string> = {
  info: "border-l-blue-400 dark:border-l-blue-500 bg-blue-50/60 dark:bg-blue-950/20",
  warning:
    "border-l-amber-400 dark:border-l-amber-500 bg-amber-50/60 dark:bg-amber-950/20",
  tip: "border-l-primary bg-primary/5",
  success:
    "border-l-emerald-400 dark:border-l-emerald-500 bg-emerald-50/60 dark:bg-emerald-950/20",
}

function parseNote(src: string): { kind: NoteKind; body: string } {
  const lines = src.split("\n")
  let kind: NoteKind = "info"
  let start = 0
  if (lines[0]?.trim().startsWith("type:")) {
    kind = lines[0].replace("type:", "").trim() as NoteKind
    start = 1
  }
  return { kind, body: lines.slice(start).join("\n").trim() }
}

function NoteDiagram({ content }: { content: string }) {
  const { kind, body } = parseNote(content)
  return (
    <div
      className={cn(
        "my-8 rounded-r-lg border border-l-4 px-5 py-4",
        noteTheme[kind],
      )}
    >
      <p className="text-sm leading-relaxed whitespace-pre-line">{body}</p>
    </div>
  )
}

/* ------------------------------------------------------------------ */
/*  Public API                                                         */
/* ------------------------------------------------------------------ */

export const DIAGRAM_LANGUAGES = [
  "chain",
  "tree",
  "flow",
  "steps",
  "note",
] as const

export type DiagramLanguage = (typeof DIAGRAM_LANGUAGES)[number]

export function isDiagramLanguage(lang: string): lang is DiagramLanguage {
  return (DIAGRAM_LANGUAGES as readonly string[]).includes(lang)
}

export function DiagramRenderer({
  content,
  type,
}: {
  content: string
  type: DiagramLanguage
}) {
  switch (type) {
    case "chain":
      return <ChainDiagram content={content} />
    case "tree":
      return <TreeDiagram content={content} />
    case "flow":
      return <FlowDiagram content={content} />
    case "steps":
      return <StepsDiagram content={content} />
    case "note":
      return <NoteDiagram content={content} />
  }
}
