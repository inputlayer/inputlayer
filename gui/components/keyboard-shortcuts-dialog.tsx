"use client"

import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog"
import { Button } from "@/components/ui/button"
import { HelpCircle } from "lucide-react"
import { useSyncExternalStore } from "react"

const subscribePlatform = () => () => {}
const getIsMac = () => typeof navigator !== "undefined" && /Mac|iPhone|iPad/.test(navigator.platform)
const getIsMacServer = () => false

interface ShortcutEntry {
  keys: string
  description: string
}

function Kbd({ children }: { children: string }) {
  return (
    <kbd className="inline-flex h-5 items-center rounded border border-border bg-muted px-1.5 font-mono text-[10px] text-muted-foreground">
      {children}
    </kbd>
  )
}

export function KeyboardShortcutsDialog() {
  const isMac = useSyncExternalStore(subscribePlatform, getIsMac, getIsMacServer)
  const mod = isMac ? "\u2318" : "Ctrl"

  const sections: { title: string; shortcuts: ShortcutEntry[] }[] = [
    {
      title: "Execution",
      shortcuts: [
        { keys: `${mod}+Enter`, description: "Run query" },
        { keys: `${mod}+Shift+Enter`, description: "Explain query" },
      ],
    },
    {
      title: "Editor",
      shortcuts: [
        { keys: "Tab", description: "Indent" },
        { keys: "Enter", description: "Auto-indent new line" },
        { keys: `${mod}+A`, description: "Select all" },
        { keys: "Ctrl+Space", description: "Open autocomplete" },
        { keys: "Ctrl+Shift+A", description: "Show all completions" },
      ],
    },
    {
      title: "Autocomplete",
      shortcuts: [
        { keys: "\u2191 / \u2193", description: "Navigate items" },
        { keys: "Enter / Tab", description: "Accept completion" },
        { keys: "Escape", description: "Dismiss" },
      ],
    },
  ]

  return (
    <Dialog>
      <DialogTrigger asChild>
        <Button variant="ghost" size="sm" className="h-7 w-7 p-0" aria-label="Keyboard shortcuts">
          <HelpCircle className="h-3.5 w-3.5" />
        </Button>
      </DialogTrigger>
      <DialogContent className="max-w-sm">
        <DialogHeader>
          <DialogTitle>Keyboard Shortcuts</DialogTitle>
        </DialogHeader>
        <div className="space-y-4 mt-2">
          {sections.map((section) => (
            <div key={section.title}>
              <h4 className="text-xs font-medium text-muted-foreground uppercase tracking-wider mb-2">
                {section.title}
              </h4>
              <div className="space-y-1.5">
                {section.shortcuts.map((shortcut) => (
                  <div key={shortcut.description} className="flex items-center justify-between text-sm">
                    <span>{shortcut.description}</span>
                    <div className="flex items-center gap-0.5">
                      {shortcut.keys.split("+").map((k, i) => (
                        <span key={i}>
                          {i > 0 && <span className="text-muted-foreground mx-0.5">+</span>}
                          <Kbd>{k.trim()}</Kbd>
                        </span>
                      ))}
                    </div>
                  </div>
                ))}
              </div>
            </div>
          ))}
        </div>
      </DialogContent>
    </Dialog>
  )
}
