"use client"

import { useEffect, useState } from "react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { useGatewayStore } from "@/lib/gateway-store"
import { gatewayHealth } from "@/lib/gateway-client"
import { CheckCircle2, AlertCircle, Loader2 } from "lucide-react"

interface GatewaySettingsProps {
  open: boolean
  onOpenChange: (open: boolean) => void
}

type Probe =
  | { state: "idle" }
  | { state: "checking" }
  | { state: "ok"; ontologies: string[]; modelKeyConfigured: boolean; version: string }
  | { state: "error"; message: string }

export function GatewaySettings({ open, onOpenChange }: GatewaySettingsProps) {
  const { url, apiKey, setUrl, setApiKey, hydrate } = useGatewayStore()
  const [draftUrl, setDraftUrl] = useState(url)
  const [draftKey, setDraftKey] = useState(apiKey)
  const [probe, setProbe] = useState<Probe>({ state: "idle" })

  useEffect(() => hydrate(), [hydrate])
  useEffect(() => {
    if (open) {
      setDraftUrl(url)
      setDraftKey(apiKey)
      setProbe({ state: "idle" })
    }
  }, [open, url, apiKey])

  const test = async () => {
    setProbe({ state: "checking" })
    try {
      const health = await gatewayHealth({ url: draftUrl, apiKey: draftKey })
      setProbe({ state: "ok", ...health })
    } catch (error) {
      setProbe({
        state: "error",
        message: error instanceof Error ? error.message : String(error),
      })
    }
  }

  const save = () => {
    setUrl(draftUrl.trim())
    setApiKey(draftKey.trim())
    onOpenChange(false)
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Model gateway</DialogTitle>
          <DialogDescription>
            The gateway serves chat completions and evaluation events. It is a separate service
            from the engine and holds the model provider key.
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-4">
          <div className="space-y-1.5">
            <Label htmlFor="gateway-url">Gateway URL</Label>
            <Input
              id="gateway-url"
              value={draftUrl}
              onChange={(event) => setDraftUrl(event.target.value)}
              placeholder="http://127.0.0.1:8081"
            />
            <p className="text-xs text-muted-foreground">
              Behind a reverse proxy this is the same origin as the Studio.
            </p>
          </div>
          <div className="space-y-1.5">
            <Label htmlFor="gateway-key">API key</Label>
            <Input
              id="gateway-key"
              type="password"
              value={draftKey}
              onChange={(event) => setDraftKey(event.target.value)}
              placeholder="GATEWAY_API_KEY"
            />
            <p className="text-xs text-muted-foreground">
              Kept for this browser session only.
            </p>
          </div>

          {probe.state === "ok" && (
            <div className="rounded-md border border-teal-500/40 bg-teal-500/5 p-3 text-sm space-y-1">
              <div className="flex items-center gap-2 text-teal-600 dark:text-teal-400 font-medium">
                <CheckCircle2 className="h-4 w-4" />
                Reachable (v{probe.version})
              </div>
              <p className="text-muted-foreground">
                Serves: {probe.ontologies.join(", ") || "no ontologies"}
              </p>
              {!probe.modelKeyConfigured && (
                <p className="text-amber-600 dark:text-amber-400">
                  No model key configured - chat will return 503.
                </p>
              )}
            </div>
          )}
          {probe.state === "error" && (
            <div className="rounded-md border border-red-500/40 bg-red-500/5 p-3 text-sm">
              <div className="flex items-center gap-2 text-red-600 dark:text-red-400 font-medium">
                <AlertCircle className="h-4 w-4" />
                Not reachable
              </div>
              <p className="text-muted-foreground mt-1">{probe.message}</p>
            </div>
          )}
        </div>

        <DialogFooter>
          <Button variant="ghost" onClick={() => void test()} disabled={probe.state === "checking"}>
            {probe.state === "checking" ? (
              <Loader2 className="h-4 w-4 mr-1.5 animate-spin" />
            ) : null}
            Test connection
          </Button>
          <Button onClick={save}>Save</Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
