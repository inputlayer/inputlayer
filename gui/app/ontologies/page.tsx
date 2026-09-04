"use client"

import { useCallback, useEffect, useState } from "react"
import { AppShell } from "@/components/app-shell"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Badge } from "@/components/ui/badge"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { Label } from "@/components/ui/label"
import { useIQLStore, type InstalledOntology } from "@/lib/iql-store"
import {
  fetchRegistry,
  shortDigest,
  DEFAULT_REGISTRY_URL,
  type RegistryOntology,
} from "@/lib/registry-client"
import { toast } from "sonner"
import {
  Package,
  Download,
  RefreshCw,
  Trash2,
  ArrowUpCircle,
  AlertCircle,
  CheckCircle2,
} from "lucide-react"

export default function OntologiesPage() {
  const knowledgeGraphs = useIQLStore((s) => s.knowledgeGraphs)
  const selectedKg = useIQLStore((s) => s.selectedKnowledgeGraph)
  const installOntology = useIQLStore((s) => s.installOntology)
  const removeOntology = useIQLStore((s) => s.removeOntology)
  const upgradeOntology = useIQLStore((s) => s.upgradeOntology)
  const listInstalledOntologies = useIQLStore((s) => s.listInstalledOntologies)

  const [registry, setRegistry] = useState<RegistryOntology[]>([])
  const [registryError, setRegistryError] = useState<string | null>(null)
  const [loadingRegistry, setLoadingRegistry] = useState(true)
  const [installed, setInstalled] = useState<Record<string, InstalledOntology[]>>({})
  const [busy, setBusy] = useState<string | null>(null)

  const [installTarget, setInstallTarget] = useState<RegistryOntology | null>(null)
  const [installKg, setInstallKg] = useState("")
  const [installVersion, setInstallVersion] = useState("")

  const loadRegistry = useCallback(async () => {
    setLoadingRegistry(true)
    setRegistryError(null)
    try {
      setRegistry(await fetchRegistry())
    } catch (error) {
      setRegistryError(error instanceof Error ? error.message : String(error))
    } finally {
      setLoadingRegistry(false)
    }
  }, [])

  const loadInstalled = useCallback(async () => {
    const next: Record<string, InstalledOntology[]> = {}
    for (const kg of knowledgeGraphs) {
      try {
        const packs = await listInstalledOntologies(kg.name)
        if (packs.length > 0) next[kg.name] = packs
      } catch {
        // A KG we cannot read (ACL) simply contributes nothing.
      }
    }
    setInstalled(next)
  }, [knowledgeGraphs, listInstalledOntologies])

  useEffect(() => {
    void loadRegistry()
  }, [loadRegistry])

  useEffect(() => {
    void loadInstalled()
  }, [loadInstalled])

  const openInstall = (ontology: RegistryOntology) => {
    setInstallTarget(ontology)
    setInstallVersion(ontology.latest.version)
    setInstallKg(selectedKg?.name ?? knowledgeGraphs[0]?.name ?? "")
  }

  const runInstall = async () => {
    if (!installTarget || !installKg) return
    const spec = `${installTarget.name}@${installVersion}`
    setBusy(spec)
    try {
      const messages = await installOntology(spec, installKg, true)
      toast.success(messages[0] ?? `Installed ${spec}`, {
        description: messages.slice(1).join(" - "),
      })
      setInstallTarget(null)
      await loadInstalled()
    } catch (error) {
      toast.error("Install failed", {
        description: error instanceof Error ? error.message : String(error),
      })
    } finally {
      setBusy(null)
    }
  }

  const runRemove = async (name: string, kgName: string) => {
    setBusy(`${kgName}/${name}`)
    try {
      const messages = await removeOntology(name, kgName)
      toast.success(messages[0] ?? `Removed ${name}`)
      await loadInstalled()
    } catch (error) {
      toast.error("Remove failed", {
        description: error instanceof Error ? error.message : String(error),
      })
    } finally {
      setBusy(null)
    }
  }

  const runUpgrade = async (name: string, kgName: string, version: string) => {
    const spec = `${name}@${version}`
    setBusy(`${kgName}/${name}`)
    try {
      const messages = await upgradeOntology(spec, kgName)
      toast.success(messages[0] ?? `Upgraded ${spec}`, {
        description: "Rules re-deployed; data kept.",
      })
      await loadInstalled()
    } catch (error) {
      toast.error("Upgrade failed", {
        description: error instanceof Error ? error.message : String(error),
      })
    } finally {
      setBusy(null)
    }
  }

  const installationsOf = (name: string) =>
    Object.entries(installed).flatMap(([kgName, packs]) =>
      packs.filter((pack) => pack.name === name).map((pack) => ({ kgName, pack })),
    )

  return (
    <AppShell>
      <div className="flex-1 overflow-auto">
        <div className="mx-auto max-w-5xl p-6 space-y-8">
          <header className="space-y-1">
            <div className="flex items-center justify-between">
              <h1 className="text-xl font-semibold flex items-center gap-2">
                <Package className="h-5 w-5 text-teal-500" />
                Ontologies
              </h1>
              <Button variant="ghost" size="sm" onClick={() => void loadRegistry()}>
                <RefreshCw className="h-4 w-4 mr-1.5" />
                Refresh
              </Button>
            </div>
            <p className="text-sm text-muted-foreground">
              Packs published in the registry. Installing deploys the pack&apos;s rules into a
              knowledge graph on this engine and pins the exact version and digest.
            </p>
          </header>

          {registryError && (
            <div className="rounded-lg border border-amber-500/40 bg-amber-500/5 p-4 text-sm">
              <div className="flex items-center gap-2 font-medium text-amber-600 dark:text-amber-400">
                <AlertCircle className="h-4 w-4" />
                Registry unavailable
              </div>
              <p className="mt-1 text-muted-foreground">{registryError}</p>
              <p className="mt-1 text-xs text-muted-foreground">
                Source: <code>{DEFAULT_REGISTRY_URL}</code>
              </p>
            </div>
          )}

          {loadingRegistry && !registryError && (
            <p className="text-sm text-muted-foreground">Loading registry...</p>
          )}

          <div className="space-y-3">
            {registry.map((ontology) => {
              const installations = installationsOf(ontology.name)
              return (
                <div
                  key={ontology.name}
                  className="rounded-lg border border-border/60 bg-card p-4 space-y-3"
                >
                  <div className="flex items-start justify-between gap-4">
                    <div className="space-y-1">
                      <div className="flex items-center gap-2">
                        <span className="font-medium">{ontology.name}</span>
                        <Badge variant="secondary">{ontology.latest.version}</Badge>
                        {ontology.latest.engine && (
                          <span className="text-xs text-muted-foreground">
                            engine {ontology.latest.engine}
                          </span>
                        )}
                      </div>
                      <p className="text-sm text-muted-foreground">{ontology.latest.title}</p>
                      <p className="text-xs font-mono text-muted-foreground">
                        {shortDigest(ontology.latest.digest)}
                      </p>
                    </div>
                    <Button
                      size="sm"
                      onClick={() => openInstall(ontology)}
                      disabled={knowledgeGraphs.length === 0}
                    >
                      <Download className="h-4 w-4 mr-1.5" />
                      Install
                    </Button>
                  </div>

                  {installations.length > 0 && (
                    <div className="border-t border-border/50 pt-3 space-y-2">
                      <p className="text-xs uppercase tracking-wider text-muted-foreground">
                        Installed
                      </p>
                      {installations.map(({ kgName, pack }) => {
                        const outdated = pack.version !== ontology.latest.version
                        const key = `${kgName}/${pack.name}`
                        return (
                          <div
                            key={key}
                            className="flex items-center justify-between gap-3 text-sm"
                          >
                            <div className="flex items-center gap-2 min-w-0">
                              <CheckCircle2 className="h-4 w-4 text-teal-500 flex-shrink-0" />
                              <span className="font-mono">{kgName}</span>
                              <Badge variant={outdated ? "outline" : "secondary"}>
                                {pack.version}
                              </Badge>
                              {outdated && (
                                <span className="text-xs text-amber-600 dark:text-amber-400">
                                  {ontology.latest.version} available
                                </span>
                              )}
                            </div>
                            <div className="flex items-center gap-1">
                              {outdated && (
                                <Button
                                  variant="ghost"
                                  size="sm"
                                  disabled={busy === key}
                                  onClick={() =>
                                    void runUpgrade(pack.name, kgName, ontology.latest.version)
                                  }
                                >
                                  <ArrowUpCircle className="h-4 w-4 mr-1.5" />
                                  Upgrade
                                </Button>
                              )}
                              <Button
                                variant="ghost"
                                size="sm"
                                disabled={busy === key}
                                onClick={() => void runRemove(pack.name, kgName)}
                              >
                                <Trash2 className="h-4 w-4 mr-1.5" />
                                Remove
                              </Button>
                            </div>
                          </div>
                        )
                      })}
                    </div>
                  )}
                </div>
              )
            })}
          </div>

          {!loadingRegistry && registry.length === 0 && !registryError && (
            <p className="text-sm text-muted-foreground">No ontologies published yet.</p>
          )}
        </div>
      </div>

      <Dialog open={installTarget !== null} onOpenChange={(open) => !open && setInstallTarget(null)}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Install {installTarget?.name}</DialogTitle>
            <DialogDescription>
              The engine fetches the pack, verifies its digest, deploys its rules into the
              knowledge graph, and pins the version.
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-4">
            <div className="space-y-1.5">
              <Label htmlFor="install-kg">Knowledge graph</Label>
              <Input
                id="install-kg"
                value={installKg}
                onChange={(event) => setInstallKg(event.target.value)}
                placeholder="support"
              />
              <p className="text-xs text-muted-foreground">
                Created if it does not exist.
              </p>
            </div>
            <div className="space-y-1.5">
              <Label htmlFor="install-version">Version</Label>
              <select
                id="install-version"
                className="w-full rounded-md border border-input bg-background px-3 py-2 text-sm"
                value={installVersion}
                onChange={(event) => setInstallVersion(event.target.value)}
              >
                {installTarget?.versions.map((entry) => (
                  <option key={entry.version} value={entry.version}>
                    {entry.version}
                  </option>
                ))}
              </select>
            </div>
          </div>
          <DialogFooter>
            <Button variant="ghost" onClick={() => setInstallTarget(null)}>
              Cancel
            </Button>
            <Button onClick={() => void runInstall()} disabled={!installKg || busy !== null}>
              {busy ? "Installing..." : "Install"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </AppShell>
  )
}
