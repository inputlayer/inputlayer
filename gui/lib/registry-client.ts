/**
 * Ontology registry client.
 *
 * The registry is a static index published on GitHub (Helm-shaped): a
 * plain JSON fetch, no server round-trip. Browsing is read-only - INSTALLING
 * an ontology is the engine's job (`.ontology install` over the WebSocket),
 * because the engine owns knowledge graphs and enforces who may write to
 * them.
 */

export const DEFAULT_REGISTRY_URL =
  "https://raw.githubusercontent.com/inputlayer/ontology-registry/main/index.json"

export interface RegistryEntry {
  version: string
  title: string
  engine: string
  urls: string[]
  digest: string
}

export interface RegistryOntology {
  name: string
  latest: RegistryEntry
  versions: RegistryEntry[]
}

interface RegistryIndex {
  entries?: Record<string, RegistryEntry[]>
}

/** Fetch the published index. Throws with a readable message on failure. */
export async function fetchRegistry(url: string = DEFAULT_REGISTRY_URL): Promise<RegistryOntology[]> {
  let response: Response
  try {
    response = await fetch(url, { cache: "no-store" })
  } catch (error) {
    throw new Error(
      `Could not reach the registry at ${url}: ${error instanceof Error ? error.message : String(error)}`,
    )
  }
  if (!response.ok) {
    throw new Error(`Registry returned ${response.status} for ${url}`)
  }
  const index = (await response.json()) as RegistryIndex
  const entries = index.entries ?? {}
  return Object.keys(entries)
    .sort()
    .flatMap((name) => {
      const versions = entries[name] ?? []
      const latest = versions[0]
      return latest ? [{ name, latest, versions }] : []
    })
}

/** Short digest for display: sha256:abcd1234... */
export function shortDigest(digest: string): string {
  const hex = digest.startsWith("sha256:") ? digest.slice(7) : digest
  return `${hex.slice(0, 12)}...`
}
