import type { Metadata } from "next"
import { comparisonPages } from "@/lib/content-bundle"
import { CompareIndexClient } from "./compare-index-client"

const title = "Compare - InputLayer"
const description =
  "How InputLayer, the live rules engine for AI agents, compares with the tools already in your stack: what it replaces, what it keeps, and where each of them wins."

export const metadata: Metadata = {
  title,
  description,
  openGraph: { title, description, type: "website" },
  twitter: { card: "summary", title, description },
}

export default function ComparePage() {
  return <CompareIndexClient pages={comparisonPages} />
}
