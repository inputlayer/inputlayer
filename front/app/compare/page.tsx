import { comparisonPages } from "@/lib/content-bundle"
import { CompareIndexClient } from "./compare-index-client"

export default function ComparePage() {
  return <CompareIndexClient pages={comparisonPages} />
}
