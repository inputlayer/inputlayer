import { useCases } from "@/lib/content-bundle"
import { UseCasesIndexClient } from "./use-cases-index-client"

export default function UseCasesPage() {
  return <UseCasesIndexClient useCases={useCases} />
}
