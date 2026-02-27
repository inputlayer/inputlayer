import { useCases } from "@/lib/content-bundle"
import { UseCaseClient } from "./use-case-client"

interface UseCasePageProps {
  params: Promise<{ slug: string }>
}

export default async function UseCasePage({ params }: UseCasePageProps) {
  const { slug } = await params
  const useCase = useCases.find((uc) => uc.slug === slug) ?? null
  return <UseCaseClient useCase={useCase} slug={slug} />
}

export function generateStaticParams() {
  return useCases.map((uc) => ({ slug: uc.slug }))
}
