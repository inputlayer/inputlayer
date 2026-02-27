import { comparisonPages } from "@/lib/content-bundle"
import { CompareClient } from "./compare-client"

interface ComparePageProps {
  params: Promise<{ slug: string }>
}

export default async function CompareDetailPage({ params }: ComparePageProps) {
  const { slug } = await params
  const page = comparisonPages.find((p) => p.slug === slug) ?? null
  return <CompareClient page={page} slug={slug} />
}

export function generateStaticParams() {
  return comparisonPages.map((p) => ({ slug: p.slug }))
}
