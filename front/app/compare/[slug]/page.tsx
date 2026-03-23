import type { Metadata } from "next"
import { comparisonPages } from "@/lib/content-bundle"
import { CompareClient } from "./compare-client"

interface ComparePageProps {
  params: Promise<{ slug: string }>
}

export async function generateMetadata({ params }: ComparePageProps): Promise<Metadata> {
  const { slug } = await params
  const page = comparisonPages.find((p) => p.slug === slug)

  if (!page) {
    return { title: "Comparison Not Found - InputLayer" }
  }

  const title = `${page.title} - InputLayer`
  const description = `See how InputLayer compares to ${page.competitors.join(", ")}.`

  return {
    title,
    description,
    openGraph: {
      title,
      description,
      type: "article",
    },
    twitter: {
      card: "summary_large_image",
      title,
      description,
    },
  }
}

export default async function CompareDetailPage({ params }: ComparePageProps) {
  const { slug } = await params
  const page = comparisonPages.find((p) => p.slug === slug) ?? null
  return <CompareClient page={page} slug={slug} />
}

export function generateStaticParams() {
  return comparisonPages.map((p) => ({ slug: p.slug }))
}
