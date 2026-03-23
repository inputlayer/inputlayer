import type { Metadata } from "next"
import { docsPages } from "@/lib/docs-bundle"
import { DocsPageClient } from "./docs-page-client"

interface DocsPageProps {
  params: Promise<{ slug?: string[] }>
}

export async function generateMetadata({ params }: DocsPageProps): Promise<Metadata> {
  const { slug } = await params
  const key = !slug || slug.length === 0 ? "index" : slug.join("/")
  const page = docsPages[key]

  if (!page) {
    return { title: "Page Not Found - InputLayer Docs" }
  }

  const title = `${page.title} - InputLayer Docs`
  const description = `InputLayer documentation - ${page.title}.`

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

export default async function DocsPage({ params }: DocsPageProps) {
  const { slug } = await params
  const key = !slug || slug.length === 0 ? "index" : slug.join("/")
  const page = docsPages[key] ?? null

  return <DocsPageClient page={page} slugKey={key} />
}

export function generateStaticParams() {
  return Object.keys(docsPages).map((key) => ({
    slug: key === "index" ? undefined : key.split("/"),
  }))
}
