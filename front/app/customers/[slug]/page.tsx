import { customerStories } from "@/lib/content-bundle"
import { CustomerClient } from "./customer-client"

export const dynamicParams = false

interface CustomerPageProps {
  params: Promise<{ slug: string }>
}

export default async function CustomerDetailPage({ params }: CustomerPageProps) {
  const { slug } = await params
  const story = customerStories.find((s) => s.slug === slug) ?? null
  return <CustomerClient story={story} slug={slug} />
}

export function generateStaticParams() {
  return customerStories.map((s) => ({ slug: s.slug }))
}
