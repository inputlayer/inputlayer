"use client"

import Link from "next/link"
import { PageLayout } from "@/components/page-layout"
import { ContentHero } from "@/components/content-hero"
import { CTABanner } from "@/components/cta-banner"
import type { UseCase } from "@/lib/content-bundle"
import { Brain, ShoppingBag, Shield, ArrowRight } from "lucide-react"

const iconMap: Record<string, React.ReactNode> = {
  Brain: <Brain className="h-8 w-8 text-primary" />,
  ShoppingBag: <ShoppingBag className="h-8 w-8 text-primary" />,
  Shield: <Shield className="h-8 w-8 text-primary" />,
}

interface UseCasesIndexClientProps {
  useCases: UseCase[]
}

export function UseCasesIndexClient({ useCases }: UseCasesIndexClientProps) {
  return (
    <PageLayout>
      <ContentHero
        heading="Use Cases"
        subtitle="See how teams use InputLayer to build AI applications that require reasoning, not just retrieval."
      />

      <section className="mx-auto max-w-6xl px-6 py-12">
        {useCases.length === 0 ? (
          <p className="text-muted-foreground text-center py-12">Use cases coming soon.</p>
        ) : (
          <div className="grid gap-6 md:grid-cols-2 lg:grid-cols-3">
            {useCases.map((uc) => (
              <Link
                key={uc.slug}
                href={`/use-cases/${uc.slug}/`}
                className="group rounded-xl border border-border bg-card p-6 space-y-4 transition-colors hover:border-primary/30 hover:bg-card/80"
              >
                {uc.icon && iconMap[uc.icon] && (
                  <div>{iconMap[uc.icon]}</div>
                )}
                <h3 className="text-lg font-semibold group-hover:text-primary transition-colors">
                  {uc.title}
                </h3>
                {uc.subtitle && (
                  <p className="text-sm text-muted-foreground">{uc.subtitle}</p>
                )}
                <span className="inline-flex items-center gap-1 text-sm text-primary font-medium">
                  Learn more <ArrowRight className="h-3.5 w-3.5" />
                </span>
              </Link>
            ))}
          </div>
        )}
      </section>

      <CTABanner
        heading="See InputLayer in action"
        description="Try the interactive demo or read the documentation to get started."
        buttons={[
          { label: "Launch demo", href: "https://demo.inputlayer.ai", external: true },
          { label: "Read the docs", href: "/docs/", variant: "secondary" },
        ]}
      />
    </PageLayout>
  )
}
