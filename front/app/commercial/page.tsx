import { PageLayout } from "@/components/page-layout"
import { ContentHero } from "@/components/content-hero"
import { CTABanner } from "@/components/cta-banner"
import type { Metadata } from "next"

export const metadata: Metadata = {
  title: "Commercial Licensing - InputLayer",
  description:
    "Commercial licensing for InputLayer. Apache 2.0 + Commons Clause for open source use. Commercial license available for redistribution and hosted services.",
}

export default function CommercialPage() {
  return (
    <PageLayout>
      <ContentHero
        heading="Commercial licensing"
        subtitle="InputLayer is source-available under Apache 2.0 + Commons Clause. You can use it commercially - the only restriction is on redistributing InputLayer itself for profit."
      />

      <section className="mx-auto max-w-3xl px-6 py-12 space-y-12">
        <div className="space-y-4">
          <h2 className="text-2xl font-bold tracking-tight">
            You do NOT need a commercial license if you are:
          </h2>
          <ul className="space-y-2 text-muted-foreground">
            <li className="flex items-start gap-3">
              <span className="text-emerald-500 mt-0.5 shrink-0">&#10003;</span>
              <span>Using InputLayer as part of your own product or service, even commercially</span>
            </li>
            <li className="flex items-start gap-3">
              <span className="text-emerald-500 mt-0.5 shrink-0">&#10003;</span>
              <span>Running InputLayer internally within your company</span>
            </li>
            <li className="flex items-start gap-3">
              <span className="text-emerald-500 mt-0.5 shrink-0">&#10003;</span>
              <span>Building and selling a product that uses InputLayer as a component</span>
            </li>
            <li className="flex items-start gap-3">
              <span className="text-emerald-500 mt-0.5 shrink-0">&#10003;</span>
              <span>An individual, researcher, or open source contributor</span>
            </li>
            <li className="flex items-start gap-3">
              <span className="text-emerald-500 mt-0.5 shrink-0">&#10003;</span>
              <span>Evaluating InputLayer for any purpose</span>
            </li>
          </ul>
        </div>

        <div className="space-y-4">
          <h2 className="text-2xl font-bold tracking-tight">
            You DO need a commercial license if you are:
          </h2>
          <ul className="space-y-2 text-muted-foreground">
            <li className="flex items-start gap-3">
              <span className="text-destructive mt-0.5 shrink-0">&#10005;</span>
              <span>Reselling or redistributing InputLayer itself as a standalone product</span>
            </li>
            <li className="flex items-start gap-3">
              <span className="text-destructive mt-0.5 shrink-0">&#10005;</span>
              <span>Offering InputLayer as a hosted or managed service, selling access to its functionality directly</span>
            </li>
            <li className="flex items-start gap-3">
              <span className="text-destructive mt-0.5 shrink-0">&#10005;</span>
              <span>Forking InputLayer and selling the fork as a competing product</span>
            </li>
          </ul>
          <p className="text-muted-foreground text-sm pt-2">
            In short: use it to build your thing, but don&apos;t sell our thing.
          </p>
        </div>

        <div className="rounded-xl border border-border bg-card p-8 space-y-4">
          <h2 className="text-2xl font-bold tracking-tight">
            What a commercial license includes
          </h2>
          <ul className="space-y-2 text-muted-foreground">
            <li>Rights to redistribute or host InputLayer commercially</li>
            <li>Access to the commercial product roadmap</li>
            <li>Direct support from the InputLayer engineering team</li>
            <li>SLA options for production deployments</li>
          </ul>
        </div>

        <div className="space-y-4">
          <h2 className="text-2xl font-bold tracking-tight">Contact</h2>
          <p className="text-muted-foreground">
            For commercial licensing:{" "}
            <a
              href="mailto:sam@inputlayer.ai"
              className="text-primary hover:underline"
            >
              sam@inputlayer.ai
            </a>
          </p>
          <p className="text-muted-foreground">
            Include a brief description of your use case and company name. We respond within 2 business days.
          </p>
        </div>

        <p className="text-xs text-muted-foreground border-t border-border pt-8">
          &ldquo;InputLayer&rdquo; is a trademark of InputLayer. Unauthorized use of the InputLayer name or brand in forks, derivatives, or commercial products requires explicit written permission.
        </p>
      </section>

      <CTABanner
        heading="Ready to get started?"
        description="Try InputLayer now or read the documentation."
        buttons={[
          {
            label: "Launch demo",
            href: "https://demo.inputlayer.ai",
            external: true,
          },
          { label: "Read the docs", href: "/docs/", variant: "secondary" },
        ]}
      />
    </PageLayout>
  )
}
