import { PageLayout } from "@/components/page-layout"
import { ContentHero } from "@/components/content-hero"
import { CTABanner } from "@/components/cta-banner"
import type { Metadata } from "next"

export const metadata: Metadata = {
  title: "Commercial Licensing - InputLayer",
  description:
    "Commercial licensing for InputLayer, the live rules engine for AI agents. Core source-available under the Elastic License 2.0; client SDKs Apache 2.0. Commercial license available for offering InputLayer as a hosted or managed service.",
}

export default function CommercialPage() {
  return (
    <PageLayout>
      <ContentHero
        heading="Commercial licensing"
        subtitle="The InputLayer core is source-available under the Elastic License 2.0, and the client SDKs are Apache 2.0. You are free to use, modify, run in production and redistribute it under the same licence. You may not offer InputLayer itself as a hosted or managed service."
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
            <li className="flex items-start gap-3">
              <span className="text-emerald-500 mt-0.5 shrink-0">&#10003;</span>
              <span>Redistributing InputLayer, modified or not, under the Elastic License 2.0 with its notices intact</span>
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
              <span>Offering InputLayer (or a fork of it) to third parties as a hosted or managed service that gives them access to a substantial set of its features</span>
            </li>
            <li className="flex items-start gap-3">
              <span className="text-destructive mt-0.5 shrink-0">&#10005;</span>
              <span>Redistributing InputLayer or a fork under any terms other than the Elastic License 2.0</span>
            </li>
            <li className="flex items-start gap-3">
              <span className="text-destructive mt-0.5 shrink-0">&#10005;</span>
              <span>Removing or obscuring the licensing, copyright or other notices in the software</span>
            </li>
          </ul>
          <p className="text-muted-foreground text-sm pt-2">
            In short: build and ship your product on InputLayer, but don&apos;t offer InputLayer itself as a service.
          </p>
        </div>

        <div className="rounded-xl border border-border bg-card p-8 space-y-4">
          <h2 className="text-2xl font-bold tracking-tight">
            What a commercial license includes
          </h2>
          <ul className="space-y-2 text-muted-foreground">
            <li>Rights to offer InputLayer as a hosted or managed service</li>
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
        heading="Take the rules out of your prompts."
        description="The live rules engine for AI agents. Self-hosted and source-available under the Elastic License 2.0."
        buttons={[
          { label: "Quickstart", href: "/docs/guides/quickstart/" },
          { label: "Read the docs", href: "/docs/", variant: "secondary" },
        ]}
      />
    </PageLayout>
  )
}
