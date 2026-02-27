import Link from "next/link"
import { ArrowRight, ExternalLink } from "lucide-react"

interface CTAButton {
  label: string
  href: string
  variant?: "primary" | "secondary"
  external?: boolean
}

interface CTABannerProps {
  heading: string
  description?: string
  buttons: CTAButton[]
}

export function CTABanner({ heading, description, buttons }: CTABannerProps) {
  return (
    <section className="border-t border-border/50">
      <div className="mx-auto max-w-6xl px-6 py-16">
        <div className="relative rounded-2xl border border-border bg-gradient-to-br from-primary/10 via-transparent to-primary/5 p-12 text-center space-y-6">
          <h2 className="text-3xl font-bold tracking-tight">{heading}</h2>
          {description && (
            <p className="text-muted-foreground text-lg max-w-xl mx-auto">
              {description}
            </p>
          )}
          <div className="flex flex-wrap justify-center gap-3 pt-2">
            {buttons.map((btn) => {
              const isExternal = btn.external
              const isPrimary = btn.variant !== "secondary"
              const className = isPrimary
                ? "inline-flex items-center gap-2 rounded-md bg-primary px-6 py-3 text-sm font-medium text-primary-foreground hover:bg-primary/90 transition-colors"
                : "inline-flex items-center gap-2 rounded-md border border-border bg-background px-6 py-3 text-sm font-medium hover:bg-secondary transition-colors"

              if (isExternal) {
                return (
                  <a
                    key={btn.label}
                    href={btn.href}
                    target="_blank"
                    rel="noopener noreferrer"
                    className={className}
                  >
                    {btn.label}
                    {isPrimary ? <ArrowRight className="h-4 w-4" /> : <ExternalLink className="h-3.5 w-3.5" />}
                  </a>
                )
              }

              return (
                <Link key={btn.label} href={btn.href} className={className}>
                  {btn.label}
                  {isPrimary && <ArrowRight className="h-4 w-4" />}
                </Link>
              )
            })}
          </div>
        </div>
      </div>
    </section>
  )
}
