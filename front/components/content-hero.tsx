import Link from "next/link"
import { ArrowRight } from "lucide-react"

interface Breadcrumb {
  label: string
  href: string
}

interface CTA {
  label: string
  href: string
  variant?: "primary" | "secondary"
  external?: boolean
}

interface ContentHeroProps {
  heading: string
  subtitle?: string
  breadcrumbs?: Breadcrumb[]
  ctas?: CTA[]
}

export function ContentHero({ heading, subtitle, breadcrumbs, ctas }: ContentHeroProps) {
  return (
    <section className="border-b border-border/50">
      <div className="relative overflow-hidden">
        <div className="absolute inset-0 bg-gradient-to-b from-primary/5 to-transparent" />
        <div className="relative mx-auto max-w-6xl px-6 py-16 lg:py-20">
          {breadcrumbs && breadcrumbs.length > 0 && (
            <nav className="mb-4 flex items-center gap-2 text-sm text-muted-foreground">
              {breadcrumbs.map((crumb, i) => (
                <span key={crumb.href} className="flex items-center gap-2">
                  {i > 0 && <span>/</span>}
                  <Link href={crumb.href} className="hover:text-foreground transition-colors">
                    {crumb.label}
                  </Link>
                </span>
              ))}
            </nav>
          )}
          <h1 className="text-3xl font-extrabold tracking-tight sm:text-4xl lg:text-5xl max-w-3xl">
            {heading}
          </h1>
          {subtitle && (
            <p className="mt-4 text-lg text-muted-foreground max-w-2xl">{subtitle}</p>
          )}
          {ctas && ctas.length > 0 && (
            <div className="mt-6 flex flex-wrap gap-3">
              {ctas.map((cta) =>
                cta.variant === "secondary" ? (
                  <Link
                    key={cta.label}
                    href={cta.href}
                    className="inline-flex items-center gap-2 rounded-md border border-border bg-background px-5 py-2.5 text-sm font-medium hover:bg-secondary transition-colors"
                  >
                    {cta.label}
                  </Link>
                ) : (
                  <Link
                    key={cta.label}
                    href={cta.href}
                    className="inline-flex items-center gap-2 rounded-md bg-primary px-5 py-2.5 text-sm font-medium text-primary-foreground hover:bg-primary/90 transition-colors"
                  >
                    {cta.label}
                    <ArrowRight className="h-4 w-4" />
                  </Link>
                )
              )}
            </div>
          )}
        </div>
      </div>
    </section>
  )
}
