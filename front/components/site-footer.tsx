import Link from "next/link"
import { Logo } from "@/components/logo"
import { ExternalLink } from "lucide-react"

interface FooterLink {
  label: string
  href: string
  external?: boolean
}

const footerLinks: Record<string, FooterLink[]> = {
  Product: [
    { label: "Features", href: "/#features" },
    { label: "Use Cases", href: "/use-cases/" },
    { label: "Compare", href: "/compare/" },
    { label: "Demo", href: "https://demo.inputlayer.ai", external: true },
  ],
  Resources: [
    { label: "Documentation", href: "/docs/" },
    { label: "Blog", href: "/blog/" },
    { label: "Quickstart", href: "/docs/guides/quickstart/" },
    { label: "Python SDK", href: "/docs/guides/python-sdk/" },
  ],
  Company: [
    { label: "GitHub", href: "https://github.com/inputlayer/inputlayer", external: true },
    { label: "License", href: "https://github.com/inputlayer/inputlayer/blob/main/LICENSE", external: true },
  ],
  Community: [
    { label: "Star on GitHub", href: "https://github.com/inputlayer/inputlayer", external: true },
    { label: "Contributing", href: "https://github.com/inputlayer/inputlayer/blob/main/CONTRIBUTING.md", external: true },
  ],
}

export function SiteFooter() {
  return (
    <footer className="border-t border-border/50 bg-card/50">
      <div className="mx-auto max-w-6xl px-6 py-12">
        <div className="grid grid-cols-2 gap-8 md:grid-cols-4 lg:gap-12">
          {Object.entries(footerLinks).map(([category, links]) => (
            <div key={category}>
              <h3 className="text-sm font-semibold text-foreground mb-3">{category}</h3>
              <ul className="space-y-2">
                {links.map((link) => (
                  <li key={link.label}>
                    {link.external ? (
                      <a
                        href={link.href}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="inline-flex items-center gap-1 text-sm text-muted-foreground hover:text-foreground transition-colors"
                      >
                        {link.label}
                        <ExternalLink className="h-3 w-3" />
                      </a>
                    ) : (
                      <Link
                        href={link.href}
                        className="text-sm text-muted-foreground hover:text-foreground transition-colors"
                      >
                        {link.label}
                      </Link>
                    )}
                  </li>
                ))}
              </ul>
            </div>
          ))}
        </div>
        <div className="mt-10 flex flex-col sm:flex-row items-center justify-between gap-4 border-t border-border/50 pt-8">
          <div className="flex items-center gap-4">
            <Logo size="sm" />
            <span className="text-sm text-muted-foreground">AGPL-3.0 License</span>
          </div>
          <p className="text-xs text-muted-foreground">
            A symbolic reasoning engine for AI agents.
          </p>
        </div>
      </div>
    </footer>
  )
}
