import Link from "next/link"

// Static export cannot issue HTTP redirects, so moved pages render a
// meta refresh (hoisted into <head> by React) plus a visible link.
export function RedirectNotice({ to, label }: { to: string; label: string }) {
  return (
    <>
      <meta httpEquiv="refresh" content={`0; url=${to}`} />
      <link rel="canonical" href={to} />
      <main className="flex min-h-dvh items-center justify-center px-6">
        <p className="text-muted-foreground">
          This page has moved to{" "}
          <Link href={to} className="text-primary hover:underline">
            {label}
          </Link>
          .
        </p>
      </main>
    </>
  )
}
