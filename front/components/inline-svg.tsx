"use client"

// Inline renderer for blog figures. The blog renders Markdown with react-markdown
// and no rehype-raw, so an `<img src="x.svg">` would be an opaque image that
// inherits nothing from the page theme. This component fetches a same-origin
// SVG and inlines it, so the figure's `var(--foreground, …)` style declarations
// resolve against the site's theme variables (next-themes toggles the `.dark`
// class on <html>). Each figure scopes its <style> to its own root class and
// prefixes its ids, so several figures on one page do not collide.
//
// Wire it in front/components/mdx-components.tsx:
//   img: (props) => <InlineSvg {...props} />

import { useEffect, useState } from "react"

const SAME_ORIGIN_SVG = /^\/[^\s]*\.svg(\?.*)?$/

export function InlineSvg({ src, alt }: { src?: string; alt?: string }) {
  const [markup, setMarkup] = useState<string | null>(null)
  const inline = !!src && SAME_ORIGIN_SVG.test(src)

  useEffect(() => {
    if (!inline || !src) return
    let cancelled = false
    fetch(src)
      .then((r) => (r.ok ? r.text() : Promise.reject(new Error(String(r.status)))))
      .then((text) => {
        if (cancelled) return
        // Only ever inline our own static figures: they are authored in-repo.
        const trimmed = text.trim()
        setMarkup(trimmed.startsWith("<svg") ? trimmed : null)
      })
      .catch(() => {
        if (!cancelled) setMarkup(null)
      })
    return () => {
      cancelled = true
    }
  }, [inline, src])

  if (!inline || !src) {
    // eslint-disable-next-line @next/next/no-img-element
    return <img src={src} alt={alt} />
  }

  if (markup === null) {
    // Fallback while loading, or if the fetch failed: the plain image still shows.
    // eslint-disable-next-line @next/next/no-img-element
    return <img src={src} alt={alt} className="w-full h-auto" />
  }

  return (
    <a href={src} target="_blank" rel="noopener noreferrer" aria-label={`${alt ?? "figure"} (open full size)`}>
      <span
        role="img"
        aria-label={alt}
        className="block w-full [&>svg]:w-full [&>svg]:h-auto my-6"
        dangerouslySetInnerHTML={{ __html: markup }}
      />
    </a>
  )
}
