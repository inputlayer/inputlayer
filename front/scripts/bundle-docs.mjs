#!/usr/bin/env node

/**
 * bundle-docs.mjs
 *
 * Reads docs/content/docs/ MDX files and _meta.json navigation,
 * then generates gui/lib/docs-bundle.ts with:
 *  - docsNavigation: tree structure for sidebar
 *  - docsPages: map of slug → { title, content, toc }
 *
 * Slug scheme: the route is /docs/[[...slug]], so slugs must NOT
 * include a "docs/" prefix.  e.g. "guides/first-program", not
 * "docs/guides/first-program".
 */

import fs from 'fs'
import path from 'path'
import { fileURLToPath } from 'url'

const __filename = fileURLToPath(import.meta.url)
const __dirname = path.dirname(__filename)

const CONTENT_DIR = path.resolve(__dirname, '../../docs/content')
const DOCS_DIR = path.join(CONTENT_DIR, 'docs')
const OUTPUT_FILE = path.resolve(__dirname, '../lib/docs-bundle.ts')

/** Extract title from MDX frontmatter or first heading */
function extractTitle(content) {
  // Check frontmatter
  const fmMatch = content.match(/^---\n([\s\S]*?)\n---/)
  if (fmMatch) {
    const titleMatch = fmMatch[1].match(/^title:\s*["']?(.+?)["']?\s*$/m)
    if (titleMatch) return titleMatch[1]
  }
  // Fall back to first heading
  const h1Match = content.match(/^#\s+(.+)$/m)
  if (h1Match) return h1Match[1]
  return 'Untitled'
}

/** Extract table of contents (h2 and h3 headings) */
function extractToc(content) {
  const toc = []
  // Strip frontmatter
  const body = content.replace(/^---\n[\s\S]*?\n---\n?/, '')
  const headingRegex = /^(#{2,3})\s+(.+)$/gm
  let match
  while ((match = headingRegex.exec(body)) !== null) {
    const level = match[1].length
    const text = match[2].replace(/[`*_~]/g, '')
    const id = text
      .toLowerCase()
      .replace(/[^a-z0-9\s-]/g, '')
      .replace(/\s+/g, '-')
      .replace(/-+/g, '-')
      .trim()
    toc.push({ level, text, id })
  }
  return toc
}

/** Strip MDX frontmatter for rendering */
function stripFrontmatter(content) {
  return content.replace(/^---\n[\s\S]*?\n---\n?/, '')
}

/** Strip Nextra-specific imports and JSX components */
function cleanMdxContent(content) {
  // Remove import statements
  let cleaned = content.replace(/^import\s+.*$/gm, '')
  // Replace <Callout> with blockquote
  cleaned = cleaned.replace(/<Callout[^>]*>/g, '> **Note:**')
  cleaned = cleaned.replace(/<\/Callout>/g, '')
  return cleaned.trim()
}

/** Read _meta.json and build navigation tree for a directory */
function buildNavTree(dir, slugPrefix) {
  const metaPath = path.join(dir, '_meta.json')
  if (!fs.existsSync(metaPath)) return []

  const meta = JSON.parse(fs.readFileSync(metaPath, 'utf-8'))
  const items = []

  for (const [key, value] of Object.entries(meta)) {
    // Skip external links
    if (typeof value === 'object' && value.href) continue

    const label = typeof value === 'string' ? value : value.title || key
    const childDir = path.join(dir, key)
    const mdxFile = path.join(dir, `${key}.mdx`)
    const slug = slugPrefix ? `${slugPrefix}/${key}` : key

    if (fs.existsSync(childDir) && fs.statSync(childDir).isDirectory()) {
      // Directory with children
      const children = buildNavTree(childDir, slug)
      items.push({
        key,
        label,
        slug,
        href: `/docs/${slug}`,
        children,
      })
    } else if (fs.existsSync(mdxFile)) {
      items.push({
        key,
        label,
        slug,
        href: `/docs/${slug}`,
        children: [],
      })
    }
  }

  return items
}

/** Recursively collect all MDX files */
function collectPages(dir, slugPrefix, pages) {
  const entries = fs.readdirSync(dir, { withFileTypes: true })

  for (const entry of entries) {
    if (entry.name.startsWith('_') || entry.name.startsWith('.')) continue

    const fullPath = path.join(dir, entry.name)

    if (entry.isDirectory()) {
      const subSlug = slugPrefix ? `${slugPrefix}/${entry.name}` : entry.name
      collectPages(fullPath, subSlug, pages)
    } else if (entry.name.endsWith('.mdx')) {
      const baseName = entry.name.replace('.mdx', '')
      const slug = slugPrefix ? `${slugPrefix}/${baseName}` : baseName
      const raw = fs.readFileSync(fullPath, 'utf-8')
      const cleaned = cleanMdxContent(raw)
      const title = extractTitle(raw)
      const toc = extractToc(cleaned)
      const content = stripFrontmatter(cleaned)

      pages[slug] = { title, content, toc }
    }
  }
}

// --- Main ---

console.log('Bundling docs from', DOCS_DIR)

// Build navigation tree from docs/content/docs/ with NO prefix
// (the /docs/ route prefix is added by the href template)
const navigation = fs.existsSync(DOCS_DIR) ? buildNavTree(DOCS_DIR, '') : []

// Collect all pages from docs/content/docs/ with NO prefix
const pages = {}
collectPages(DOCS_DIR, '', pages)

// Also include the root landing page (docs/content/index.mdx) as "index"
const rootIndex = path.join(CONTENT_DIR, 'index.mdx')
if (fs.existsSync(rootIndex)) {
  const raw = fs.readFileSync(rootIndex, 'utf-8')
  const cleaned = cleanMdxContent(raw)
  const title = extractTitle(raw)
  const toc = extractToc(cleaned)
  const content = stripFrontmatter(cleaned)
  pages['index'] = { title, content, toc }
}

// Also include docs/content/docs/index.mdx as "docs-index" if it exists
// (accessible at /docs/index or as the default for /docs)
const docsIndex = path.join(DOCS_DIR, 'index.mdx')
if (fs.existsSync(docsIndex) && !pages['index']) {
  const raw = fs.readFileSync(docsIndex, 'utf-8')
  const cleaned = cleanMdxContent(raw)
  const title = extractTitle(raw)
  const toc = extractToc(cleaned)
  const content = stripFrontmatter(cleaned)
  pages['index'] = { title, content, toc }
}

// Generate TypeScript output
const output = `// AUTO-GENERATED — do not edit. Run "node scripts/bundle-docs.mjs" to regenerate.

export interface TocEntry {
  level: number
  text: string
  id: string
}

export interface NavItem {
  key: string
  label: string
  slug: string
  href: string
  children: NavItem[]
}

export interface DocPage {
  title: string
  content: string
  toc: TocEntry[]
}

export const docsNavigation: NavItem[] = ${JSON.stringify(navigation, null, 2)}

export const docsPages: Record<string, DocPage> = ${JSON.stringify(pages, null, 2)}
`

fs.writeFileSync(OUTPUT_FILE, output, 'utf-8')
console.log(`Wrote ${Object.keys(pages).length} pages to ${OUTPUT_FILE}`)
