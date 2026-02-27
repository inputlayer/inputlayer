#!/usr/bin/env node

/**
 * bundle-content.mjs
 *
 * Reads content/ MDX files (blog, use-cases, compare, customers),
 * parses frontmatter, and generates front/lib/content-bundle.ts with
 * typed exports: blogPosts[], useCases[], comparisonPages[], customerStories[].
 */

import fs from 'fs'
import path from 'path'
import { fileURLToPath } from 'url'

const __filename = fileURLToPath(import.meta.url)
const __dirname = path.dirname(__filename)

const CONTENT_DIR = path.resolve(__dirname, '../../content')
const OUTPUT_FILE = path.resolve(__dirname, '../lib/content-bundle.ts')

/** Parse YAML-like frontmatter into an object */
function parseFrontmatter(content) {
  const match = content.match(/^---\n([\s\S]*?)\n---/)
  if (!match) return {}

  const fm = {}
  const lines = match[1].split('\n')
  let currentKey = null
  let currentList = null

  for (const line of lines) {
    // Array item
    const listItem = line.match(/^\s+-\s+"?([^"]*)"?$/)
    if (listItem && currentKey) {
      if (!currentList) currentList = []
      currentList.push(listItem[1])
      fm[currentKey] = currentList
      continue
    }

    // Save any pending list
    if (currentList) {
      currentList = null
    }

    // Key-value pair
    const kvMatch = line.match(/^(\w+):\s*(.*)$/)
    if (kvMatch) {
      currentKey = kvMatch[1]
      let value = kvMatch[2].trim()

      // Remove surrounding quotes
      if ((value.startsWith('"') && value.endsWith('"')) ||
          (value.startsWith("'") && value.endsWith("'"))) {
        value = value.slice(1, -1)
      }

      // Check if this starts a list (value is empty, next lines are list items)
      if (value === '') {
        fm[currentKey] = ''
        continue
      }

      fm[currentKey] = value
    }
  }

  return fm
}

/** Extract title from frontmatter or first heading */
function extractTitle(content) {
  const fm = parseFrontmatter(content)
  if (fm.title) return fm.title
  const h1Match = content.match(/^#\s+(.+)$/m)
  if (h1Match) return h1Match[1]
  return 'Untitled'
}

/** Extract table of contents (h2 and h3 headings) */
function extractToc(content) {
  const toc = []
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

/** Strip frontmatter for rendering */
function stripFrontmatter(content) {
  return content.replace(/^---\n[\s\S]*?\n---\n?/, '')
}

/** Strip imports and Nextra-specific components */
function cleanMdxContent(content) {
  let cleaned = content.replace(/^import\s+.*$/gm, '')
  cleaned = cleaned.replace(/<Callout[^>]*>/g, '> **Note:**')
  cleaned = cleaned.replace(/<\/Callout>/g, '')
  return cleaned.trim()
}

/** Read all MDX files from a directory */
function readContentDir(dirPath) {
  if (!fs.existsSync(dirPath)) return []

  const entries = fs.readdirSync(dirPath, { withFileTypes: true })
  const items = []

  for (const entry of entries) {
    if (!entry.name.endsWith('.mdx')) continue
    const fullPath = path.join(dirPath, entry.name)
    const raw = fs.readFileSync(fullPath, 'utf-8')
    const slug = entry.name.replace('.mdx', '')
    const fm = parseFrontmatter(raw)
    const cleaned = cleanMdxContent(raw)
    const title = extractTitle(raw)
    const toc = extractToc(cleaned)
    const content = stripFrontmatter(cleaned)

    items.push({ slug, title, content, toc, ...fm })
  }

  return items
}

// --- Main ---

console.log('Bundling content from', CONTENT_DIR)

// Blog posts
const blogDir = path.join(CONTENT_DIR, 'blog')
const blogPosts = readContentDir(blogDir).map(item => ({
  slug: item.slug,
  title: item.title,
  date: item.date || '',
  author: item.author || '',
  category: item.category || '',
  excerpt: item.excerpt || '',
  content: item.content,
  toc: item.toc,
}))
// Sort by date descending
blogPosts.sort((a, b) => b.date.localeCompare(a.date))

// Use cases
const useCasesDir = path.join(CONTENT_DIR, 'use-cases')
const useCases = readContentDir(useCasesDir).map(item => ({
  slug: item.slug,
  title: item.title,
  icon: item.icon || '',
  subtitle: item.subtitle || '',
  content: item.content,
  toc: item.toc,
}))

// Comparison pages
const compareDir = path.join(CONTENT_DIR, 'compare')
const comparisonPages = readContentDir(compareDir).map(item => ({
  slug: item.slug,
  title: item.title,
  competitors: item.competitors || [],
  content: item.content,
  toc: item.toc,
}))

// Customer stories
const customersDir = path.join(CONTENT_DIR, 'customers')
const customerStories = readContentDir(customersDir).map(item => ({
  slug: item.slug,
  title: item.title,
  industry: item.industry || '',
  keyMetric: item.keyMetric || '',
  content: item.content,
  toc: item.toc,
}))

// Generate TypeScript output
const output = `// AUTO-GENERATED - do not edit. Run "node scripts/bundle-content.mjs" to regenerate.

export interface TocEntry {
  level: number
  text: string
  id: string
}

export interface BlogPost {
  slug: string
  title: string
  date: string
  author: string
  category: string
  excerpt: string
  content: string
  toc: TocEntry[]
}

export interface UseCase {
  slug: string
  title: string
  icon: string
  subtitle: string
  content: string
  toc: TocEntry[]
}

export interface ComparisonPage {
  slug: string
  title: string
  competitors: string[]
  content: string
  toc: TocEntry[]
}

export interface CustomerStory {
  slug: string
  title: string
  industry: string
  keyMetric: string
  content: string
  toc: TocEntry[]
}

export const blogPosts: BlogPost[] = ${JSON.stringify(blogPosts, null, 2)}

export const useCases: UseCase[] = ${JSON.stringify(useCases, null, 2)}

export const comparisonPages: ComparisonPage[] = ${JSON.stringify(comparisonPages, null, 2)}

export const customerStories: CustomerStory[] = ${JSON.stringify(customerStories, null, 2)}
`

fs.writeFileSync(OUTPUT_FILE, output, 'utf-8')
console.log(`Wrote ${blogPosts.length} blog posts, ${useCases.length} use cases, ${comparisonPages.length} comparisons, ${customerStories.length} customer stories to ${OUTPUT_FILE}`)
