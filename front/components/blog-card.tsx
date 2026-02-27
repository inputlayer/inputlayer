import Link from "next/link"

interface BlogCardProps {
  slug: string
  title: string
  date: string
  author: string
  excerpt: string
  category: string
}

export function BlogCard({ slug, title, date, author, excerpt, category }: BlogCardProps) {
  return (
    <Link
      href={`/blog/${slug}/`}
      className="group rounded-xl border border-border bg-card p-6 space-y-3 transition-colors hover:border-primary/30 hover:bg-card/80"
    >
      {category && (
        <span className="inline-flex items-center rounded-full border border-border bg-secondary/50 px-2.5 py-0.5 text-xs font-medium text-muted-foreground">
          {category}
        </span>
      )}
      <h3 className="text-lg font-semibold group-hover:text-primary transition-colors line-clamp-2">
        {title}
      </h3>
      {excerpt && (
        <p className="text-sm text-muted-foreground line-clamp-3">{excerpt}</p>
      )}
      <div className="flex items-center gap-2 text-xs text-muted-foreground">
        {date && (
          <time dateTime={date}>
            {new Date(date).toLocaleDateString("en-US", {
              year: "numeric",
              month: "long",
              day: "numeric",
            })}
          </time>
        )}
        {date && author && <span>·</span>}
        {author && <span>{author}</span>}
      </div>
    </Link>
  )
}
