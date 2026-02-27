interface StatCardProps {
  value: string
  label: string
  description?: string
}

export function StatCard({ value, label, description }: StatCardProps) {
  return (
    <div className="rounded-xl border border-border bg-card p-6 text-center space-y-2">
      <p className="text-4xl font-extrabold text-primary">{value}</p>
      <p className="text-sm font-semibold text-foreground">{label}</p>
      {description && (
        <p className="text-xs text-muted-foreground">{description}</p>
      )}
    </div>
  )
}
