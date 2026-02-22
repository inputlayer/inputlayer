import type { LucideIcon } from "lucide-react"
import { cn } from "@/lib/utils"

interface EmptyStateProps {
  icon: LucideIcon
  title: string
  subtitle?: string
  className?: string
  children?: React.ReactNode
}

export function EmptyState({ icon: Icon, title, subtitle, className, children }: EmptyStateProps) {
  return (
    <div className={cn("py-8 text-center", className)}>
      <Icon className="mx-auto h-8 w-8 text-muted-foreground/50" />
      <p className="mt-2 text-xs text-muted-foreground">{title}</p>
      {subtitle && (
        <p className="mt-1 text-[10px] text-muted-foreground/70">{subtitle}</p>
      )}
      {children}
    </div>
  )
}
