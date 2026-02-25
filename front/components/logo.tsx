import { cn } from "@/lib/utils"

interface LogoProps {
  size?: "sm" | "md" | "lg"
  className?: string
}

const sizes = {
  sm: { icon: 20, text: "text-base" },
  md: { icon: 24, text: "text-lg" },
  lg: { icon: 32, text: "text-2xl" },
} as const

export function Logo({ size = "md", className }: LogoProps) {
  const { icon, text } = sizes[size]

  return (
    <span className={cn("inline-flex items-center gap-1.5", className)}>
      <svg
        xmlns="http://www.w3.org/2000/svg"
        width={icon}
        height={icon}
        viewBox="0 0 24 24"
        fill="none"
        stroke="currentColor"
        strokeWidth="2"
        strokeLinecap="round"
        strokeLinejoin="round"
        className="flex-shrink-0"
      >
        <path d="M12 2L2 7l10 5 10-5-10-5z" />
        <path d="M2 17l10 5 10-5" />
        <path d="M2 12l10 5 10-5" />
      </svg>
      <span className={cn("font-extrabold tracking-tight", text)}>
        InputLayer
      </span>
    </span>
  )
}
