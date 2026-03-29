"use client"

import { useTheme } from "next-themes"
import { useEffect, useState } from "react"
import Image from "next/image"
import { cn } from "@/lib/utils"

interface LogoProps {
  size?: "sm" | "md" | "lg"
  className?: string
}

// Heights in px; width derived from 920:210 aspect ratio
const sizes = {
  sm: 22,
  md: 28,
  lg: 36,
} as const

const ASPECT = 920 / 210

export function Logo({ size = "md", className }: LogoProps) {
  const { resolvedTheme } = useTheme()
  const [mounted, setMounted] = useState(false)
  const h = sizes[size]
  const w = Math.round(h * ASPECT)

  useEffect(() => {
    setMounted(true)
  }, [])

  // Dark theme: white text logo. Light theme: dark text logo.
  const src = mounted && resolvedTheme === "light"
    ? "/inputlayer_logo_long_white.png"
    : "/inputlayer_logo_long_dark.png"

  return (
    <Image
      src={src}
      alt="InputLayer"
      width={w}
      height={h}
      className={cn("flex-shrink-0", className)}
      priority
    />
  )
}
