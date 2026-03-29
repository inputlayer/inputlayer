"use client"

import { useState, useEffect, useCallback } from "react"

const phrases: [string, string][] = [
  ["Your AI approximates.", "InputLayer reasons."],
  ["Vectors find similar.", "Rules find correct."],
  ["One fact changes.", "Milliseconds, not minutes."],
  ["Why was this flagged?", "Ask the proof tree."],
  ["One path cleared. Two remain.", "Retraction done right."],
  ["Facts change.", "Conclusions update in realtime."],
  ["Three hops deep.", "Recursive reasoning, any depth."],
]

export function RotatingHero() {
  const [current, setCurrent] = useState(0)
  const [visible, setVisible] = useState(true)
  const [paused, setPaused] = useState(false)

  const advance = useCallback(() => {
    if (paused) return
    setVisible(false)
    setTimeout(() => {
      setCurrent((prev) => (prev + 1) % phrases.length)
      setVisible(true)
    }, 420)
  }, [paused])

  useEffect(() => {
    const id = setInterval(advance, 3800)
    return () => clearInterval(id)
  }, [advance])

  const [plain, accent] = phrases[current]

  return (
    <div
      className="space-y-5"
      onMouseEnter={() => setPaused(true)}
      onMouseLeave={() => setPaused(false)}
    >
      {/* Grid stack: all phrases occupy the same cell so the container
          always sizes to the tallest one. Only `current` is visible. */}
      <div className="grid cursor-default">
        {phrases.map(([p, a], i) => (
          <h1
            key={i}
            aria-hidden={i !== current}
            className="text-4xl sm:text-5xl lg:text-6xl font-extrabold tracking-tight leading-[1.15] max-w-[640px] col-start-1 row-start-1 transition-all duration-[420ms] ease-[cubic-bezier(0.16,1,0.3,1)]"
            style={{
              opacity: i === current && visible ? 1 : 0,
              transform: i === current && visible ? "translateY(0)" : "translateY(9px)",
              pointerEvents: i === current ? "auto" : "none",
            }}
          >
            <span className="text-foreground/80">{p}</span>
            <br />
            <span className="text-primary">{a}</span>
          </h1>
        ))}
      </div>

      {/* Progress dots */}
      <div className="flex gap-1.5 items-center">
        {phrases.map((_, i) => (
          <div
            key={i}
            className="h-[2px] rounded-full transition-all duration-350"
            style={{
              width: i === current ? 28 : 8,
              backgroundColor:
                i === current
                  ? "var(--primary)"
                  : "var(--muted-foreground)",
              opacity: i === current ? 1 : 0.2,
            }}
          />
        ))}
      </div>
    </div>
  )
}
