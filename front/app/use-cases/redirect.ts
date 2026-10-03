import type { Metadata } from "next"

// The use-case pages were retired in favour of one flagship article; old
// URLs keep working by redirecting there.
export const USE_CASE_REDIRECT = "/blog/building-a-voice-agent-that-knows/"
export const USE_CASE_REDIRECT_LABEL = "How to build a voice agent that knows"
export const RETIRED_USE_CASE_SLUGS = ["agentic-ai", "commerce", "financial-risk", "manufacturing", "supply-chain"]

export const redirectMetadata: Metadata = {
  title: "Moved - InputLayer",
  robots: { index: false },
}
