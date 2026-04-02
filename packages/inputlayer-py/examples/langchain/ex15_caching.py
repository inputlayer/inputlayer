"""Semantic caching."""

import asyncio

from examples.langchain._common import *


async def run(kg):
    """Semantic caching: cache LLM responses as KG facts.

    Rules match incoming queries to cached answers based on topic
    overlap. Exact matches return instantly; topic matches suggest
    relevant cached answers that may avoid an LLM call.
    """
    header("Semantic caching", 15)

    # ── Schema ───────────────────────────────────────────────────────

    await kg.execute("+llm_cache(question: string, answer: string, timestamp: int)")
    await kg.execute("+cache_topic(question: string, topic: string)")

    # ── Rules ────────────────────────────────────────────────────────

    # Exact cache hit
    await kg.execute("+cache_exact_hit(Question, Answer) <- llm_cache(Question, Answer, Ts)")

    # Topic-based match: incoming question shares a topic with cache
    await kg.execute(
        "+cache_topic_hit(IncomingQ, CachedQ, Answer, Topic) <- "
        "cache_topic(IncomingQ, Topic), "
        "cache_topic(CachedQ, Topic), "
        "llm_cache(CachedQ, Answer, Ts), "
        "IncomingQ != CachedQ"
    )

    subheader("Rules defined")
    print(f"{DIM}  cache_exact_hit <- direct question match{RESET}")
    print(f"{DIM}  cache_topic_hit <- same topic, different question{RESET}")

    # ── Seed the cache ───────────────────────────────────────────────

    cached = [
        ("What is Python?", "Python is a high-level programming language.", 1000),
        ("Explain transformers", "Transformers use self-attention for sequences.", 1001),
        ("What is Rust?", "Rust is a memory-safe systems language.", 1002),
        ("How does BERT work?", "BERT uses masked language modeling on transformers.", 1003),
        ("What is Docker?", "Docker containers package apps with dependencies.", 1004),
    ]

    topics = [
        ("What is Python?", "python"),
        ("What is Python?", "programming"),
        ("Explain transformers", "transformers"),
        ("Explain transformers", "nlp"),
        ("What is Rust?", "rust"),
        ("What is Rust?", "programming"),
        ("How does BERT work?", "bert"),
        ("How does BERT work?", "transformers"),
        ("How does BERT work?", "nlp"),
        ("What is Docker?", "docker"),
        ("What is Docker?", "devops"),
    ]

    for q, a, ts in cached:
        eq = q.replace('"', '\\"')
        ea = a.replace('"', '\\"')
        await kg.execute(f'+llm_cache("{eq}", "{ea}", {ts})')

    for q, t in topics:
        eq = q.replace('"', '\\"')
        await kg.execute(f'+cache_topic("{eq}", "{t}")')

    subheader("Step 1: Cache populated")
    print(f"  {DIM}{len(cached)} cached responses, {len(topics)} topic tags{RESET}")

    # ── Step 2: Test cache hits ──────────────────────────────────────

    subheader("Step 2: Query the cache")

    # Exact hit
    r = await kg.execute('?cache_exact_hit("What is Python?", Answer)')
    print(f'\n  {WHITE}Exact: "What is Python?"{RESET}')
    if r.rows:
        print(f"  {GREEN}HIT: {r.rows[0][1]}{RESET}")
    else:
        print(f"  {RED}MISS{RESET}")

    # Miss
    r = await kg.execute('?cache_exact_hit("What is Go?", Answer)')
    print(f'\n  {WHITE}Exact: "What is Go?"{RESET}')
    if r.rows:
        print(f"  {GREEN}HIT: {r.rows[0][1]}{RESET}")
    else:
        print(f"  {YELLOW}MISS — no exact match{RESET}")

    # Topic match — new question about transformers
    new_q = "How do transformers handle long sequences?"
    await kg.execute(f'+cache_topic("{new_q}", "transformers")')

    r = await kg.execute(f'?cache_topic_hit("{new_q}", CachedQ, Answer, Topic)')
    print(f'\n  {WHITE}Topic: "{new_q}"{RESET}')
    if r.rows:
        seen = set()
        for row in r.rows:
            cached_q = row[1]
            if cached_q not in seen:
                seen.add(cached_q)
                print(f'  {CYAN}RELATED:{RESET} "{cached_q}" {DIM}(topic: {row[3]}){RESET}')
                print(f"    {DIM}{row[2]}{RESET}")
    else:
        print(f"  {YELLOW}No related cache entries{RESET}")

    # ── Step 3: Show savings ─────────────────────────────────────────

    subheader("Step 3: Cache efficiency")

    queries = [
        "What is Python?",
        "Explain transformers",
        "What is Kubernetes?",
        "How does BERT work?",
        "What is Go?",
    ]

    hits = 0
    misses = 0
    for q in queries:
        eq = q.replace('"', '\\"')
        r = await kg.execute(f'?cache_exact_hit("{eq}", Answer)')
        if r.rows:
            hits += 1
            print(f'  {GREEN}HIT{RESET}  "{q}"')
        else:
            misses += 1
            print(f'  {RED}MISS{RESET} "{q}"')

    rate = (hits / len(queries)) * 100
    print(
        f"\n  {WHITE}Hit rate: {hits}/{len(queries)} ({rate:.0f}%) — saved {hits} LLM calls{RESET}"
    )


if __name__ == "__main__":

    async def main():
        il, kg = await connect()
        try:
            await run(kg)
        finally:
            await cleanup(il)

    asyncio.run(main())
