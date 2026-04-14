"""Recommendation engine."""

import asyncio

from examples.langchain._common import *

from inputlayer.integrations.langchain.params import iql_literal


async def run(kg):
    """Collaborative filtering via IQL rules.

    Users rate items. Rules derive similar users (shared high ratings)
    and recommendations (items liked by similar users). The LLM
    explains recommendations in natural language.
    """
    header("Recommendation engine", 16)

    # ── Schema ───────────────────────────────────────────────────────

    await kg.execute("+user_rating(user: string, item: string, score: int)")
    await kg.execute("+item_info(name: string, category: string)")

    # ── Data ─────────────────────────────────────────────────────────

    ratings = [
        ("alice", "python_masterclass", 5),
        ("alice", "rust_handbook", 4),
        ("alice", "ml_fundamentals", 5),
        ("alice", "deep_learning_101", 5),
        ("bob", "python_masterclass", 5),
        ("bob", "go_concurrency", 4),
        ("bob", "ml_fundamentals", 4),
        ("bob", "data_engineering", 5),
        ("carol", "rust_handbook", 5),
        ("carol", "systems_programming", 5),
        ("carol", "linux_internals", 4),
        ("dave", "ml_fundamentals", 5),
        ("dave", "deep_learning_101", 5),
        ("dave", "nlp_with_transformers", 5),
        ("dave", "python_masterclass", 4),
    ]

    items = [
        ("python_masterclass", "programming"),
        ("rust_handbook", "programming"),
        ("go_concurrency", "programming"),
        ("ml_fundamentals", "machine_learning"),
        ("deep_learning_101", "machine_learning"),
        ("nlp_with_transformers", "machine_learning"),
        ("data_engineering", "data"),
        ("systems_programming", "systems"),
        ("linux_internals", "systems"),
    ]

    for user, item, score in ratings:
        await kg.execute(f'+user_rating({iql_literal(user)}, {iql_literal(item)}, {score})')
    for name, cat in items:
        await kg.execute(f'+item_info({iql_literal(name)}, {iql_literal(cat)})')

    # ── Rules ────────────────────────────────────────────────────────

    # Similar users: both rated the same item >= 4
    await kg.execute(
        "+similar_users(A, B, SharedItem) <- "
        "user_rating(A, SharedItem, ScoreA), "
        "user_rating(B, SharedItem, ScoreB), "
        "A != B, ScoreA >= 4, ScoreB >= 4"
    )

    # Recommend: items that similar users rated highly
    await kg.execute(
        "+raw_recommendation(TargetUser, Item, Via, Score) <- "
        "similar_users(TargetUser, Via, SharedItem), "
        "user_rating(Via, Item, Score), "
        "Score >= 4, TargetUser != Via"
    )

    subheader("Data loaded")
    print(f"  {DIM}{len(ratings)} ratings, {len(items)} items, 4 users{RESET}")

    # ── Step 1: Similar users ────────────────────────────────────────

    subheader("Step 1: Similar users (shared high ratings)")

    for user in ["alice", "bob", "carol", "dave"]:
        r = await kg.execute(f'?similar_users({iql_literal(user)}, Other, SharedItem)')
        others: dict[str, list[str]] = {}
        for row in r.rows:
            others.setdefault(row[1], []).append(row[2])
        if others:
            parts = []
            for other, shared in sorted(others.items()):
                parts.append(f"{other} ({', '.join(shared)})")
            print(f"  {GREEN}{user}{RESET}: {DIM}{'; '.join(parts)}{RESET}")

    # ── Step 2: Recommendations ──────────────────────────────────────

    subheader("Step 2: Recommendations for alice")

    r = await kg.execute(f'?raw_recommendation({iql_literal("alice")}, Item, Via, Score)')
    # Deduplicate and filter out items alice already rated
    r_alice = await kg.execute(f'?user_rating({iql_literal("alice")}, Item, Score)')
    alice_items = {row[0] for row in r_alice.rows}

    recs: dict[str, list[str]] = {}
    for row in r.rows:
        item, via = row[0], row[1]
        if item not in alice_items:
            recs.setdefault(item, []).append(via)

    print()
    for item, recommenders in sorted(recs.items()):
        r_info = await kg.execute(f'?item_info({iql_literal(item)}, Category)')
        cat = r_info.rows[0][1] if r_info.rows else "?"
        print(
            f"  {GREEN}{item}{RESET} {DIM}[{cat}]{RESET} — "
            f"recommended by {CYAN}{', '.join(sorted(set(recommenders)))}{RESET}"
        )

    # ── Step 3: LLM explains recommendations ─────────────────────────

    if not check_llm():
        print(f"\n{DIM}  No LLM — skipping explanation.{RESET}")
        return

    from langchain_core.output_parsers import StrOutputParser
    from langchain_core.prompts import ChatPromptTemplate

    llm = get_llm()

    context_parts = ["Alice's ratings:"]
    for row in r_alice.rows:
        context_parts.append(f"  - {row[0]}: {row[1]}/5")

    context_parts.append("\nRecommendations (from collaborative filtering):")
    for item, recommenders in sorted(recs.items()):
        context_parts.append(f"  - {item} (recommended by {', '.join(sorted(set(recommenders)))})")

    prompt = ChatPromptTemplate.from_template(
        "You are a learning recommendation system. Based on "
        "collaborative filtering results from a knowledge graph, "
        "explain why these items are recommended for Alice.\n\n"
        "{context}\n\n"
        "Write a brief, personalized recommendation message."
    )

    chain = prompt | llm | StrOutputParser()

    subheader("Step 3: LLM explains recommendations")
    answer = await chain.ainvoke({"context": "\n".join(context_parts)})
    print(f"\n{GREEN}  {answer.strip()}{RESET}")


if __name__ == "__main__":

    async def main():
        il, kg = await connect()
        try:
            await run(kg)
        finally:
            await cleanup(il)

    asyncio.run(main())
