"""Explainable RAG with .why()."""

import asyncio

from examples.langchain._common import *


class Expertise(Relation):
    person: str
    skill: str


class TeamMember(Relation):
    person: str
    team: str


class TeamExpert(Derived):
    """People who are experts in their team's domain — derived via rules."""

    person: str
    team: str
    skill: str

    rules = [  # noqa: RUF012
        From(TeamMember, Expertise)
        .where(lambda tm, e: tm.person == e.person)
        .select(person=TeamMember.person, team=TeamMember.team, skill=Expertise.skill),
    ]


def _print_proof_tree(tree: dict, depth: int) -> None:
    """Recursively print a proof tree with indentation."""
    indent = "    " * depth
    node_type = tree.get("node_type", "")

    if node_type == "base_fact":
        rel = tree.get("relation", "?")
        vals = tree.get("values", [])
        print(f"  {indent}{YELLOW}fact:{RESET} {rel}({', '.join(str(v) for v in vals)})")
    elif node_type == "rule_application":
        rule = tree.get("rule_name", "?")
        bindings = tree.get("bindings", [])
        bind_str = ", ".join(f"{b['variable']}={b['value']}" for b in bindings)
        clause = tree.get("clause_text", "")
        print(f"  {indent}{CYAN}rule:{RESET} {rule}({bind_str})")
        if clause:
            print(f"  {indent}{DIM}{clause}{RESET}")
        for child in tree.get("children", []):
            _print_proof_tree(child, depth + 1)
    else:
        # Generic: may have alternative proofs
        alt_count = tree.get("clause_text", "")
        if "alternative" in str(alt_count):
            print(f"  {indent}{DIM}{alt_count}{RESET}")
        for child in tree.get("children", []):
            _print_proof_tree(child, depth + 1)


async def run(kg):
    """Explainable RAG: retrieve results AND their proof trees.

    Uses .why() to get structured explanations of WHY each result was derived,
    then feeds both the results and the reasoning chain to the LLM.
    """
    header("Explainable RAG with .why()", 6)

    # ── Setup additional relations for this example ──────────────────

    await kg.define(Expertise, TeamMember)

    await kg.insert(
        [
            Expertise(person="alice", skill="machine-learning"),
            Expertise(person="alice", skill="python"),
            Expertise(person="bob", skill="rust"),
            Expertise(person="bob", skill="databases"),
            Expertise(person="carol", skill="machine-learning"),
            Expertise(person="carol", skill="deployment"),
            Expertise(person="dave", skill="javascript"),
            Expertise(person="dave", skill="react"),
        ]
    )

    await kg.insert(
        [
            TeamMember(person="alice", team="ml-platform"),
            TeamMember(person="bob", team="infrastructure"),
            TeamMember(person="carol", team="ml-platform"),
            TeamMember(person="dave", team="frontend"),
        ]
    )

    await kg.define_rules(TeamExpert)

    # ── Step 1: Query with .why() to get proof trees ─────────────────

    subheader("Step 1: Query with proof trees (.why)")

    raw_result = await kg._conn.execute(".why ?team_expert(Person, Team, Skill)")
    rows = raw_result.rows
    proof_trees = raw_result.proof_trees or []

    print(f"\n{DIM}  Found {len(rows)} team experts with proof trees{RESET}\n")

    for i, row in enumerate(rows):
        person, team, skill = row[0], row[1], row[2]
        print(f"  {GREEN}{person}{RESET} on {CYAN}{team}{RESET} — skill: {YELLOW}{skill}{RESET}")

        if i < len(proof_trees):
            tree = proof_trees[i]
            if "children" in tree:
                for child in tree["children"]:
                    rel = child.get("relation", "?")
                    vals = child.get("values", [])
                    print(f"    {DIM}because {rel}({', '.join(str(v) for v in vals)}){RESET}")

    # ── Step 2: Feed results + provenance to the LLM ────────────────

    base_url = os.environ.get("LLM_BASE_URL", "http://localhost:1234/v1")
    model = os.environ.get("LLM_MODEL", "deepseek/deepseek-r1-0528-qwen3-8b")

    try:
        import httpx

        resp = httpx.get(f"{base_url}/models", timeout=2)
        resp.raise_for_status()
    except Exception:
        print(f"\n{DIM}  No LLM server detected — skipping LLM step.{RESET}")
        return

    from langchain_core.output_parsers import StrOutputParser
    from langchain_core.prompts import ChatPromptTemplate
    from langchain_openai import ChatOpenAI

    llm = ChatOpenAI(base_url=base_url, api_key="lm-studio", model=model, temperature=0)

    # Build context with proof trees
    context_lines = []
    for i, row in enumerate(rows):
        person, team, skill = row[0], row[1], row[2]
        line = f"- {person} is a {skill} expert on team {team}"
        if i < len(proof_trees) and "children" in proof_trees[i]:
            reasons = []
            for child in proof_trees[i]["children"]:
                rel = child.get("relation", "")
                vals = child.get("values", [])
                reasons.append(f"{rel}({', '.join(str(v) for v in vals)})")
            line += f" [derived from: {'; '.join(reasons)}]"
        context_lines.append(line)

    context = "\n".join(context_lines)

    subheader("Step 2: LLM reasons over results + provenance")
    print(f"{DIM}  Context includes proof trees so the LLM can cite its reasoning{RESET}")

    prompt = ChatPromptTemplate.from_template(
        "You are analyzing a team's expertise based on a knowledge graph.\n"
        "Each result includes the derivation chain showing WHY it was concluded.\n\n"
        "Team experts and their derivations:\n{context}\n\n"
        "Question: {question}\n\n"
        "Answer concisely, citing the specific facts that support your answer."
    )

    chain = prompt | llm | StrOutputParser()

    question = "Who should lead a machine learning project, and why?"
    print(f'{DIM}  Question: "{question}"{RESET}')

    subheader("LLM Response (with provenance-backed reasoning):")
    answer = await chain.ainvoke({"context": context, "question": question})
    print(f"\n{GREEN}  {answer.strip()}{RESET}")

    # ── Step 3: .why_not — explain missing results ───────────────────

    subheader("Step 3: .why_not — why isn't dave an ML expert?")

    why_not_query = '.why_not team_expert("dave", "frontend", "machine-learning")'
    why_not_result = await kg._conn.execute(why_not_query)

    print()
    for row in why_not_result.rows:
        text = str(row[0])
        if text.startswith("  "):
            if "Blocker" in text:
                print(f"  {YELLOW}{text.strip()}{RESET}")
            else:
                print(f"  {DIM}{text.strip()}{RESET}")
        elif text:
            print(f"  {WHITE}{text}{RESET}")


if __name__ == "__main__":

    async def main():
        il, kg = await connect()
        try:
            await run(kg)
        finally:
            await cleanup(il)

    asyncio.run(main())
