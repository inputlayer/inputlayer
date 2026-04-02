"""Multi-hop reasoning."""

import asyncio

from examples.langchain._common import *


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
    """Multi-hop graph reasoning: the KG computes transitive closure,
    the LLM interprets the results.

    Shows that complex graph traversal happens in Datalog rules,
    not in the LLM — the LLM just reads the derived conclusions.
    """
    header("Multi-hop reasoning", 7)

    # ── Setup: company org graph ─────────────────────────────────────

    await kg.execute("+reports_to(employee: string, manager: string)")
    await kg.execute("+collaborates(person_a: string, person_b: string)")
    await kg.execute("+employee_info(name: string, role: string, team: string)")

    # Org structure:
    #   CEO (grace)
    #    ├── VP Eng (henry)
    #    │    ├── alice (ml-platform)
    #    │    └── bob (infrastructure)
    #    └── VP Product (irene)
    #         ├── carol (ml-platform)
    #         └── dave (frontend)
    #   frank (intern, reports to alice)

    await kg.execute(
        '+employee_info[("grace", "CEO", "executive"), '
        '("henry", "VP Engineering", "engineering"), '
        '("irene", "VP Product", "product"), '
        '("alice", "Senior Engineer", "ml-platform"), '
        '("bob", "Senior Engineer", "infrastructure"), '
        '("carol", "Product Manager", "ml-platform"), '
        '("dave", "Frontend Lead", "frontend"), '
        '("frank", "Intern", "ml-platform")]'
    )

    await kg.execute(
        '+reports_to[("henry", "grace"), ("irene", "grace"), '
        '("alice", "henry"), ("bob", "henry"), '
        '("carol", "irene"), ("dave", "irene"), '
        '("frank", "alice")]'
    )

    await kg.execute(
        '+collaborates[("alice", "carol"), ("carol", "alice"), '
        '("bob", "dave"), ("dave", "bob"), '
        '("alice", "bob"), ("bob", "alice")]'
    )

    # ── Recursive rule: chain of command (transitive closure) ────────

    # Direct reports
    await kg.execute("+chain_of_command(E, M) <- reports_to(E, M)")
    # Transitive: if E reports to X and X is under M, then E is under M
    await kg.execute("+chain_of_command(E, M) <- reports_to(E, X), chain_of_command(X, M)")

    # Cross-team influence: people who collaborate AND share a chain of command
    await kg.execute(
        "+can_influence(A, B) <- collaborates(A, B), "
        "chain_of_command(A, Mgr), chain_of_command(B, Mgr)"
    )

    # ── Step 1: Show derived chain of command ────────────────────────

    subheader("Step 1: Chain of command (transitive closure)")
    print(f"{DIM}  Rules: reports_to(E, M) => chain_of_command(E, M){RESET}")
    print(f"{DIM}         reports_to(E, X), chain_of_command(X, M)")
    print(f"           => chain_of_command(E, M){RESET}\n")

    r = await kg.execute("?chain_of_command(E, M)")
    # Group by manager
    by_mgr: dict[str, list[str]] = {}
    for row in r.rows:
        by_mgr.setdefault(row[1], []).append(row[0])

    for mgr, reports in sorted(by_mgr.items()):
        names = ", ".join(sorted(reports))
        print(f"  {GREEN}{mgr}{RESET} has authority over: {CYAN}{names}{RESET}")

    # ── Step 2: Multi-hop proof — how does frank reach grace? ────────

    subheader("Step 2: Proof tree — how does frank report to grace?")

    raw = await kg._conn.execute('.why ?chain_of_command("frank", "grace")')
    trees = raw.proof_trees or []

    if trees:
        _print_proof_tree(trees[0], depth=0)

    # ── Step 3: Cross-team influence ─────────────────────────────────

    subheader("Step 3: Cross-team influence (collaboration + shared authority)")
    print(f"{DIM}  Rule: collaborates(A, B) AND shared manager => can_influence(A, B){RESET}\n")

    r = await kg.execute("?can_influence(A, B)")
    for row in r.rows:
        print(f"  {GREEN}{row[0]}{RESET} can influence {CYAN}{row[1]}{RESET}")

    # ── Step 4: LLM analyzes the org graph ───────────────────────────

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

    # Build context from derived facts
    chain_lines = []
    for mgr, reports in sorted(by_mgr.items()):
        chain_lines.append(f"- {mgr} has authority over: {', '.join(sorted(reports))}")

    influence_result = await kg.execute("?can_influence(A, B)")
    influence_lines = [f"- {r[0]} can influence {r[1]}" for r in influence_result.rows]

    employee_result = await kg.execute("?employee_info(Name, Role, Team)")
    emp_lines = [f"- {r[0]}: {r[1]} on {r[2]}" for r in employee_result.rows]

    context = (
        "Employees:\n" + "\n".join(emp_lines) + "\n\n"
        "Chain of command (derived via multi-hop reasoning):\n" + "\n".join(chain_lines) + "\n\n"
        "Cross-team influence (derived from collaboration + shared authority):\n"
        + "\n".join(influence_lines)
    )

    prompt = ChatPromptTemplate.from_template(
        "You are an organizational analyst. The following facts were derived "
        "from a knowledge graph using Datalog rules — the chain of command "
        "and influence relationships were computed automatically via "
        "transitive closure, not manually defined.\n\n"
        "{context}\n\n"
        "Question: {question}\n\n"
        "Answer concisely based on the derived facts."
    )

    chain = prompt | llm | StrOutputParser()

    question = (
        "If we need to staff a cross-functional ML project requiring "
        "both engineering and product input, who are the key people "
        "and what's the shortest approval chain?"
    )

    subheader("Step 4: LLM analyzes the org graph")
    print(f'{DIM}  Question: "{question}"{RESET}')

    subheader("LLM Response:")
    answer = await chain.ainvoke({"context": context, "question": question})
    print(f"\n{GREEN}  {answer.strip()}{RESET}")


if __name__ == "__main__":

    async def main():
        il, kg = await connect()
        try:
            await run(kg)
        finally:
            await cleanup(il)

    asyncio.run(main())
