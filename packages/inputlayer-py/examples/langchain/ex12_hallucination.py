"""Hallucination detection / answer grounding."""

import asyncio

from examples.langchain._common import *


async def _insert_hallucination_claims(kg):
    """Fallback claims when no LLM is available."""
    claims = [
        (1, "Python", "created_by", "Guido van Rossum"),  # correct
        (2, "Python", "first_released", "1989"),  # wrong (1991)
        (3, "Rust", "created_by", "Mozilla"),  # wrong (Graydon Hoare)
        (4, "Rust", "first_released", "2010"),  # correct
        (5, "Go", "created_by", "Google"),  # wrong (Rob Pike...)
        (6, "Go", "first_released", "2009"),  # correct
    ]

    subheader("Claims to verify (hardcoded)")
    for cid, subj, pred, obj in claims:
        await kg.execute(f'+llm_claim({cid}, "{subj}", "{pred}", "{obj}")')
        print(f"  {DIM}claim {cid}:{RESET} {subj} {DIM}{pred}{RESET} {obj}")

    subheader("Datalog rules verify claims (instant)")


async def _show_grounding_results(kg):
    """Display grounded vs hallucinated claims."""
    r = await kg.execute("?grounded_claim(Id, Subj, Pred, Obj)")
    print(f"\n  {GREEN}Grounded ({len(r.rows)}):{RESET}")
    for row in r.rows:
        print(f"    {GREEN}ok{RESET} {row[1]} {DIM}{row[2]}{RESET} {row[3]}")

    r = await kg.execute("?hallucinated_claim(Id, Subj, Pred, Claimed, Actual)")
    print(f"\n  {RED}Hallucinated ({len(r.rows)}):{RESET}")
    for row in r.rows:
        print(
            f"    {RED}!!{RESET} {row[1]} {DIM}{row[2]}{RESET}: "
            f'said {RED}"{row[3]}"{RESET}, '
            f'actually {GREEN}"{row[4]}"{RESET}'
        )

    # Count unverifiable (claims with no matching subject+predicate)
    r_all = await kg.execute("?llm_claim(Id, Subj, Pred, Obj)")
    r_grounded = await kg.execute("?grounded_claim(Id, S, P, O)")
    r_halluc = await kg.execute("?hallucinated_claim(Id, S, P, C, A)")
    verified_ids = {row[0] for row in r_grounded.rows} | {row[0] for row in r_halluc.rows}
    unverifiable = [row for row in r_all.rows if row[0] not in verified_ids]

    if unverifiable:
        print(f"\n  {YELLOW}Unverifiable ({len(unverifiable)}):{RESET}")
        for row in unverifiable:
            print(
                f"    {YELLOW}?{RESET} {row[1]} {DIM}{row[2]}{RESET} "
                f"{row[3]} {DIM}(no matching fact){RESET}"
            )


async def run(kg):
    """Ground LLM outputs against KG facts to detect hallucinations.

    The LLM generates an answer, we extract claims from it, then
    Datalog rules cross-reference each claim against known facts.
    Claims are classified as: grounded, hallucinated, or unverifiable.
    """
    header("Hallucination detection / answer grounding", 12)

    # ── Ground truth knowledge base ──────────────────────────────────

    await kg.execute("+ground_truth(subject: string, predicate: string, object: string)")

    facts = [
        ("Python", "created_by", "Guido van Rossum"),
        ("Python", "first_released", "1991"),
        ("Python", "typing", "dynamically typed"),
        ("Rust", "created_by", "Graydon Hoare"),
        ("Rust", "first_released", "2010"),
        ("Rust", "typing", "statically typed"),
        ("JavaScript", "created_by", "Brendan Eich"),
        ("JavaScript", "first_released", "1995"),
        ("Go", "created_by", "Rob Pike and Ken Thompson"),
        ("Go", "first_released", "2009"),
        ("TypeScript", "created_by", "Microsoft"),
        ("TypeScript", "superset_of", "JavaScript"),
        ("Linux", "created_by", "Linus Torvalds"),
        ("Git", "created_by", "Linus Torvalds"),
    ]

    for subj, pred, obj in facts:
        escaped = obj.replace('"', '\\"')
        await kg.execute(f'+ground_truth("{subj}", "{pred}", "{escaped}")')

    # ── Claims extracted from LLM output ─────────────────────────────

    await kg.execute("+llm_claim(id: int, subject: string, predicate: string, object: string)")

    # ── Verification rules ───────────────────────────────────────────

    # Grounded: claim exactly matches a known fact
    await kg.execute(
        "+grounded_claim(Id, Subj, Pred, Obj) <- "
        "llm_claim(Id, Subj, Pred, Obj), "
        "ground_truth(Subj, Pred, Obj)"
    )

    # Hallucinated: claim contradicts a known fact
    await kg.execute(
        "+hallucinated_claim(Id, Subj, Pred, Claimed, Actual) <- "
        "llm_claim(Id, Subj, Pred, Claimed), "
        "ground_truth(Subj, Pred, Actual), Claimed != Actual"
    )

    subheader("Setup")
    print(f"  {DIM}{len(facts)} ground truth facts loaded{RESET}")
    print(f"  {DIM}Rules: grounded_claim, hallucinated_claim{RESET}")

    # ── Step 1: Generate an LLM answer ───────────────────────────────

    base_url = os.environ.get("LLM_BASE_URL", "http://localhost:1234/v1")
    model = os.environ.get("LLM_MODEL", "deepseek/deepseek-r1-0528-qwen3-8b")

    try:
        import httpx

        resp = httpx.get(f"{base_url}/models", timeout=2)
        resp.raise_for_status()
    except Exception:
        print(f"\n{DIM}  No LLM — using hardcoded claims.{RESET}")
        await _insert_hallucination_claims(kg)
        await _show_grounding_results(kg)
        return

    from langchain_core.output_parsers import StrOutputParser
    from langchain_core.prompts import ChatPromptTemplate
    from langchain_openai import ChatOpenAI
    from pydantic import BaseModel
    from pydantic import Field as PydanticField

    llm = ChatOpenAI(base_url=base_url, api_key="lm-studio", model=model, temperature=0.7)

    # Ask the LLM a question it might hallucinate on
    question = (
        "Tell me about the history of Python, Rust, and Go "
        "programming languages — who created them and when."
    )

    subheader("Step 1: LLM generates an answer")
    print(f'{DIM}  Question: "{question}"{RESET}')

    prompt = ChatPromptTemplate.from_template(
        "Answer this question concisely in 3-4 sentences: {question}"
    )
    chain = prompt | llm | StrOutputParser()
    llm_answer = await chain.ainvoke({"question": question})
    print(f"\n  {WHITE}{llm_answer.strip()}{RESET}")

    # ── Step 2: Extract claims from the answer ───────────────────────

    subheader("Step 2: Extract claims from the answer")

    class Claim(BaseModel):
        subject: str = PydanticField(description="Programming language")
        predicate: str = PydanticField(description="One of: created_by, first_released, typing")
        object: str = PydanticField(description="The claimed value")

    class ClaimList(BaseModel):
        claims: list[Claim] = PydanticField(description="Factual claims extracted from the text")

    extractor = llm.with_structured_output(ClaimList)
    extracted = extractor.invoke(
        "Extract factual claims about programming languages from "
        "this text. Use predicates: created_by, first_released.\n\n"
        f"Text: {llm_answer}"
    )

    for i, c in enumerate(extracted.claims):
        escaped_obj = c.object.replace('"', '\\"')
        await kg.execute(f'+llm_claim({i + 1}, "{c.subject}", "{c.predicate}", "{escaped_obj}")')
        print(f"  {DIM}claim {i + 1}:{RESET} {c.subject} {DIM}{c.predicate}{RESET} {c.object}")

    # ── Step 3: Datalog rules verify claims ──────────────────────────

    subheader("Step 3: Datalog rules verify claims (instant)")
    await _show_grounding_results(kg)

    # ── Step 4: Produce grounded summary ─────────────────────────────

    r_halluc = await kg.execute("?hallucinated_claim(Id, Subj, Pred, Claimed, Actual)")

    corrections = []
    for row in r_halluc.rows:
        corrections.append(f'- WRONG: {row[1]} {row[2]} "{row[3]}" → CORRECT: "{row[4]}"')

    if corrections:
        subheader("Step 4: LLM corrects its answer")

        fix_prompt = ChatPromptTemplate.from_template(
            "Your previous answer contained factual errors. "
            "Here are the corrections:\n\n{corrections}\n\n"
            "Original answer: {original}\n\n"
            "Rewrite the answer with the corrections applied. "
            "Be concise."
        )
        fix_chain = fix_prompt | llm | StrOutputParser()
        fixed = await fix_chain.ainvoke(
            {
                "corrections": "\n".join(corrections),
                "original": llm_answer,
            }
        )
        print(f"\n{GREEN}  {fixed.strip()}{RESET}")
    else:
        print(f"\n  {GREEN}All claims verified — no corrections needed.{RESET}")


if __name__ == "__main__":

    async def main():
        il, kg = await connect()
        try:
            await run(kg)
        finally:
            await cleanup(il)

    asyncio.run(main())
