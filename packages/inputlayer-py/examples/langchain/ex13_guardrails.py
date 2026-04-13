"""Guardrails / output safety."""

import asyncio

from examples.langchain._common import *
from inputlayer.integrations.langchain.params import iql_literal


async def run(kg):
    """Rule-based guardrails: IQL rules define content policies,
    LLM output is checked against them before returning to the user.

    Policies are declarative facts — add/remove rules without code changes.
    The KG enforces them automatically via joins.
    """
    header("Guardrails / output safety", 13)

    # ── Policy definitions as facts ──────────────────────────────────

    await kg.execute("+blocked_topic(topic: string, reason: string, severity: string)")
    await kg.execute("+required_disclaimer(topic: string, disclaimer: string)")
    await kg.execute("+pii_pattern(pattern_name: string, example: string)")

    # Blocked topics
    blocked = [
        ("weapons_manufacturing", "Safety policy", "critical"),
        ("illegal_activity", "Legal compliance", "critical"),
        ("self_harm", "Safety policy", "critical"),
        ("competitor_disparagement", "Brand policy", "warning"),
        ("price_fixing", "Antitrust compliance", "critical"),
    ]

    for topic, reason, severity in blocked:
        await kg.execute(f"+blocked_topic({iql_literal(topic)}, {iql_literal(reason)}, {iql_literal(severity)})")

    # Required disclaimers
    disclaimers = [
        ("medical", "This is not medical advice. Consult a doctor."),
        ("financial", "Not financial advice. Consult a professional."),
        ("legal", "Not legal advice. Consult a licensed attorney."),
    ]

    for topic, disc in disclaimers:
        await kg.execute(f"+required_disclaimer({iql_literal(topic)}, {iql_literal(disc)})")

    # PII patterns
    pii = [
        ("ssn", "123-45-6789"),
        ("credit_card", "4111-1111-1111-1111"),
        ("email_address", "user@example.com"),
        ("phone_number", "+1-555-0123"),
    ]

    for name, example in pii:
        await kg.execute(f"+pii_pattern({iql_literal(name)}, {iql_literal(example)})")

    # ── Content to check (simulating LLM outputs) ────────────────────

    await kg.execute("+output_content(id: int, text: string)")
    await kg.execute("+output_topic(content_id: int, topic: string)")
    await kg.execute("+output_contains_pii(content_id: int, pattern_name: string)")

    # Simulated LLM outputs with their detected topics/PII
    outputs = [
        (1, "Python is great for data science and ML."),
        (2, "You should take 500mg of ibuprofen daily."),
        (3, "Buy ACME stock now, it will double."),
        (4, "Here is how to pick a lock to enter a building."),
        (5, "Our competitor's product is terrible and overpriced."),
        (6, "Your account number is 4111-1111-1111-1111."),
        (7, "Consider consulting a financial advisor for this."),
    ]

    for oid, text in outputs:
        await kg.execute(f"+output_content({oid}, {iql_literal(text)})")

    # Topic classifications (in production, the LLM classifies these)
    topic_tags = [
        (1, "programming"),
        (2, "medical"),
        (3, "financial"),
        (4, "illegal_activity"),
        (5, "competitor_disparagement"),
        (6, "customer_service"),
        (7, "financial"),
    ]

    for cid, topic in topic_tags:
        await kg.execute(f"+output_topic({cid}, {iql_literal(topic)})")

    # PII detection
    await kg.execute('+output_contains_pii(6, "credit_card")')

    # ── Guardrail rules ──────────────────────────────────────────────

    # Rule: blocked content
    await kg.execute(
        "+policy_violation(ContentId, Topic, Reason, Severity) <- "
        "output_topic(ContentId, Topic), "
        "blocked_topic(Topic, Reason, Severity)"
    )

    # Rule: needs disclaimer
    await kg.execute(
        "+needs_disclaimer(ContentId, Topic, Disclaimer) <- "
        "output_topic(ContentId, Topic), "
        "required_disclaimer(Topic, Disclaimer)"
    )

    # Rule: PII leak
    await kg.execute(
        "+pii_violation(ContentId, Pattern, Example) <- "
        "output_contains_pii(ContentId, Pattern), "
        "pii_pattern(Pattern, Example)"
    )

    subheader("Policies loaded")
    print(f"  {DIM}{len(blocked)} blocked topics{RESET}")
    print(f"  {DIM}{len(disclaimers)} required disclaimers{RESET}")
    print(f"  {DIM}{len(pii)} PII patterns{RESET}")

    # ── Step 1: Check all outputs ────────────────────────────────────

    subheader("Step 1: Scan 7 LLM outputs against policies")

    # Show each output with its status
    r_violations = await kg.execute("?policy_violation(Id, Topic, Reason, Severity)")
    r_disclaimers = await kg.execute("?needs_disclaimer(Id, Topic, Disclaimer)")
    r_pii = await kg.execute("?pii_violation(Id, Pattern, Example)")

    violation_ids = {row[0] for row in r_violations.rows}
    disclaimer_ids = {row[0] for row in r_disclaimers.rows}
    pii_ids = {row[0] for row in r_pii.rows}

    print()
    for oid, text in outputs:
        if oid in violation_ids:
            violations = [r for r in r_violations.rows if r[0] == oid]
            for v in violations:
                sev_color = RED if v[3] == "critical" else YELLOW
                print(f'  {sev_color}BLOCKED{RESET} [{v[3]}] "{text[:60]}"')
                print(f"    {DIM}Reason: {v[2]} (topic: {v[1]}){RESET}")
        elif oid in pii_ids:
            pii_matches = [r for r in r_pii.rows if r[0] == oid]
            for p in pii_matches:
                print(f'  {RED}PII LEAK{RESET} "{text[:60]}"')
                print(f"    {DIM}Detected: {p[1]} pattern{RESET}")
        elif oid in disclaimer_ids:
            discs = [r for r in r_disclaimers.rows if r[0] == oid]
            for d in discs:
                print(f'  {YELLOW}DISCLAIMER{RESET} "{text[:60]}"')
                print(f"    {DIM}Append: {d[2]}{RESET}")
        else:
            print(f'  {GREEN}PASS{RESET} "{text[:60]}"')

    # ── Step 2: Summary stats ────────────────────────────────────────

    subheader("Step 2: Summary")
    total = len(outputs)
    blocked_count = len(violation_ids) + len(pii_ids)
    disclaimer_count = len(disclaimer_ids - violation_ids - pii_ids)
    passed = total - blocked_count - disclaimer_count
    print(f"\n  {GREEN}Passed: {passed}{RESET}")
    print(f"  {YELLOW}Need disclaimer: {disclaimer_count}{RESET}")
    print(f"  {RED}Blocked: {blocked_count}{RESET}")

    # ── Step 3: LLM rewrites blocked content safely ──────────────────

    if not check_llm():
        print(f"\n{DIM}  No LLM — skipping rewrite step.{RESET}")
        return

    from langchain_core.output_parsers import StrOutputParser
    from langchain_core.prompts import ChatPromptTemplate

    llm = get_llm()

    # Pick a blocked output and ask the LLM to rewrite safely
    blocked_output = next((text for oid, text in outputs if oid in violation_ids), None)

    if blocked_output:
        subheader("Step 3: LLM rewrites blocked content safely")
        print(f'  {RED}Original: "{blocked_output}"{RESET}')

        prompt = ChatPromptTemplate.from_template(
            "The following response was blocked by a safety policy. "
            "Rewrite it to be helpful but safe — decline the "
            "specific request while offering a legitimate alternative.\n\n"
            "Blocked response: {blocked}\n\n"
            "Safe rewrite:"
        )
        chain = prompt | llm | StrOutputParser()
        safe = await chain.ainvoke({"blocked": blocked_output})
        print(f"  {GREEN}Rewrite: {safe.strip()}{RESET}")


if __name__ == "__main__":

    async def main():
        il, kg = await connect()
        try:
            await run(kg)
        finally:
            await cleanup(il)

    asyncio.run(main())
