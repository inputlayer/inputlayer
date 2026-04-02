"""Anomaly detection."""

import asyncio

from examples.langchain._common import *


async def run(kg):
    """Rule-based anomaly detection: Datalog rules define expected patterns,
    automatically flag violations, and the LLM explains them.

    No vector DB can do this — anomaly detection requires logical rules
    that compare values, check ranges, and cross-reference relationships.
    """
    header("Anomaly detection", 11)

    # ── Schema ───────────────────────────────────────────────────────

    await kg.execute(
        "+staff(name: string, role: string, salary: int, dept: string, hire_year: int)"
    )
    await kg.execute("+salary_band(role: string, min_sal: int, max_sal: int)")
    await kg.execute("+dept_budget(dept: string, budget: int)")
    await kg.execute("+dept_headcount_limit(dept: string, max_hc: int)")

    # ── Salary bands (expected ranges per role) ──────────────────────

    bands = [
        ("intern", 40000, 65000),
        ("junior_engineer", 70000, 110000),
        ("senior_engineer", 110000, 180000),
        ("staff_engineer", 170000, 250000),
        ("manager", 130000, 220000),
        ("director", 200000, 350000),
        ("vp", 300000, 500000),
    ]

    for role, mn, mx in bands:
        await kg.execute(f'+salary_band("{role}", {mn}, {mx})')

    # ── Department constraints ───────────────────────────────────────

    await kg.execute(
        '+dept_budget[("engineering", 2000000), ("product", 800000), ("data", 600000)]'
    )
    await kg.execute('+dept_headcount_limit[("engineering", 8), ("product", 4), ("data", 3)]')

    # ── Employee data (with deliberate anomalies) ────────────────────

    employees = [
        # Normal employees
        ("alice", "senior_engineer", 160000, "engineering", 2020),
        ("bob", "junior_engineer", 95000, "engineering", 2022),
        ("carol", "manager", 185000, "product", 2019),
        ("dave", "senior_engineer", 145000, "engineering", 2021),
        ("eve", "intern", 55000, "engineering", 2024),
        # ANOMALIES:
        ("frank", "intern", 150000, "engineering", 2024),  # intern salary way too high
        ("grace", "director", 120000, "data", 2018),  # director underpaid
        ("henry", "senior_engineer", 170000, "data", 2020),
        ("irene", "junior_engineer", 85000, "data", 2023),
        ("jack", "manager", 200000, "product", 2021),
        ("kate", "junior_engineer", 90000, "product", 2023),
        ("leo", "staff_engineer", 400000, "engineering", 2017),  # way over band
    ]

    for name, role, salary, dept, year in employees:
        await kg.execute(f'+staff("{name}", "{role}", {salary}, "{dept}", {year})')

    subheader("Data loaded")
    print(f"  {DIM}{len(employees)} employees, {len(bands)} salary bands{RESET}")
    print(f"  {DIM}3 departments with budget and headcount limits{RESET}")

    # ── Anomaly rules ────────────────────────────────────────────────

    # Rule 1: Salary above band maximum
    await kg.execute(
        "+overpaid(Name, Role, Salary, Max) <- "
        "staff(Name, Role, Salary, Dept, Year), "
        "salary_band(Role, Min, Max), Salary > Max"
    )

    # Rule 2: Salary below band minimum
    await kg.execute(
        "+underpaid(Name, Role, Salary, Min) <- "
        "staff(Name, Role, Salary, Dept, Year), "
        "salary_band(Role, Min, Max), Salary < Min"
    )

    subheader("Anomaly rules")
    print(f"  {DIM}overpaid(Name, Role, Salary, Max)")
    print(f"    <- staff salary > salary_band max{RESET}")
    print(f"  {DIM}underpaid(Name, Role, Salary, Min)")
    print(f"    <- staff salary < salary_band min{RESET}")

    # ── Step 1: Show detected anomalies ──────────────────────────────

    subheader("Step 1: Detected anomalies (instant — Datalog rules)")

    r = await kg.execute("?overpaid(Name, Role, Salary, Max)")
    print(f"\n  {RED}Overpaid ({len(r.rows)}):{RESET}")
    for row in r.rows:
        excess = row[2] - row[3]
        pct = (excess / row[3]) * 100
        print(
            f"    {RED}!{RESET} {GREEN}{row[0]}{RESET} ({row[1]}): "
            f"${row[2]:,} — {RED}${excess:,} over max "
            f"(+{pct:.0f}%){RESET}"
        )

    r = await kg.execute("?underpaid(Name, Role, Salary, Min)")
    print(f"\n  {YELLOW}Underpaid ({len(r.rows)}):{RESET}")
    for row in r.rows:
        deficit = row[3] - row[2]
        pct = (deficit / row[3]) * 100
        print(
            f"    {YELLOW}!{RESET} {GREEN}{row[0]}{RESET} ({row[1]}): "
            f"${row[2]:,} — {YELLOW}${deficit:,} under min "
            f"(-{pct:.0f}%){RESET}"
        )

    # ── Step 2: Show normal employees for contrast ───────────────────

    subheader("Step 2: Normal employees (within salary bands)")

    r_over = await kg.execute("?overpaid(Name, Role, Salary, Max)")
    r_under = await kg.execute("?underpaid(Name, Role, Salary, Min)")
    flagged = {row[0] for row in r_over.rows} | {row[0] for row in r_under.rows}

    print()
    for name, role, salary, dept, _year in employees:
        if name not in flagged:
            print(f"    {GREEN}ok{RESET} {name} ({role}): ${salary:,} {DIM}[{dept}]{RESET}")

    # ── Step 3: LLM explains anomalies ───────────────────────────────

    base_url = os.environ.get("LLM_BASE_URL", "http://localhost:1234/v1")
    model = os.environ.get("LLM_MODEL", "deepseek/deepseek-r1-0528-qwen3-8b")

    try:
        import httpx

        resp = httpx.get(f"{base_url}/models", timeout=2)
        resp.raise_for_status()
    except Exception:
        print(f"\n{DIM}  No LLM server — skipping explanation.{RESET}")
        return

    from langchain_core.output_parsers import StrOutputParser
    from langchain_core.prompts import ChatPromptTemplate
    from langchain_openai import ChatOpenAI

    llm = ChatOpenAI(base_url=base_url, api_key="lm-studio", model=model, temperature=0)

    # Build context from all anomalies
    context_parts = ["Salary band violations detected:\n"]

    r = await kg.execute("?overpaid(Name, Role, Salary, Max)")
    for row in r.rows:
        context_parts.append(
            f"- OVERPAID: {row[0]} ({row[1]}): ${row[2]:,} (max for role: ${row[3]:,})"
        )

    r = await kg.execute("?underpaid(Name, Role, Salary, Min)")
    for row in r.rows:
        context_parts.append(
            f"- UNDERPAID: {row[0]} ({row[1]}): ${row[2]:,} (min for role: ${row[3]:,})"
        )

    context_parts.append("\nSalary bands:")
    for role, mn, mx in bands:
        context_parts.append(f"- {role}: ${mn:,} - ${mx:,}")

    context = "\n".join(context_parts)

    prompt = ChatPromptTemplate.from_template(
        "You are an HR analytics system. Datalog rules in a knowledge "
        "graph automatically detected the following salary anomalies "
        "by comparing employee data against defined salary bands.\n\n"
        "{context}\n\n"
        "For each anomaly, provide a brief assessment: is this likely "
        "a data error, a misclassification, or something that needs "
        "HR review? Be concise."
    )

    chain = prompt | llm | StrOutputParser()

    subheader("Step 3: LLM explains anomalies")
    answer = await chain.ainvoke({"context": context})
    print(f"\n{GREEN}  {answer.strip()}{RESET}")


if __name__ == "__main__":

    async def main():
        il, kg = await connect()
        try:
            await run(kg)
        finally:
            await cleanup(il)

    asyncio.run(main())
