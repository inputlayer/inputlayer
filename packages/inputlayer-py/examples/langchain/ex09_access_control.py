"""Access-controlled RAG."""

import asyncio

from examples.langchain._common import *

from inputlayer.integrations.langchain.params import iql_literal


async def run(kg):
    """Access-controlled RAG: IQL rules enforce document visibility.

    Documents have clearance levels. Users have roles. Rules compute
    which documents each user can see. The retriever automatically
    respects these rules — no application-level filtering needed.

    This is impossible with a plain vector store: you'd need to
    manually filter results after retrieval, risking data leaks.
    With InputLayer, access control IS the query.
    """
    header("Access-controlled RAG", 9)

    # ── Schema ───────────────────────────────────────────────────────

    await kg.execute("+classified_doc(id: int, title: string, content: string, clearance: string)")
    await kg.execute("+acl_user(username: string, role: string)")
    await kg.execute("+clearance_grants(role: string, level: string)")

    # ── Data: documents with clearance levels ────────────────────────

    docs = [
        (1, "Q4 Revenue Report", "Revenue grew 12% YoY to $4.2B.", "public"),
        (2, "Product Roadmap 2025", "Launch AI assistant in Q2, expand to APAC in Q3.", "internal"),
        (3, "Competitive Analysis", "Competitor X is 6 months behind on AI features.", "internal"),
        (4, "M&A Target List", "Evaluating acquisition of StartupY for $50M.", "confidential"),
        (5, "Board Compensation", "CEO base: $800K, stock: 2M shares.", "restricted"),
        (6, "Engineering Blog Draft", "How we scaled ML pipeline to 10x.", "public"),
        (7, "Security Audit Results", "3 critical vulns found in auth.", "restricted"),
        (8, "HR Investigation", "Complaint filed regarding team lead conduct.", "restricted"),
    ]

    for doc_id, title, content, clearance in docs:
        await kg.execute(
            f"+classified_doc({doc_id}, {iql_literal(title)}, {iql_literal(content)}, {iql_literal(clearance)})"
        )

    # ── Users with roles ─────────────────────────────────────────────

    users = [
        ("alice", "executive"),
        ("bob", "engineer"),
        ("carol", "contractor"),
        ("dave", "board_member"),
    ]

    for username, role in users:
        await kg.execute(f"+acl_user({iql_literal(username)}, {iql_literal(role)})")

    # ── Clearance hierarchy ──────────────────────────────────────────

    grants = [
        ("contractor", "public"),
        ("engineer", "public"),
        ("engineer", "internal"),
        ("executive", "public"),
        ("executive", "internal"),
        ("executive", "confidential"),
        ("board_member", "public"),
        ("board_member", "internal"),
        ("board_member", "confidential"),
        ("board_member", "restricted"),
    ]

    for role, level in grants:
        await kg.execute(f"+clearance_grants({iql_literal(role)}, {iql_literal(level)})")

    # The access control query joins three relations in InputLayer Query Language:
    #   classified_doc(Id, Title, Content, Clr),
    #   acl_user("<user>", Role),
    #   clearance_grants(Role, Clr)
    # No application-level filtering — access control IS the query.

    def _acl_query(username: str) -> str:
        return (
            f"?classified_doc(Id, Title, Content, Clr), "
            f"acl_user({iql_literal(username)}, Role), "
            f"clearance_grants(Role, Clr)"
        )

    subheader("Setup complete")
    print(f"  {DIM}8 documents across 4 clearance levels{RESET}")
    print(f"  {DIM}4 users: executive, engineer, contractor, board member{RESET}")
    print(f"{DIM}  Access query: classified_doc JOIN acl_user JOIN clearance_grants{RESET}")

    # ── Step 1: Show what each user can see ──────────────────────────

    subheader("Step 1: Document visibility per user")

    clearance_colors = {
        "public": GREEN,
        "internal": YELLOW,
        "confidential": MAGENTA,
        "restricted": RED,
    }

    for username, role in users:
        r = await kg.execute(_acl_query(username))
        count = len(r.rows)
        print(f"\n  {GREEN}{username}{RESET} ({role}) — {count} documents:")
        for row in r.rows:
            clr = row[3]
            color = clearance_colors.get(clr, DIM)
            print(f"    {color}[{clr}]{RESET} {row[1]}")

    # ── Step 2: Same query, different users, different results ───────

    subheader("Step 2: Same retriever, different users")
    print(
        f"{DIM}  The retriever query is identical — access control is in the IQL rules{RESET}"
    )

    for username, _role in users:
        retriever = InputLayerRetriever(
            kg=kg,
            query=_acl_query(username),
            page_content_columns=["Content"],
            metadata_columns=["Title", "Clr"],
        )

        lc_docs = await retriever.ainvoke(username)
        blocked = len(docs) - len(lc_docs)
        print(
            f"\n  {GREEN}{username}{RESET}: {len(lc_docs)} visible, {RED}{blocked} blocked{RESET}"
        )
        for d in lc_docs:
            clr = d.metadata.get("Clr", "")
            color = clearance_colors.get(clr, DIM)
            print(f"    {color}[{clr}]{RESET} {d.metadata.get('Title', '')}")

    # ── Step 3: Prove access denial ─────────────────────────────────

    subheader("Step 3: Why can't bob see the M&A target list?")

    # Bob is an engineer — try to access a confidential doc
    r = await kg.execute('?clearance_grants("engineer", "confidential")')
    has_access = len(r.rows) > 0

    print(f"\n  {DIM}Query: does engineer role grant confidential access?{RESET}")
    if has_access:
        print(f"  {GREEN}Yes — clearance granted{RESET}")
    else:
        print(
            f'  {RED}No — clearance_grants("engineer", "confidential") has no matching facts{RESET}'
        )
        print(f"  {DIM}Engineer role only grants: public, internal{RESET}")
        print(f"  {YELLOW}The join query returns zero rows for bob + confidential docs{RESET}")

    # ── Step 4: LLM answers using only visible docs ──────────────────

    if not check_llm():
        print(f"\n{DIM}  No LLM server detected — skipping LLM step.{RESET}")
        return

    from langchain_core.output_parsers import StrOutputParser
    from langchain_core.prompts import ChatPromptTemplate

    llm = get_llm()

    prompt = ChatPromptTemplate.from_template(
        "You are a corporate assistant. You can ONLY use the documents "
        "provided — do not hallucinate or reference information not shown.\n\n"
        "Visible documents for this user:\n{context}\n\n"
        "Question: {question}\n\n"
        "If the answer requires information not in the visible documents, "
        "say you don't have access to that information."
    )

    question = "What are our strategic plans and financial performance?"

    subheader("Step 4: LLM answers — same question, different access")

    for username, role in [("carol", "contractor"), ("alice", "executive")]:
        r = await kg.execute(_acl_query(username))
        context = "\n".join(f"- [{row[3]}] {row[1]}: {row[2]}" for row in r.rows)

        chain = prompt | llm | StrOutputParser()
        answer = await chain.ainvoke({"context": context, "question": question})

        print(f"\n  {GREEN}{username}{RESET} ({role}):")
        print(f"{GREEN}  {answer.strip()}{RESET}")


if __name__ == "__main__":

    async def main():
        il, kg = await connect()
        try:
            await run(kg)
        finally:
            await cleanup(il)

    asyncio.run(main())
