"""Semantic memory: InputLayerMemory stores turns, rules derive context.

A multi-turn conversation where:
1. Each message is stored as a fact in the KG
2. Topics are auto-extracted per turn
3. Rules derive: active topics, relevant context, topic threads
4. The recall_node injects derived context into the graph state
5. The LLM uses this derived context (not raw history) for its response

Shows the difference: raw history is "what was said", derived context
is "what matters for the next response."
"""

import asyncio
import base64

# ── State ────────────────────────────────────────────────────────────
from typing import Any, TypedDict

from examples.langgraph._common import (
    CYAN,
    DIM,
    GREEN,
    MAGENTA,
    RESET,
    WHITE,
    YELLOW,
    check_llm,
    drop_kg_if_exists,
    get_llm,
    header,
    os,
    step,
    success,
)

from inputlayer import InputLayer
from inputlayer.integrations.langgraph import InputLayerMemory
from langgraph.graph import END, StateGraph


class ChatState(TypedDict, total=False):
    thread_id: str
    new_message: dict[str, str]
    memory_context: dict[str, Any]
    response: str


# ── Conversation to replay ──────────────────────────────────────────

CONVERSATION = [
    ("user", "I'm building a machine learning pipeline in Python."),
    ("assistant", "Great! What stage are you at? Data prep, training, or deployment?"),
    ("user", "Training. The model is slow on our GPU cluster, taking 6 hours per epoch."),
    ("assistant", "For performance, consider mixed precision training and DataLoader workers."),
    ("user", "Good point. We're also having trouble with our REST API for serving predictions."),
    ("assistant", "For the API, consider FastAPI with async endpoints and model caching."),
    ("user", "What about deploying the whole thing with Docker and Kubernetes?"),
]


# ── Main ─────────────────────────────────────────────────────────────


async def run() -> None:
    header("Semantic memory with InputLayerMemory", 11)

    async with InputLayer(
        os.environ.get("INPUTLAYER_URL", "ws://localhost:8080/ws"),
        username=os.environ.get("INPUTLAYER_USER", "admin"),
        password=os.environ.get("INPUTLAYER_PASSWORD", "admin"),
    ) as il:
        await drop_kg_if_exists(il, "lg_memory")
        kg = il.knowledge_graph("lg_memory")
        try:
            memory = InputLayerMemory(kg=kg)
            await memory.setup()

            # ── Step 1: Replay conversation into memory ──────────────────

            step(1, "Store conversation turns")
            print(f"{DIM}  {len(CONVERSATION)} turns -> KG facts + auto topic extraction{RESET}\n")

            for role, content in CONVERSATION:
                turn_id = await memory.astore("chat-1", role, content)
                # Recall immediately after storing to show which topics were derived
                ctx = await memory.arecall("chat-1")
                turn_topics = [
                    t
                    for t, turns in ctx["relevant"].items()
                    if any(tr["turn_id"] == turn_id for tr in turns)
                ]
                topic_str = (
                    f" {MAGENTA}[{', '.join(sorted(turn_topics))}]{RESET}" if turn_topics else ""
                )
                color = GREEN if role == "user" else DIM
                print(f"  {color}{role:10s}{RESET} {content[:60]}{topic_str}")

            # ── Step 2: Show derived context ─────────────────────────────

            step(2, "Recall derived context (computed by rules)")

            ctx = await memory.arecall("chat-1")

            print(f"\n  {WHITE}Active topics:{RESET}")
            for topic in ctx["topics"]:
                print(f"    {CYAN}{topic}{RESET}")

            print(f"\n  {WHITE}Related topic pairs:{RESET}")
            for pair in ctx["related_topics"]:
                print(f"    {YELLOW}{pair[0]}{RESET} <-> {YELLOW}{pair[1]}{RESET}")

            print(f"\n  {WHITE}Relevant turns by topic:{RESET}")
            for topic, turns in sorted(ctx["relevant"].items()):
                print(f"    {CYAN}{topic}{RESET}:")
                for t in turns[:2]:  # show max 2 per topic
                    print(f"      {DIM}[{t['role']}] {t['content'][:50]}...{RESET}")

            # ── Step 3: Use as LangGraph nodes ───────────────────────────

            step(3, "Use recall_node in a LangGraph")

            recall = memory.recall_node(state_key="memory_context")
            store = memory.store_node(state_key="new_message", thread_key="thread_id")

            async def respond(state: dict[str, Any]) -> dict[str, Any]:
                """Generate a response using memory context."""
                ctx = state.get("memory_context", {})
                msg = state.get("new_message", {})
                question = msg.get("content", "")

                topics = ctx.get("topics", [])
                relevant = ctx.get("relevant", {})

                # Build context string from derived facts
                context_parts = []
                context_parts.append(f"Active topics: {', '.join(topics)}")
                for topic, turns in relevant.items():
                    for t in turns[:1]:
                        context_parts.append(f"[{topic}] {t['role']}: {t['content'][:60]}")

                context_str = "\n".join(context_parts)

                if check_llm():
                    from langchain_core.output_parsers import StrOutputParser
                    from langchain_core.prompts import ChatPromptTemplate

                    llm = get_llm()
                    prompt = ChatPromptTemplate.from_template(
                        "You are a helpful assistant. Use this context "
                        "derived from conversation memory:\n\n{context}\n\n"
                        "Answer the user's question briefly: {question}"
                    )
                    chain = prompt | llm | StrOutputParser()
                    answer = await chain.ainvoke({"context": context_str, "question": question})
                else:
                    answer = f"Based on topics [{', '.join(topics)}], here's guidance on: {question[:50]}"

                return {"response": answer}

            graph = StateGraph(ChatState)
            graph.add_node("recall", recall)
            graph.add_node("respond", respond)
            graph.add_node("store", store)

            graph.set_entry_point("recall")
            graph.add_edge("recall", "respond")
            graph.add_edge("respond", "store")
            graph.add_edge("store", END)

            app = graph.compile()

            # Ask a new question that wasn't in the replayed conversation.
            # The memory already contains the full history above; this new
            # question will be stored as an additional turn after the recall.
            new_question = "How do I set up GPU passthrough in Docker for the training job?"

            print(f"\n  {WHITE}New question:{RESET} {new_question}")
            print(f"{DIM}  Graph: recall -> respond -> store{RESET}")

            result = await app.ainvoke(
                {
                    "thread_id": "chat-1",
                    "new_message": {"role": "user", "content": new_question},
                    "memory_context": {},
                    "response": "",
                }
            )

            step(4, "Response (informed by derived memory)")
            print(f"\n{GREEN}  {result['response'].strip()}{RESET}")

            # ── Step 5: Show the proof tree for a derived turn ───────────

            step(5, "Why is this turn relevant? (.why proof tree)")
            # .why returns the structured derivation chain for each row in
            # the query. We pick one performance-tagged relevant turn and
            # print the rules and base facts that produced it. Memory
            # base64-encodes thread_id and topic before inserting, so the
            # query has to use the same encoding.
            def _b64(s: str) -> str:
                return base64.b64encode(s.encode("utf-8")).decode("ascii").rstrip("=")

            why = await kg._conn.execute(
                f'.why ?relevant_turn("{_b64("chat-1")}", TurnId, Role, Content, '
                f'"{_b64("performance")}")'
            )
            if why.rows and why.proof_trees:
                row = why.rows[0]
                tree = why.proof_trees[0]
                turn_id = row[0] if row else "?"
                print(f"\n  {CYAN}relevant_turn{RESET}(thread='chat-1', turn={turn_id}, topic='performance')")
                for child in tree.get("children", []):
                    rel = child.get("relation", "?")
                    print(f"    {DIM}because {rel}(...)  [base fact]{RESET}")
                print(
                    f"\n  {DIM}The proof tree is the exact reasoning chain. Paste it "
                    f"into your LLM prompt or surface it in the UI to show users why "
                    f"a memory was included.{RESET}"
                )
            else:
                print(f"  {DIM}(no proof trees available - verify .why is enabled){RESET}")

            # ── Step 6: Show what the memory looks like after ─────────────

            step(6, "Memory state after the interaction")
            final_ctx = await memory.arecall("chat-1")
            print(f"  Topics: {', '.join(final_ctx['topics'])}")
            print(f"  Total turns: {len(final_ctx['recent'])}")
            print(f"  Topic pairs: {len(final_ctx['related_topics'])} connections")

            success("Done!")
        finally:
            await drop_kg_if_exists(il, "lg_memory")


if __name__ == "__main__":
    asyncio.run(run())
