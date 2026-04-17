"""Resumable chat agent: checkpointer + memory in one graph.

The canonical LangGraph pattern for a production chat agent:

- ``InputLayerCheckpointer`` makes every turn resumable after crashes,
  restarts, or long idle periods.
- ``InputLayerMemory`` makes the conversation coherent by deriving
  "what matters" from every prior turn.

This example replays a short conversation, simulates a process crash
mid-conversation, then spins up a fresh graph instance that resumes
from the last checkpoint *and* still has the full memory of earlier
turns. Both pieces share a single KG.
"""

import asyncio
from typing import Any, TypedDict

from examples.langgraph._common import (
    DIM,
    GREEN,
    MAGENTA,
    RESET,
    WHITE,
    YELLOW,
    drop_kg_if_exists,
    header,
    os,
    step,
    success,
)

from inputlayer import InputLayer
from inputlayer.integrations.langgraph import (
    InputLayerCheckpointer,
    InputLayerMemory,
)
from langgraph.graph import END, StateGraph


class ChatState(TypedDict, total=False):
    thread_id: str
    new_message: dict[str, str]
    context: dict[str, Any]
    response: str


CONVERSATION = [
    {"role": "user", "content": "I'm building a machine learning pipeline in Python."},
    {"role": "user", "content": "The training loop is slow on our GPU cluster."},
    # After this turn we "crash" and resume.
    {"role": "user", "content": "What about deploying the whole thing with Docker?"},
]


async def build_app(memory: InputLayerMemory, checkpointer: InputLayerCheckpointer) -> Any:
    """Build a fresh graph instance wired to the shared memory and checkpointer."""

    async def respond(state: dict[str, Any]) -> dict[str, Any]:
        ctx = state.get("context", {})
        topics = ctx.get("topics", [])
        recent = ctx.get("recent", [])
        msg = state.get("new_message", {})
        user_content = msg.get("content", "")
        # Stand-in for an LLM call: summarize what the agent "knows" so the
        # example is deterministic without needing an LLM server.
        reply = (
            f"Thinking about {', '.join(topics) or 'your question'}. "
            f"Seen {len(recent)} prior turns. Answering: {user_content[:40]}..."
        )
        return {"response": reply}

    graph = StateGraph(ChatState)
    graph.add_node("recall", memory.recall_node(state_key="context"))
    graph.add_node("respond", respond)
    graph.add_node("store", memory.store_node(state_key="new_message"))

    graph.set_entry_point("recall")
    graph.add_edge("recall", "respond")
    graph.add_edge("respond", "store")
    graph.add_edge("store", END)

    return graph.compile(checkpointer=checkpointer)


async def run() -> None:
    header("Resumable chat: checkpointer + memory on one KG", 12)

    async with InputLayer(
        os.environ.get("INPUTLAYER_URL", "ws://localhost:8080/ws"),
        username=os.environ.get("INPUTLAYER_USER", "admin"),
        password=os.environ.get("INPUTLAYER_PASSWORD", "admin"),
    ) as il:
        await drop_kg_if_exists(il, "lg_chat_agent")
        kg = il.knowledge_graph("lg_chat_agent")
        try:
            step(1, "Set up shared memory and checkpointer on one KG")
            memory = InputLayerMemory(kg=kg)
            checkpointer = InputLayerCheckpointer(kg=kg)
            await memory.setup()
            await checkpointer.setup()
            print(f"  {DIM}Two relation families, one knowledge graph.{RESET}")

            thread = "alex-session-42"
            config = {"configurable": {"thread_id": thread}}

            # ── Process A: run the first two turns ──────────────────────
            step(2, "Process A handles the first two turns")
            app_a = await build_app(memory, checkpointer)
            for msg in CONVERSATION[:2]:
                result = await app_a.ainvoke(
                    {"thread_id": thread, "new_message": msg}, config=config
                )
                print(
                    f"  {GREEN}user{RESET}      {msg['content']}\n"
                    f"  {DIM}assistant{RESET} {result['response']}\n"
                )

            print(f"  {YELLOW}!! Process A crashes here !!{RESET}")

            # ── Process B: fresh instance resumes with full memory ──────
            step(3, "Process B starts fresh and resumes")
            app_b = await build_app(memory, checkpointer)

            threads_in_store = await checkpointer.alist_threads()
            print(f"  {DIM}Checkpointer knows threads: {threads_in_store}{RESET}")
            remembered = await memory.alist_threads()
            print(f"  {DIM}Memory knows threads:       {remembered}{RESET}\n")

            next_msg = CONVERSATION[2]
            result = await app_b.ainvoke(
                {"thread_id": thread, "new_message": next_msg}, config=config
            )
            topics = result.get("context", {}).get("topics", [])
            print(
                f"  {GREEN}user{RESET}      {next_msg['content']}\n"
                f"  {DIM}assistant{RESET} {result['response']}"
            )
            print(
                f"\n  {MAGENTA}topics after resume:{RESET} {', '.join(topics) or '(none)'}"
            )

            # ── Step 4: show audit trail ────────────────────────────────
            step(4, "Full checkpoint history across both processes")
            all_checkpoints = [tup async for tup in checkpointer.alist(config)]
            print(
                f"  {WHITE}{len(all_checkpoints)} checkpoints{RESET} "
                f"{DIM}(you can resume from any of them){RESET}"
            )

            success("Done!")
        finally:
            await drop_kg_if_exists(il, "lg_chat_agent")


if __name__ == "__main__":
    asyncio.run(run())
