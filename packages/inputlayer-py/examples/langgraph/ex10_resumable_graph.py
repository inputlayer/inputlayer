"""Resumable graph: persist state in InputLayer, resume after interruption.

Demonstrates InputLayerCheckpointer: a LangGraph BaseCheckpointSaver
backed by an InputLayer KG. The graph runs to a checkpoint, the process
"crashes" (we abandon the graph), and then a new graph instance resumes
from the persisted state.

This is the canonical use case for checkpointing: long-running agents
that must survive process restarts, network outages, or human-in-the-loop
pauses.
"""

import asyncio
from typing import Any, TypedDict

from examples.langgraph._common import (
    DIM,
    GREEN,
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
from inputlayer.integrations.langgraph import InputLayerCheckpointer
from langgraph.graph import END, StateGraph

# ── State ────────────────────────────────────────────────────────────


class WorkflowState(TypedDict, total=False):
    """Plain TypedDict. Checkpoint state must be msgpack-serializable,
    so we don't put the KG handle in here. The checkpointer accesses
    its own KG via self.kg, not via state.
    """

    task: str
    steps_done: list[str]
    current_step: int


# ── Graph nodes ──────────────────────────────────────────────────────


async def step_one(state: dict[str, Any]) -> dict[str, Any]:
    print(f"  {GREEN}+{RESET} Step 1: gathering data")
    steps = list(state.get("steps_done", []))
    steps.append("gathered_data")
    return {"steps_done": steps, "current_step": 1}


async def step_two(state: dict[str, Any]) -> dict[str, Any]:
    print(f"  {GREEN}+{RESET} Step 2: processing data")
    steps = list(state.get("steps_done", []))
    steps.append("processed_data")
    return {"steps_done": steps, "current_step": 2}


async def step_three(state: dict[str, Any]) -> dict[str, Any]:
    print(f"  {GREEN}+{RESET} Step 3: generating report")
    steps = list(state.get("steps_done", []))
    steps.append("generated_report")
    return {"steps_done": steps, "current_step": 3}


async def step_four(state: dict[str, Any]) -> dict[str, Any]:
    print(f"  {GREEN}+{RESET} Step 4: sending notifications")
    steps = list(state.get("steps_done", []))
    steps.append("sent_notifications")
    return {"steps_done": steps, "current_step": 4}


def build_graph(checkpointer: InputLayerCheckpointer, *, interrupt_after: list[str] | None = None):
    graph = StateGraph(WorkflowState)
    graph.add_node("one", step_one)
    graph.add_node("two", step_two)
    graph.add_node("three", step_three)
    graph.add_node("four", step_four)

    graph.set_entry_point("one")
    graph.add_edge("one", "two")
    graph.add_edge("two", "three")
    graph.add_edge("three", "four")
    graph.add_edge("four", END)

    return graph.compile(
        checkpointer=checkpointer,
        interrupt_after=interrupt_after or [],
    )


# ── Main ─────────────────────────────────────────────────────────────


async def run() -> None:
    header("Resumable graph with InputLayerCheckpointer", 10)

    async with InputLayer(
        os.environ.get("INPUTLAYER_URL", "ws://localhost:8080/ws"),
        username=os.environ.get("INPUTLAYER_USER", "admin"),
        password=os.environ.get("INPUTLAYER_PASSWORD", "admin"),
    ) as il:
        await drop_kg_if_exists(il, "lg_resumable")
        kg = il.knowledge_graph("lg_resumable")
        try:
            # ── Setup ────────────────────────────────────────────────────

            step(1, "Create checkpointer backed by InputLayer KG")
            checkpointer = InputLayerCheckpointer(kg=kg)
            await checkpointer.setup()
            print(f"  {DIM}Schema created in lg_resumable KG{RESET}")

            # ── First run: run partially, then "crash" ───────────────────

            step(2, "Run graph until step 2, then simulate a crash")

            thread_id = "workflow-001"
            config = {"configurable": {"thread_id": thread_id}}

            # Build first graph instance with interrupt after step 2
            # This naturally stops the graph at the checkpoint, simulating
            # a clean shutdown / paused state.
            app1 = build_graph(checkpointer, interrupt_after=["two"])

            print(f"\n  {WHITE}Process A starts:{RESET}")
            await app1.ainvoke(
                {
                    "task": "Generate quarterly report",
                    "steps_done": [],
                    "current_step": 0,
                },
                config=config,
            )
            print(f"\n  {YELLOW}!! Process A interrupted after step 2 !!{RESET}")

            # Show what's in the checkpoint store
            step(3, "Check what was persisted to the KG")
            checkpoints = []
            async for tup in checkpointer.alist(config):
                checkpoints.append(tup)
            print(
                f"  {DIM}{len(checkpoints)} checkpoint(s) stored in KG for thread '{thread_id}'{RESET}"
            )
            if checkpoints:
                latest = checkpoints[0]
                steps_so_far = latest.checkpoint.get("channel_values", {}).get("steps_done", [])
                print(f"  {DIM}Latest checkpoint steps: {steps_so_far}{RESET}")

            # ── Second run: resume in a fresh graph instance ─────────────

            step(4, "Process B starts and resumes from checkpoint")
            print(f"\n  {WHITE}Process B starts (new graph instance):{RESET}")

            # Build a brand new graph instance, same checkpointer
            app2 = build_graph(checkpointer)

            # Resuming: pass None as input, the checkpointer loads state
            result = await app2.ainvoke(None, config=config)

            # ── Results ──────────────────────────────────────────────────

            step(5, "Final state after resume")
            all_steps = result.get("steps_done", [])
            print(f"\n  {GREEN}Completed {len(all_steps)} steps:{RESET}")
            for i, s in enumerate(all_steps, 1):
                print(f"    {GREEN}{i}.{RESET} {s}")

            # Show the full checkpoint history
            step(6, "Full checkpoint history")
            all_checkpoints = []
            async for tup in checkpointer.alist(config):
                all_checkpoints.append(tup)
            print(f"  {DIM}{len(all_checkpoints)} total checkpoints across both runs{RESET}")
            print(f"  {DIM}This is the audit trail. You can resume from ANY of these{RESET}")

            # ── Cleanup ──────────────────────────────────────────────────

            success("Done!")
        finally:
            await drop_kg_if_exists(il, "lg_resumable")


if __name__ == "__main__":
    asyncio.run(run())
