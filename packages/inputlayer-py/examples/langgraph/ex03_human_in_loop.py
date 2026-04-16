"""Human-in-the-loop: KG rules classify actions, router gates risky ones.

The KG acts as a policy engine. Rules classify proposed actions as
"safe" (auto-execute) or "needs_approval" (requires human review).
The router checks these derived classifications to decide the path.

Shows InputLayer as a declarative policy layer for agentic workflows.
"""

import asyncio
import contextlib
from typing import Any

from examples.langgraph._common import (
    CYAN,
    DIM,
    GREEN,
    RED,
    RESET,
    WHITE,
    YELLOW,
    header,
    os,
    step,
    success,
)

from inputlayer import InputLayer
from inputlayer.integrations.langgraph import InputLayerState, escape_iql
from langgraph.graph import END, StateGraph

# ── State ────────────────────────────────────────────────────────────


class ActionState(InputLayerState):
    actions: list[dict[str, Any]]
    current_action: dict[str, Any]
    approved: list[str]
    rejected: list[str]
    executed: list[str]
    action_index: int


# ── Graph nodes ──────────────────────────────────────────────────────


async def pick_next_action(state: dict[str, Any]) -> dict[str, Any]:
    """Pick the next action to process."""
    actions = state.get("actions", [])
    idx = state.get("action_index", 0)

    if idx >= len(actions):
        return {"current_action": {}, "action_index": idx}

    action = actions[idx]
    print(f"\n  {WHITE}Processing action {idx + 1}/{len(actions)}:{RESET}")
    print(f"  {CYAN}{action['type']}{RESET}: {action['description']}")

    # Classify this action in the KG
    kg = state["kg"]
    # Clear any existing pending_action facts to avoid accumulation
    await kg.execute("-pending_action(T, D, A, Tgt) <- pending_action(T, D, A, Tgt)")
    await kg.execute(
        f'+pending_action("{escape_iql(action["type"])}", "{escape_iql(action["description"])}", '
        f'{action["amount"]}, "{escape_iql(action["target"])}")'
    )

    return {"current_action": action, "action_index": idx + 1}


async def classify_action(state: dict[str, Any]) -> str:
    """Router: check KG rules to classify the action."""
    action = state.get("current_action", {})
    kg = state["kg"]

    if not action:
        return "done"

    # Check if this action triggers any risk rules
    a_type = escape_iql(action["type"])
    amount = action["amount"]
    target = escape_iql(action["target"])

    r = await kg.execute(f'?risk_flag("{a_type}", {amount}, "{target}", Level, Reason)')

    if r.rows:
        # risk_flag(action_type, amount, target, level, reason)
        # All columns are returned; level=col[3], reason=col[4]
        level = r.rows[0][3]
        reason = r.rows[0][4]
        print(f"  {YELLOW}Risk: {level}: {reason}{RESET}")
        if level == "high":
            return "needs_approval"
        return "auto_approve"
    else:
        print(f"  {GREEN}No risk flags{RESET}")
        return "auto_approve"


async def auto_execute(state: dict[str, Any]) -> dict[str, Any]:
    """Execute a safe action automatically."""
    action = state["current_action"]
    executed = list(state.get("executed", []))
    desc = f"{action['type']}: {action['description']}"
    executed.append(desc)
    print(f"  {GREEN}AUTO-EXECUTED{RESET}: {desc}")
    return {"executed": executed}


async def request_approval(state: dict[str, Any]) -> dict[str, Any]:
    """Simulate human review of a risky action."""
    action = state["current_action"]
    desc = f"{action['type']}: {action['description']}"

    # Simulate human decision based on amount
    # In production, this would pause and wait for human input
    if action["amount"] > 50000:
        rejected = list(state.get("rejected", []))
        rejected.append(desc)
        print(f"  {RED}REJECTED by reviewer{RESET}: {desc}")
        print(f"  {DIM}(simulated: amount ${action['amount']:,} too high){RESET}")
        return {"rejected": rejected}
    else:
        approved = list(state.get("approved", []))
        approved.append(desc)
        print(f"  {YELLOW}APPROVED by reviewer{RESET}: {desc}")
        print(f"  {DIM}(simulated: amount ${action['amount']:,} within limit){RESET}")
        return {"approved": approved}


async def check_more_actions(state: dict[str, Any]) -> str:
    """Check if there are more actions to process."""
    idx = state.get("action_index", 0)
    total = len(state.get("actions", []))
    if idx < total:
        return "next"
    return "done"


# ── Main ─────────────────────────────────────────────────────────────


async def run():
    header("Human-in-the-loop with policy rules", 3)

    async with InputLayer(
        os.environ.get("INPUTLAYER_URL", "ws://localhost:8080/ws"),
        username=os.environ.get("INPUTLAYER_USER", "admin"),
        password=os.environ.get("INPUTLAYER_PASSWORD", "admin"),
    ) as il:
        with contextlib.suppress(Exception):
            await il.drop_knowledge_graph("lg_hitl")
        kg = il.knowledge_graph("lg_hitl")
        try:
            # ── Setup policy rules ───────────────────────────────────────

            await kg.execute(
                "+pending_action(action_type: string, description: string, amount: int, target: string)"
            )
            await kg.execute(
                "+risk_flag(action_type: string, amount: int, "
                "target: string, level: string, reason: string)"
            )
            await kg.execute("+approved_vendor(name: string)")
            await kg.execute("+spending_limit(action_type: string, max_amount: int)")

            # Policy data
            await kg.execute(
                '+approved_vendor[("Acme Corp", ), ("CloudHost Inc", ), ("DataPipe LLC", )]'
            )
            await kg.execute(
                '+spending_limit[("purchase", 10000), ("subscription", 5000), ("transfer", 25000)]'
            )

            # Rule: over spending limit -> high risk
            await kg.execute(
                '+risk_flag(Type, Amount, Target, "high", "Over spending limit") <- '
                "pending_action(Type, Desc, Amount, Target), "
                "spending_limit(Type, MaxAmt), Amount > MaxAmt"
            )

            step(1, "Policy rules defined")
            print(f"{DIM}  spending_limit: purchase=$10K, subscription=$5K, transfer=$25K{RESET}")
            print(f"{DIM}  risk_flag: amount > limit -> high risk -> needs approval{RESET}")

            # ── Define actions to process ────────────────────────────────

            actions = [
                {
                    "type": "purchase",
                    "description": "Office supplies",
                    "amount": 500,
                    "target": "Acme Corp",
                },
                {
                    "type": "purchase",
                    "description": "New server hardware",
                    "amount": 25000,
                    "target": "CloudHost Inc",
                },
                {
                    "type": "subscription",
                    "description": "Annual SaaS license",
                    "amount": 3000,
                    "target": "DataPipe LLC",
                },
                {
                    "type": "transfer",
                    "description": "Vendor payment Q1",
                    "amount": 75000,
                    "target": "External LLC",
                },
                {
                    "type": "subscription",
                    "description": "Premium API access",
                    "amount": 8000,
                    "target": "AI Service Co",
                },
            ]

            # ── Build graph ──────────────────────────────────────────────

            step(2, "Build the approval workflow graph")
            print(
                f"{DIM}  pick_action -> classify -> [safe: execute | risky: review] -> loop{RESET}"
            )

            graph = StateGraph(ActionState)
            graph.add_node("pick", pick_next_action)
            graph.add_node("execute", auto_execute)
            graph.add_node("review", request_approval)

            graph.set_entry_point("pick")
            graph.add_conditional_edges(
                "pick",
                classify_action,
                {
                    "auto_approve": "execute",
                    "needs_approval": "review",
                    "done": END,
                },
            )
            graph.add_conditional_edges(
                "execute",
                check_more_actions,
                {"next": "pick", "done": END},
            )
            graph.add_conditional_edges(
                "review",
                check_more_actions,
                {"next": "pick", "done": END},
            )

            app = graph.compile()

            # ── Run ──────────────────────────────────────────────────────

            step(3, f"Process {len(actions)} actions")

            result = await app.ainvoke(
                {
                    "kg": kg,
                    "actions": actions,
                    "current_action": {},
                    "approved": [],
                    "rejected": [],
                    "executed": [],
                    "action_index": 0,
                }
            )

            # ── Summary ──────────────────────────────────────────────────

            step(4, "Summary")
            print(f"\n  {GREEN}Auto-executed ({len(result['executed'])}):{RESET}")
            for a in result["executed"]:
                print(f"    {GREEN}ok{RESET} {a}")

            print(f"\n  {YELLOW}Approved after review ({len(result['approved'])}):{RESET}")
            for a in result["approved"]:
                print(f"    {YELLOW}ok{RESET} {a}")

            print(f"\n  {RED}Rejected ({len(result['rejected'])}):{RESET}")
            for a in result["rejected"]:
                print(f"    {RED}x{RESET} {a}")

            success("Done!")
        finally:
            with contextlib.suppress(Exception):
                await il.drop_knowledge_graph("lg_hitl")


if __name__ == "__main__":
    asyncio.run(run())
