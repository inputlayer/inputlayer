"""Collaborative planning: multiple experts contribute, rules detect conflicts.

Three expert nodes (engineer, product, security) each propose steps for
a project plan. Rules detect conflicts between steps (dependency
violations, resource conflicts, scheduling issues). The router blocks
until all conflicts are resolved.

Shows the KG as a coordination layer between agents. They don't talk
to each other directly, they write facts and rules reconcile them.
"""

import asyncio
import contextlib
from typing import Any

from examples.langgraph._common import (
    CYAN,
    DIM,
    GREEN,
    MAGENTA,
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


class PlanState(InputLayerState):
    project: str
    phase: str
    conflicts: list[dict[str, str]]
    iteration: int


# ── Expert proposals ─────────────────────────────────────────────────

ENGINEER_STEPS = [
    ("setup_infra", 1, 2, "engineering", "none", "Provision cloud infrastructure"),
    ("build_api", 3, 5, "engineering", "setup_infra", "Build REST API and database"),
    ("build_frontend", 3, 6, "engineering", "setup_infra", "Build React frontend"),
    ("integration_test", 7, 8, "engineering", "build_api", "Run integration tests"),
    ("deploy_staging", 9, 9, "engineering", "integration_test", "Deploy to staging"),
]

PRODUCT_STEPS = [
    ("user_research", 1, 2, "product", "none", "Conduct user interviews"),
    ("write_specs", 2, 3, "product", "user_research", "Write product specs"),
    ("design_review", 4, 4, "product", "write_specs", "Design review with stakeholders"),
    ("build_api", 5, 7, "product", "design_review", "Build API (product timeline)"),
    ("user_testing", 8, 9, "product", "build_frontend", "User acceptance testing"),
]

SECURITY_STEPS = [
    ("threat_model", 1, 1, "security", "none", "Threat modeling session"),
    ("auth_design", 2, 3, "security", "threat_model", "Design auth system"),
    ("build_api", 4, 6, "security", "auth_design", "Build API (security timeline)"),
    ("security_audit", 7, 8, "security", "build_api", "Security audit and pen test"),
    ("deploy_staging", 9, 9, "security", "security_audit", "Deploy after audit"),
]


# ── Graph nodes ──────────────────────────────────────────────────────


async def engineering_expert(state: dict[str, Any]) -> dict[str, Any]:
    """Engineering team proposes their plan steps."""
    kg = state["kg"]
    print(f"\n  {CYAN}Engineering team proposing...{RESET}")

    for name, start, end, team, dep, desc in ENGINEER_STEPS:
        await kg.execute(
            f'+plan_step("{escape_iql(name)}", {start}, {end}, "{escape_iql(team)}", "{escape_iql(dep)}", "{escape_iql(desc)}")'
        )
        print(f"    {CYAN}+{RESET} {name} (week {start}-{end})")

    return {"phase": "proposals_in"}


async def product_expert(state: dict[str, Any]) -> dict[str, Any]:
    """Product team proposes their plan steps."""
    kg = state["kg"]
    print(f"\n  {MAGENTA}Product team proposing...{RESET}")

    for name, start, end, team, dep, desc in PRODUCT_STEPS:
        await kg.execute(
            f'+plan_step("{escape_iql(name)}", {start}, {end}, "{escape_iql(team)}", "{escape_iql(dep)}", "{escape_iql(desc)}")'
        )
        print(f"    {MAGENTA}+{RESET} {name} (week {start}-{end})")

    return {"phase": "proposals_in"}


async def security_expert(state: dict[str, Any]) -> dict[str, Any]:
    """Security team proposes their plan steps."""
    kg = state["kg"]
    print(f"\n  {YELLOW}Security team proposing...{RESET}")

    for name, start, end, team, dep, desc in SECURITY_STEPS:
        await kg.execute(
            f'+plan_step("{escape_iql(name)}", {start}, {end}, "{escape_iql(team)}", "{escape_iql(dep)}", "{escape_iql(desc)}")'
        )
        print(f"    {YELLOW}+{RESET} {name} (week {start}-{end})")

    return {"phase": "proposals_in"}


async def detect_conflicts(state: dict[str, Any]) -> dict[str, Any]:
    """Check for conflicts detected by rules."""
    kg = state["kg"]

    r = await kg.execute("?schedule_conflict(Step, TeamA, StartA, TeamB, StartB)")
    conflicts = []
    seen = set()
    for row in r.rows:
        key = tuple(sorted([str(row[1]) + str(row[2]), str(row[3]) + str(row[4])]))
        if key not in seen:
            seen.add(key)
            conflicts.append(
                {
                    "step": row[0],
                    "detail": f"{row[1]} says week {row[2]}, {row[3]} says week {row[4]}",
                }
            )

    r = await kg.execute("?dependency_issue(Step, DepStep, StepStart, DepEnd)")
    dep_issues = []
    for row in r.rows:
        dep_issues.append(
            {
                "step": row[0],
                "detail": f"starts week {row[2]} but depends on {row[1]} which ends week {row[3]}",
            }
        )

    all_conflicts = conflicts + dep_issues

    if all_conflicts:
        print(f"\n  {RED}Conflicts detected ({len(all_conflicts)}):{RESET}")
        for c in all_conflicts:
            print(f"    {RED}!{RESET} {c['step']}: {c['detail']}")
    else:
        print(f"\n  {GREEN}No conflicts, plan is consistent!{RESET}")

    return {"conflicts": all_conflicts}


async def resolve_conflicts(state: dict[str, Any]) -> dict[str, Any]:
    """Resolve conflicts by adjusting the plan."""
    kg = state["kg"]
    conflicts = state.get("conflicts", [])
    iteration = state.get("iteration", 0)

    print(f"\n  {WHITE}Resolving {len(conflicts)} conflict(s)...{RESET}")

    # Fix dependency issues: push steps to start after their dependency ends
    r = await kg.execute("?dependency_issue(Step, DepStep, StepStart, DepEnd)")
    for row in r.rows:
        step_name, dep_step, step_start, dep_end = row[0], row[1], row[2], row[3]
        new_start = dep_end + 1

        # Find the current plan_step to get its full details for retraction
        r2 = await kg.execute(
            f'?plan_step("{escape_iql(step_name)}", {step_start}, End, Team, "{escape_iql(dep_step)}", Desc)'
        )
        for ps in r2.rows:
            end, team, desc = ps[2], ps[3], ps[5]
            duration = end - step_start
            new_end = new_start + duration

            # Retract the old step and insert an adjusted one
            await kg.execute(
                f'-plan_step("{escape_iql(step_name)}", {step_start}, {end}, '
                f'"{escape_iql(team)}", "{escape_iql(dep_step)}", "{escape_iql(desc)}")'
            )
            await kg.execute(
                f'+plan_step("{escape_iql(step_name)}", {new_start}, {new_end}, '
                f'"{escape_iql(team)}", "{escape_iql(dep_step)}", "{escape_iql(desc)}")'
            )
            print(
                f"    {GREEN}Fixed{RESET}: {step_name} ({team}) moved from week {step_start} to {new_start}"
            )

    # Note schedule conflicts (different teams proposing different timelines)
    for c in conflicts:
        if "depends on" not in c["detail"]:
            print(f"    {DIM}Noted: {c['step']}: {c['detail']}{RESET}")

    return {"iteration": iteration + 1}


async def route_after_conflict_check(state: dict[str, Any]) -> str:
    """Route based on conflict detection."""
    conflicts = state.get("conflicts", [])
    iteration = state.get("iteration", 0)
    if not conflicts:
        return "finalize"
    if iteration >= 2:
        return "finalize"  # Accept with noted conflicts
    return "resolve"


async def finalize_plan(state: dict[str, Any]) -> dict[str, Any]:
    """Produce the final unified plan."""
    kg = state["kg"]

    r = await kg.execute("?plan_step(Name, Start, End, Team, Dep, Desc)")

    # Deduplicate: group by step name, pick consensus
    steps: dict[str, list] = {}
    for row in r.rows:
        name = row[0]
        steps.setdefault(name, []).append(row)

    team_colors = {
        "engineering": CYAN,
        "product": MAGENTA,
        "security": YELLOW,
    }

    print(f"\n  {WHITE}Unified project plan:{RESET}")
    print(f"  {'─' * 55}")

    for name in sorted(steps.keys(), key=lambda n: min(s[1] for s in steps[n])):
        proposals = steps[name]
        # Show all team proposals for this step
        teams = sorted(set(p[3] for p in proposals))
        starts = sorted(set(p[1] for p in proposals))
        ends = sorted(set(p[2] for p in proposals))
        desc = proposals[0][5]

        team_str = ", ".join(f"{team_colors.get(t, DIM)}{t}{RESET}" for t in teams)

        if len(starts) > 1:
            timing = f"week {min(starts)}-{max(ends)} {YELLOW}(disputed){RESET}"
        else:
            timing = f"week {starts[0]}-{ends[0]}"

        print(f"    {GREEN}{name:20s}{RESET} {timing:30s} [{team_str}]")
        print(f"    {DIM}{desc}{RESET}")

    conflicts = state.get("conflicts", [])
    if conflicts:
        print(f"\n  {YELLOW}Note: {len(conflicts)} scheduling conflicts remain{RESET}")

    return {"phase": "complete"}


# ── Main ─────────────────────────────────────────────────────────────


async def run():
    header("Collaborative planning", 6)

    async with InputLayer(
        os.environ.get("INPUTLAYER_URL", "ws://localhost:8080/ws"),
        username=os.environ.get("INPUTLAYER_USER", "admin"),
        password=os.environ.get("INPUTLAYER_PASSWORD", "admin"),
    ) as il:
        with contextlib.suppress(Exception):
            await il.drop_knowledge_graph("lg_planning")
        kg = il.knowledge_graph("lg_planning")
        try:
            # ── Schema ───────────────────────────────────────────────────

            await kg.execute(
                "+plan_step(name: string, start_week: int, end_week: int, "
                "team: string, depends_on: string, description: string)"
            )

            # ── Conflict detection rules ─────────────────────────────────

            # Rule: same step proposed with different timelines
            await kg.execute(
                "+schedule_conflict(Name, TeamA, StartA, TeamB, StartB) <- "
                "plan_step(Name, StartA, EndA, TeamA, DepA, DescA), "
                "plan_step(Name, StartB, EndB, TeamB, DepB, DescB), "
                "TeamA != TeamB, StartA != StartB"
            )

            # Rule: step starts before its dependency ends
            await kg.execute(
                "+dependency_issue(Step, DepStep, StepStart, DepEnd) <- "
                "plan_step(Step, StepStart, StepEnd, Team, DepStep, Desc), "
                "plan_step(DepStep, DepStart, DepEnd, DepTeam, DepDep, DepDesc), "
                "StepStart <= DepEnd, "
                'DepStep != "none"'
            )

            step(1, "Three expert teams will propose plan steps")
            print(f"  {CYAN}Engineering{RESET} | {MAGENTA}Product{RESET} | {YELLOW}Security{RESET}")
            print(f"{DIM}  Rules detect: schedule conflicts, dependency violations{RESET}")

            # ── Build graph ──────────────────────────────────────────────

            step(2, "Build collaborative planning graph")
            print(
                f"{DIM}  [eng + product + security] -> detect_conflicts -> [resolve or finalize]{RESET}"
            )

            graph = StateGraph(PlanState)
            graph.add_node("engineering", engineering_expert)
            graph.add_node("product", product_expert)
            graph.add_node("security", security_expert)
            graph.add_node("detect", detect_conflicts)
            graph.add_node("resolve", resolve_conflicts)
            graph.add_node("finalize", finalize_plan)

            # All experts run, then detect conflicts
            graph.set_entry_point("engineering")
            graph.add_edge("engineering", "product")
            graph.add_edge("product", "security")
            graph.add_edge("security", "detect")

            graph.add_conditional_edges(
                "detect",
                route_after_conflict_check,
                {"resolve": "resolve", "finalize": "finalize"},
            )
            graph.add_edge("resolve", "detect")
            graph.add_edge("finalize", END)

            app = graph.compile()

            # ── Run ──────────────────────────────────────────────────────

            step(3, "Execute planning session")

            await app.ainvoke(
                {
                    "kg": kg,
                    "project": "New product launch",
                    "phase": "start",
                    "conflicts": [],
                    "iteration": 0,
                }
            )

            success("Done!")
        finally:
            with contextlib.suppress(Exception):
                await il.drop_knowledge_graph("lg_planning")


if __name__ == "__main__":
    asyncio.run(run())
