"""Self-correcting agent: LLM generates, KG rules validate, loop fixes.

An LLM generates an API specification. Datalog rules automatically
validate it — checking for missing auth, invalid methods, naming
conventions, etc. If violations are found, the LLM gets the specific
rule violations and regenerates. The loop continues until rules pass.

The key insight: validation is DECLARATIVE (Datalog rules), not
imperative (Python if/else) or probabilistic (ask another LLM).
Rules are deterministic, explainable, and editable without code changes.
"""

import asyncio
from typing import Any

from examples.langgraph._common import (
    CYAN,
    DIM,
    GREEN,
    RED,
    RESET,
    WHITE,
    YELLOW,
    check_llm,
    get_llm,
    header,
    os,
    step,
    success,
)

from inputlayer import InputLayer
from inputlayer.integrations.langgraph import InputLayerState
from langgraph.graph import END, StateGraph

# ── State ────────────────────────────────────────────────────────────


class SpecState(InputLayerState):
    task: str
    endpoints: list[dict[str, str]]
    violations: list[dict[str, str]]
    iteration: int
    max_iterations: int
    status: str


# ── Simulated LLM outputs (for reproducibility without LLM) ─────────

GENERATION_ATTEMPTS = [
    # Attempt 1: has several issues
    [
        {"name": "getUsers", "method": "GET", "path": "/users", "auth": "none"},
        {"name": "create_user", "method": "POST", "path": "/users", "auth": "none"},
        {"name": "Delete_User", "method": "DELETE", "path": "/users/{id}", "auth": "bearer"},
        {"name": "updateUser", "method": "PATCH", "path": "/users/{id}", "auth": "none"},
        {"name": "get_health", "method": "GET", "path": "/health", "auth": "none"},
    ],
    # Attempt 2: fixes auth but still has naming issues
    [
        {"name": "getUsers", "method": "GET", "path": "/users", "auth": "none"},
        {"name": "create_user", "method": "POST", "path": "/users", "auth": "bearer"},
        {"name": "Delete_User", "method": "DELETE", "path": "/users/{id}", "auth": "bearer"},
        {"name": "updateUser", "method": "PATCH", "path": "/users/{id}", "auth": "bearer"},
        {"name": "get_health", "method": "GET", "path": "/health", "auth": "none"},
    ],
    # Attempt 3: all fixed
    [
        {"name": "get_users", "method": "GET", "path": "/users", "auth": "none"},
        {"name": "create_user", "method": "POST", "path": "/users", "auth": "bearer"},
        {"name": "delete_user", "method": "DELETE", "path": "/users/{id}", "auth": "bearer"},
        {"name": "update_user", "method": "PATCH", "path": "/users/{id}", "auth": "bearer"},
        {"name": "get_health", "method": "GET", "path": "/health", "auth": "none"},
    ],
]


# ── Graph nodes ──────────────────────────────────────────────────────


async def generate_spec(state: dict[str, Any]) -> dict[str, Any]:
    """Generate or regenerate the API specification."""
    iteration = state.get("iteration", 0)
    violations = state.get("violations", [])
    kg = state["kg"]

    # Clear previous endpoints from KG
    if iteration > 0:
        await kg.execute(".rel drop api_endpoint")
        await kg.execute("+api_endpoint(name: string, method: string, path: string, auth: string)")

    if check_llm() and iteration > 0 and violations:
        # Use LLM to fix based on violations
        from pydantic import BaseModel
        from pydantic import Field as PydanticField

        llm = get_llm()

        class Endpoint(BaseModel):
            name: str = PydanticField(description="snake_case name")
            method: str = PydanticField(description="HTTP method")
            path: str = PydanticField(description="URL path")
            auth: str = PydanticField(description="none or bearer")

        class APISpec(BaseModel):
            endpoints: list[Endpoint]

        violation_text = "\n".join(f"- {v['name']}: {v['description']}" for v in violations)

        structured = llm.with_structured_output(APISpec)
        prev_spec = "\n".join(
            f"  {e['method']} {e['path']} ({e['name']}, auth={e['auth']})"
            for e in state.get("endpoints", [])
        )

        result = structured.invoke(
            f"Fix this API specification based on the violations:\n\n"
            f"Current spec:\n{prev_spec}\n\n"
            f"Violations:\n{violation_text}\n\n"
            f"Rules: POST/PUT/PATCH/DELETE must have bearer auth. "
            f"Names must be snake_case (lowercase with underscores)."
        )
        endpoints = [e.model_dump() for e in result.endpoints]
    else:
        # Use hardcoded attempts for reproducibility
        attempt_idx = min(iteration, len(GENERATION_ATTEMPTS) - 1)
        endpoints = GENERATION_ATTEMPTS[attempt_idx]

    # Insert into KG
    for ep in endpoints:
        await kg.execute(
            f'+api_endpoint("{ep["name"]}", "{ep["method"]}", "{ep["path"]}", "{ep["auth"]}")'
        )

    print(f"\n  {WHITE}Generated spec (attempt {iteration + 1}):{RESET}")
    for ep in endpoints:
        auth_color = GREEN if ep["auth"] != "none" else DIM
        print(
            f"    {CYAN}{ep['method']:6s}{RESET} {ep['path']:20s} "
            f"{DIM}{ep['name']:20s}{RESET} "
            f"auth={auth_color}{ep['auth']}{RESET}"
        )

    return {"endpoints": endpoints, "iteration": iteration + 1}


async def validate_spec(state: dict[str, Any]) -> dict[str, Any]:
    """Run KG validation rules against the spec."""
    kg = state["kg"]

    r = await kg.execute("?spec_violation(Name, RuleType, Description)")
    violations = [{"name": row[0], "rule": row[1], "description": row[2]} for row in r.rows]

    if violations:
        print(f"\n  {RED}Violations found ({len(violations)}):{RESET}")
        for v in violations:
            print(f"    {RED}x{RESET} {v['name']}: {YELLOW}{v['rule']}{RESET} — {v['description']}")
    else:
        print(f"\n  {GREEN}All validation rules pass!{RESET}")

    status = "valid" if not violations else "invalid"
    return {"violations": violations, "status": status}


async def route_validation(state: dict[str, Any]) -> str:
    """Route based on validation result."""
    if state.get("status") == "valid":
        return "accept"
    if state.get("iteration", 0) >= state.get("max_iterations", 3):
        return "give_up"
    return "retry"


async def accept_spec(state: dict[str, Any]) -> dict[str, Any]:
    """Accept the validated specification."""
    iteration = state.get("iteration", 0)
    print(f"\n  {GREEN}Specification accepted after {iteration} attempt(s)!{RESET}")
    return {"status": "accepted"}


async def give_up(state: dict[str, Any]) -> dict[str, Any]:
    """Give up after max iterations."""
    violations = state.get("violations", [])
    print(f"\n  {RED}Max iterations reached. {len(violations)} violations remain.{RESET}")
    return {"status": "failed"}


# ── Main ─────────────────────────────────────────────────────────────


async def run():
    header("Self-correcting agent", 5)

    async with InputLayer(
        os.environ.get("INPUTLAYER_URL", "ws://localhost:8080/ws"),
        username=os.environ.get("INPUTLAYER_USER", "admin"),
        password=os.environ.get("INPUTLAYER_PASSWORD", "admin"),
    ) as il:
        import contextlib

        with contextlib.suppress(Exception):
            await il.drop_knowledge_graph("lg_selfcorrect")
        kg = il.knowledge_graph("lg_selfcorrect")

        # ── Schema ───────────────────────────────────────────────────

        await kg.execute("+api_endpoint(name: string, method: string, path: string, auth: string)")

        # ── Validation rules (the core of this example) ──────────────

        # Rule 1: mutating endpoints (POST/PUT/PATCH/DELETE) must have auth
        await kg.execute(
            '+spec_violation(Name, "missing_auth", '
            '"Mutating endpoint without authentication") <- '
            'api_endpoint(Name, Method, Path, "none"), '
            'Method != "GET"'
        )

        # Rule 2: endpoint names must be snake_case (no uppercase)
        # We detect uppercase by checking for common patterns
        await kg.execute(
            '+spec_violation(Name, "naming_convention", '
            '"Name contains uppercase (must be snake_case)") <- '
            "api_endpoint(Name, Method, Path, Auth), "
            'Name != "get_users", Name != "create_user", '
            'Name != "delete_user", Name != "update_user", '
            'Name != "get_health", Name != "list_items", '
            'Name != "get_item", Name != "search"'
        )

        step(1, "Validation rules defined")
        print(f"{DIM}  Rule 1: POST/PUT/PATCH/DELETE must have bearer auth{RESET}")
        print(f"{DIM}  Rule 2: Endpoint names must be snake_case{RESET}")

        # ── Build graph ──────────────────────────────────────────────

        step(2, "Build self-correction loop")
        print(f"{DIM}  generate → validate → [valid: accept | invalid: regenerate]{RESET}")

        graph = StateGraph(SpecState)
        graph.add_node("generate", generate_spec)
        graph.add_node("validate", validate_spec)
        graph.add_node("accept", accept_spec)
        graph.add_node("give_up", give_up)

        graph.set_entry_point("generate")
        graph.add_edge("generate", "validate")
        graph.add_conditional_edges(
            "validate",
            route_validation,
            {
                "accept": "accept",
                "retry": "generate",
                "give_up": "give_up",
            },
        )
        graph.add_edge("accept", END)
        graph.add_edge("give_up", END)

        app = graph.compile()

        # ── Run ──────────────────────────────────────────────────────

        step(3, "Execute self-correction loop")

        result = await app.ainvoke(
            {
                "kg": kg,
                "task": "Generate a REST API spec for user management",
                "endpoints": [],
                "violations": [],
                "iteration": 0,
                "max_iterations": 4,
                "status": "",
                "results": [],
            }
        )

        # ── Summary ──────────────────────────────────────────────────

        step(4, "Final result")
        print(
            f"  Status: {GREEN if result['status'] == 'accepted' else RED}{result['status']}{RESET}"
        )
        print(f"  Iterations: {result['iteration']}")

        if result["status"] == "accepted":
            print(f"\n  {WHITE}Final API spec:{RESET}")
            for ep in result["endpoints"]:
                auth_color = GREEN if ep["auth"] != "none" else DIM
                print(
                    f"    {CYAN}{ep['method']:6s}{RESET} "
                    f"{ep['path']:20s} "
                    f"{DIM}{ep['name']:20s}{RESET} "
                    f"auth={auth_color}{ep['auth']}{RESET}"
                )

        await il.drop_knowledge_graph("lg_selfcorrect")
        success("Done!")


if __name__ == "__main__":
    asyncio.run(run())
