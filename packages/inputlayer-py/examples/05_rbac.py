"""RBAC: transitive role inheritance."""

import asyncio
from typing import ClassVar

from inputlayer import Derived, From, InputLayer, Relation


class RoleHierarchy(Relation):
    parent_role: str
    child_role: str


class UserRole(Relation):
    user_id: int
    role: str


class EffectiveRole(Derived):
    user_id: int
    role: str

    rules: ClassVar[list] = []


# Direct role assignment
# Inherited via hierarchy
EffectiveRole.rules = [
    From(UserRole).select(user_id=UserRole.user_id, role=UserRole.role),
    From(EffectiveRole, RoleHierarchy)
    .where(lambda er, rh: er.role == rh.parent_role)
    .select(user_id=EffectiveRole.user_id, role=RoleHierarchy.child_role),
]


async def main():
    async with InputLayer("ws://localhost:8080/ws", username="admin", password="admin") as il:
        kg = il.knowledge_graph("rbac")

        await kg.define(RoleHierarchy, UserRole)
        await kg.insert([
            RoleHierarchy(parent_role="admin", child_role="editor"),
            RoleHierarchy(parent_role="editor", child_role="viewer"),
        ])
        await kg.insert([
            UserRole(user_id=1, role="admin"),
            UserRole(user_id=2, role="editor"),
        ])

        await kg.define_rules(EffectiveRole)

        result = await kg.query(EffectiveRole, where=lambda er: er.user_id == 1)
        print("User 1 effective roles:")
        for r in result:
            print(f"  {r.Role}")

        await il.drop_knowledge_graph("rbac")


if __name__ == "__main__":
    asyncio.run(main())
