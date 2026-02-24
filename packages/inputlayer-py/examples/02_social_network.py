"""Social network: graph traversal, transitive closure, mutual follows."""

import asyncio
from typing import ClassVar

from inputlayer import Derived, From, InputLayer, Relation


class Follow(Relation):
    follower: int
    followee: int


class Reachable(Derived):
    src: int
    dst: int

    rules: ClassVar[list] = []


# Base case: direct follows
# Recursive case: transitivity
Reachable.rules = [
    From(Follow).select(src=Follow.follower, dst=Follow.followee),
    From(Reachable, Follow)
    .where(lambda r, f: r.dst == f.follower)
    .select(src=Reachable.src, dst=Follow.followee),
]


async def main():
    async with InputLayer("ws://localhost:8080/ws", username="admin", password="admin") as il:
        kg = il.knowledge_graph("social")

        await kg.define(Follow)
        await kg.insert([
            Follow(follower=1, followee=2),
            Follow(follower=2, followee=3),
            Follow(follower=3, followee=4),
            Follow(follower=2, followee=1),  # mutual follow
        ])

        # Define transitive closure
        await kg.define_rules(Reachable)

        # Who can user 1 reach?
        result = await kg.query(Reachable, where=lambda r: r.src == 1)
        print("User 1 can reach:")
        for r in result:
            print(f"  -> User {r.Dst}")

        await il.drop_knowledge_graph("social")


if __name__ == "__main__":
    asyncio.run(main())
