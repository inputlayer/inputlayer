"""Session rules: ad-hoc ephemeral views."""

import asyncio
from typing import ClassVar

from inputlayer import Derived, From, InputLayer, Relation


class Employee(Relation):
    id: int
    name: str
    department: str
    salary: float
    active: bool


class ActiveEngineer(Derived):
    id: int
    name: str
    salary: float

    rules: ClassVar[list] = []


ActiveEngineer.rules = [
    From(Employee)
    .where(lambda e: (e.department == "eng") & (e.active == True))  # noqa
    .select(id=Employee.id, name=Employee.name, salary=Employee.salary),
]


async def main():
    async with InputLayer("ws://localhost:8080/ws", username="admin", password="admin") as il:
        kg = il.knowledge_graph("session_demo")

        await kg.define(Employee)
        await kg.insert([
            Employee(id=1, name="Alice", department="eng", salary=120000.0, active=True),
            Employee(id=2, name="Bob", department="hr", salary=90000.0, active=True),
            Employee(id=3, name="Charlie", department="eng", salary=110000.0, active=False),
        ])

        # Session rule - ephemeral, cleared on disconnect
        await kg.session.define_rules(ActiveEngineer)

        result = await kg.query(ActiveEngineer, join=[ActiveEngineer])
        print("Active engineers (session view):")
        for eng in result:
            print(f"  {eng}")

        # Clear session
        await kg.session.clear()

        await il.drop_knowledge_graph("session_demo")


if __name__ == "__main__":
    asyncio.run(main())
