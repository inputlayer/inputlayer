"""Quickstart: connect, define, insert, query."""

import asyncio
from inputlayer import InputLayer, Relation


class Employee(Relation):
    id: int
    name: str
    department: str
    salary: float
    active: bool


async def main():
    async with InputLayer("ws://localhost:8080/ws", username="admin", password="admin") as il:
        kg = il.knowledge_graph("quickstart")

        # Define schema
        await kg.define(Employee)

        # Insert data
        await kg.insert([
            Employee(id=1, name="Alice", department="eng", salary=120000.0, active=True),
            Employee(id=2, name="Bob", department="hr", salary=90000.0, active=True),
            Employee(id=3, name="Charlie", department="eng", salary=110000.0, active=False),
        ])

        # Query all employees
        result = await kg.query(Employee)
        for emp in result:
            print(f"{emp.Name}: ${emp.Salary}")

        # Query with filter
        engineers = await kg.query(
            Employee,
            where=lambda e: (e.department == "eng") & (e.active == True),  # noqa
        )
        print(f"\nActive engineers: {len(engineers)}")

        # Cleanup
        await il.drop_knowledge_graph("quickstart")


if __name__ == "__main__":
    asyncio.run(main())
