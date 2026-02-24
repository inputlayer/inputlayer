"""E-commerce: collaborative filtering, revenue aggregation."""

import asyncio

from inputlayer import InputLayer, Relation, count, sum_


class Purchase(Relation):
    user_id: int
    product_id: int
    amount: float


async def main():
    async with InputLayer("ws://localhost:8080/ws", username="admin", password="admin") as il:
        kg = il.knowledge_graph("ecommerce")

        await kg.define(Purchase)
        await kg.insert([
            Purchase(user_id=1, product_id=101, amount=29.99),
            Purchase(user_id=1, product_id=102, amount=49.99),
            Purchase(user_id=2, product_id=101, amount=29.99),
            Purchase(user_id=2, product_id=103, amount=19.99),
            Purchase(user_id=3, product_id=102, amount=49.99),
        ])

        # Total revenue per product
        result = await kg.query(
            Purchase.product_id,
            sum_(Purchase.amount),
            count(Purchase.user_id),
            join=[Purchase],
        )
        print("Revenue by product:")
        for row in result:
            print(f"  Product {row}")

        await il.drop_knowledge_graph("ecommerce")


if __name__ == "__main__":
    asyncio.run(main())
