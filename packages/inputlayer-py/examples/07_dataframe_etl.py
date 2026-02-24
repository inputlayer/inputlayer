"""DataFrame ETL: load from pandas, export back to pandas."""

import asyncio

import pandas as pd

from inputlayer import InputLayer, Relation


class Product(Relation):
    id: int
    name: str
    price: float
    category: str


async def main():
    async with InputLayer("ws://localhost:8080/ws", username="admin", password="admin") as il:
        kg = il.knowledge_graph("etl")

        await kg.define(Product)

        # Load from DataFrame
        df = pd.DataFrame({
            "id": [1, 2, 3, 4],
            "name": ["Widget", "Gadget", "Doohickey", "Thingamajig"],
            "price": [9.99, 19.99, 5.99, 14.99],
            "category": ["tools", "electronics", "tools", "electronics"],
        })
        await kg.insert(Product, data=df)

        # Query and export back to DataFrame
        result = await kg.query(Product, where=lambda p: p.category == "tools")
        export_df = result.to_df()
        print("Tools:")
        print(export_df)

        await il.drop_knowledge_graph("etl")


if __name__ == "__main__":
    asyncio.run(main())
