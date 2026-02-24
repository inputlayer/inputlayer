"""Realtime dashboard: notifications + aggregation."""

import asyncio

from inputlayer import InputLayer, Relation, count, sum_


class SensorReading(Relation):
    sensor_id: int
    temperature: float
    humidity: float


async def main():
    async with InputLayer("ws://localhost:8080/ws", username="admin", password="admin") as il:
        kg = il.knowledge_graph("dashboard")

        # Register notification handler
        @il.on("persistent_update", relation="sensor_reading")
        def on_sensor_update(event):
            print(f"[notification] {event.count} new readings inserted")

        await kg.define(SensorReading)

        # Insert sensor data
        await kg.insert([
            SensorReading(sensor_id=1, temperature=22.5, humidity=45.0),
            SensorReading(sensor_id=2, temperature=23.1, humidity=50.2),
            SensorReading(sensor_id=1, temperature=22.8, humidity=44.5),
        ])

        # Aggregate
        result = await kg.query(
            SensorReading.sensor_id,
            count(SensorReading.temperature),
            join=[SensorReading],
        )
        print("Readings per sensor:")
        for row in result:
            print(f"  {row}")

        await il.drop_knowledge_graph("dashboard")


if __name__ == "__main__":
    asyncio.run(main())
