"""Access control: user/ACL management."""

import asyncio

from inputlayer import InputLayer


async def main():
    async with InputLayer("ws://localhost:8080/ws", username="admin", password="admin") as il:
        # List users
        users = await il.list_users()
        print("Current users:")
        for u in users:
            print(f"  {u.username} ({u.role})")

        # Create a new user
        await il.create_user("analyst", "pass123", role="viewer")
        print("\nCreated user 'analyst' with role 'viewer'")

        # Create API key
        key = await il.create_api_key("analyst_key")
        print(f"Created API key: {key}")

        # Grant per-KG access
        kg = il.knowledge_graph("default")
        await kg.grant_access("analyst", "reader")
        print("Granted 'reader' access to 'analyst' on 'default' KG")

        # List ACL
        acl = await kg.list_acl()
        print("\nACL for 'default':")
        for entry in acl:
            print(f"  {entry.username}: {entry.role}")

        # Cleanup
        await kg.revoke_access("analyst")
        await il.revoke_api_key("analyst_key")
        await il.drop_user("analyst")
        print("\nCleaned up")


if __name__ == "__main__":
    asyncio.run(main())
