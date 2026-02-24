"""Shared test fixtures for integration tests."""

from __future__ import annotations

import asyncio
import os
import subprocess
import time
from typing import AsyncIterator

import pytest
import pytest_asyncio

from inputlayer.client import InputLayer


# Integration tests are only run when INPUTLAYER_TEST_SERVER is set
SKIP_INTEGRATION = not os.environ.get("INPUTLAYER_TEST_SERVER")


@pytest.fixture(scope="session")
def server_url() -> str:
    """Get the server URL from environment or default."""
    return os.environ.get("INPUTLAYER_TEST_SERVER", "ws://localhost:8080/ws")


@pytest_asyncio.fixture
async def client(server_url: str) -> AsyncIterator[InputLayer]:
    """Create and connect an InputLayer client."""
    username = os.environ.get("INPUTLAYER_TEST_USER", "admin")
    password = os.environ.get("INPUTLAYER_TEST_PASSWORD", "admin")

    il = InputLayer(
        server_url,
        username=username,
        password=password,
    )
    await il.connect()
    yield il
    await il.close()
