from __future__ import annotations

import asyncio
import json
import os
import secrets
import socket
from collections.abc import AsyncIterator
from pathlib import Path

import aiohttp
import pytest
from aiohttp import web

from ingest import iql
from ingest.applier import Applier, Outcome
from ingest.changes import FactChange, Relation
from ingest.config import EngineSettings
from ingest.engine import EngineSession, EngineUnavailable
from ingest.mappings import CUSTOMER, PAYMENT_ISSUE, RELATIONS


@pytest.fixture
async def real_engine(tmp_path: Path) -> AsyncIterator[EngineSettings]:
    default = Path(__file__).resolve().parents[4] / "target/debug/inputlayer-server"
    binary = Path(os.environ.get("INGEST_TEST_ENGINE", str(default)))
    if not binary.is_file():
        pytest.skip("build inputlayer-server or set INGEST_TEST_ENGINE to run engine regressions")
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    key = secrets.token_hex(32)
    config = tmp_path / "engine.toml"
    config.write_text(
        '[storage]\ndata_dir = "data"\n'
        "[storage.performance]\nnum_threads = 2\n"
        f'[http]\nenabled = true\nhost = "127.0.0.1"\nport = {port}\n'
        '[http.auth]\nbootstrap_admin_password = "regression-only"\n'
        'credentials_file = "credentials.toml"\n'
        "[http.rate_limit]\nws_max_messages_per_sec = 0\nper_ip_max_rps = 0\n"
        '[logging]\nlevel = "warn"\n'
    )
    env = {k: v for k, v in os.environ.items() if not k.startswith("INPUTLAYER_")}
    env.update(INPUTLAYER_BOOTSTRAP_API_KEY=key, TOKIO_WORKER_THREADS="2")
    with (tmp_path / "engine.log").open("w") as log:
        process = await asyncio.create_subprocess_exec(
            str(binary),
            "--config",
            str(config),
            cwd=tmp_path,
            env=env,
            stdout=log,
            stderr=log,
        )
        try:
            async with aiohttp.ClientSession() as http:
                async with asyncio.timeout(60):
                    while True:
                        assert process.returncode is None, (tmp_path / "engine.log").read_text()
                        try:
                            async with http.get(f"http://127.0.0.1:{port}/health") as response:
                                if response.status == 200:
                                    break
                        except aiohttp.ClientError:
                            pass
                        await asyncio.sleep(0.05)
            yield EngineSettings(f"ws://127.0.0.1:{port}/ws", key, "default")
        finally:
            if process.returncode is None:
                process.terminate()
            await process.wait()


@pytest.mark.parametrize(
    ("relation", "key", "old_row", "new_row"),
    [
        (CUSTOMER, (1,), (1, "Old", "free"), (1, "New", "enterprise")),
        (PAYMENT_ISSUE, ("inv_1",), ("inv_1", 1, 10), ("inv_1", 1, 20)),
    ],
)
@pytest.mark.parametrize(
    "old_delete,new_delete", [(False, False), (False, True), (True, False), (True, True)]
)
@pytest.mark.parametrize("seeded", [False, True])
@pytest.mark.parametrize("disconnected_revision", [10, 12])
async def test_disconnected_write_preserves_revision_order_and_committed_counts(
    real_engine: EngineSettings,
    relation: Relation,
    key: tuple,
    old_row: tuple,
    new_row: tuple,
    old_delete: bool,
    new_delete: bool,
    seeded: bool,
    disconnected_revision: int,
    caplog: pytest.LogCaptureFixture,
) -> None:
    release = asyncio.Event()
    completed: asyncio.Future[dict] = asyncio.get_running_loop().create_future()
    pause_write = False
    resume_on_next_write = False
    async with aiohttp.ClientSession() as http:

        async def proxy(request: web.Request) -> web.WebSocketResponse:
            nonlocal pause_write, resume_on_next_write
            downstream = web.WebSocketResponse()
            await downstream.prepare(request)
            async with http.ws_connect(
                f"{real_engine.url}?kg={real_engine.knowledge_graph}"
            ) as upstream:
                async for msg in downstream:
                    frame = json.loads(msg.data)
                    paused = (
                        pause_write
                        and frame["type"] == "execute"
                        and not frame["program"].startswith("?")
                    )
                    if paused:
                        pause_write = False
                        resume_on_next_write = disconnected_revision > 11
                        await downstream.close()
                        await release.wait()
                    elif (
                        resume_on_next_write
                        and frame["type"] == "execute"
                        and not frame["program"].startswith("?")
                    ):
                        resume_on_next_write = False
                        release.set()
                        await asyncio.wait_for(asyncio.shield(completed), 5)
                    await upstream.send_json(frame)
                    while True:
                        reply = await upstream.receive_json()
                        if not paused:
                            await downstream.send_json(reply)
                        if reply.get("id") == frame["id"]:
                            break
                    if paused:
                        completed.set_result(reply)
                        break
            return downstream

        app = web.Application()
        app.router.add_get("/ws", proxy)
        runner = web.AppRunner(app)
        await runner.setup()
        await web.TCPSite(runner, "127.0.0.1", 0).start()
        settings = EngineSettings(
            f"ws://127.0.0.1:{runner.addresses[0][1]}/ws", real_engine.api_key, "default"
        )
        applier = Applier(http, settings)
        auditor = await EngineSession.open(http, real_engine)
        try:
            await applier.declare(RELATIONS)
            if seeded:
                assert await applier.apply([FactChange(relation, key, 9, old_row)]) == Outcome(1, 0)
            disconnected = FactChange(
                relation, key, disconnected_revision, None if old_delete else old_row
            )
            reconnected = FactChange(relation, key, 11, None if new_delete else new_row)
            pause_write = True
            with pytest.raises(EngineUnavailable):
                await asyncio.wait_for(applier.apply([disconnected]), 5)
            expected = Outcome(1, 0) if disconnected_revision < 11 else Outcome(0, 1)
            with caplog.at_level("INFO", logger="ingest.applier"):
                assert await applier.apply([reconnected]) == expected
            assert f"applied={expected.applied} skipped={expected.skipped}" in caplog.messages[-1]
            release.set()
            reply = await asyncio.wait_for(completed, 5)
            assert reply["type"] == "result" and not reply.get("errors"), reply
            newest = reconnected if disconnected_revision < 11 else disconnected
            stored = await auditor.execute(iql.revision_query(newest))
            assert stored.rows == [[*newest.identity, newest.revision]]
            query = f"?{relation.name}(A, B, C)"
            expected_rows = [] if newest.row is None else [list(newest.row)]
            assert (await auditor.execute(query)).rows == expected_rows
            assert await applier.apply([disconnected, reconnected]) == Outcome(0, 2)
        finally:
            release.set()
            await applier.close()
            await auditor.close()
            await runner.cleanup()


async def test_batch_replay_delete_and_recreate(real_engine: EngineSettings) -> None:
    async with aiohttp.ClientSession() as http:
        applier = Applier(http, real_engine)
        auditor = await EngineSession.open(http, real_engine)
        try:
            await applier.declare(RELATIONS)
            customer = FactChange(CUSTOMER, (1,), 0, (1, 'Ac"me\n', "enterprise"))
            payment = FactChange(PAYMENT_ISSUE, ("inv_1",), 0, ("inv_1", 1, 42))
            batch = [customer, payment]
            assert await applier.apply([]) == Outcome(0, 0)
            assert await applier.apply([customer, customer, payment]) == Outcome(2, 1)
            assert await applier.apply(batch) == Outcome(0, 2)
            for change in batch:
                stored = await auditor.execute(iql.revision_query(change))
                assert stored.rows == [[*change.identity, 0]]
                rows = (await auditor.execute(f"?{change.relation.name}(A, B, C)")).rows
                assert rows == [list(change.row)]
            await auditor.execute(iql.apply_program(batch))
            deleted = [FactChange(c.relation, c.key, 1, None) for c in batch]
            assert await applier.apply(deleted) == Outcome(2, 0)
            await auditor.execute(iql.apply_program(batch))
            for change in deleted:
                stored = await auditor.execute(iql.revision_query(change))
                assert stored.rows == [[*change.identity, 1]]
                assert not (await auditor.execute(f"?{change.relation.name}(A, B, C)")).rows
            absent = FactChange(PAYMENT_ISSUE, ("absent",), 1, None)
            assert await applier.apply([*deleted, absent]) == Outcome(1, 2)
            assert await applier.apply([absent]) == Outcome(0, 1)
            recreated = [FactChange(c.relation, c.key, 2, c.row) for c in batch]
            assert await applier.apply(recreated) == Outcome(2, 0)
            for change in recreated:
                stored = await auditor.execute(iql.revision_query(change))
                assert stored.rows == [[*change.identity, 2]]
                rows = (await auditor.execute(f"?{change.relation.name}(A, B, C)")).rows
                assert rows == [list(change.row)]
            unchanged = [FactChange(c.relation, c.key, 3, c.row) for c in batch]
            assert await applier.apply(unchanged) == Outcome(2, 0)
            assert await applier.apply([*unchanged, *recreated]) == Outcome(0, 4)
        finally:
            await applier.close()
            await auditor.close()
