"""WebSocket connection management with authentication and streaming support."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import Any

import websockets
from websockets.asyncio.client import ClientConnection

from inputlayer._protocol import (
    AuthenticatedResponse,
    AuthenticateMessage,
    AuthErrorResponse,
    ErrorResponse,
    ExecuteMessage,
    LoginMessage,
    NoticeResponse,
    NotificationResponse,
    PingMessage,
    PongResponse,
    ResultChunkResponse,
    ResultEndResponse,
    ResultResponse,
    ResultStartResponse,
    ServerMessage,
    SubscriptionDeltaChunkResponse,
    SubscriptionDeltaEndResponse,
    SubscriptionDeltaResponse,
    SubscriptionDeltaStartResponse,
    SubscriptionErrorResponse,
    SubscriptionResetResponse,
    deserialize_message,
)
from inputlayer.exceptions import (
    AuthenticationError,
    ConnectionError,
    InternalError,
    QueryError,
    StatementFailedError,
)
from inputlayer.notifications import NotificationDispatcher, NotificationEvent

logger = logging.getLogger("inputlayer")

_SUBSCRIPTION_PUSHES = (
    SubscriptionDeltaResponse,
    SubscriptionDeltaStartResponse,
    SubscriptionDeltaChunkResponse,
    SubscriptionDeltaEndResponse,
    SubscriptionErrorResponse,
    SubscriptionResetResponse,
)


class Connection:
    """Manages the WebSocket connection to an InputLayer server."""

    def __init__(
        self,
        url: str,
        *,
        username: str | None = None,
        password: str | None = None,
        api_key: str | None = None,
        auto_reconnect: bool = True,
        reconnect_delay: float = 1.0,
        max_reconnect_attempts: int = 10,
        initial_kg: str | None = None,
        last_seq: int | None = None,
    ) -> None:
        self._url = url
        self._username = username
        self._password = password
        self._api_key = api_key
        self._auto_reconnect = auto_reconnect
        self._reconnect_delay = reconnect_delay
        self._max_reconnect_attempts = max_reconnect_attempts
        self._initial_kg = initial_kg
        self._last_seq = last_seq

        self._ws: ClientConnection | None = None
        self._session_id: str | None = None
        self._server_version: str | None = None
        self._role: str | None = None
        self._current_kg: str | None = None
        self._connected = False

        self._dispatcher = NotificationDispatcher()
        self._recv_task: asyncio.Task | None = None
        self._execute_lock: asyncio.Lock | None = None


    def _get_execute_lock(self) -> asyncio.Lock:
        """Lazily create the execute lock in the current event loop.

        Created on first use rather than in __init__ so the Connection
        can be constructed outside an async context (the common case).
        """
        if self._execute_lock is None:
            self._execute_lock = asyncio.Lock()
        return self._execute_lock

    # ── Properties ────────────────────────────────────────────────────

    @property
    def connected(self) -> bool:
        return self._connected

    @property
    def session_id(self) -> str | None:
        return self._session_id

    @property
    def server_version(self) -> str | None:
        return self._server_version

    @property
    def role(self) -> str | None:
        return self._role

    @property
    def current_kg(self) -> str | None:
        return self._current_kg

    @property
    def dispatcher(self) -> NotificationDispatcher:
        return self._dispatcher

    @property
    def last_seq(self) -> int:
        return self._dispatcher.last_seq

    # ── Connection lifecycle ──────────────────────────────────────────

    async def connect(self) -> None:
        """Connect and authenticate."""
        ws_url = self._url
        params = []
        if self._initial_kg:
            params.append(f"kg={self._initial_kg}")
        if self._last_seq is not None:
            params.append(f"last_seq={self._last_seq}")
        if params:
            separator = "&" if "?" in ws_url else "?"
            ws_url = f"{ws_url}{separator}{'&'.join(params)}"

        try:
            self._ws = await websockets.connect(ws_url)
        except Exception as e:
            raise ConnectionError(f"Failed to connect to {ws_url}: {e}") from e

        await self._authenticate()
        self._connected = True

        # NOTE: _receive_loop is NOT started automatically because it races
        # with execute() for WebSocket messages. Notifications are handled
        # inline during _read_result() instead. The loop can be started
        # manually for idle notification listening if needed:
        #   self._recv_task = asyncio.create_task(self._receive_loop())

    async def close(self) -> None:
        """Close the connection gracefully."""
        self._connected = False
        if self._recv_task and not self._recv_task.done():
            self._recv_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._recv_task
        if self._ws:
            with contextlib.suppress(Exception):
                await self._ws.close()
            self._ws = None

    async def _authenticate(self) -> None:
        """Send authentication message and wait for response."""
        assert self._ws is not None

        if self._api_key:
            msg = AuthenticateMessage(api_key=self._api_key)
        elif self._username and self._password:
            msg = LoginMessage(username=self._username, password=self._password)
        else:
            raise AuthenticationError("No credentials provided (need username/password or api_key)")

        await self._ws.send(msg.to_json())
        raw = await self._ws.recv()
        response = deserialize_message(raw)

        if isinstance(response, (AuthErrorResponse, NoticeResponse)):
            raise AuthenticationError(response.message)
        if isinstance(response, AuthenticatedResponse):
            self._session_id = response.session_id
            self._server_version = response.version
            self._role = response.role
            self._current_kg = response.knowledge_graph
            return

        raise AuthenticationError(f"Unexpected auth response: {response!r}")

    # ── Command execution ─────────────────────────────────────────────

    async def execute(self, program: str) -> ResultResponse:
        """Send a program/command and wait for the result.

        Transparently assembles streamed results (result_start -> chunks -> result_end).

        The lock serializes the entire send+recv cycle so that concurrent
        coroutines on the same event loop cannot interleave their messages
        on the shared WebSocket (the protocol has no request-ID multiplexing).
        """
        if not self._connected or not self._ws:
            raise ConnectionError("Not connected")

        async with self._get_execute_lock():
            return await self._send_and_recv(program)

    async def execute_with_preamble(
        self,
        preamble: str | None,
        program: str,
    ) -> ResultResponse:
        """Execute *program* atomically, optionally sending *preamble* first.

        Both the preamble (typically a KG switch) and the program are
        sent under a single lock hold, so no other coroutine can
        interleave and change server-side state between them.
        """
        if not self._connected or not self._ws:
            raise ConnectionError("Not connected")

        async with self._get_execute_lock():
            if preamble is not None:
                await self._send_and_recv(preamble)
            return await self._send_and_recv(program)

    async def execute_sequence(self, programs: list[str]) -> list[ResultResponse]:
        """Execute multiple programs atomically under a single lock hold.

        Used for compound operations (aggregate query setup/run/cleanup,
        OR-split queries, etc.) where interleaving would corrupt state.
        Stops at the first failed program and raises its error.
        """
        if not self._connected or not self._ws:
            raise ConnectionError("Not connected")

        async with self._get_execute_lock():
            return [await self._send_and_recv(program) for program in programs]

    async def _send_and_recv(self, program: str) -> ResultResponse:
        """Send a single program and read its result. Caller must hold the lock."""
        assert self._ws is not None
        msg = ExecuteMessage(program=program)
        await self._ws.send(msg.to_json())
        return await self._read_result()

    async def _read_result(self) -> ResultResponse:
        """Read messages until we get a complete result, dispatching notifications.

        Raises ``QueryError`` for an ``error`` frame and
        ``StatementFailedError`` for a result whose ``errors`` is not empty,
        so no caller can read a failed program as data.
        """
        assert self._ws is not None
        while True:
            raw = await self._ws.recv()
            response = deserialize_message(raw)

            if self._handle_push(response):
                continue

            if isinstance(response, PongResponse):
                continue

            if isinstance(response, ResultResponse):
                return self._accept(response)

            if isinstance(response, ErrorResponse):
                raise _query_error(response)

            if isinstance(response, ResultStartResponse):
                return self._accept(await self._assemble_stream(response))

            raise InternalError(f"Unexpected message during result read: {response!r}")

    def _accept(self, result: ResultResponse) -> ResultResponse:
        """Track a KG switch, then raise if any statement failed."""
        if result.switched_kg:
            self._current_kg = result.switched_kg
        if result.errors:
            raise StatementFailedError(result.errors, result)
        return result

    async def _assemble_stream(self, start: ResultStartResponse) -> ResultResponse:
        """Assemble a streamed result from chunks."""
        assert self._ws is not None
        all_rows: list[list[Any]] = []
        all_provenance: list[str] = []
        chunks = 0

        while True:
            raw = await self._ws.recv()
            response = deserialize_message(raw)

            if self._handle_push(response):
                continue

            if isinstance(response, ResultChunkResponse):
                if response.chunk_index != chunks:
                    raise InternalError(
                        f"Streamed result chunk {response.chunk_index} arrived, expected {chunks}"
                    )
                chunks += 1
                all_rows.extend(response.rows)
                if response.row_provenance:
                    all_provenance.extend(response.row_provenance)
                continue

            if isinstance(response, ResultEndResponse):
                if (response.chunk_count, response.row_count) != (chunks, len(all_rows)):
                    raise InternalError(
                        f"Incomplete streamed result: {chunks} chunk(s) and {len(all_rows)} "
                        f"row(s) arrived, end announces {response.chunk_count} and "
                        f"{response.row_count}"
                    )
                return ResultResponse(
                    columns=start.columns,
                    rows=all_rows,
                    row_count=response.row_count,
                    total_count=start.total_count,
                    truncated=start.truncated,
                    execution_time_ms=start.execution_time_ms,
                    row_provenance=all_provenance or None,
                    metadata=start.metadata,
                    switched_kg=start.switched_kg,
                    proof_trees=start.proof_trees,
                    timing_breakdown=start.timing_breakdown,
                    errors=start.errors,
                    subscribed=start.subscribed,
                )

            if isinstance(response, ErrorResponse):
                raise _query_error(response)

            raise InternalError(f"Unexpected message during streaming: {response!r}")

    # ── Notification handling ─────────────────────────────────────────

    def _handle_push(self, response: ServerMessage) -> bool:
        """Handle a frame the server sent unprompted; ``False`` for replies.

        Notices are logged (a closing one is followed by the socket closing,
        which fails the pending call). Subscription pushes have no consumer
        in this SDK yet and are dropped.
        """
        if isinstance(response, NotificationResponse):
            self._dispatch_notification(response)
            return True
        if isinstance(response, NoticeResponse):
            level = logging.WARNING if response.closes_connection else logging.INFO
            logger.log(level, "server notice (%s): %s", response.code, response.message)
            return True
        return isinstance(response, _SUBSCRIPTION_PUSHES)

    def _dispatch_notification(self, notif: NotificationResponse) -> None:
        event = NotificationEvent(
            type=notif.type,
            seq=notif.seq,
            timestamp_ms=notif.timestamp_ms,
            session_id=notif.session_id,
            knowledge_graph=notif.knowledge_graph,
            relation=notif.relation,
            operation=notif.operation,
            count=notif.count,
            rule_name=notif.rule_name,
            entity=notif.entity,
        )
        self._dispatcher.dispatch(event)

    async def _receive_loop(self) -> None:
        """Background task that receives notifications when idle."""
        assert self._ws is not None
        try:
            async for raw in self._ws:
                with contextlib.suppress(Exception):
                    self._handle_push(deserialize_message(raw))
        except asyncio.CancelledError:
            pass
        except Exception:
            self._connected = False
            if self._auto_reconnect:
                await self._reconnect()

    async def _reconnect(self) -> None:
        """Attempt reconnection with exponential backoff."""
        delay = self._reconnect_delay
        for attempt in range(self._max_reconnect_attempts):
            logger.info(f"Reconnecting (attempt {attempt + 1}/{self._max_reconnect_attempts})...")
            await asyncio.sleep(delay)
            try:
                self._last_seq = self._dispatcher.last_seq
                await self.connect()
                logger.info("Reconnected successfully")
                return
            except Exception:
                delay = min(delay * 2, 60.0)
        raise ConnectionError(
            f"Failed to reconnect after {self._max_reconnect_attempts} attempts"
        )

    async def ping(self) -> None:
        """Send a keep-alive ping."""
        if not self._ws:
            raise ConnectionError("Not connected")
        async with self._get_execute_lock():
            await self._ws.send(PingMessage().to_json())


def _query_error(response: ErrorResponse) -> QueryError:
    return QueryError(
        response.message,
        code=response.code,
        validation_errors=response.validation_errors,
    )
