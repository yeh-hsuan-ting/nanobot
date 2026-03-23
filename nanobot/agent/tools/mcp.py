"""MCP client: connects to MCP servers and wraps their tools as native nanobot tools."""

import asyncio
from contextlib import AsyncExitStack
from typing import Any

import anyio
import httpx
from loguru import logger

from nanobot.agent.tools.base import Tool
from nanobot.agent.tools.registry import ToolRegistry


def _is_session_dead(exc: BaseException) -> bool:
    """
    Return True if exc indicates the MCP session is dead and we should reconnect.

    The MCP SDK (streamable_http transport) surfaces session death as:
    1. McpError code 32600 "Session terminated" — server reaped stale session
    2. httpx.RemoteProtocolError — TCP closed mid-response
    3. httpx.ConnectError — server unreachable
    4. httpx.HTTPStatusError 5xx — e.g. 502 Bad Gateway during server restart
    5. anyio.ClosedResourceError — anyio stream torn down
    6. EOFError — stdio transport EOF
    7. BaseExceptionGroup — anyio task groups wrap transport errors
    """
    from mcp.shared.exceptions import McpError

    # McpError code 32600 = "Session terminated"
    if isinstance(exc, McpError):
        return exc.error.code == 32600 and "terminated" in exc.error.message.lower()

    # Transport-level failures
    if isinstance(exc, (
        httpx.RemoteProtocolError,
        httpx.ConnectError,
        anyio.ClosedResourceError,
        EOFError,
    )):
        return True

    # HTTP 5xx (e.g. 502 Bad Gateway during server restart)
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code >= 500

    # Unwrap ExceptionGroups (anyio task groups wrap errors this way)
    if isinstance(exc, BaseExceptionGroup):
        return any(_is_session_dead(sub) for sub in exc.exceptions)

    return False


def _is_anyio_cancel_scope_error(exc: asyncio.CancelledError) -> bool:
    """
    Return True if this CancelledError came from an anyio cancel scope
    (transport teardown), NOT from an external asyncio task.cancel() call.

    When the MCP session's internal anyio task group is cancelled (e.g. because
    the transport died), it generates a CancelledError with message:
      "Cancelled via cancel scope <hex_id> by <task_info>"

    External task cancellation (e.g. /stop) uses the standard asyncio mechanism
    and does NOT produce this message prefix.
    """
    if exc.args and isinstance(exc.args[0], str):
        return exc.args[0].startswith("Cancelled via cancel scope ")
    return False


async def _create_transport_and_session(name: str, cfg):
    """
    Create transport + ClientSession for a given server config.

    Returns (session, local_stack) where local_stack.aclose() tears it down.
    Does NOT use the AgentLoop's AsyncExitStack — manages its own lifecycle.
    """
    from mcp import ClientSession, StdioServerParameters
    from mcp.client.sse import sse_client
    from mcp.client.stdio import stdio_client
    from mcp.client.streamable_http import streamable_http_client

    transport_type = cfg.type
    if not transport_type:
        if cfg.command:
            transport_type = "stdio"
        elif cfg.url:
            transport_type = (
                "sse" if cfg.url.rstrip("/").endswith("/sse") else "streamableHttp"
            )
        else:
            raise ValueError(f"MCP server '{name}': no command or url configured")

    local_stack = AsyncExitStack()
    await local_stack.__aenter__()

    try:
        if transport_type == "stdio":
            params = StdioServerParameters(
                command=cfg.command, args=cfg.args, env=cfg.env or None
            )
            read, write = await local_stack.enter_async_context(stdio_client(params))

        elif transport_type == "sse":
            def httpx_client_factory(
                headers: dict[str, str] | None = None,
                timeout: httpx.Timeout | None = None,
                auth: httpx.Auth | None = None,
            ) -> httpx.AsyncClient:
                merged_headers = {**(cfg.headers or {}), **(headers or {})}
                return httpx.AsyncClient(
                    headers=merged_headers or None,
                    follow_redirects=True,
                    timeout=timeout,
                    auth=auth,
                )

            read, write = await local_stack.enter_async_context(
                sse_client(cfg.url, httpx_client_factory=httpx_client_factory)
            )

        elif transport_type == "streamableHttp":
            http_client = await local_stack.enter_async_context(
                httpx.AsyncClient(
                    headers=cfg.headers or None,
                    follow_redirects=True,
                    timeout=None,
                )
            )
            read, write, _ = await local_stack.enter_async_context(
                streamable_http_client(cfg.url, http_client=http_client)
            )

        else:
            raise ValueError(
                f"MCP server '{name}': unknown transport type '{transport_type}'"
            )

        session = await local_stack.enter_async_context(ClientSession(read, write))
        await session.initialize()

    except BaseException:
        await local_stack.aclose()
        raise

    return session, local_stack


class MCPConnectionManager:
    """
    Owns the lifecycle of one MCP server connection. Reconnects transparently
    on session death (server restart, stale session reaped, network error).

    The asyncio.Lock ensures that concurrent tool call failures trigger exactly
    one reconnect (thundering-herd protection). Coroutines waiting behind the
    lock see a fresh session and proceed without reconnecting again.
    """

    def __init__(self, name: str, cfg):
        self._name = name
        self._cfg = cfg
        self._session = None
        self._local_stack: AsyncExitStack | None = None
        self._lock = asyncio.Lock()
        self._reconnect_count: int = 0  # incremented on each successful reconnect

    async def connect(self, stack: AsyncExitStack) -> None:
        """
        Initial connection — called once from connect_mcp_servers().

        Uses the AgentLoop's AsyncExitStack so that normal shutdown (aclose())
        tears it down gracefully.
        """
        from mcp import ClientSession, StdioServerParameters
        from mcp.client.sse import sse_client
        from mcp.client.stdio import stdio_client
        from mcp.client.streamable_http import streamable_http_client

        transport_type = self._cfg.type
        if not transport_type:
            if self._cfg.command:
                transport_type = "stdio"
            elif self._cfg.url:
                transport_type = (
                    "sse"
                    if self._cfg.url.rstrip("/").endswith("/sse")
                    else "streamableHttp"
                )
            else:
                raise ValueError(
                    f"MCP server '{self._name}': no command or url configured"
                )

        if transport_type == "stdio":
            params = StdioServerParameters(
                command=self._cfg.command,
                args=self._cfg.args,
                env=self._cfg.env or None,
            )
            read, write = await stack.enter_async_context(stdio_client(params))

        elif transport_type == "sse":
            def httpx_client_factory(
                headers: dict[str, str] | None = None,
                timeout: httpx.Timeout | None = None,
                auth: httpx.Auth | None = None,
            ) -> httpx.AsyncClient:
                merged_headers = {**(self._cfg.headers or {}), **(headers or {})}
                return httpx.AsyncClient(
                    headers=merged_headers or None,
                    follow_redirects=True,
                    timeout=timeout,
                    auth=auth,
                )

            read, write = await stack.enter_async_context(
                sse_client(self._cfg.url, httpx_client_factory=httpx_client_factory)
            )

        elif transport_type == "streamableHttp":
            http_client = await stack.enter_async_context(
                httpx.AsyncClient(
                    headers=self._cfg.headers or None,
                    follow_redirects=True,
                    timeout=None,
                )
            )
            read, write, _ = await stack.enter_async_context(
                streamable_http_client(self._cfg.url, http_client=http_client)
            )

        else:
            raise ValueError(
                f"MCP server '{self._name}': unknown transport type '{transport_type}'"
            )

        self._session = await stack.enter_async_context(ClientSession(read, write))
        await self._session.initialize()

    async def reconnect(self, dead_at_count: int = -1) -> None:
        """
        Tear down dead session, establish a fresh one.

        dead_at_count: value of _reconnect_count when session death was detected.
        If _reconnect_count has changed since (another coroutine reconnected while
        we waited for the lock), skip reconnect — just return with fresh session.

        The asyncio.Lock prevents thundering herd: if N tools fail simultaneously,
        only 1 reconnect executes. The others wait behind the lock, see the count
        has changed, and return immediately.
        """
        async with self._lock:
            # Skip if another coroutine already reconnected while we waited
            if dead_at_count >= 0 and self._reconnect_count != dead_at_count:
                return

            # Close the previous local stack (from prior reconnect, if any)
            if self._local_stack is not None:
                try:
                    await self._local_stack.aclose()
                except Exception as e:
                    logger.debug(
                        "mcp_reconnect_cleanup_error",
                        server=self._name,
                        error=str(e),
                    )
                self._local_stack = None

            logger.info("mcp_reconnecting", server=self._name)
            session, local_stack = await _create_transport_and_session(
                self._name, self._cfg
            )
            self._session = session
            self._local_stack = local_stack
            self._reconnect_count += 1
            logger.info("mcp_session_created", server=self._name)

    async def call_tool(
        self, tool_name: str, arguments: dict, timeout: int
    ) -> Any:
        """
        Call a tool with reconnect-on-failure.

        On session death, reconnect once and retry. If the retry also fails,
        propagate the exception so the caller can return an error to the LLM.

        CancelledError handling:
        - External task cancellation (e.g. /stop): task.cancelling() > 0
          AND NOT an anyio cancel-scope message → re-raise
        - anyio cancel-scope from transport death: "Cancelled via cancel scope"
          prefix → treat as session dead → reconnect

        Reconnect runs in a SEPARATE asyncio Task (via asyncio.create_task +
        wait) because the anyio cancel scope from the dead session persists
        inside the current task. Running reconnect in a new task means the
        fresh session setup is unaffected by the old session's cancel scope.
        """
        try:
            return await asyncio.wait_for(
                self._session.call_tool(tool_name, arguments=arguments),
                timeout=timeout,
            )
        except asyncio.TimeoutError:
            raise
        except asyncio.CancelledError as exc:
            # Re-raise if this task was externally cancelled (e.g. /stop command)
            task = asyncio.current_task()
            if task is not None and task.cancelling() > 0 and not _is_anyio_cancel_scope_error(exc):
                raise
            # anyio cancel scope from transport death
            logger.warning(
                "mcp_session_dead_cancelled",
                server=self._name,
                tool=tool_name,
            )
        except Exception as exc:
            if not _is_session_dead(exc):
                raise
            logger.warning(
                "mcp_session_dead",
                server=self._name,
                tool=tool_name,
                error=str(exc),
            )

        # Reconnect in a separate asyncio Task so the dead session's anyio
        # cancel scope cannot propagate into the new transport setup.
        # dead_at_count ensures only 1 of N concurrent failures actually reconnects;
        # the others skip (thundering herd protection via compare-and-skip in lock).
        dead_at = self._reconnect_count
        reconnect_task = asyncio.ensure_future(self.reconnect(dead_at))
        try:
            await asyncio.shield(reconnect_task)
        except asyncio.CancelledError:
            # Our task was externally cancelled. Let reconnect finish anyway
            # (don't abort a half-initialized session), then re-raise.
            await reconnect_task
            raise

        return await asyncio.wait_for(
            self._session.call_tool(tool_name, arguments=arguments),
            timeout=timeout,
        )


class MCPToolWrapper(Tool):
    """Wraps a single MCP server tool as a nanobot Tool."""

    def __init__(
        self,
        manager: MCPConnectionManager,
        server_name: str,
        tool_def,
        tool_timeout: int = 30,
    ):
        self._manager = manager
        self._original_name = tool_def.name
        self._name = f"mcp_{server_name}_{tool_def.name}"
        self._description = tool_def.description or tool_def.name
        self._parameters = tool_def.inputSchema or {"type": "object", "properties": {}}
        self._tool_timeout = tool_timeout

    @property
    def name(self) -> str:
        return self._name

    @property
    def description(self) -> str:
        return self._description

    @property
    def parameters(self) -> dict[str, Any]:
        return self._parameters

    async def execute(self, **kwargs: Any) -> str:
        from mcp import types

        try:
            result = await self._manager.call_tool(
                self._original_name,
                arguments=kwargs,
                timeout=self._tool_timeout,
            )
        except asyncio.TimeoutError:
            logger.warning(
                "MCP tool '{}' timed out after {}s", self._name, self._tool_timeout
            )
            return f"(MCP tool call timed out after {self._tool_timeout}s)"
        except asyncio.CancelledError:
            # Re-raise only if our task was externally cancelled (e.g. /stop).
            task = asyncio.current_task()
            if task is not None and task.cancelling() > 0:
                raise
            logger.warning("MCP tool '{}' was cancelled by server/SDK", self._name)
            return "(MCP tool call was cancelled)"
        except Exception as exc:
            logger.exception(
                "MCP tool '{}' failed: {}: {}",
                self._name,
                type(exc).__name__,
                exc,
            )
            return f"(MCP tool call failed: {type(exc).__name__})"

        parts = []
        for block in result.content:
            if isinstance(block, types.TextContent):
                parts.append(block.text)
            else:
                parts.append(str(block))
        return "\n".join(parts) or "(no output)"


async def connect_mcp_servers(
    mcp_servers: dict, registry: ToolRegistry, stack: AsyncExitStack
) -> None:
    """Connect to configured MCP servers and register their tools."""
    for name, cfg in mcp_servers.items():
        try:
            manager = MCPConnectionManager(name, cfg)
            await manager.connect(stack)

            tools = await manager._session.list_tools()
            enabled_tools = set(cfg.enabled_tools)
            allow_all_tools = "*" in enabled_tools
            registered_count = 0
            matched_enabled_tools: set[str] = set()
            available_raw_names = [tool_def.name for tool_def in tools.tools]
            available_wrapped_names = [
                f"mcp_{name}_{tool_def.name}" for tool_def in tools.tools
            ]
            for tool_def in tools.tools:
                wrapped_name = f"mcp_{name}_{tool_def.name}"
                if (
                    not allow_all_tools
                    and tool_def.name not in enabled_tools
                    and wrapped_name not in enabled_tools
                ):
                    logger.debug(
                        "MCP: skipping tool '{}' from server '{}' (not in enabledTools)",
                        wrapped_name,
                        name,
                    )
                    continue
                wrapper = MCPToolWrapper(
                    manager, name, tool_def, tool_timeout=cfg.tool_timeout
                )
                registry.register(wrapper)
                logger.debug(
                    "MCP: registered tool '{}' from server '{}'",
                    wrapper.name,
                    name,
                )
                registered_count += 1
                if enabled_tools:
                    if tool_def.name in enabled_tools:
                        matched_enabled_tools.add(tool_def.name)
                    if wrapped_name in enabled_tools:
                        matched_enabled_tools.add(wrapped_name)

            if enabled_tools and not allow_all_tools:
                unmatched_enabled_tools = sorted(enabled_tools - matched_enabled_tools)
                if unmatched_enabled_tools:
                    logger.warning(
                        "MCP server '{}': enabledTools entries not found: {}. Available raw names: {}. "
                        "Available wrapped names: {}",
                        name,
                        ", ".join(unmatched_enabled_tools),
                        ", ".join(available_raw_names) or "(none)",
                        ", ".join(available_wrapped_names) or "(none)",
                    )

            logger.info(
                "MCP server '{}': connected, {} tools registered",
                name,
                registered_count,
            )
        except Exception as e:
            logger.error("MCP server '{}': failed to connect: {}", name, e)
