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
    """Return True if exc indicates the MCP session is dead and we should reconnect."""
    from mcp.shared.exceptions import McpError

    # McpError with code 32600 = "Session terminated" (server reaped stale session)
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

    # Unwrap ExceptionGroups (anyio task groups wrap errors this way)
    if isinstance(exc, BaseExceptionGroup):
        return any(_is_session_dead(sub) for sub in exc.exceptions)

    return False


async def _create_transport_and_session(name: str, cfg):
    """
    Create transport + ClientSession for a given server config.

    Returns (session, cleanup_coro) where cleanup_coro() must be called when
    the session is no longer needed (closes transport without using AsyncExitStack).
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

    # We use an AsyncExitStack LOCAL to this function call so the transport lifecycle
    # can be captured and closed independently of the AgentLoop's stack.
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
            raise ValueError(f"MCP server '{name}': unknown transport type '{transport_type}'")

        session = await local_stack.enter_async_context(ClientSession(read, write))
        await session.initialize()

    except BaseException:
        await local_stack.aclose()
        raise

    return session, local_stack


class MCPConnectionManager:
    """
    Owns the lifecycle of one MCP server connection. Reconnects transparently
    on session death (e.g., server restart, stale session reaped, network error).

    The lock ensures that concurrent tool call failures trigger exactly one
    reconnect — the thundering-herd problem. Coroutines waiting behind the lock
    see a fresh session and proceed without reconnecting again.
    """

    def __init__(self, name: str, cfg):
        self._name = name
        self._cfg = cfg
        self._session = None
        self._local_stack: AsyncExitStack | None = None  # lifecycle of current session
        self._lock = asyncio.Lock()

    async def connect(self, stack: AsyncExitStack) -> None:
        """
        Initial connection — called once from connect_mcp_servers().

        Uses the AgentLoop's AsyncExitStack for the initial session so that
        normal shutdown (via aclose()) tears it down gracefully.
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

    async def reconnect(self) -> None:
        """
        Tear down dead session, create a fresh one.

        The asyncio.Lock prevents thundering herd: if 3 tools fail simultaneously,
        only 1 reconnect executes. The other 2 wait, then see a fresh session.
        """
        async with self._lock:
            # Close old local stack if this is a post-initial reconnect
            if self._local_stack is not None:
                try:
                    await self._local_stack.aclose()
                except Exception as e:
                    logger.debug(
                        "mcp_reconnect_cleanup_error", server=self._name, error=str(e)
                    )
                self._local_stack = None

            logger.info("mcp_reconnecting", server=self._name)
            session, local_stack = await _create_transport_and_session(
                self._name, self._cfg
            )
            self._session = session
            self._local_stack = local_stack
            logger.info("mcp_session_created", server=self._name)

    async def call_tool(
        self, tool_name: str, arguments: dict, timeout: int
    ) -> Any:
        """
        Call a tool. On session death, reconnect once and retry.

        If the retry also fails, let the exception propagate so the caller
        can log it and return an error string to the LLM.
        """
        try:
            return await asyncio.wait_for(
                self._session.call_tool(tool_name, arguments=arguments),
                timeout=timeout,
            )
        except (asyncio.TimeoutError, asyncio.CancelledError):
            raise
        except Exception as exc:
            if _is_session_dead(exc):
                logger.warning(
                    "mcp_session_dead",
                    server=self._name,
                    tool=tool_name,
                    error=str(exc),
                )
                await self.reconnect()
                # Retry once — if this also fails, propagate
                return await asyncio.wait_for(
                    self._session.call_tool(tool_name, arguments=arguments),
                    timeout=timeout,
                )
            raise


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
            # MCP SDK's anyio cancel scopes can leak CancelledError on timeout/failure.
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
                wrapper = MCPToolWrapper(manager, name, tool_def, tool_timeout=cfg.tool_timeout)
                registry.register(wrapper)
                logger.debug(
                    "MCP: registered tool '{}' from server '{}'", wrapper.name, name
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
                "MCP server '{}': connected, {} tools registered", name, registered_count
            )
        except Exception as e:
            logger.error("MCP server '{}': failed to connect: {}", name, e)
