"""Async message queue for decoupled channel-agent communication."""

import asyncio

from nanobot.bus.events import InboundMessage, OutboundMessage


class MessageBus:
    """
    Async message bus that decouples chat channels from the agent core.

    Channels push messages to per-session inbound queues so that rapid
    messages from one user queue behind each other, not behind other users.
    Responses are pushed to a shared outbound queue.
    """

    def __init__(self):
        self._session_queues: dict[str, asyncio.Queue[InboundMessage]] = {}
        self._dispatch: asyncio.Queue[str] = asyncio.Queue()
        self.outbound: asyncio.Queue[OutboundMessage] = asyncio.Queue()

    def _get_or_create_session_queue(self, key: str) -> asyncio.Queue[InboundMessage]:
        if key not in self._session_queues:
            self._session_queues[key] = asyncio.Queue()
        return self._session_queues[key]

    async def publish_inbound(self, msg: InboundMessage) -> None:
        """Route an inbound message to its session queue and notify the dispatcher."""
        key = msg.session_key
        await self._get_or_create_session_queue(key).put(msg)
        await self._dispatch.put(key)

    async def consume_dispatch(self) -> str:
        """Consume the next session key that has a pending inbound message."""
        return await self._dispatch.get()

    def get_session_queue(self, key: str) -> asyncio.Queue[InboundMessage]:
        """Get (or create) the inbound queue for a given session."""
        return self._get_or_create_session_queue(key)

    def drop_session_queue(self, key: str) -> None:
        """Remove an empty session queue to reclaim memory."""
        q = self._session_queues.get(key)
        if q is not None and q.empty():
            self._session_queues.pop(key, None)

    async def publish_outbound(self, msg: OutboundMessage) -> None:
        """Publish a response from the agent to channels."""
        await self.outbound.put(msg)

    async def consume_outbound(self) -> OutboundMessage:
        """Consume the next outbound message (blocks until available)."""
        return await self.outbound.get()

    @property
    def inbound_size(self) -> int:
        """Total number of pending inbound messages across all sessions."""
        return sum(q.qsize() for q in self._session_queues.values())

    @property
    def outbound_size(self) -> int:
        """Number of pending outbound messages."""
        return self.outbound.qsize()
