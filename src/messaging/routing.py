"""Cross-gateway routing.

Each user's WebSocket connections live on some gateway instance. The session registry maps user → (gateway,
connection); the bus delivers events to a gateway's channel. In production the registry is Redis (hash with TTL per
connection) and the bus is Redis pub/sub or a message broker; these in-process implementations keep the same
interfaces and are thread-safe so several gateway apps (each with its own event loop) can share them in tests.
"""

from __future__ import annotations

import asyncio
import threading
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

Event = dict[str, Any]


class SessionRegistry:
    def __init__(self) -> None:
        self._sessions: dict[str, set[tuple[str, str]]] = defaultdict(set)
        self._lock = threading.Lock()

    def add(self, user: str, gateway: str, connection: str) -> None:
        with self._lock:
            self._sessions[user].add((gateway, connection))

    def remove(self, user: str, gateway: str, connection: str) -> None:
        with self._lock:
            self._sessions[user].discard((gateway, connection))

    def sessions(self, user: str) -> set[tuple[str, str]]:
        with self._lock:
            return set(self._sessions.get(user, set()))


class Bus:
    """Gateway channels. `subscribe` registers a callback bound to the subscriber's event loop."""

    def __init__(self) -> None:
        self._subscribers: dict[str, tuple[asyncio.AbstractEventLoop, Callable[[str, Event], None]]] = {}
        self._lock = threading.Lock()
        self.published = 0

    def subscribe(self, gateway: str, loop: asyncio.AbstractEventLoop, handler: Callable[[str, Event], None]) -> None:
        with self._lock:
            self._subscribers[gateway] = (loop, handler)

    def unsubscribe(self, gateway: str) -> None:
        with self._lock:
            self._subscribers.pop(gateway, None)

    def publish(self, gateway: str, connection: str, event: Event) -> bool:
        with self._lock:
            sub = self._subscribers.get(gateway)
        if sub is None:
            return False  # gateway gone: the message stays in the store and is synced on reconnect
        loop, handler = sub
        loop.call_soon_threadsafe(handler, connection, event)
        self.published += 1
        return True


@dataclass
class PushNotifier:
    """Stand-in for APNs/FCM. With end-to-end encryption the payload carries no message content."""

    sent: list[Event] = field(default_factory=list)

    def notify(self, user: str, conversation_id: str, sender: str) -> None:
        self.sent.append({"user": user, "conversationId": conversation_id, "title": "New message", "from": sender})
