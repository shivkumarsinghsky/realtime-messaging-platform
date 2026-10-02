"""Presence with heartbeat leases.

A user is online while any of their connections renewed its lease within `ttl` seconds. Crashed clients or gateways
therefore go offline automatically, without an explicit disconnect. `last_seen` is shown to contacts only when the
user allows it (privacy setting).
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass, field


@dataclass
class PresenceService:
    ttl: float = 45.0
    clock: Callable[[], float] = time.monotonic
    wall_clock: Callable[[], float] = time.time
    _leases: dict[tuple[str, str], float] = field(default_factory=dict)  # (user, connection) → expiry
    _last_seen: dict[str, float] = field(default_factory=dict)
    hide_last_seen: set[str] = field(default_factory=set)

    def heartbeat(self, user: str, connection_id: str) -> None:
        self._leases[(user, connection_id)] = self.clock() + self.ttl
        self._last_seen[user] = self.wall_clock()

    def disconnect(self, user: str, connection_id: str) -> None:
        self._leases.pop((user, connection_id), None)
        self._last_seen[user] = self.wall_clock()

    def is_online(self, user: str) -> bool:
        now = self.clock()
        return any(u == user and exp > now for (u, _), exp in self._leases.items())

    def view(self, user: str, viewer: str) -> dict[str, object]:
        online = self.is_online(user)
        result: dict[str, object] = {"user": user, "online": online}
        if not online and user not in self.hide_last_seen:
            result["lastSeen"] = self._last_seen.get(user)
        return result
