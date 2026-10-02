"""WebSocket gateway (FastAPI).

Client → server frames                         Server → client frames
  {"type": "send", conversationId, clientMsgId,  {"type": "ack", clientMsgId, seq, messageId, duplicate}
   body}                                         {"type": "message", conversationId, seq, messageId, sender, body}
  {"type": "delivered"|"read", conversationId,   {"type": "receipt", conversationId, user, state, upToSeq}
   upToSeq}                                      {"type": "messages", conversationId, items:[...]}
  {"type": "sync", conversationId, afterSeq}     {"type": "presence", user, online, lastSeen?}
  {"type": "ping"}                               {"type": "pong"}
  {"type": "presence", user}                     {"type": "group", conversationId}
  {"type": "create_group", members:[...]}        {"type": "hello", conversations:[{id, lastSeq, delivered}]}
  {"type": "direct", user}                       {"type": "error", code, message}
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import uuid
from dataclasses import asdict, dataclass
from typing import Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect

from messaging.domain import MessageStore, MessagingError, NotAMember, ReceiptState
from messaging.presence import PresenceService
from messaging.routing import Bus, Event, PushNotifier, SessionRegistry

log = logging.getLogger("messaging.gateway")


@dataclass
class Cluster:
    """Shared services that, in production, are separate systems (store, Redis registry, bus, push provider)."""

    store: MessageStore
    presence: PresenceService
    registry: SessionRegistry
    bus: Bus
    push: PushNotifier
    tokens: dict[str, str]  # token → user (stand-in for verified JWTs)

    @classmethod
    def create(cls, tokens: dict[str, str]) -> Cluster:
        return cls(MessageStore(), PresenceService(), SessionRegistry(), Bus(), PushNotifier(), tokens)


def message_event(m: Any) -> Event:
    d = asdict(m)
    return {
        "type": "message",
        "conversationId": d["conversation_id"],
        "seq": d["seq"],
        "messageId": d["message_id"],
        "clientMsgId": d["client_msg_id"],
        "sender": d["sender"],
        "body": d["body"],
        "sentAt": d["sent_at"],
    }


def create_gateway(cluster: Cluster, gateway_id: str) -> FastAPI:
    app = FastAPI(title=f"Messaging gateway {gateway_id}")
    outboxes: dict[str, asyncio.Queue[Event]] = {}

    def deliver_local(connection: str, event: Event) -> None:
        queue = outboxes.get(connection)
        if queue is not None:
            queue.put_nowait(event)

    def route(user: str, event: Event, conversation_id: str, sender: str) -> None:
        sessions = cluster.registry.sessions(user)
        if not sessions:
            if event["type"] == "message":
                cluster.push.notify(user, conversation_id, sender)
            return
        for gateway, connection in sessions:  # every device of the user
            cluster.bus.publish(gateway, connection, event)

    @app.get("/health/live")
    def live() -> dict[str, str]:
        return {"status": "ok", "gateway": gateway_id}

    @app.websocket("/ws")
    async def ws(websocket: WebSocket, token: str = "") -> None:
        user = cluster.tokens.get(token)
        if user is None:
            await websocket.close(code=4401)
            return
        await websocket.accept()
        connection = uuid.uuid4().hex
        queue: asyncio.Queue[Event] = asyncio.Queue(maxsize=1000)
        outboxes[connection] = queue
        cluster.bus.subscribe(gateway_id, asyncio.get_running_loop(), deliver_local)
        cluster.registry.add(user, gateway_id, connection)
        cluster.presence.heartbeat(user, connection)
        await websocket.send_json(
            {
                "type": "hello",
                "user": user,
                "conversations": [
                    {"id": c.id, "lastSeq": c.next_seq - 1, "delivered": c.delivered.get(user, 0)}
                    for c in cluster.store.conversations_of(user)
                ],
            }
        )

        async def writer() -> None:
            while True:
                await websocket.send_json(await queue.get())

        writer_task = asyncio.create_task(writer())
        try:
            while True:
                frame = await websocket.receive_json()
                try:
                    reply = handle(user, connection, frame)
                except (MessagingError, NotAMember, KeyError, TypeError, ValueError) as e:
                    reply = {"type": "error", "code": type(e).__name__, "message": str(e)}
                if reply is not None:
                    await websocket.send_json(reply)
        except WebSocketDisconnect:
            pass
        finally:
            writer_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await writer_task
            outboxes.pop(connection, None)
            cluster.registry.remove(user, gateway_id, connection)
            cluster.presence.disconnect(user, connection)

    def handle(user: str, connection: str, f: dict[str, Any]) -> Event | None:
        kind = f.get("type")
        store = cluster.store
        if kind == "send":
            msg, created = store.append(f["conversationId"], user, str(f["clientMsgId"]), str(f["body"]))
            if created:  # fan out once; a retried send is acked again but not re-delivered
                conv = store.conversation(msg.conversation_id, user)
                for member in conv.members - {user}:
                    route(member, message_event(msg), msg.conversation_id, user)
            return {
                "type": "ack",
                "clientMsgId": msg.client_msg_id,
                "seq": msg.seq,
                "messageId": msg.message_id,
                "duplicate": not created,
            }
        if kind in ("delivered", "read"):
            state = ReceiptState(kind)
            mark = store.mark(f["conversationId"], user, state, int(f["upToSeq"]))
            conv = store.conversation(f["conversationId"], user)
            receipt = {
                "type": "receipt",
                "conversationId": conv.id,
                "user": user,
                "state": state.value,
                "upToSeq": mark,
            }
            for member in conv.members - {user}:
                route(member, receipt, conv.id, user)
            return None
        if kind == "sync":
            items = store.messages_after(f["conversationId"], user, int(f.get("afterSeq", 0)))
            return {
                "type": "messages",
                "conversationId": f["conversationId"],
                "items": [message_event(m) for m in items],
            }
        if kind == "ping":
            cluster.presence.heartbeat(user, connection)
            return {"type": "pong"}
        if kind == "presence":
            return {"type": "presence", **cluster.presence.view(str(f["user"]), user)}
        if kind == "create_group":
            conv = store.create_group(user, set(map(str, f["members"])))
            return {"type": "group", "conversationId": conv.id}
        if kind == "direct":
            return {"type": "direct", "conversationId": store.direct(user, str(f["user"])).id}
        return {"type": "error", "code": "UnknownFrame", "message": f"unknown frame type {kind!r}"}

    return app
