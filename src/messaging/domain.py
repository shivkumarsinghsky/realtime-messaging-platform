"""Core messaging model: conversations, ordered message log, idempotent sends and receipts.

Ordering guarantee: messages are totally ordered **per conversation** by a server-assigned sequence number. There is
no global order (it is neither needed nor scalable). Clients display by `seq`, detect gaps, and sync with
`messages_after(seq)`.
"""

from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum

MAX_GROUP_SIZE = 1024
MAX_BODY_BYTES = 64 * 1024


class ReceiptState(str, Enum):
    SENT = "sent"  # stored by the server
    DELIVERED = "delivered"  # recipient device acknowledged
    READ = "read"  # recipient opened the conversation


@dataclass(frozen=True)
class Message:
    conversation_id: str
    seq: int
    message_id: str
    client_msg_id: str
    sender: str
    #: Opaque payload. With end-to-end encryption this is ciphertext the server cannot read.
    body: str
    sent_at: float


@dataclass
class Conversation:
    id: str
    kind: str  # "direct" | "group"
    members: set[str]
    next_seq: int = 1
    log: list[Message] = field(default_factory=list)
    #: (sender, client_msg_id) → message, so client retries never duplicate a message
    dedup: dict[tuple[str, str], Message] = field(default_factory=dict)
    #: per member: highest seq delivered / read
    delivered: dict[str, int] = field(default_factory=dict)
    read: dict[str, int] = field(default_factory=dict)


class MessagingError(ValueError):
    pass


class NotAMember(PermissionError):
    pass


class MessageStore:
    """In-memory store with the semantics a production store must provide (see docs/architecture.md#data-model):
    atomic per-conversation sequence assignment, idempotency on (sender, client id), range reads by seq."""

    def __init__(self) -> None:
        self._conversations: dict[str, Conversation] = {}
        self._lock = threading.Lock()

    # ---- conversations -------------------------------------------------------------------------------

    def direct(self, a: str, b: str) -> Conversation:
        if a == b:
            raise MessagingError("cannot start a direct conversation with yourself")
        cid = "d:" + ":".join(sorted((a, b)))
        with self._lock:
            return self._conversations.setdefault(cid, Conversation(cid, "direct", {a, b}))

    def create_group(self, creator: str, members: set[str]) -> Conversation:
        members = members | {creator}
        if len(members) > MAX_GROUP_SIZE:
            raise MessagingError(f"groups are limited to {MAX_GROUP_SIZE} members")
        cid = "g:" + uuid.uuid4().hex[:12]
        with self._lock:
            conv = self._conversations[cid] = Conversation(cid, "group", set(members))
        return conv

    def conversation(self, cid: str, user: str) -> Conversation:
        conv = self._conversations.get(cid)
        if conv is None or user not in conv.members:
            raise NotAMember("conversation not found")  # same answer for missing and forbidden
        return conv

    # ---- messages ------------------------------------------------------------------------------------

    def append(self, cid: str, sender: str, client_msg_id: str, body: str) -> tuple[Message, bool]:
        """Returns (message, created). A retry with the same client id returns the original message."""
        if not client_msg_id or len(client_msg_id) > 64:
            raise MessagingError("client_msg_id is required (max 64 chars)")
        if len(body.encode()) > MAX_BODY_BYTES:
            raise MessagingError("message too large; send media as an attachment reference")
        with self._lock:
            conv = self.conversation(cid, sender)
            existing = conv.dedup.get((sender, client_msg_id))
            if existing:
                return existing, False
            msg = Message(cid, conv.next_seq, uuid.uuid4().hex, client_msg_id, sender, body, time.time())
            conv.next_seq += 1
            conv.log.append(msg)
            conv.dedup[(sender, client_msg_id)] = msg
            conv.delivered[sender] = msg.seq  # the sender trivially has their own message
            conv.read[sender] = msg.seq
            return msg, True

    def messages_after(self, cid: str, user: str, after_seq: int, limit: int = 200) -> list[Message]:
        conv = self.conversation(cid, user)
        return [m for m in conv.log if m.seq > after_seq][:limit]

    def mark(self, cid: str, user: str, state: ReceiptState, up_to_seq: int) -> int:
        """Receipts are cumulative watermarks ("everything up to seq N"), so one ack covers many messages and
        out-of-order acks are harmless. Returns the new watermark."""
        with self._lock:
            conv = self.conversation(cid, user)
            up_to_seq = min(up_to_seq, conv.next_seq - 1)
            marks = conv.read if state is ReceiptState.READ else conv.delivered
            marks[user] = max(marks.get(user, 0), up_to_seq)
            if state is ReceiptState.READ:  # read implies delivered
                conv.delivered[user] = max(conv.delivered.get(user, 0), up_to_seq)
            return marks[user]

    def status(self, cid: str, user: str, seq: int) -> ReceiptState:
        """Aggregate status of message `seq` for its sender: READ when all other members read it, etc."""
        conv = self.conversation(cid, user)
        others = conv.members - {user}
        if others and all(conv.read.get(m, 0) >= seq for m in others):
            return ReceiptState.READ
        if others and all(conv.delivered.get(m, 0) >= seq for m in others):
            return ReceiptState.DELIVERED
        return ReceiptState.SENT

    def conversations_of(self, user: str) -> list[Conversation]:
        return [c for c in self._conversations.values() if user in c.members]
