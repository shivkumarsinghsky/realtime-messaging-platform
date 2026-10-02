import pytest

from messaging.domain import MAX_GROUP_SIZE, MessageStore, MessagingError, NotAMember, ReceiptState
from messaging.presence import PresenceService


def test_per_conversation_ordering_and_idempotent_send():
    s = MessageStore()
    c = s.direct("alice", "bob")
    m1, created1 = s.append(c.id, "alice", "c-1", "hi")
    m2, _ = s.append(c.id, "bob", "c-1", "hey")  # same client id, different sender: a different message
    retry, created_retry = s.append(c.id, "alice", "c-1", "hi")
    assert (m1.seq, m2.seq) == (1, 2)
    assert created1 and not created_retry and retry.message_id == m1.message_id
    assert [m.seq for m in s.messages_after(c.id, "bob", 0)] == [1, 2]
    assert [m.seq for m in s.messages_after(c.id, "bob", 1)] == [2]
    assert s.direct("bob", "alice").id == c.id


def test_membership_and_limits():
    s = MessageStore()
    c = s.direct("alice", "bob")
    with pytest.raises(NotAMember):
        s.append(c.id, "mallory", "x", "spam")
    with pytest.raises(NotAMember):
        s.messages_after(c.id, "mallory", 0)
    with pytest.raises(MessagingError):
        s.append(c.id, "alice", "", "no id")
    with pytest.raises(MessagingError):
        s.append(c.id, "alice", "big", "x" * 70_000)
    with pytest.raises(MessagingError):
        s.create_group("alice", {f"u{i}" for i in range(MAX_GROUP_SIZE)})
    with pytest.raises(MessagingError):
        s.direct("alice", "alice")


def test_receipts_are_cumulative_watermarks_and_aggregate_per_group():
    s = MessageStore()
    g = s.create_group("alice", {"bob", "carol"})
    for i in range(3):
        s.append(g.id, "alice", f"c{i}", "x")
    assert s.status(g.id, "alice", 3) is ReceiptState.SENT
    assert s.mark(g.id, "bob", ReceiptState.DELIVERED, 3) == 3
    assert s.mark(g.id, "bob", ReceiptState.DELIVERED, 1) == 3  # late, out-of-order ack does not move it back
    assert s.status(g.id, "alice", 3) is ReceiptState.SENT  # carol has not received it yet
    s.mark(g.id, "carol", ReceiptState.READ, 99)  # clamped to the last seq; read implies delivered
    assert s.status(g.id, "alice", 3) is ReceiptState.DELIVERED
    s.mark(g.id, "bob", ReceiptState.READ, 2)
    assert s.status(g.id, "alice", 2) is ReceiptState.READ
    assert s.status(g.id, "alice", 3) is ReceiptState.DELIVERED


def test_presence_leases_and_privacy():
    now = [0.0]
    p = PresenceService(ttl=30, clock=lambda: now[0], wall_clock=lambda: 1_000 + now[0])
    p.heartbeat("alice", "phone")
    assert p.is_online("alice")
    now[0] = 31  # missed heartbeats: lease expired (crashed app or gateway)
    assert p.view("alice", "bob") == {"user": "alice", "online": False, "lastSeen": 1_000.0}
    p.hide_last_seen.add("alice")
    assert "lastSeen" not in p.view("alice", "bob")
    p.heartbeat("alice", "laptop")
    p.disconnect("alice", "phone")
    assert p.is_online("alice")  # still online on another device
