"""Two gateway instances share the cluster services: users connected to different gateways can chat, which is the
horizontal-scaling property of the design."""

import pytest
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from messaging.gateway import Cluster, create_gateway

USERS = ["alice", "bob", "carol"]


@pytest.fixture
def cluster():
    return Cluster.create({f"token-{u}": u for u in USERS})


def connect(client: TestClient, user: str):
    ws = client.websocket_connect(f"/ws?token=token-{user}")
    session = ws.__enter__()
    hello = session.receive_json()
    assert hello["type"] == "hello" and hello["user"] == user
    return ws, session, hello


def test_cross_gateway_delivery_receipts_and_retries(cluster):
    gw1, gw2 = TestClient(create_gateway(cluster, "gw-1")), TestClient(create_gateway(cluster, "gw-2"))
    a_cm, alice, _ = connect(gw1, "alice")
    b_cm, bob, _ = connect(gw2, "bob")
    try:
        alice.send_json({"type": "direct", "user": "bob"})
        cid = alice.receive_json()["conversationId"]

        alice.send_json({"type": "send", "conversationId": cid, "clientMsgId": "m-1", "body": "ciphertext-1"})
        ack = alice.receive_json()
        assert ack == {"type": "ack", "clientMsgId": "m-1", "seq": 1, "messageId": ack["messageId"], "duplicate": False}
        received = bob.receive_json()
        assert (received["type"], received["seq"], received["sender"], received["body"]) == (
            "message",
            1,
            "alice",
            "ciphertext-1",
        )

        # client retry after a lost ack: same id → same seq, flagged duplicate, NOT delivered to bob again
        alice.send_json({"type": "send", "conversationId": cid, "clientMsgId": "m-1", "body": "ciphertext-1"})
        assert alice.receive_json()["duplicate"] is True

        bob.send_json({"type": "read", "conversationId": cid, "upToSeq": 1})
        receipt = alice.receive_json()
        assert receipt == {"type": "receipt", "conversationId": cid, "user": "bob", "state": "read", "upToSeq": 1}

        bob.send_json({"type": "send", "conversationId": cid, "clientMsgId": "b-1", "body": "ciphertext-2"})
        assert bob.receive_json()["seq"] == 2
        assert alice.receive_json()["seq"] == 2  # the next frame alice gets is bob's message: no duplicate of m-1

        bob.send_json({"type": "presence", "user": "alice"})
        assert bob.receive_json() == {"type": "presence", "user": "alice", "online": True}
    finally:
        a_cm.__exit__(None, None, None)
        b_cm.__exit__(None, None, None)
    assert cluster.store.status(cid, "alice", 1).value == "read"


def test_offline_member_gets_push_then_syncs_on_reconnect(cluster):
    gw = TestClient(create_gateway(cluster, "gw-1"))
    a_cm, alice, _ = connect(gw, "alice")
    try:
        alice.send_json({"type": "create_group", "members": ["bob", "carol"]})
        gid = alice.receive_json()["conversationId"]
        for i in range(3):
            alice.send_json({"type": "send", "conversationId": gid, "clientMsgId": f"g{i}", "body": f"msg {i}"})
            alice.receive_json()
    finally:
        a_cm.__exit__(None, None, None)
    pushes = [p for p in cluster.push.sent if p["user"] == "carol"]
    assert len(pushes) == 3 and all("body" not in p for p in pushes)  # no content in push payloads

    c_cm, carol, hello = connect(gw, "carol")
    try:
        conv = next(c for c in hello["conversations"] if c["id"] == gid)
        assert (conv["lastSeq"], conv["delivered"]) == (3, 0)  # tells the client what to sync
        carol.send_json({"type": "sync", "conversationId": gid, "afterSeq": conv["delivered"]})
        items = carol.receive_json()["items"]
        assert [m["seq"] for m in items] == [1, 2, 3]
        carol.send_json({"type": "delivered", "conversationId": gid, "upToSeq": 3})
        carol.send_json({"type": "ping"})
        assert carol.receive_json() == {"type": "pong"}
    finally:
        c_cm.__exit__(None, None, None)
    assert cluster.store.conversation(gid, "alice").delivered["carol"] == 3


def test_multi_device_fan_out(cluster):
    gw1, gw2 = TestClient(create_gateway(cluster, "gw-1")), TestClient(create_gateway(cluster, "gw-2"))
    a_cm, alice, _ = connect(gw1, "alice")
    phone_cm, bob_phone, _ = connect(gw1, "bob")
    laptop_cm, bob_laptop, _ = connect(gw2, "bob")
    try:
        alice.send_json({"type": "direct", "user": "bob"})
        cid = alice.receive_json()["conversationId"]
        alice.send_json({"type": "send", "conversationId": cid, "clientMsgId": "x", "body": "both devices"})
        alice.receive_json()
        assert bob_phone.receive_json()["body"] == "both devices"
        assert bob_laptop.receive_json()["body"] == "both devices"
    finally:
        for cm in (a_cm, phone_cm, laptop_cm):
            cm.__exit__(None, None, None)


def test_auth_and_authorization(cluster):
    gw = TestClient(create_gateway(cluster, "gw-1"))
    with pytest.raises(WebSocketDisconnect), gw.websocket_connect("/ws?token=forged") as ws:
        ws.receive_json()
    conv = cluster.store.direct("alice", "bob")
    cm, carol, _ = connect(gw, "carol")
    try:
        carol.send_json({"type": "send", "conversationId": conv.id, "clientMsgId": "x", "body": "intrude"})
        assert carol.receive_json()["code"] == "NotAMember"
        carol.send_json({"type": "teleport"})
        assert carol.receive_json()["code"] == "UnknownFrame"
    finally:
        cm.__exit__(None, None, None)
