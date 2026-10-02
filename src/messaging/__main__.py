"""Run one gateway: python -m messaging [--port 8000] [--gateway gw-1]. Dev tokens: token-<user>."""

from __future__ import annotations

import argparse

import uvicorn

from messaging.gateway import Cluster, create_gateway


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--port", type=int, default=8000)
    p.add_argument("--gateway", default="gw-1")
    args = p.parse_args()
    users = ["alice", "bob", "carol", "dave"]
    cluster = Cluster.create({f"token-{u}": u for u in users})
    uvicorn.run(create_gateway(cluster, args.gateway), host="0.0.0.0", port=args.port)


if __name__ == "__main__":
    main()
