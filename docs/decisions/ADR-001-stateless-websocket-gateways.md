# ADR-001: Stateless WebSocket Gateways With a Session Registry

- **Status:** Accepted
- **Date:** 2026-10-01

## Context

Tens of millions of long-lived connections must be spread over hundreds of nodes, which are deployed, scaled and
replaced continuously. A message for Bob arrives at whatever node Alice is connected to.

## Decision

Gateways only hold sockets and per-connection outbound queues. A registry (`user → (gateway, connection)` with TTL)
records where users are connected; the message service publishes delivery events to the recipient gateway's channel.

## Alternatives Considered

- **Sticky routing by user id (consistent hashing)** — no registry lookup, but rebalancing on node changes moves many
  connections and multi-device users span nodes anyway.
- **Gateways forward to each other (mesh)** — no central registry; N² connectivity and harder failure handling.

## Trade-offs

A registry lookup per delivery and a pub/sub hop. Registry staleness is bounded by lease TTL.

## Consequences

Gateways can be drained and replaced freely; the prototype tests delivery between two gateway instances.
