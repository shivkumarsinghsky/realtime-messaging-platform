# ADR-004: End-to-End Encryption — Server Stores Ciphertext Only

- **Status:** Accepted
- **Date:** 2026-10-01

## Context

Private messaging requires that a compromise of servers (or an insider) does not expose message content.

## Decision

Adopt a Signal-protocol-style design: device identity keys and prekeys in a key directory, X3DH session setup, Double
Ratchet per message, sender keys for groups, client-side media encryption. The server routes and stores opaque
ciphertext; push notifications carry no content.

## Alternatives Considered

- **Transport encryption only (TLS)** — protects the network path only; the server can read everything.
- **Server-side encryption at rest** — protects disks, not against the service itself.

## Trade-offs

No server-side search, previews or content moderation; key management for multi-device and backups is complex.

## Consequences

The prototype treats bodies as opaque and does not implement cryptography; the design documents the key flows.
