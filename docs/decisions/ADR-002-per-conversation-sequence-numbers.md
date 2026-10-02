# ADR-002: Per-Conversation Sequence Numbers

- **Status:** Accepted
- **Date:** 2026-10-01

## Context

Users must see messages in the same order on every device, detect missing messages and resynchronise after being
offline. Client clocks cannot be trusted for ordering, and global ordering across all conversations does not scale.

## Decision

The store assigns a monotonically increasing `seq` per conversation at write time. Clients order by `seq`, use gaps to
trigger `sync(afterSeq)`, and acknowledge with `seq` watermarks.

## Alternatives Considered

- **Timestamps** — clock skew causes reordering; ties need tie-breakers.
- **Global sequence (e.g. a single counter or Snowflake ids)** — unique and roughly ordered, but no gap detection per
  conversation.
- **Vector clocks / CRDT ordering** — needed for offline multi-writer editing; overkill for chat.

## Trade-offs

All writes to one conversation go through one partition leader; extremely hot conversations (huge groups) are bounded
by that leader's throughput.

## Consequences

Sync, receipts and duplicate detection are all expressed in terms of `seq`.
