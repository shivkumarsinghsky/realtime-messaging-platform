# ADR-005: Wide-Column Message Store Partitioned by Conversation

- **Status:** Accepted
- **Date:** 2026-10-01

## Context

The workload is billions of small appends per day and range reads "messages of conversation C after seq N".
Joins and ad-hoc queries are not needed on the hot path.

## Decision

Use a wide-column store (Cassandra/ScyllaDB-style) with partition key `conversation_id` (bucketed for very large
conversations) and clustering key `seq`; replicate across zones; write with quorum consistency.

## Alternatives Considered

- **Sharded relational database** — strong consistency and familiar tooling; resharding and write scaling are harder
  at this volume.
- **Kafka as the store** — great append log, poor random access per conversation.
- **Key-value store with one key per message** — range reads need a secondary index.

## Trade-offs

Atomic sequence assignment needs a lightweight transaction or a per-conversation leader (e.g. the message service
owning conversation partitions).

## Consequences

The prototype's `MessageStore` encodes the required semantics (atomic seq, dedup index, range reads) behind one class.
