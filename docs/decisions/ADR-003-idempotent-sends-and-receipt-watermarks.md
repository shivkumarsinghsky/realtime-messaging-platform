# ADR-003: Idempotent Sends and Cumulative Receipt Watermarks

- **Status:** Accepted
- **Date:** 2026-10-01

## Context

Mobile networks drop acks; clients must retry sends. Receipts for every message would multiply traffic, and receipts
arrive out of order across devices.

## Decision

- Clients generate `clientMsgId` once per message; the store deduplicates on `(sender, clientMsgId)` and returns the
  original `seq`; duplicates are not fanned out again.
- Delivered/read receipts are monotonic watermarks per member (`max(current, upToSeq)`); read implies delivered.

## Alternatives Considered

- **Server-generated ids only** — retries create duplicates.
- **Per-message receipts** — precise but chatty and order-sensitive.

## Trade-offs

The dedup index must be retained at least as long as clients may retry (hours/days).

## Consequences

Tests verify that a retried send is acked as duplicate and that late receipts cannot move status backwards.
