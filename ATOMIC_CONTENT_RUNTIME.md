# Experimental atomic content creation

This local prototype adds an opt-in runtime boundary for content writes on
SQLite. The full runtime performs normalization and schema validation through
a transaction scoped with AsyncLocalStorage. Content, the saved result and
a pending effect event commit together. Ordinary create calls keep their
current behavior.

The host calls `handleContentCreateOnce(collection, body, operation)` after
authenticating and authorizing each request and validating upfront settlement.
The runtime is payment-agnostic: it does not validate a Kaspa payment, contact
a facilitator or derive PaymentRequirements. Those responsibilities remain
with the resource server and its adapter.

## Operation identity

The operation contains `namespace`, `tenant`, `actorId`, `key` and `binding`.
Supply these from trusted host state. A client idempotency key alone is
insufficient. The binding must include authoritative payment/resource terms
and request identity. The runtime also binds collection and the captured
body, rejects a conflicting actor, and uses versioned structured JSON plus
SHA-256 to avoid delimiter aliases. JSON object key order does not affect
identity. Non-JSON values, including undefined properties and sparse arrays,
are rejected. Caller changes after invocation cannot alter the captured input.

## Transaction and effects

The method requires its own real transaction and currently accepts SQLite
only. It rejects an existing transaction so it returns a result only after
the owning transaction commits. It does not use D1's non-transactional
fallback. A validation or handler failure aborts the claim. Failure to store
the replay result or effect event rolls back the actual content write.

The runtime uses a nested request context for its database override. It
does not mutate the singleton connection. Schema registry, normalization,
validation and repositories use the transaction through the existing
request-aware database getter.

Active beforeSave or afterSave hooks return NOT_SUPPORTED before content
work. The prototype cannot make arbitrary plugin effects transactional.
Existing create calls still execute their configured hooks.

Creation queues one durable effect event containing its ID, collection and
content ID. The host calls `deliverContentCreateEffects(deliver, limit)`
after commit. The callback must complete media refresh and cache invalidation,
or arrange them durably, before resolving. Delivery errors leave the event
pending. Losing acknowledgement can deliver an event again; use its stable ID
for consumer deduplication. Delivery is at least once, including when two
consumers overlap. No automatic scheduler or plugin dispatcher is provided.

## Reproduce runtime tests

Install the pinned workspace dependencies and build from the root before
running runtime tests.

```sh
pnpm install --frozen-lockfile
pnpm build
pnpm --filter emdash exec vitest run tests/integration/runtime/atomic-content-create.test.ts
```

The tests use real file-backed SQLite and complete runtime initialization.
They cover normalization and replay through a reopened runtime, overlapping
calls on one or two SQLite connections, request-context isolation, changed
binding/body, delimiter aliases,
validation failure, caller mutation, hook rejection, replay/outbox persistence
failure, uncertain delivery, nested transactions and migration retry after
partial completion. A concurrent connection may receive a SQLite lock error;
retrying the same identity replays the committed result.

The previous adapter database probe independently exercised process kills
before and after commit against the lower-level content handler. Those
results do not replace a process-crash proof for this runtime implementation.

## Release and storage limits

The prototype keeps completed operations and effects indefinitely and does
not offer deletion, lease-based takeover or result reconstruction. Results
are limited to one MiB. Corrupt or absent stored results fail without invoking
content creation again.

The guarantee is one committed database creation per identity. A transaction
that rolls back can invoke normalization and the handler again on retry.
External media providers, plugins, HTTP response construction, settlement,
D1/libSQL/PostgreSQL parity, multi-host deployment and power-loss recovery
need separate evidence. SQLite's existing WAL adapter uses synchronous NORMAL.
Keep this local branch experimental; do not infer production action-once for
arbitrary work from these database boundaries.
