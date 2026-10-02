---
"emdash": minor
---

Adds experimental `locals.emdash.handleContentCreateOnce()` for opt-in content creation on SQLite with durable result replay. The host supplies a tenant, authenticated actor, operation namespace/key and authoritative binding after authorization and payment enforcement. Reusing an identity with changed input returns a conflict.

Content, replay result and an effect event commit together. Active save hooks and surrounding transactions are rejected. Ordinary content creation keeps its existing behavior.

Use `locals.emdash.deliverContentCreateEffects()` to process pending events after commit. The host's delivery callback owns media refresh and cache invalidation and must tolerate repeated delivery. D1, libSQL, PostgreSQL, arbitrary plugin effects and power-loss durability are outside this experimental contract.
