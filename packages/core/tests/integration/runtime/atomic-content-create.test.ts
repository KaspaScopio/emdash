import { randomUUID } from "node:crypto";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { sql, SqliteDialect } from "kysely";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { NodeSqliteCompatDatabase } from "#node-sqlite";

import type { ContentCreateOperation } from "../../../src/content/atomic-create.js";
import { up as addAtomicTables } from "../../../src/database/migrations/092_atomic_content_create.js";
import { EmDashRuntime } from "../../../src/emdash-runtime.js";
import type { RuntimeDependencies } from "../../../src/emdash-runtime.js";
import { definePlugin } from "../../../src/plugins/define-plugin.js";
import type { ResolvedPlugin } from "../../../src/plugins/types.js";
import { getRequestContext, runWithContext } from "../../../src/request-context.js";
import { SchemaRegistry } from "../../../src/schema/registry.js";

describe("atomic runtime content creation", () => {
	const operation: ContentCreateOperation = {
		namespace: "cms:posts:create",
		tenant: "tenant-one",
		actorId: "editor-one",
		key: "operation-one",
		binding: "validated-host-binding-one",
	};
	let runtime: EmDashRuntime;
	let path: string;
	let scratch: string;
	const runtimes: EmDashRuntime[] = [];

	async function boot(plugins: ResolvedPlugin[] = [], database = path) {
		const sqlite = new NodeSqliteCompatDatabase(database, { journalMode: "wal" });
		const dependencies: RuntimeDependencies = {
			config: {
				database: { entrypoint: "atomic-runtime-" + randomUUID(), config: {}, type: "sqlite" },
			},
			plugins,
			createDialect: () => new SqliteDialect({ database: sqlite }),
			createStorage: null,
			sandboxEnabled: false,
			sandboxedPluginEntries: [],
			createSandboxRunner: null,
		};
		const instance = await EmDashRuntime.create(dependencies);
		runtimes.push(instance);
		return instance;
	}
	async function counts(instance = runtime) {
		const content = await instance.handleContentList("posts", { limit: 100 });
		expect(content.success).toBe(true);
		return {
			posts: content.success ? content.data.items.length : -1,
			operations: (await instance.db.selectFrom("_emdash_content_operations").selectAll().execute())
				.length,
			effects: (
				await instance.db.selectFrom("_emdash_content_create_effects").selectAll().execute()
			).length,
		};
	}
	beforeEach(async () => {
		scratch = mkdtempSync(join(tmpdir(), "emdash-atomic-runtime-"));
		path = join(scratch, "content.db");
		runtime = await boot();
		const registry = new SchemaRegistry(runtime.db);
		await registry.createCollection({ slug: "posts", label: "Posts" });
		await registry.createField("posts", {
			slug: "title",
			label: "Title",
			type: "string",
			required: true,
		});
	});
	afterEach(async () => {
		for (const instance of runtimes.splice(0)) {
			await instance.shutdown();
			await instance.db.destroy();
		}
		rmSync(scratch, { recursive: true, force: true });
	});

	it("stores and replays a normalized real runtime result after opening a new runtime", async () => {
		await runtime.schemaRegistry.createField("posts", {
			slug: "tags",
			label: "Tags",
			type: "multiSelect",
		});
		const body = { data: { title: "Content once", tags: "" } };
		const first = await runtime.handleContentCreateOnce("posts", body, operation);
		expect(first).toMatchObject({
			success: true,
			data: { item: { data: { title: "Content once" } } },
		});
		if (!first.success) throw new Error("Content creation failed");
		expect(first.data.item.data).not.toHaveProperty("tags");
		const stored = await sql<{
			tags: string | null;
		}>`SELECT tags FROM ec_posts WHERE id = ${first.data.item.id}`.execute(runtime.db);
		expect(stored.rows[0]?.tags).toBeNull();
		const reopened = await boot();
		const retry = await reopened.handleContentCreateOnce("posts", body, operation);
		expect(retry).toEqual(first);
		expect(await counts(reopened)).toEqual({ posts: 1, operations: 1, effects: 1 });
	});
	it("coalesces overlapping calls without leaking the transaction into the caller context", async () => {
		const initial = runtime.db;
		const context = { editMode: true, locale: "en", db: initial };
		await runWithContext(context, async () => {
			const body = { data: { title: "Concurrent content" } };
			const [first, second] = await Promise.all([
				runtime.handleContentCreateOnce("posts", body, operation),
				runtime.handleContentCreateOnce("posts", body, operation),
			]);
			expect(first.success).toBe(true);
			expect(second).toEqual(first);
			expect(getRequestContext()).toBe(context);
			expect(runtime.db).toBe(initial);
		});
		expect(await counts()).toEqual({ posts: 1, operations: 1, effects: 1 });
	});
	it("keeps one committed creation across two independent SQLite connections", async () => {
		const other = await boot();
		const body = { data: { title: "Connection race" } };
		const results = await Promise.all([
			runtime.handleContentCreateOnce("posts", body, operation),
			other.handleContentCreateOnce("posts", body, operation),
		]);
		const success = results.find((result) => result.success);
		expect(success?.success).toBe(true);
		expect(await runtime.handleContentCreateOnce("posts", body, operation)).toEqual(success);
		expect(await other.handleContentCreateOnce("posts", body, operation)).toEqual(success);
		expect(await counts(other)).toEqual({ posts: 1, operations: 1, effects: 1 });
	}, 20_000);
	it("rejects changed body or host binding under the same identity", async () => {
		await runtime.handleContentCreateOnce("posts", { data: { title: "Original" } }, operation);
		const changedBody = await runtime.handleContentCreateOnce(
			"posts",
			{ data: { title: "Changed" } },
			operation,
		);
		const changedBinding = await runtime.handleContentCreateOnce(
			"posts",
			{ data: { title: "Original" } },
			{ ...operation, binding: "different-settlement-or-terms" },
		);
		expect(changedBody).toMatchObject({ success: false, error: { code: "CONFLICT" } });
		expect(changedBinding).toMatchObject({ success: false, error: { code: "CONFLICT" } });
		expect(await counts()).toEqual({ posts: 1, operations: 1, effects: 1 });
	});
	it("keeps structured identities distinct even when delimiter joins would alias", async () => {
		const body = { data: { title: "Separate tenants" } };
		const first = await runtime.handleContentCreateOnce("posts", body, {
			...operation,
			namespace: "a/b",
			tenant: "c",
		});
		const second = await runtime.handleContentCreateOnce("posts", body, {
			...operation,
			namespace: "a",
			tenant: "b/c",
		});
		expect(first.success && second.success && first.data.item.id !== second.data.item.id).toBe(
			true,
		);
		expect(await counts()).toEqual({ posts: 2, operations: 2, effects: 2 });
	});
	it("reserves nothing when schema validation rejects content", async () => {
		const failed = await runtime.handleContentCreateOnce(
			"posts",
			{ data: { title: 42 } },
			operation,
		);
		expect(failed.success).toBe(false);
		expect(await counts()).toEqual({ posts: 0, operations: 0, effects: 0 });
		const corrected = await runtime.handleContentCreateOnce(
			"posts",
			{ data: { title: "Corrected" } },
			operation,
		);
		expect(corrected.success).toBe(true);
		expect(await counts()).toEqual({ posts: 1, operations: 1, effects: 1 });
	});
	it("captures caller input before asynchronous identity derivation", async () => {
		const body = { data: { title: "Captured" } };
		const identity = { ...operation };
		const pending = runtime.handleContentCreateOnce("posts", body, identity);
		body.data.title = "Mutated";
		identity.key = "mutated-key";
		const created = await pending;
		expect(created).toMatchObject({
			success: true,
			data: { item: { data: { title: "Captured" } } },
		});
		expect(
			await runtime.handleContentCreateOnce("posts", { data: { title: "Captured" } }, operation),
		).toEqual(created);
		expect(await counts()).toEqual({ posts: 1, operations: 1, effects: 1 });
	});
	it.each(["content:beforeSave", "content:afterSave"] as const)(
		"rejects active %s without running it",
		async (hook) => {
			let hookRuns = 0;
			const hooked = await boot([
				definePlugin({
					id: "save-side-effect",
					version: "1.0.0",
					capabilities: ["content:write"],
					hooks: {
						[hook]: async () => {
							hookRuns += 1;
						},
					},
				}),
			]);
			const rejected = await hooked.handleContentCreateOnce(
				"posts",
				{ data: { title: "Protected" } },
				operation,
			);
			expect(rejected).toMatchObject({ success: false, error: { code: "NOT_SUPPORTED" } });
			expect(hookRuns).toBe(0);
			expect(await counts(hooked)).toEqual({ posts: 0, operations: 0, effects: 0 });
			const ordinary = await hooked.handleContentCreate("posts", { data: { title: "Ordinary" } });
			expect(ordinary.success).toBe(true);
		},
	);
	it("rolls back the actual CMS write when replay persistence fails", async () => {
		await sql`CREATE TRIGGER fail_replay BEFORE UPDATE ON _emdash_content_operations
			BEGIN SELECT RAISE(ABORT, 'injected replay failure'); END`.execute(runtime.db);
		const failed = await runtime.handleContentCreateOnce(
			"posts",
			{ data: { title: "Must roll back" } },
			operation,
		);
		expect(failed.success).toBe(false);
		expect(await counts()).toEqual({ posts: 0, operations: 0, effects: 0 });
		await sql`DROP TRIGGER fail_replay`.execute(runtime.db);
		expect(
			(
				await runtime.handleContentCreateOnce(
					"posts",
					{ data: { title: "Must roll back" } },
					operation,
				)
			).success,
		).toBe(true);
		expect(await counts()).toEqual({ posts: 1, operations: 1, effects: 1 });
	});
	it("rolls back content and replay when outbox persistence fails", async () => {
		await sql`CREATE TRIGGER fail_outbox BEFORE INSERT ON _emdash_content_create_effects
			BEGIN SELECT RAISE(ABORT, 'injected outbox failure'); END`.execute(runtime.db);
		expect(
			(
				await runtime.handleContentCreateOnce(
					"posts",
					{ data: { title: "Must roll back" } },
					operation,
				)
			).success,
		).toBe(false);
		expect(await counts()).toEqual({ posts: 0, operations: 0, effects: 0 });
	});
	it("retains uncertain effect delivery for retry without repeating content creation", async () => {
		await runtime.handleContentCreateOnce(
			"posts",
			{ data: { title: "Queued effects" } },
			operation,
		);
		const delivered: string[] = [];
		await expect(
			runtime.deliverContentCreateEffects(async (effect) => {
				delivered.push(effect.id);
				throw new Error("Delivery acknowledgement lost");
			}),
		).rejects.toThrow("Delivery acknowledgement lost");
		const pending = await runtime.db
			.selectFrom("_emdash_content_create_effects")
			.selectAll()
			.executeTakeFirstOrThrow();
		expect(pending.completed_at).toBeNull();
		const reopened = await boot();
		expect(
			await reopened.deliverContentCreateEffects(async (effect) => {
				delivered.push(effect.id);
				const content = await reopened.handleContentGet(effect.collection, effect.contentId);
				expect(content).toMatchObject({
					success: true,
					data: { item: { data: { title: "Queued effects" } } },
				});
			}),
		).toBe(1);
		expect(delivered).toEqual([pending.id, pending.id]);
		expect(
			await reopened.deliverContentCreateEffects(async () => {
				throw new Error("Already acknowledged");
			}),
		).toBe(0);
		expect(await counts(reopened)).toEqual({ posts: 1, operations: 1, effects: 1 });
	});
	it("refuses a surrounding uncommitted transaction", async () => {
		const result = await runtime.db
			.transaction()
			.execute((trx) =>
				runWithContext({ editMode: false, db: trx }, () =>
					runtime.handleContentCreateOnce("posts", { data: { title: "Nested" } }, operation),
				),
			);
		expect(result).toMatchObject({ success: false, error: { code: "NOT_SUPPORTED" } });
		expect(await counts()).toEqual({ posts: 0, operations: 0, effects: 0 });
	});

	it("does not reexecute content when a stored replay result is missing", async () => {
		const body = { data: { title: "Stored before corruption" } };
		expect((await runtime.handleContentCreateOnce("posts", body, operation)).success).toBe(true);
		await runtime.db.updateTable("_emdash_content_operations").set({ result: null }).execute();
		const retry = await runtime.handleContentCreateOnce("posts", body, operation);
		expect(retry).toMatchObject({ success: false, error: { code: "CONTENT_CREATE_ERROR" } });
		expect(await counts()).toEqual({ posts: 1, operations: 1, effects: 1 });
	});
	it("rejects a conflicting request actor before any write", async () => {
		const result = await runtime.handleContentCreateOnce(
			"posts",
			{
				data: { title: "Not authorized by this operation" },
				actor: { id: "another-actor", role: 100 },
			},
			operation,
		);
		expect(result).toMatchObject({ success: false, error: { code: "FORBIDDEN" } });
		expect(await counts()).toEqual({ posts: 0, operations: 0, effects: 0 });
	});
	it("rejects invalid identity or non-JSON input without consuming the key", async () => {
		const body = { data: { title: "Valid" } };
		expect(
			await runtime.handleContentCreateOnce("posts", body, { ...operation, key: "" }),
		).toMatchObject({ success: false, error: { code: "INVALID_INPUT" } });
		expect(
			await runtime.handleContentCreateOnce("posts", { data: { title: new Date() } }, operation),
		).toMatchObject({ success: false, error: { code: "INVALID_INPUT" } });
		for (const data of [
			{ title: "Valid", tags: undefined },
			{ title: "Valid", tags: Array(1) },
		]) {
			expect(await runtime.handleContentCreateOnce("posts", { data }, operation)).toMatchObject({
				success: false,
				error: { code: "INVALID_INPUT" },
			});
		}
		expect(await counts()).toEqual({ posts: 0, operations: 0, effects: 0 });
		expect((await runtime.handleContentCreateOnce("posts", body, operation)).success).toBe(true);
	});
	it("restarts migration after only the operation table was created", async () => {
		await runtime.db.schema.dropTable("_emdash_content_create_effects").execute();
		await addAtomicTables(runtime.db);
		const created = await runtime.handleContentCreateOnce(
			"posts",
			{ data: { title: "After partial migration" } },
			operation,
		);
		expect(created.success).toBe(true);
		expect(await counts()).toEqual({ posts: 1, operations: 1, effects: 1 });
	});
	it("retains existing content and replay identities when the migration is retried", async () => {
		const first = await runtime.handleContentCreateOnce(
			"posts",
			{ data: { title: "Retained" } },
			operation,
		);
		await addAtomicTables(runtime.db);
		const retry = await runtime.handleContentCreateOnce(
			"posts",
			{ data: { title: "Retained" } },
			operation,
		);
		expect(retry).toEqual(first);
		expect(await counts()).toEqual({ posts: 1, operations: 1, effects: 1 });
	});
});
