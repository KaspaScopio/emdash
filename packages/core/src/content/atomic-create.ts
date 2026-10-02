import type { Kysely, Transaction } from "kysely";
import { z } from "zod";

import { ErrorCode } from "../api/errors.js";
import type { ApiResult, ContentResponse } from "../api/types.js";
import type { Database } from "../database/types.js";

export interface ContentCreateOperation {
	namespace: string;
	tenant: string;
	actorId: string;
	key: string;
	binding: string;
}
export interface ContentCreateEffect {
	id: string;
	collection: string;
	contentId: string;
}
const operationSchema = z.object({
	namespace: z.string().min(1).max(128),
	tenant: z.string().min(1).max(128),
	actorId: z.string().min(1).max(128),
	key: z.string().min(1).max(256),
	binding: z.string().min(1).max(2048),
});
const responseSchema = z.object({
	success: z.literal(true),
	data: z
		.object({
			item: z
				.object({
					id: z.string(),
					type: z.string(),
					slug: z.string().nullable(),
					status: z.string(),
					data: z.record(z.string(), z.unknown()),
					authorId: z.string().nullable(),
					primaryBylineId: z.string().nullable(),
					createdAt: z.string(),
					updatedAt: z.string(),
					publishedAt: z.string().nullable(),
					scheduledAt: z.string().nullable(),
					liveRevisionId: z.string().nullable(),
					draftRevisionId: z.string().nullable(),
					version: z.number(),
					locale: z.string().nullable(),
					translationGroup: z.string().nullable(),
				})
				.passthrough(),
			_rev: z.string().optional(),
		})
		.passthrough(),
});
class OperationAborted extends Error {
	constructor(readonly result: ApiResult<ContentResponse>) {
		super("Content operation aborted");
	}
}
function fail(code: string, message: string): never {
	throw new OperationAborted({ success: false, error: { code, message } });
}
export function canonicalContentJson(value: unknown): string {
	if (value === null || typeof value === "string" || typeof value === "boolean")
		return JSON.stringify(value);
	if (typeof value === "number" && Number.isFinite(value)) return JSON.stringify(value);
	if (Array.isArray(value)) return "[" + Array.from(value, canonicalContentJson).join(",") + "]";
	if (
		typeof value === "object" &&
		value !== null &&
		(Object.getPrototypeOf(value) === Object.prototype || Object.getPrototypeOf(value) === null)
	) {
		return (
			"{" +
			Object.entries(value)
				.toSorted(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
				.map(([key, entry]) => JSON.stringify(key) + ":" + canonicalContentJson(entry))
				.join(",") +
			"}"
		);
	}
	throw new Error("Content operation requires JSON input");
}
async function digest(value: string): Promise<string> {
	const bytes = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(value));
	return Array.from(new Uint8Array(bytes), (byte) => byte.toString(16).padStart(2, "0")).join("");
}
export async function runContentCreateOperation(
	db: Kysely<Database>,
	operation: ContentCreateOperation,
	collection: string,
	body: unknown,
	create: (trx: Transaction<Database>) => Promise<ApiResult<ContentResponse>>,
): Promise<ApiResult<ContentResponse>> {
	try {
		const parsed = operationSchema.safeParse(operation);
		if (!parsed.success) fail(ErrorCode.INVALID_INPUT, "Invalid content operation identity");
		const { namespace, tenant, actorId, key, binding } = parsed.data;
		let canonical: string;
		try {
			canonical = canonicalContentJson([collection, body, binding]);
		} catch {
			fail(ErrorCode.INVALID_INPUT, "Content operation requires JSON input");
		}
		const identity = await digest(
			canonicalContentJson(["content-create:v1", namespace, tenant, actorId, key]),
		);
		const fingerprint = await digest(canonical);
		return await db.transaction().execute(async (trx) => {
			const claim = await trx
				.insertInto("_emdash_content_operations")
				.values({ id: identity, binding: fingerprint, result: null })
				.onConflict((conflict) => conflict.column("id").doNothing())
				.returning("id")
				.executeTakeFirst();
			if (!claim) {
				const stored = await trx
					.selectFrom("_emdash_content_operations")
					.selectAll()
					.where("id", "=", identity)
					.executeTakeFirstOrThrow();
				if (stored.binding !== fingerprint)
					fail(ErrorCode.CONFLICT, "Content operation identity has a different binding");
				if (!stored.result)
					fail(ErrorCode.CONTENT_CREATE_ERROR, "Content operation has no replayable result");
				return responseSchema.parse(JSON.parse(stored.result));
			}
			const result = await create(trx);
			if (!result.success) throw new OperationAborted(result);
			const serialized = JSON.stringify(result);
			if (new TextEncoder().encode(serialized).length > 1_048_576)
				fail(ErrorCode.INVALID_INPUT, "Content operation result exceeds one MiB");
			const replayable = responseSchema.parse(JSON.parse(serialized));
			await trx
				.updateTable("_emdash_content_operations")
				.set({ result: serialized })
				.where("id", "=", identity)
				.executeTakeFirstOrThrow();
			await trx
				.insertInto("_emdash_content_create_effects")
				.values({
					id: identity,
					collection,
					entry_id: result.data.item.id,
					completed_at: null,
				})
				.execute();
			return replayable;
		});
	} catch (error) {
		if (error instanceof OperationAborted) return error.result;
		console.error("Atomic content creation failed:", error);
		return {
			success: false,
			error: {
				code: ErrorCode.CONTENT_CREATE_ERROR,
				message: "Atomic content creation failed; retry with the same operation identity",
			},
		};
	}
}
export async function deliverContentCreateEffects(
	db: Kysely<Database>,
	deliver: (effect: ContentCreateEffect) => Promise<void>,
	limit = 50,
): Promise<number> {
	if (!Number.isInteger(limit) || limit < 1 || limit > 100)
		throw new Error("Content effect delivery limit must be between 1 and 100");
	if (db.isTransaction) throw new Error("Content effects must run after commit");
	const pending = await db
		.selectFrom("_emdash_content_create_effects")
		.selectAll()
		.where("completed_at", "is", null)
		.orderBy("created_at")
		.orderBy("id")
		.limit(limit)
		.execute();
	let completed = 0;
	for (const event of pending) {
		await deliver({ id: event.id, collection: event.collection, contentId: event.entry_id });
		const updated = await db
			.updateTable("_emdash_content_create_effects")
			.set({ completed_at: new Date().toISOString() })
			.where("id", "=", event.id)
			.where("completed_at", "is", null)
			.executeTakeFirst();
		completed += Number(updated.numUpdatedRows);
	}
	return completed;
}
