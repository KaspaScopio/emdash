import { sql } from "kysely";
import type { Kysely } from "kysely";

export async function up(db: Kysely<unknown>): Promise<void> {
	await db.schema
		.createTable("_emdash_content_operations")
		.ifNotExists()
		.addColumn("id", "text", (column) => column.primaryKey())
		.addColumn("binding", "text", (column) => column.notNull())
		.addColumn("result", "text")
		.addColumn("created_at", "text", (column) => column.notNull().defaultTo(sql`CURRENT_TIMESTAMP`))
		.execute();
	await db.schema
		.createTable("_emdash_content_create_effects")
		.ifNotExists()
		.addColumn("id", "text", (column) =>
			column.primaryKey().references("_emdash_content_operations.id"),
		)
		.addColumn("collection", "text", (column) => column.notNull())
		.addColumn("entry_id", "text", (column) => column.notNull())
		.addColumn("created_at", "text", (column) => column.notNull().defaultTo(sql`CURRENT_TIMESTAMP`))
		.addColumn("completed_at", "text")
		.execute();
	await db.schema
		.createIndex("idx_content_create_effects_pending")
		.ifNotExists()
		.on("_emdash_content_create_effects")
		.columns(["completed_at", "created_at", "id"])
		.execute();
}
export async function down(_db: Kysely<unknown>): Promise<void> {
	// Replay identities survive application rollback.
}
