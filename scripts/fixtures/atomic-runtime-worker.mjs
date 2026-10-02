import assert from "node:assert/strict";
import { createRequire, register } from "node:module";
import { DatabaseSync } from "node:sqlite";
import { pathToFileURL } from "node:url";

register(new URL("./atomic-runtime-loader.mjs", import.meta.url));
const { EmDashRuntime } = await import("emdash/internal/plugin-test-runtime");
const { createDialect } = await import("emdash/db/sqlite");
const { SchemaRegistry } = await import("emdash");
const resolveDependency = createRequire(
	new URL("../../packages/core/package.json", import.meta.url),
).resolve;
const { sql } = await import(pathToFileURL(resolveDependency("kysely")));
const [path, mode, pauseList, key = "operation-one", binding = "host-binding-one"] =
	process.argv.slice(2);
assert.ok(path && process.send, "A database and parent IPC are required");
const pauses = new Set(pauseList.split(",").filter(Boolean));
let armed = false;
function send(message) {
	return new Promise((resolve, reject) => {
		process.send(message, (error) => {
			if (error) reject(error);
			else resolve();
		});
	});
}
async function checkpoint(name, details = {}) {
	const continuation = pauses.has(name)
		? new Promise((resolve, reject) => {
				process.once("message", (message) => {
					if (message?.type !== "continue") reject(new Error("Unexpected continuation"));
					else resolve();
				});
			})
		: undefined;
	await send({ type: "stage", name, ...details });
	await continuation;
}

function observedDialect(config) {
	const dialect = createDialect(config);
	const driver = dialect.createDriver();
	const originals = new WeakMap();
	const observed = new WeakMap();
	const observingDriver = new Proxy(driver, {
		get(target, property) {
			if (property === "acquireConnection")
				return async () => {
					const connection = await target.acquireConnection();
					if (observed.has(connection)) return observed.get(connection);
					const proxy = {
						async executeQuery(query) {
							const statement = query.sql.toLowerCase().trim();
							const claim = statement.startsWith('insert into "_emdash_content_operations"');
							if (armed && claim) await checkpoint("claim-attempt");
							const result = await connection.executeQuery(query);
							if (armed) {
								if (claim && result.rows.length) await checkpoint("claimed");
								else if (statement.startsWith('insert into "ec_posts"'))
									await checkpoint("content-written");
								else if (statement.startsWith('update "_emdash_content_operations"'))
									await checkpoint("replay-written");
								else if (statement.startsWith('insert into "_emdash_content_create_effects"'))
									await checkpoint("outbox-written");
								else if (statement.startsWith('update "_emdash_content_create_effects"'))
									await checkpoint("effect-acknowledged");
							}
							return result;
						},
						streamQuery: connection.streamQuery.bind(connection),
					};
					originals.set(proxy, connection);
					observed.set(connection, proxy);
					return proxy;
				};
			if (["beginTransaction", "rollbackTransaction", "releaseConnection"].includes(property))
				return async (connection, ...args) => {
					if (armed && mode === "unsafe-no-transaction" && property !== "releaseConnection") return;
					return Reflect.apply(target[property], target, [
						originals.get(connection) ?? connection,
						...args,
					]);
				};
			if (property === "commitTransaction")
				return async (connection) => {
					if (armed) await checkpoint("before-commit");
					if (!armed || mode !== "unsafe-no-transaction")
						await target.commitTransaction(originals.get(connection) ?? connection);
					if (armed) await checkpoint("committed");
				};
			const value = Reflect.get(target, property);
			return typeof value === "function" ? value.bind(target) : value;
		},
	});
	dialect.createDriver = () => observingDriver;
	return dialect;
}

const runtime = await EmDashRuntime.create({
	config: {
		database: {
			entrypoint: "atomic-runtime-process-proof",
			config: { url: "file:" + path },
			type: "sqlite",
		},
	},
	plugins: [],
	createDialect: observedDialect,
	createStorage: null,
	sandboxEnabled: false,
	sandboxedPluginEntries: [],
	createSandboxRunner: null,
	createScheduler: null,
});
const journal = await sql`PRAGMA journal_mode`.execute(runtime.db);
const synchronous = await sql`PRAGMA synchronous`.execute(runtime.db);
const busy = await sql`PRAGMA busy_timeout`.execute(runtime.db);
await checkpoint("environment", {
	sqlite: {
		journalMode: journal.rows[0].journal_mode,
		synchronous: synchronous.rows[0].synchronous,
		busyTimeout: busy.rows[0].timeout,
	},
});
const operation = {
	namespace: "cms:posts:create",
	tenant: "tenant-one",
	actorId: "editor-one",
	key,
	binding,
};
const body = { data: { title: "Real runtime content", tags: "" } };
const create = runtime.createContent;
assert.equal(
	typeof create,
	"function",
	"The observed runtime must expose its real creation pipeline",
);
runtime.createContent = async function (...args) {
	await checkpoint("action-started");
	return Reflect.apply(create, this, args);
};

async function deliver(effect) {
	const consumer = new DatabaseSync(path + ".consumer.db");
	try {
		consumer.exec(
			"CREATE TABLE IF NOT EXISTS receipts (id TEXT PRIMARY KEY);" +
				"CREATE TABLE IF NOT EXISTS deliveries (id INTEGER PRIMARY KEY, event_id TEXT NOT NULL);" +
				"CREATE TABLE IF NOT EXISTS counter (id INTEGER PRIMARY KEY, value INTEGER NOT NULL);" +
				"INSERT OR IGNORE INTO counter VALUES (1, 0);",
		);
		consumer.exec("BEGIN IMMEDIATE");
		consumer.prepare("INSERT INTO deliveries(event_id) VALUES (?)").run(effect.id);
		const receipt = consumer
			.prepare("INSERT OR IGNORE INTO receipts(id) VALUES (?)")
			.run(effect.id);
		if (mode === "effect-no-dedup" || Number(receipt.changes) === 1)
			consumer.prepare("UPDATE counter SET value = value + 1 WHERE id = 1").run();
		consumer.exec("COMMIT");
	} finally {
		consumer.close();
	}
	await checkpoint("effect-applied", { effect });
}

try {
	if (mode === "seed") {
		const registry = new SchemaRegistry(runtime.db);
		await registry.createCollection({ slug: "posts", label: "Posts" });
		await registry.createField("posts", {
			slug: "title",
			label: "Title",
			type: "string",
			required: true,
		});
		await registry.createField("posts", { slug: "tags", label: "Tags", type: "multiSelect" });
		await send({ type: "result", result: { initialized: true } });
	} else {
		armed = true;
		await checkpoint("ready");
		let result;
		if (mode.startsWith("effect")) result = await runtime.deliverContentCreateEffects(deliver);
		else if (mode === "ordinary") result = await runtime.handleContentCreate("posts", body);
		else result = await runtime.handleContentCreateOnce("posts", body, operation);
		await checkpoint("response-ready", { result });
		await send({ type: "result", result });
	}
} catch (error) {
	await send({ type: "failure", error: error.message });
	process.exitCode = 1;
} finally {
	await runtime.shutdown();
	await runtime.db.destroy();
	process.disconnect();
}
