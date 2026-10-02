import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { test } from "node:test";
import { fileURLToPath } from "node:url";

const worker = fileURLToPath(new URL("./fixtures/atomic-runtime-worker.mjs", import.meta.url));
const scratch = mkdtempSync(join(tmpdir(), "emdash-atomic-runtime-process-"));
const seed = join(scratch, "seed.db");
const children = new Set();
const reports = [];
const SQLITE_LOCK_ERROR = /database is locked/;
let sequence = 0;
let failed = false;

function query(path, statement) {
	const db = new DatabaseSync(path, { readOnly: true });
	try {
		return db.prepare(statement).all();
	} finally {
		db.close();
	}
}
function counts(path) {
	return {
		posts: Number(query(path, "SELECT count(*) AS n FROM ec_posts")[0].n),
		operations: Number(query(path, "SELECT count(*) AS n FROM _emdash_content_operations")[0].n),
		effects: Number(query(path, "SELECT count(*) AS n FROM _emdash_content_create_effects")[0].n),
	};
}
function rows(path, posts, operations, effects) {
	assert.deepEqual(counts(path), { posts, operations, effects });
}
function freshDatabase() {
	const path = join(scratch, "case-" + ++sequence + ".db");
	const db = new DatabaseSync(seed);
	try {
		db.prepare("VACUUM INTO ?").run(path);
	} finally {
		db.close();
	}
	return path;
}
function savedResult(path) {
	const records = query(path, "SELECT result FROM _emdash_content_operations");
	assert.equal(records.length, 1);
	assert.equal(typeof records[0].result, "string");
	return JSON.parse(records[0].result);
}
function persistedResult(path, result) {
	assert.equal(result.success, true);
	const posts = query(path, "SELECT id, title, tags FROM ec_posts");
	const post = posts.find((row) => row.id === result.data.item.id);
	assert.ok(post, "The returned ID must identify a real CMS row");
	assert.equal(post.title, "Real runtime content");
	assert.equal(post.tags, null, "The real runtime must normalize the empty multiSelect input");
	assert.equal(Object.hasOwn(result.data.item.data, "tags"), false);
	assert.deepEqual(savedResult(path), result);
	const effects = query(path, "SELECT id, entry_id FROM _emdash_content_create_effects");
	assert.equal(effects.length, 1);
	assert.equal(effects[0].entry_id, post.id);
}
function actions(...results) {
	return results.reduce(
		(total, result) =>
			total + result.stages.filter((stage) => stage.name === "action-started").length,
		0,
	);
}
function start(
	path,
	{ mode = "atomic", pauseAt = "", key = "operation-one", binding = "host-binding-one" } = {},
) {
	const child = spawn(process.execPath, [worker, path, mode, pauseAt, key, binding], {
		cwd: fileURLToPath(new URL("../", import.meta.url)),
		stdio: ["ignore", "pipe", "pipe", "ipc"],
	});
	const stages = [];
	const waiting = new Map();
	let result,
		error,
		output = "",
		closed = false;
	const instance = { child, stages };
	children.add(instance);
	const deadline = setTimeout(() => {
		error = "Worker exceeded its 30 second deadline";
		child.kill("SIGKILL");
	}, 30_000);
	for (const stream of [child.stdout, child.stderr])
		stream.on("data", (data) => {
			output = (output + data).slice(-12_000);
		});
	child.on("message", (message) => {
		if (message.type === "result") result = message.result;
		else if (message.type === "failure") error = message.error;
		else if (message.type === "stage") {
			stages.push(message);
			waiting.get(message.name)?.resolve(message);
		}
	});
	child.once("error", (cause) => {
		error = cause.message;
	});
	instance.done = new Promise((resolve) => {
		child.once("close", (code, signal) => {
			closed = true;
			clearTimeout(deadline);
			children.delete(instance);
			for (const pending of waiting.values())
				pending.reject(new Error("Worker closed before checkpoint: " + (error ?? output)));
			const report = {
				pid: child.pid,
				path,
				mode,
				pauseAt,
				key,
				binding,
				code,
				signal,
				result,
				error,
				stages,
				output,
			};
			reports.push(report);
			resolve(report);
		});
	});
	instance.resume = () => {
		assert.equal(closed, false);
		child.send({ type: "continue" });
	};
	instance.wait = (name) => {
		const found = stages.find((stage) => stage.name === name);
		if (found) return Promise.resolve(found);
		if (closed) return Promise.reject(new Error("Worker already closed: " + (error ?? output)));
		assert.equal(waiting.has(name), false);
		return new Promise((resolve, reject) => {
			const timer = setTimeout(() => {
				waiting.delete(name);
				reject(new Error("Checkpoint timed out: " + name));
			}, 20_000);
			waiting.set(name, {
				resolve(value) {
					clearTimeout(timer);
					waiting.delete(name);
					resolve(value);
				},
				reject(cause) {
					clearTimeout(timer);
					waiting.delete(name);
					reject(cause);
				},
			});
		});
	};
	return instance;
}
function environment(report) {
	const observed = report.stages.find((stage) => stage.name === "environment");
	assert.deepEqual(observed?.sqlite, {
		journalMode: "wal",
		synchronous: 1,
		busyTimeout: 5000,
	});
}
function successful(report) {
	environment(report);
	assert.equal(report.signal, null, report.output);
	assert.equal(report.code, 0, report.error ?? report.output);
	assert.equal(report.result.success, true, JSON.stringify(report.result));
	return report.result;
}
async function kill(instance) {
	assert.equal(instance.child.kill("SIGKILL"), true);
	const report = await instance.done;
	assert.equal(report.signal, "SIGKILL");
	assert.equal(report.result, undefined, "The killed worker must not have reported a response");
	environment(report);
	assert.equal(report.error, undefined, report.error);
	return report;
}
async function crashAt(path, phase, options = {}) {
	const instance = start(path, { ...options, pauseAt: phase });
	const observed = await instance.wait(phase);
	return { observed, report: await kill(instance) };
}
async function cleanupChildren() {
	const pending = [...children];
	for (const instance of pending) instance.child.kill("SIGKILL");
	await Promise.all(pending.map((instance) => instance.done));
}
async function check(name, run) {
	await test(name, async () => {
		try {
			await run();
		} catch (error) {
			failed = true;
			throw error;
		} finally {
			await cleanupChildren();
		}
	});
}
function pendingEffects(path) {
	return Number(
		query(
			path,
			"SELECT count(*) AS n FROM _emdash_content_create_effects WHERE completed_at IS NULL",
		)[0].n,
	);
}
function consumerCounts(path) {
	const consumer = path + ".consumer.db";
	return {
		deliveries: Number(query(consumer, "SELECT count(*) AS n FROM deliveries")[0].n),
		effects: Number(query(consumer, "SELECT value FROM counter WHERE id = 1")[0].value),
	};
}
async function delivered(instance) {
	const report = await instance.done;
	environment(report);
	assert.equal(report.code, 0, report.error ?? report.output);
	assert.equal(report.signal, null);
	assert.equal(typeof report.result, "number");
	return report;
}

try {
	const initialized = await start(seed, { mode: "seed" }).done;
	assert.equal(initialized.code, 0, initialized.error ?? initialized.output);
	assert.deepEqual(initialized.result, { initialized: true });
	environment(initialized);
	rows(seed, 0, 0, 0);

	await check("fresh processes replay the exact normalized runtime result", async () => {
		const path = freshDatabase();
		const first = await start(path).done;
		const second = await start(path).done;
		assert.deepEqual(successful(second), successful(first));
		persistedResult(path, second.result);
		assert.equal(actions(first, second), 1);
		rows(path, 1, 1, 1);
	});

	for (const phase of [
		"ready",
		"claimed",
		"action-started",
		"content-written",
		"replay-written",
		"outbox-written",
		"before-commit",
	]) {
		await check("SIGKILL at " + phase + " rolls back and permits one committed retry", async () => {
			const path = freshDatabase();
			const { report } = await crashAt(path, phase);
			rows(path, 0, 0, 0);
			const retry = await start(path).done;
			persistedResult(path, successful(retry));
			rows(path, 1, 1, 1);
			assert.equal(actions(retry), 1);
			assert.equal(actions(report), ["ready", "claimed"].includes(phase) ? 0 : 1);
		});
	}

	for (const phase of ["committed", "response-ready"]) {
		await check(
			"SIGKILL at " + phase + " preserves the result without another action",
			async () => {
				const path = freshDatabase();
				const { observed, report } = await crashAt(path, phase);
				rows(path, 1, 1, 1);
				const stored = savedResult(path);
				if (phase === "response-ready") assert.deepEqual(stored, observed.result);
				const retry = await start(path).done;
				assert.deepEqual(successful(retry), stored);
				persistedResult(path, retry.result);
				assert.equal(actions(report, retry), 1);
				assert.equal(actions(retry), 0);
				rows(path, 1, 1, 1);
			},
		);
	}

	await check("two processes contend while the first claim is held and create once", async () => {
		const path = freshDatabase();
		const owner = start(path, { pauseAt: "ready,claimed" });
		const contender = start(path, { pauseAt: "ready" });
		await Promise.all([owner.wait("ready"), contender.wait("ready")]);
		owner.resume();
		await owner.wait("claimed");
		contender.resume();
		await contender.wait("claim-attempt");
		owner.resume();
		const [first, second] = await Promise.all([owner.done, contender.done]);
		assert.deepEqual(successful(first), successful(second));
		assert.equal(actions(first, second), 1);
		persistedResult(path, first.result);
		rows(path, 1, 1, 1);
	});

	await check(
		"a concurrent retry recovers after an uncommitted content writer is killed",
		async () => {
			const path = freshDatabase();
			const owner = start(path, { pauseAt: "ready,content-written" });
			const contender = start(path, { pauseAt: "ready" });
			await Promise.all([owner.wait("ready"), contender.wait("ready")]);
			owner.resume();
			await owner.wait("content-written");
			contender.resume();
			await contender.wait("claim-attempt");
			const killed = await kill(owner);
			const retry = await contender.done;
			persistedResult(path, successful(retry));
			assert.equal(
				actions(killed, retry),
				2,
				"An uncommitted pipeline can run again after rollback",
			);
			rows(path, 1, 1, 1);
		},
	);

	await check(
		"a real SQLite lock timeout returns an error and the same identity safely replays",
		async () => {
			const path = freshDatabase();
			const owner = start(path, { pauseAt: "ready,claimed" });
			const contender = start(path, { pauseAt: "ready" });
			await Promise.all([owner.wait("ready"), contender.wait("ready")]);
			owner.resume();
			await owner.wait("claimed");
			contender.resume();
			const timedOut = await contender.done;
			assert.equal(timedOut.code, 0, timedOut.error ?? timedOut.output);
			assert.equal(timedOut.result.success, false);
			assert.equal(timedOut.result.error.code, "CONTENT_CREATE_ERROR");
			assert.match(timedOut.output, SQLITE_LOCK_ERROR);
			assert.equal(actions(timedOut), 0);
			owner.resume();
			const committed = await owner.done;
			const retry = await start(path).done;
			assert.deepEqual(successful(retry), successful(committed));
			assert.equal(actions(committed, timedOut, retry), 1);
			rows(path, 1, 1, 1);
		},
	);

	await check("distinct identities on contending processes create distinct content", async () => {
		const path = freshDatabase();
		const owner = start(path, { pauseAt: "ready,claimed" });
		const contender = start(path, { pauseAt: "ready", key: "another-operation" });
		await Promise.all([owner.wait("ready"), contender.wait("ready")]);
		owner.resume();
		await owner.wait("claimed");
		contender.resume();
		await contender.wait("claim-attempt");
		owner.resume();
		const [first, second] = await Promise.all([owner.done, contender.done]);
		assert.notEqual(successful(first).data.item.id, successful(second).data.item.id);
		assert.equal(actions(first, second), 2);
		rows(path, 2, 2, 2);
	});

	await check(
		"a changed authoritative binding conflicts after a committed process crash",
		async () => {
			const path = freshDatabase();
			await crashAt(path, "committed");
			const changed = await start(path, { binding: "different-host-binding" }).done;
			assert.equal(changed.code, 0, changed.error ?? changed.output);
			assert.equal(changed.result.success, false);
			assert.equal(changed.result.error.code, "CONFLICT");
			assert.equal(actions(changed), 0);
			rows(path, 1, 1, 1);
		},
	);

	await check(
		"SIGKILL after external delivery retains an event and an idempotent consumer effect",
		async () => {
			const path = freshDatabase();
			successful(await start(path).done);
			await crashAt(path, "effect-applied", { mode: "effect" });
			assert.equal(pendingEffects(path), 1);
			assert.deepEqual(consumerCounts(path), { deliveries: 1, effects: 1 });
			const retry = await delivered(start(path, { mode: "effect" }));
			assert.equal(retry.result, 1);
			assert.deepEqual(consumerCounts(path), { deliveries: 2, effects: 1 });
			assert.equal(pendingEffects(path), 0);
			assert.equal((await delivered(start(path, { mode: "effect" }))).result, 0);
			rows(path, 1, 1, 1);
		},
	);

	await check("SIGKILL after delivery acknowledgement does not redeliver the event", async () => {
		const path = freshDatabase();
		successful(await start(path).done);
		await crashAt(path, "effect-acknowledged", { mode: "effect" });
		assert.equal(pendingEffects(path), 0);
		assert.equal((await delivered(start(path, { mode: "effect" }))).result, 0);
		assert.deepEqual(consumerCounts(path), { deliveries: 1, effects: 1 });
		rows(path, 1, 1, 1);
	});

	await check("two effect consumers overlap and deduplicate one external effect", async () => {
		const path = freshDatabase();
		successful(await start(path).done);
		const owner = start(path, { mode: "effect", pauseAt: "ready,effect-applied" });
		const contender = start(path, { mode: "effect", pauseAt: "ready" });
		await Promise.all([owner.wait("ready"), contender.wait("ready")]);
		owner.resume();
		await owner.wait("effect-applied");
		contender.resume();
		const second = await delivered(contender);
		owner.resume();
		const first = await delivered(owner);
		assert.equal(first.result + second.result, 1);
		assert.deepEqual(consumerCounts(path), { deliveries: 2, effects: 1 });
		assert.equal(pendingEffects(path), 0);
		rows(path, 1, 1, 1);
	});

	await check(
		"negative control: an external consumer without deduplication applies twice",
		async () => {
			const path = freshDatabase();
			successful(await start(path).done);
			await crashAt(path, "effect-applied", { mode: "effect-no-dedup" });
			await delivered(start(path, { mode: "effect-no-dedup" }));
			assert.deepEqual(consumerCounts(path), { deliveries: 2, effects: 2 });
			rows(path, 1, 1, 1);
		},
	);

	await check(
		"negative control: ordinary runtime creation duplicates after a committed crash",
		async () => {
			const path = freshDatabase();
			await crashAt(path, "committed", { mode: "ordinary" });
			rows(path, 1, 0, 0);
			successful(await start(path, { mode: "ordinary" }).done);
			rows(path, 2, 0, 0);
			assert.throws(() => rows(path, 1, 1, 1), assert.AssertionError);
		},
	);

	await check(
		"mutation control: disabling transactions breaks the crash rollback assertion",
		async () => {
			const path = freshDatabase();
			await crashAt(path, "content-written", { mode: "unsafe-no-transaction" });
			rows(path, 1, 1, 0);
			assert.throws(() => rows(path, 0, 0, 0), assert.AssertionError);
			const retry = await start(path).done;
			assert.equal(retry.code, 0, retry.error ?? retry.output);
			assert.equal(retry.result.success, false);
			assert.equal(retry.result.error.code, "CONTENT_CREATE_ERROR");
			assert.equal(actions(retry), 0);
			rows(path, 1, 1, 0);
		},
	);
} catch (error) {
	failed = true;
	throw error;
} finally {
	await cleanupChildren();
	writeFileSync(join(scratch, "workers.json"), JSON.stringify(reports, null, 2) + "\n");
	if (failed || process.env.KEEP_ATOMIC_RUNTIME_PROOF === "1") {
		console.log("Kept runtime process proof: " + scratch);
	} else {
		rmSync(scratch, { recursive: true, force: true });
	}
}
