/**
 * Integration tests for async (background) agent execution.
 *
 * Tests the async support utilities: jiti availability check,
 * status file reading/caching.
 *
 * Requires pi packages to be importable. Skips gracefully if unavailable.
 */

import { describe, it, before, after, type TestContext } from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import * as fs from "node:fs";
import * as path from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { ASYNC_DIR, DEFAULT_ARTIFACT_CONFIG, RESULTS_DIR } from "../types.ts";
import { captureMockPiModels, createMockPi, createTempDir, makeAgent, removeTempDir, tryImport, type MockPi } from "./helpers.ts";

// Top-level await
const asyncMod = await tryImport<any>("./async-execution.ts");
const utils = await tryImport<any>("./utils.ts");
const available = !!(asyncMod && utils);

const isAsyncAvailable = asyncMod?.isAsyncAvailable;
const readStatus = utils?.readStatus;

describe("async execution utilities", { skip: !available ? "pi packages not available" : undefined }, () => {
	it("reports jiti availability as boolean", () => {
		const result = isAsyncAvailable();
		assert.equal(typeof result, "boolean");
	});

	it("readStatus returns null for missing directory", () => {
		const status = readStatus("/nonexistent/path/abc123");
		assert.equal(status, null);
	});

	it("readStatus parses valid status file", () => {
		const dir = createTempDir();
		try {
			const statusData = {
				runId: "test-123",
				state: "running",
				mode: "single",
				startedAt: Date.now(),
				lastUpdate: Date.now(),
				steps: [{ agent: "test", status: "running" }],
			};
			fs.writeFileSync(path.join(dir, "status.json"), JSON.stringify(statusData));

			const status = readStatus(dir);
			assert.ok(status, "should parse status");
			assert.equal(status.runId, "test-123");
			assert.equal(status.state, "running");
			assert.equal(status.mode, "single");
		} finally {
			removeTempDir(dir);
		}
	});

	it("readStatus caches by mtime (second call uses cache)", () => {
		const dir = createTempDir();
		try {
			const statusData = {
				runId: "cache-test",
				state: "running",
				mode: "single",
				startedAt: Date.now(),
			};
			fs.writeFileSync(path.join(dir, "status.json"), JSON.stringify(statusData));

			const s1 = readStatus(dir);
			const s2 = readStatus(dir);
			assert.ok(s1);
			assert.ok(s2);
			assert.equal(s1.runId, s2.runId);
		} finally {
			removeTempDir(dir);
		}
	});
});

describe("async model CLI arguments", { skip: !available ? "pi packages not available" : undefined }, () => {
	let mockPi: MockPi;

	before(() => {
		mockPi = createMockPi();
		mockPi.install();
		mockPi.onCall({ output: "Done" });
	});
	after(() => mockPi.uninstall());

	async function captureRun(t: TestContext, run: (id: string, params: any) => void) {
		assert.ok(isAsyncAvailable(), "async runner requires jiti");
		const id = randomUUID();
		const cwd = createTempDir();
		const resultPath = path.join(RESULTS_DIR, `${id}.json`);
		t.after(() => {
			removeTempDir(cwd);
			removeTempDir(path.join(ASYNC_DIR, id));
			fs.rmSync(resultPath, { force: true });
		});
		const models = captureMockPiModels(t, cwd);
		run(id, {
			ctx: {
				pi: { events: { emit() {} } },
				cwd,
				currentSessionId: id,
				parentModel: "mock/parent",
			},
			artifactConfig: { ...DEFAULT_ARTIFACT_CONFIG, enabled: false },
			shareEnabled: false,
		});

		const deadline = Date.now() + 10_000;
		while (Date.now() < deadline) {
			try {
				const result = JSON.parse(fs.readFileSync(resultPath, "utf-8"));
				assert.equal(result.success, true, JSON.stringify(result));
				return models();
			} catch (e) {
				if (!(e instanceof SyntaxError) && (e as NodeJS.ErrnoException).code !== "ENOENT") throw e;
			}
			await delay(25);
		}
		assert.fail("async runner did not write a result");
	}

	for (const [frontmatter, override, expected] of [
		["mock/frontmatter", undefined, "mock/frontmatter"],
		[undefined, undefined, "mock/parent"],
		["mock/frontmatter", "mock/override", "mock/override"],
	]) {
		it(`uses ${expected} for a single agent`, async (t) => {
			const models = await captureRun(t, (id, params) => asyncMod.executeAsyncSingle(id, {
				...params,
				agent: "echo",
				task: "Task",
				agentConfig: makeAgent("echo", { model: frontmatter }),
				modelOverride: override,
			}));
			assert.deepEqual(models, [expected]);
		});
	}

	it("prefers step, agent, then parent models for async chain spawns", async (t) => {
		const models = await captureRun(t, (id, params) => asyncMod.executeAsyncChain(id, {
			...params,
			agents: [makeAgent("configured", { model: "mock/frontmatter" }), makeAgent("unconfigured")],
			chain: [
				{ agent: "configured", task: "First" },
				{ agent: "unconfigured", task: "Second" },
				{ agent: "configured", task: "Third", model: "mock/override" },
				{ parallel: [{ agent: "configured", task: "Fourth" }] },
			],
		}));
		assert.deepEqual(models, ["mock/frontmatter", "mock/parent", "mock/override", "mock/frontmatter"]);
	});
});
