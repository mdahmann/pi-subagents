import { test } from "node:test";
import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@mariozechner/pi-coding-agent";
import type { AsyncJobState } from "../types.ts";

test("a completed run expires from the widget without returning on the next poll", async (t) => {
	const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "pi-widget-test-"));
	for (const key of ["TMPDIR", "TMP", "TEMP"]) {
		const previous = process.env[key];
		process.env[key] = tempDir;
		t.after(() => {
			if (previous === undefined) delete process.env[key];
			else process.env[key] = previous;
		});
	}
	t.after(() => fs.rmSync(tempDir, { recursive: true, force: true }));

	const { default: register } = await import("../index.ts");
	const { ASYNC_DIR, POLL_INTERVAL_MS } = await import("../types.ts");
	const now = Date.now();
	t.mock.timers.enable({ apis: ["Date", "setInterval", "setTimeout"], now });

	const lifecycle = new EventEmitter();
	const pi = {
		on: lifecycle.on.bind(lifecycle),
		events: new EventEmitter(),
		registerTool() {},
		registerCommand() {},
		registerShortcut() {},
	} as unknown as ExtensionAPI;
	let widget: string[] | undefined;
	const ctx = {
		cwd: tempDir,
		hasUI: true,
		sessionManager: { getSessionFile: () => null },
		ui: {
			theme: { fg: (_color: string, text: string) => text },
			setWidget: (_key: string, lines: string[] | undefined) => { widget = lines; },
			setStatus() {},
		},
	} as unknown as ExtensionContext;
	register(pi);
	t.after(() => lifecycle.emit("session_shutdown"));

	const runDir = path.join(ASYNC_DIR, "12345678-1234-1234-1234-123456789012");
	fs.mkdirSync(runDir);
	const statusPath = path.join(runDir, "status.json");
	fs.writeFileSync(statusPath, JSON.stringify({
		state: "complete",
		startedAt: now,
		endedAt: now,
		steps: [{ agent: "scout" }],
	}));

	lifecycle.emit("session_start", {}, ctx);
	assert.equal(widget?.[0], "Async subagents (1): 1 done");
	t.mock.timers.tick(10_000);
	t.mock.timers.tick(POLL_INTERVAL_MS);
	assert.equal(widget, undefined);
	assert.ok(fs.existsSync(statusPath), "run history remains on disk");
});

test("a job beyond the first four updates the widget counts", async () => {
	const { renderWidget } = await import("../render.ts");
	let widget: string[] | undefined;
	const ctx = {
		hasUI: true,
		ui: {
			theme: { fg: (_color: string, text: string) => text },
			setWidget: (_key: string, lines: string[] | undefined) => { widget = lines; },
			setStatus() {},
		},
	} as unknown as ExtensionContext;
	const jobs = Array.from({ length: 5 }, (_, index): AsyncJobState => ({
		asyncId: String(index),
		asyncDir: "",
		status: index === 4 ? "running" : "complete",
		agents: ["scout"],
		updatedAt: Date.now(),
	}));

	renderWidget(ctx, jobs);
	assert.equal(widget?.[0], "Async subagents (5): 1 active · 4 done");
	jobs[4].status = "failed";
	renderWidget(ctx, jobs);
	assert.equal(widget?.[0], "Async subagents (5): 4 done · 1 failed");
});
