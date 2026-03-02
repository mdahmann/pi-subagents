import { spawn, spawnSync } from "node:child_process";
import * as fs from "node:fs";
import { createRequire } from "node:module";
import * as os from "node:os";
import * as path from "node:path";
import { pathToFileURL } from "node:url";
import { appendJsonl, getArtifactPaths } from "./artifacts.js";
import { getPiSpawnCommand } from "./pi-spawn.js";
import { persistSingleOutput } from "./single-output.js";
import {
	type ArtifactConfig,
	type ArtifactPaths,
	DEFAULT_MAX_OUTPUT,
	type MaxOutputConfig,
	truncateOutput,
	getSubagentDepthEnv,
} from "./types.js";
import {
	type RunnerSubagentStep as SubagentStep,
	type RunnerStep,
	isParallelGroup,
	flattenSteps,
	mapConcurrent,
	aggregateParallelOutputs,
	MAX_PARALLEL_CONCURRENCY,
} from "./parallel-utils.js";

interface SubagentRunConfig {
	id: string;
	steps: RunnerStep[];
	resultPath: string;
	cwd: string;
	placeholder: string;
	taskIndex?: number;
	totalTasks?: number;
	maxOutput?: MaxOutputConfig;
	artifactsDir?: string;
	artifactConfig?: Partial<ArtifactConfig>;
	share?: boolean;
	sessionDir?: string;
	asyncDir: string;
	sessionId?: string | null;
	piPackageRoot?: string;
}

interface StepResult {
	agent: string;
	output: string;
	success: boolean;
	skipped?: boolean;
	artifactPaths?: ArtifactPaths;
	truncated?: boolean;
}

const require = createRequire(import.meta.url);

function findLatestSessionFile(sessionDir: string): string | null {
	try {
		const files = fs
			.readdirSync(sessionDir)
			.filter((f) => f.endsWith(".jsonl"))
			.map((f) => path.join(sessionDir, f));
		if (files.length === 0) return null;
		files.sort((a, b) => fs.statSync(b).mtimeMs - fs.statSync(a).mtimeMs);
		return files[0] ?? null;
	} catch {
		return null;
	}
}

interface TokenUsage {
	input: number;
	output: number;
	total: number;
}

function parseSessionTokens(sessionDir: string): TokenUsage | null {
	const sessionFile = findLatestSessionFile(sessionDir);
	if (!sessionFile) return null;
	try {
		const content = fs.readFileSync(sessionFile, "utf-8");
		let input = 0;
		let output = 0;
		for (const line of content.split("\n")) {
			if (!line.trim()) continue;
			try {
				const entry = JSON.parse(line);
				if (entry.usage) {
					input += entry.usage.inputTokens ?? entry.usage.input ?? 0;
					output += entry.usage.outputTokens ?? entry.usage.output ?? 0;
				}
			} catch {}
		}
		return { input, output, total: input + output };
	} catch {
		return null;
	}
}

/** Lightweight activity tracker parsed from JSONL events */
interface ToolActivity {
	currentTool?: string;
	currentToolArgs?: string;
	toolCount: number;
	lastToolAt?: number;
	tokensIn: number;
	tokensOut: number;
	cost: number;
	turns: number;
	model?: string;
	lastAssistantText?: string;
}

const MAX_OUTPUT_LOG_BYTES = 500 * 1024 * 1024; // 500MB hard cap per running subagent log
const OUTPUT_LOG_CAPPED_NOTICE = "\n[pi-subagents] output log capped at 500MB; further output omitted\n";

function runPiStreaming(
	args: string[],
	cwd: string,
	outputFile: string,
	env?: Record<string, string | undefined>,
	piPackageRoot?: string,
	onPid?: (pid: number) => void,
	toolActivity?: ToolActivity,
): Promise<{ stdout: string; exitCode: number | null }> {
	return new Promise((resolve) => {
		const outputStream = fs.createWriteStream(outputFile, { flags: "w" });
		const spawnEnv = { ...process.env, ...(env ?? {}), ...getSubagentDepthEnv() };
		const spawnSpec = getPiSpawnCommand(args, piPackageRoot ? { piPackageRoot } : undefined);
		const child = spawn(spawnSpec.command, spawnSpec.args, { cwd, stdio: ["ignore", "pipe", "pipe"], env: spawnEnv });
		let lineBuf = "";
		let writtenBytes = 0;
		let logCapped = false;

		const writeWithCap = (text: string): void => {
			if (!text || logCapped) return;
			const buf = Buffer.from(text);
			if (writtenBytes + buf.length <= MAX_OUTPUT_LOG_BYTES) {
				outputStream.write(buf);
				writtenBytes += buf.length;
				return;
			}

			const remaining = MAX_OUTPUT_LOG_BYTES - writtenBytes;
			if (remaining > 0) {
				outputStream.write(buf.subarray(0, remaining));
				writtenBytes += remaining;
			}
			outputStream.write(OUTPUT_LOG_CAPPED_NOTICE);
			logCapped = true;
		};

		const parseActivityLine = (line: string): void => {
			if (!toolActivity || !line.startsWith("{")) return;
			try {
				const evt = JSON.parse(line);
				if (evt.type === "tool_execution_start") {
					toolActivity.currentTool = evt.toolName;
					toolActivity.currentToolArgs = typeof evt.args === "object"
						? Object.keys(evt.args as Record<string, unknown>).slice(0, 2).join(",")
						: undefined;
					toolActivity.toolCount++;
					toolActivity.lastToolAt = Date.now();
				} else if (evt.type === "tool_execution_end") {
					toolActivity.currentTool = undefined;
					toolActivity.currentToolArgs = undefined;
				} else if (evt.type === "message_end" && evt.message?.role === "assistant") {
					const u = evt.message.usage;
					if (u) {
						toolActivity.tokensIn += u.input || 0;
						toolActivity.tokensOut += u.output || 0;
						toolActivity.cost += u.cost?.total || 0;
					}
					toolActivity.turns++;
					if (evt.message.model) toolActivity.model = evt.message.model;
					const content = evt.message?.content;
					if (Array.isArray(content)) {
						for (const part of content) {
							if (part?.type === "text" && typeof part.text === "string") {
								toolActivity.lastAssistantText = part.text;
							}
						}
					} else if (typeof content === "string") {
						toolActivity.lastAssistantText = content;
					}
				}
			} catch {}
		};

		if (child.pid && onPid) onPid(child.pid);

		child.stdout.on("data", (chunk: Buffer) => {
			const text = chunk.toString();
			lineBuf += text;
			const lines = lineBuf.split("\n");
			lineBuf = lines.pop() ?? "";

			for (const line of lines) {
				parseActivityLine(line);
				// Skip huge incremental thinking deltas from persisted logs.
				if (line.startsWith("{") && line.includes('"message_update"')) continue;
				writeWithCap(`${line}\n`);
			}
		});

		child.stderr.on("data", (chunk: Buffer) => {
			writeWithCap(chunk.toString());
		});

		child.on("close", (exitCode) => {
			if (lineBuf) {
				parseActivityLine(lineBuf);
				if (!(lineBuf.startsWith("{") && lineBuf.includes('"message_update"'))) {
					writeWithCap(lineBuf);
				}
			}
			outputStream.end();
			resolve({ stdout: outputFile, exitCode });
		});

		child.on("error", () => {
			outputStream.end();
			resolve({ stdout: outputFile, exitCode: 1 });
		});
	});
}

function resolvePiPackageRootFallback(): string {
	// Try to resolve the main entry point and walk up to find the package root
	const entryPoint = require.resolve("@mariozechner/pi-coding-agent");
	// Entry point is typically /path/to/dist/index.js, so go up to find package root
	let dir = path.dirname(entryPoint);
	while (dir !== path.dirname(dir)) {
		const pkgJsonPath = path.join(dir, "package.json");
		try {
			const pkg = JSON.parse(fs.readFileSync(pkgJsonPath, "utf-8"));
			if (pkg.name === "@mariozechner/pi-coding-agent") return dir;
		} catch {}
		dir = path.dirname(dir);
	}
	throw new Error("Could not resolve @mariozechner/pi-coding-agent package root");
}

async function exportSessionHtml(sessionFile: string, outputDir: string, piPackageRoot?: string): Promise<string> {
	const pkgRoot = piPackageRoot ?? resolvePiPackageRootFallback();
	const exportModulePath = path.join(pkgRoot, "dist", "core", "export-html", "index.js");
	const moduleUrl = pathToFileURL(exportModulePath).href;
	const mod = await import(moduleUrl);
	const exportFromFile = (mod as { exportFromFile?: (inputPath: string, options?: { outputPath?: string }) => string })
		.exportFromFile;
	if (typeof exportFromFile !== "function") {
		throw new Error("exportFromFile not available");
	}
	const outputPath = path.join(outputDir, `${path.basename(sessionFile, ".jsonl")}.html`);
	return exportFromFile(sessionFile, { outputPath });
}

function createShareLink(htmlPath: string): { shareUrl: string; gistUrl: string } | { error: string } {
	try {
		const auth = spawnSync("gh", ["auth", "status"], { encoding: "utf-8" });
		if (auth.status !== 0) {
			return { error: "GitHub CLI is not logged in. Run 'gh auth login' first." };
		}
	} catch {
		return { error: "GitHub CLI (gh) is not installed." };
	}

	try {
		const result = spawnSync("gh", ["gist", "create", htmlPath], { encoding: "utf-8" });
		if (result.status !== 0) {
			const err = (result.stderr || "").trim() || "Failed to create gist.";
			return { error: err };
		}
		const gistUrl = (result.stdout || "").trim();
		const gistId = gistUrl.split("/").pop();
		if (!gistId) return { error: "Failed to parse gist ID." };
		const shareUrl = `https://shittycodingagent.ai/session/?${gistId}`;
		return { shareUrl, gistUrl };
	} catch (err) {
		return { error: String(err) };
	}
}

function writeJson(filePath: string, payload: object): void {
	fs.mkdirSync(path.dirname(filePath), { recursive: true });
	fs.writeFileSync(filePath, JSON.stringify(payload, null, 2), "utf-8");
}

function formatDuration(ms: number): string {
	if (ms < 1000) return `${ms}ms`;
	if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`;
	const minutes = Math.floor(ms / 60000);
	const seconds = Math.floor((ms % 60000) / 1000);
	return `${minutes}m${seconds}s`;
}

function writeRunLog(
	logPath: string,
	input: {
		id: string;
		mode: "single" | "chain";
		cwd: string;
		startedAt: number;
		endedAt: number;
		steps: Array<{
			agent: string;
			status: string;
			durationMs?: number;
		}>;
		summary: string;
		truncated: boolean;
		artifactsDir?: string;
		sessionFile?: string;
		shareUrl?: string;
		shareError?: string;
	},
): void {
	const lines: string[] = [];
	lines.push(`# Subagent run ${input.id}`);
	lines.push("");
	lines.push(`- **Mode:** ${input.mode}`);
	lines.push(`- **CWD:** ${input.cwd}`);
	lines.push(`- **Started:** ${new Date(input.startedAt).toISOString()}`);
	lines.push(`- **Ended:** ${new Date(input.endedAt).toISOString()}`);
	lines.push(`- **Duration:** ${formatDuration(input.endedAt - input.startedAt)}`);
	if (input.sessionFile) lines.push(`- **Session:** ${input.sessionFile}`);
	if (input.shareUrl) lines.push(`- **Share:** ${input.shareUrl}`);
	if (input.shareError) lines.push(`- **Share error:** ${input.shareError}`);
	if (input.artifactsDir) lines.push(`- **Artifacts:** ${input.artifactsDir}`);
	lines.push("");
	lines.push("## Steps");
	lines.push("| Step | Agent | Status | Duration |");
	lines.push("| --- | --- | --- | --- |");
	input.steps.forEach((step, i) => {
		const duration = step.durationMs !== undefined ? formatDuration(step.durationMs) : "-";
		lines.push(`| ${i + 1} | ${step.agent} | ${step.status} | ${duration} |`);
	});
	lines.push("");
	lines.push("## Summary");
	if (input.truncated) {
		lines.push("_Output truncated_");
		lines.push("");
	}
	lines.push(input.summary.trim() || "(no output)");
	lines.push("");
	fs.writeFileSync(logPath, lines.join("\n"), "utf-8");
}

/** Context for running a single step */
interface SingleStepContext {
	previousOutput: string;
	placeholder: string;
	cwd: string;
	sessionEnabled: boolean;
	sessionDir?: string;
	artifactsDir?: string;
	artifactConfig?: Partial<ArtifactConfig>;
	id: string;
	flatIndex: number;
	flatStepCount: number;
	outputFile: string;
	piPackageRoot?: string;
	/** For heartbeat updates during long-running steps */
	statusPayload?: Record<string, unknown>;
	statusPath?: string;
}

/** Run a single pi agent step, returning output and metadata */
async function runSingleStep(
	step: SubagentStep,
	ctx: SingleStepContext,
): Promise<{ agent: string; output: string; exitCode: number | null; artifactPaths?: ArtifactPaths }> {
	const args = ["--mode", "json", "-p"];
	if (!ctx.sessionEnabled) {
		args.push("--no-session");
	}
	if (ctx.sessionDir) {
		try { fs.mkdirSync(ctx.sessionDir, { recursive: true }); } catch {}
		args.push("--session-dir", ctx.sessionDir);
	}
	if (step.model) args.push("--models", step.model);

	const toolExtensionPaths: string[] = [];
	if (step.tools?.length) {
		const KNOWN_BUILTIN_TOOLS = new Set(["read", "bash", "edit", "write", "grep", "find", "ls"]);
		const builtinTools: string[] = [];
		for (const tool of step.tools) {
			if (tool.includes("/") || tool.endsWith(".ts") || tool.endsWith(".js")) {
				toolExtensionPaths.push(tool);
			} else if (KNOWN_BUILTIN_TOOLS.has(tool)) {
				builtinTools.push(tool);
			}
			// Extension-provided tools (e.g. "subagent") are silently skipped —
			// they're made available by the extensions list, not --tools.
		}
		if (builtinTools.length > 0) args.push("--tools", builtinTools.join(","));
	}
	if (step.extensions !== undefined) {
		args.push("--no-extensions");
		for (const extPath of step.extensions) args.push("--extension", extPath);
	} else {
		for (const extPath of toolExtensionPaths) args.push("--extension", extPath);
	}

	let tmpDir: string | null = null;
	if (step.systemPrompt) {
		tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "pi-subagent-"));
		const promptPath = path.join(tmpDir, "prompt.md");
		fs.writeFileSync(promptPath, step.systemPrompt);
		args.push("--append-system-prompt", promptPath);
	}

	const placeholderRegex = new RegExp(ctx.placeholder.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "g");
	const task = step.task.replace(placeholderRegex, () => ctx.previousOutput);

	const TASK_ARG_LIMIT = 8000;
	if (task.length > TASK_ARG_LIMIT) {
		if (!tmpDir) tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "pi-subagent-"));
		const taskFilePath = path.join(tmpDir, "task.md");
		fs.writeFileSync(taskFilePath, `Task: ${task}`, { mode: 0o600 });
		args.push(`@${taskFilePath}`);
	} else {
		args.push(`Task: ${task}`);
	}

	let artifactPaths: ArtifactPaths | undefined;
	if (ctx.artifactsDir && ctx.artifactConfig?.enabled !== false) {
		const index = ctx.flatStepCount > 1 ? ctx.flatIndex : undefined;
		artifactPaths = getArtifactPaths(ctx.artifactsDir, ctx.id, step.agent, index);
		fs.mkdirSync(ctx.artifactsDir, { recursive: true });
		if (ctx.artifactConfig?.includeInput !== false) {
			fs.writeFileSync(artifactPaths.inputPath, `# Task for ${step.agent}\n\n${task}`, "utf-8");
		}
	}

	const mcpEnv: Record<string, string | undefined> = {};
	if (step.mcpDirectTools?.length) {
		mcpEnv.MCP_DIRECT_TOOLS = step.mcpDirectTools.join(",");
	} else {
		mcpEnv.MCP_DIRECT_TOOLS = "__none__";
	}

	// Activity tracker — populated by JSONL event parsing in runPiStreaming
	const toolActivity: ToolActivity = { toolCount: 0, tokensIn: 0, tokensOut: 0, cost: 0, turns: 0 };

	// Heartbeat: periodically update status.json with output log size + tool activity
	let heartbeatPid: number | undefined;
	const heartbeatInterval = setInterval(() => {
		try {
			if (ctx.statusPayload && ctx.statusPath) {
				const logSize = fs.existsSync(ctx.outputFile)
					? fs.statSync(ctx.outputFile).size
					: 0;
				ctx.statusPayload.lastUpdate = Date.now();
				if (ctx.statusPayload.steps?.[ctx.flatIndex]) {
					const stepStatus = ctx.statusPayload.steps[ctx.flatIndex] as Record<string, unknown>;
					stepStatus.outputBytes = logSize;

					// Tool activity from JSONL event parsing
					if (toolActivity.toolCount > 0) {
						stepStatus.toolCount = toolActivity.toolCount;
					}
					if (toolActivity.currentTool) {
						stepStatus.currentTool = toolActivity.currentTool;
						if (toolActivity.currentToolArgs) {
							stepStatus.currentToolArgs = toolActivity.currentToolArgs;
						}
					} else {
						delete stepStatus.currentTool;
						delete stepStatus.currentToolArgs;
					}

					// Live token/cost tracking from message_end events
					if (toolActivity.turns > 0) {
						stepStatus.liveTokens = {
							input: toolActivity.tokensIn,
							output: toolActivity.tokensOut,
							total: toolActivity.tokensIn + toolActivity.tokensOut,
						};
						if (toolActivity.cost > 0) {
							stepStatus.liveCost = toolActivity.cost;
						}
						stepStatus.turns = toolActivity.turns;
					}
					if (toolActivity.model && !stepStatus.resolvedModel) {
						stepStatus.resolvedModel = toolActivity.model;
					}

					// Detect child pi processes (sync subagent calls) for visibility
					if (heartbeatPid) {
						try {
							const { execSync } = require("node:child_process");
							const out = execSync(`pgrep -P ${heartbeatPid} -a 2>/dev/null || true`, { encoding: "utf-8", timeout: 2000 });
							const piChildren = out.split("\n").filter((l: string) => l.includes("pi") && l.includes("-p")).length;
							if (piChildren > 0) stepStatus.activeChildren = piChildren;
							else delete stepStatus.activeChildren;
						} catch {}
					}
				}
				writeJson(ctx.statusPath, ctx.statusPayload);
			}
		} catch {}
	}, 15_000);

	const result = await runPiStreaming(args, step.cwd ?? ctx.cwd, ctx.outputFile, mcpEnv, ctx.piPackageRoot, (pid) => { heartbeatPid = pid; }, toolActivity);
	clearInterval(heartbeatInterval);

	if (tmpDir) {
		try { fs.rmSync(tmpDir, { recursive: true }); } catch {}
	}

	// Extract final assistant text from JSONL output file
	// Streams line-by-line to avoid loading 500MB+ files into memory
	// (thinking:high models can produce huge message_update deltas)
	let output = "";
	try {
		const rl = require("readline").createInterface({
			input: fs.createReadStream(ctx.outputFile, { encoding: "utf-8" }),
			crlfDelay: Infinity,
		});
		for await (const line of rl) {
			if (!line.startsWith("{")) continue;
			// Skip message_update lines (can be 800KB+ with thinking deltas)
			if (line.includes('"message_update"')) continue;
			try {
				const evt = JSON.parse(line);
				if (evt.type === "message_end" && evt.message?.role === "assistant") {
					const content = evt.message.content;
					if (Array.isArray(content)) {
						for (const part of content) {
							if (part.type === "text" && part.text) output = part.text;
						}
					} else if (typeof content === "string") {
						output = content;
					}
				}
			} catch {}
		}
	} catch {}
	// Fallback: if no structured output found, read last 10KB of file
	if (!output) {
		try {
			const stat = fs.statSync(ctx.outputFile);
			const start = Math.max(0, stat.size - 10240);
			const buf = Buffer.alloc(Math.min(stat.size, 10240));
			const fd = fs.openSync(ctx.outputFile, "r");
			fs.readSync(fd, buf, 0, buf.length, start);
			fs.closeSync(fd);
			output = buf.toString("utf-8");
		} catch {}
	}
	let outputForSummary = output;
	if (step.outputPath && result.exitCode === 0) {
		const persisted = persistSingleOutput(step.outputPath, output);
		if (persisted.savedPath) {
			outputForSummary = output
				? `${output}\n\n📄 Output saved to: ${persisted.savedPath}`
				: `📄 Output saved to: ${persisted.savedPath}`;
		} else if (persisted.error) {
			outputForSummary = output
				? `${output}\n\n⚠️ Failed to save output to: ${step.outputPath}\n${persisted.error}`
				: `⚠️ Failed to save output to: ${step.outputPath}\n${persisted.error}`;
		}
	}

	if (artifactPaths && ctx.artifactConfig?.enabled !== false) {
		if (ctx.artifactConfig?.includeOutput !== false) {
			fs.writeFileSync(artifactPaths.outputPath, output, "utf-8");
		}
		if (ctx.artifactConfig?.includeMetadata !== false) {
			fs.writeFileSync(
				artifactPaths.metadataPath,
				JSON.stringify({
					runId: ctx.id,
					agent: step.agent,
					task,
					exitCode: result.exitCode,
					skills: step.skills,
					timestamp: Date.now(),
				}, null, 2),
				"utf-8",
			);
		}
	}

	// Delete the output log file — it can be 500MB+ with --mode json thinking deltas.
	// The useful content has been extracted to `output` and saved to artifacts above.
	// Keep log on failure so subagent_status failure triage can pattern-match errors.
	if (result.exitCode === 0) {
		try {
			if (fs.existsSync(ctx.outputFile)) fs.unlinkSync(ctx.outputFile);
		} catch {}
	}

	return { agent: step.agent, output: outputForSummary, exitCode: result.exitCode, artifactPaths };
}

async function runSubagent(config: SubagentRunConfig): Promise<void> {
	const { id, steps, resultPath, cwd, placeholder, taskIndex, totalTasks, maxOutput, artifactsDir, artifactConfig } =
		config;
	let previousOutput = "";
	const results: StepResult[] = [];
	const overallStartTime = Date.now();
	const shareEnabled = config.share === true;
	const sessionEnabled = Boolean(config.sessionDir) || shareEnabled;
	const asyncDir = config.asyncDir;
	const statusPath = path.join(asyncDir, "status.json");
	const eventsPath = path.join(asyncDir, "events.jsonl");
	const logPath = path.join(asyncDir, `subagent-log-${id}.md`);
	let previousCumulativeTokens: TokenUsage = { input: 0, output: 0, total: 0 };

	// Register paths for crash handlers
	_activeAsyncDir = asyncDir;
	_activeEventsPath = eventsPath;
	_activeStatusPath = statusPath;
	_activeRunId = id;
	_activeStartedAt = overallStartTime;

	// Flatten steps for status tracking (parallel groups expand to individual entries)
	const flatSteps = flattenSteps(steps);
	const statusPayload: {
		runId: string;
		mode: "single" | "chain";
		state: "queued" | "running" | "complete" | "failed";
		startedAt: number;
		endedAt?: number;
		lastUpdate: number;
		pid: number;
		cwd: string;
		currentStep: number;
		steps: Array<{
			agent: string;
			model?: string;
			status: "pending" | "running" | "complete" | "failed";
			startedAt?: number;
			endedAt?: number;
			durationMs?: number;
			exitCode?: number | null;
			error?: string;
			tokens?: TokenUsage;
			skills?: string[];
		}>;
		artifactsDir?: string;
		sessionDir?: string;
		outputFile?: string;
		totalTokens?: TokenUsage;
		sessionFile?: string;
		shareUrl?: string;
		gistUrl?: string;
		shareError?: string;
		error?: string;
	} = {
		runId: id,
		mode: flatSteps.length > 1 ? "chain" : "single",
		state: "running",
		startedAt: overallStartTime,
		lastUpdate: overallStartTime,
		pid: process.pid,
		cwd,
		currentStep: 0,
		steps: flatSteps.map((step) => ({ agent: step.agent, model: step.model, status: "pending", skills: step.skills })),
		artifactsDir,
		sessionDir: config.sessionDir,
		outputFile: path.join(asyncDir, "output-0.log"),
	};

	fs.mkdirSync(asyncDir, { recursive: true });
	writeJson(statusPath, statusPayload);
	appendJsonl(
		eventsPath,
		JSON.stringify({
			type: "subagent.run.started",
			ts: overallStartTime,
			runId: id,
			mode: statusPayload.mode,
			cwd,
			pid: process.pid,
		}),
	);

	// Track the flat index into statusPayload.steps across sequential + parallel steps
	let flatIndex = 0;

	for (let stepIndex = 0; stepIndex < steps.length; stepIndex++) {
		const step = steps[stepIndex];

		if (isParallelGroup(step)) {
			// === PARALLEL STEP GROUP ===
			const group = step;
			const concurrency = group.concurrency ?? MAX_PARALLEL_CONCURRENCY;
			const failFast = group.failFast ?? false;
			const groupStartFlatIndex = flatIndex;
			let aborted = false;

			// Mark all tasks in the group as running
			const groupStartTime = Date.now();
			for (let t = 0; t < group.parallel.length; t++) {
				const fi = groupStartFlatIndex + t;
				statusPayload.steps[fi].status = "running";
				statusPayload.steps[fi].startedAt = groupStartTime;
			}
			statusPayload.currentStep = groupStartFlatIndex;
			statusPayload.lastUpdate = groupStartTime;
			statusPayload.outputFile = path.join(asyncDir, `output-${groupStartFlatIndex}.log`);
			writeJson(statusPath, statusPayload);

			appendJsonl(eventsPath, JSON.stringify({
				type: "subagent.parallel.started",
				ts: groupStartTime,
				runId: id,
				stepIndex,
				agents: group.parallel.map((t) => t.agent),
				count: group.parallel.length,
			}));

			const parallelResults = await mapConcurrent(
				group.parallel,
				concurrency,
				async (task, taskIdx) => {
					if (aborted && failFast) {
						return { agent: task.agent, output: "(skipped — fail-fast)", exitCode: -1 as number | null, skipped: true };
					}

					const fi = groupStartFlatIndex + taskIdx;
					const taskStartTime = Date.now();

					appendJsonl(eventsPath, JSON.stringify({
						type: "subagent.step.started", ts: taskStartTime, runId: id, stepIndex: fi, agent: task.agent,
					}));

					// Each parallel task gets its own session subdirectory to avoid conflicts
					const taskSessionDir = config.sessionDir
						? path.join(config.sessionDir, `parallel-${taskIdx}`)
						: undefined;

					const singleResult = await runSingleStep(task, {
						previousOutput, placeholder, cwd, sessionEnabled,
						sessionDir: taskSessionDir,
						artifactsDir, artifactConfig, id,
						flatIndex: fi, flatStepCount: flatSteps.length,
						outputFile: path.join(asyncDir, `output-${fi}.log`),
						piPackageRoot: config.piPackageRoot,
						statusPayload: statusPayload as Record<string, unknown>,
						statusPath,
					});

					const taskEndTime = Date.now();
					const taskDuration = taskEndTime - taskStartTime;

					statusPayload.steps[fi].status = singleResult.exitCode === 0 ? "complete" : "failed";
					statusPayload.steps[fi].endedAt = taskEndTime;
					statusPayload.steps[fi].durationMs = taskDuration;
					statusPayload.steps[fi].exitCode = singleResult.exitCode;
					statusPayload.lastUpdate = taskEndTime;
					writeJson(statusPath, statusPayload);

					appendJsonl(eventsPath, JSON.stringify({
						type: singleResult.exitCode === 0 ? "subagent.step.completed" : "subagent.step.failed",
						ts: taskEndTime, runId: id, stepIndex: fi, agent: task.agent,
						exitCode: singleResult.exitCode, durationMs: taskDuration,
					}));

					if (singleResult.exitCode !== 0 && failFast) aborted = true;
					return { ...singleResult, skipped: false };
				},
			);

			flatIndex += group.parallel.length;

			// Aggregate token usage from parallel task session dirs
			if (config.sessionDir) {
				for (let t = 0; t < group.parallel.length; t++) {
					const taskSessionDir = path.join(config.sessionDir, `parallel-${t}`);
					const taskTokens = parseSessionTokens(taskSessionDir);
					if (taskTokens) {
						const fi = groupStartFlatIndex + t;
						statusPayload.steps[fi].tokens = taskTokens;
						previousCumulativeTokens = {
							input: previousCumulativeTokens.input + taskTokens.input,
							output: previousCumulativeTokens.output + taskTokens.output,
							total: previousCumulativeTokens.total + taskTokens.total,
						};
					}
				}
				statusPayload.totalTokens = { ...previousCumulativeTokens };
				statusPayload.lastUpdate = Date.now();
				writeJson(statusPath, statusPayload);
			}

			// Collect results
			for (const pr of parallelResults) {
				results.push({
					agent: pr.agent,
					output: pr.output,
					success: pr.exitCode === 0,
					skipped: pr.skipped,
					artifactPaths: pr.artifactPaths,
				});
			}

			// Aggregate parallel outputs for {previous}
			previousOutput = aggregateParallelOutputs(
				parallelResults.map((r) => ({ agent: r.agent, output: r.output, exitCode: r.exitCode })),
			);

			appendJsonl(eventsPath, JSON.stringify({
				type: "subagent.parallel.completed",
				ts: Date.now(),
				runId: id,
				stepIndex,
				success: parallelResults.every((r) => r.exitCode === 0 || r.exitCode === -1),
			}));

			// If any parallel task failed (not skipped), stop the chain
			if (parallelResults.some((r) => r.exitCode !== 0 && r.exitCode !== -1)) {
				break;
			}
		} else {
			// === SEQUENTIAL STEP ===
			const seqStep = step as SubagentStep;
			const stepStartTime = Date.now();
			statusPayload.currentStep = flatIndex;
			statusPayload.steps[flatIndex].status = "running";
			statusPayload.steps[flatIndex].skills = seqStep.skills;
			statusPayload.steps[flatIndex].startedAt = stepStartTime;
			statusPayload.lastUpdate = stepStartTime;
			statusPayload.outputFile = path.join(asyncDir, `output-${flatIndex}.log`);
			writeJson(statusPath, statusPayload);

			appendJsonl(eventsPath, JSON.stringify({
				type: "subagent.step.started",
				ts: stepStartTime,
				runId: id,
				stepIndex: flatIndex,
				agent: seqStep.agent,
			}));

			const singleResult = await runSingleStep(seqStep, {
				previousOutput, placeholder, cwd, sessionEnabled,
				sessionDir: config.sessionDir,
				artifactsDir, artifactConfig, id,
				flatIndex, flatStepCount: flatSteps.length,
				outputFile: path.join(asyncDir, `output-${flatIndex}.log`),
				piPackageRoot: config.piPackageRoot,
				statusPayload: statusPayload as Record<string, unknown>,
				statusPath,
			});

			previousOutput = singleResult.output;
			results.push({
				agent: singleResult.agent,
				output: singleResult.output,
				success: singleResult.exitCode === 0,
				artifactPaths: singleResult.artifactPaths,
			});

			const cumulativeTokens = config.sessionDir ? parseSessionTokens(config.sessionDir) : null;
			const stepTokens: TokenUsage | null = cumulativeTokens
				? {
						input: cumulativeTokens.input - previousCumulativeTokens.input,
						output: cumulativeTokens.output - previousCumulativeTokens.output,
						total: cumulativeTokens.total - previousCumulativeTokens.total,
					}
				: null;
			if (cumulativeTokens) {
				previousCumulativeTokens = cumulativeTokens;
			}

			const stepEndTime = Date.now();
			statusPayload.steps[flatIndex].status = singleResult.exitCode === 0 ? "complete" : "failed";
			statusPayload.steps[flatIndex].endedAt = stepEndTime;
			statusPayload.steps[flatIndex].durationMs = stepEndTime - stepStartTime;
			statusPayload.steps[flatIndex].exitCode = singleResult.exitCode;
			if (stepTokens) {
				statusPayload.steps[flatIndex].tokens = stepTokens;
				statusPayload.totalTokens = { ...previousCumulativeTokens };
			}
			statusPayload.lastUpdate = stepEndTime;
			writeJson(statusPath, statusPayload);

			appendJsonl(eventsPath, JSON.stringify({
				type: singleResult.exitCode === 0 ? "subagent.step.completed" : "subagent.step.failed",
				ts: stepEndTime,
				runId: id,
				stepIndex: flatIndex,
				agent: seqStep.agent,
				exitCode: singleResult.exitCode,
				durationMs: stepEndTime - stepStartTime,
				tokens: stepTokens,
			}));

			flatIndex++;
			if (singleResult.exitCode !== 0) {
				break;
			}
		}
	}

	let summary = results.map((r) => `${r.agent}:\n${r.output}`).join("\n\n");
	let truncated = false;

	if (maxOutput) {
		const config = { ...DEFAULT_MAX_OUTPUT, ...maxOutput };
		const lastArtifactPath = results[results.length - 1]?.artifactPaths?.outputPath;
		const truncResult = truncateOutput(summary, config, lastArtifactPath);
		if (truncResult.truncated) {
			summary = truncResult.text;
			truncated = true;
		}
	}

	const agentName = flatSteps.length === 1
		? flatSteps[0].agent
		: `chain:${flatSteps.map((s) => s.agent).join("->")}`;
	let sessionFile: string | undefined;
	let shareUrl: string | undefined;
	let gistUrl: string | undefined;
	let shareError: string | undefined;

	if (shareEnabled && config.sessionDir) {
		sessionFile = findLatestSessionFile(config.sessionDir) ?? undefined;
		if (sessionFile) {
			try {
				const htmlPath = await exportSessionHtml(sessionFile, config.sessionDir, config.piPackageRoot);
				const share = createShareLink(htmlPath);
				if ("error" in share) shareError = share.error;
				else {
					shareUrl = share.shareUrl;
					gistUrl = share.gistUrl;
				}
			} catch (err) {
				shareError = String(err);
			}
		} else {
			shareError = "Session file not found.";
		}
	}

	const runEndedAt = Date.now();
	statusPayload.state = results.every((r) => r.success) ? "complete" : "failed";
	statusPayload.endedAt = runEndedAt;
	statusPayload.lastUpdate = runEndedAt;
	statusPayload.sessionFile = sessionFile;
	statusPayload.shareUrl = shareUrl;
	statusPayload.gistUrl = gistUrl;
	statusPayload.shareError = shareError;
	if (statusPayload.state === "failed") {
		const failedStep = statusPayload.steps.find((s) => s.status === "failed");
		if (failedStep?.agent) {
			statusPayload.error = `Step failed: ${failedStep.agent}`;
		}
	}
	writeJson(statusPath, statusPayload);
	appendJsonl(
		eventsPath,
		JSON.stringify({
			type: "subagent.run.completed",
			ts: runEndedAt,
			runId: id,
			status: statusPayload.state,
			durationMs: runEndedAt - overallStartTime,
		}),
	);
	writeRunLog(logPath, {
		id,
		mode: statusPayload.mode,
		cwd,
		startedAt: overallStartTime,
		endedAt: runEndedAt,
		steps: statusPayload.steps.map((step) => ({
			agent: step.agent,
			status: step.status,
			durationMs: step.durationMs,
		})),
		summary,
		truncated,
		artifactsDir,
		sessionFile,
		shareUrl,
		shareError,
	});

	try {
		fs.mkdirSync(path.dirname(resultPath), { recursive: true });
		fs.writeFileSync(
			resultPath,
			JSON.stringify({
				id,
				agent: agentName,
				success: results.every((r) => r.success),
				summary,
				results: results.map((r) => ({
					agent: r.agent,
					output: r.output,
					success: r.success,
					skipped: r.skipped || undefined,
					artifactPaths: r.artifactPaths,
					truncated: r.truncated,
				})),
				exitCode: results.every((r) => r.success) ? 0 : 1,
				timestamp: runEndedAt,
				durationMs: runEndedAt - overallStartTime,
				truncated,
				artifactsDir,
				cwd,
				asyncDir,
				sessionId: config.sessionId,
				sessionFile,
				shareUrl,
				gistUrl,
				shareError,
				...(taskIndex !== undefined && { taskIndex }),
				...(totalTasks !== undefined && { totalTasks }),
			}),
		);
	} catch (err) {
		console.error(`Failed to write result file ${resultPath}:`, err);
	}
}

// --- Crash / signal handlers ---
// If the runner dies unexpectedly, mark status as failed so the widget
// doesn't show "running" forever and notifications can fire.
let _activeAsyncDir: string | undefined;
let _activeEventsPath: string | undefined;
let _activeStatusPath: string | undefined;
let _activeRunId: string | undefined;
let _activeStartedAt: number | undefined;

function writeCrashStatus(reason: string): void {
	const now = Date.now();
	if (_activeStatusPath) {
		try {
			const existing = JSON.parse(fs.readFileSync(_activeStatusPath, "utf-8"));
			existing.state = "failed";
			existing.endedAt = now;
			existing.lastUpdate = now;
			existing.error = reason;
			fs.writeFileSync(_activeStatusPath, JSON.stringify(existing, null, 2));
		} catch {
			// status.json may not exist yet
			try {
				fs.writeFileSync(_activeStatusPath, JSON.stringify({
					runId: _activeRunId ?? "unknown",
					state: "failed",
					endedAt: now,
					lastUpdate: now,
					error: reason,
				}));
			} catch {}
		}
	}
	if (_activeEventsPath) {
		try {
			appendJsonl(_activeEventsPath, JSON.stringify({
				type: "subagent.run.crashed",
				ts: now,
				runId: _activeRunId ?? "unknown",
				reason,
				durationMs: _activeStartedAt ? now - _activeStartedAt : undefined,
			}));
		} catch {}
	}
}

for (const sig of ["SIGTERM", "SIGINT", "SIGHUP"] as const) {
	process.on(sig, () => {
		writeCrashStatus(`Process killed by ${sig}`);
		process.exit(128);
	});
}
process.on("uncaughtException", (err) => {
	writeCrashStatus(`Uncaught exception: ${err?.message ?? String(err)}`);
	console.error("Subagent runner uncaught exception:", err);
	process.exit(1);
});
process.on("unhandledRejection", (reason) => {
	writeCrashStatus(`Unhandled rejection: ${reason instanceof Error ? reason.message : String(reason)}`);
	console.error("Subagent runner unhandled rejection:", reason);
	process.exit(1);
});

const configArg = process.argv[2];
if (configArg) {
	try {
		const configJson = fs.readFileSync(configArg, "utf-8");
		const config = JSON.parse(configJson) as SubagentRunConfig;
		try {
			fs.unlinkSync(configArg);
		} catch {}
		runSubagent(config).catch((runErr) => {
			writeCrashStatus(`runSubagent error: ${runErr?.message ?? String(runErr)}`);
			console.error("Subagent runner error:", runErr);
			process.exit(1);
		});
	} catch (err) {
		console.error("Subagent runner error:", err);
		process.exit(1);
	}
} else {
	let input = "";
	process.stdin.setEncoding("utf-8");
	process.stdin.on("data", (chunk) => {
		input += chunk;
	});
	process.stdin.on("end", () => {
		try {
			const config = JSON.parse(input) as SubagentRunConfig;
			runSubagent(config).catch((runErr) => {
				writeCrashStatus(`runSubagent error: ${runErr?.message ?? String(runErr)}`);
				console.error("Subagent runner error:", runErr);
				process.exit(1);
			});
		} catch (err) {
			console.error("Subagent runner error:", err);
			process.exit(1);
		}
	});
}
