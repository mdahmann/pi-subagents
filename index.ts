/**
 * Subagent Tool
 *
 * Full-featured subagent with sync and async modes.
 * - Sync (default): Streams output, renders markdown, tracks usage
 * - Async: Background execution, emits events when done
 *
 * Modes: single (agent + task), parallel (tasks[]), chain (chain[] with {previous})
 * Toggle: async parameter (default: false, configurable via config.json)
 *
 * Config file: ~/.pi/agent/extensions/subagent/config.json
 *   { "asyncByDefault": true, "asyncLogNoisyEvents": false }
 */

import { randomUUID } from "node:crypto";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { type ExtensionAPI, type ExtensionContext, type ToolDefinition } from "@mariozechner/pi-coding-agent";
import { Text, matchesKey, Key, truncateToWidth } from "@mariozechner/pi-tui";
import { type AgentConfig, type AgentScope, discoverAgents, discoverAgentsAll } from "./agents.js";
import { resolveExecutionAgentScope } from "./agent-scope.js";
import { cleanupOldChainDirs, getStepAgents, isParallelStep, resolveStepBehavior, type ChainStep, type SequentialStep } from "./settings.js";
import { ChainClarifyComponent, type ChainClarifyResult, type ModelInfo } from "./chain-clarify.js";
import { cleanupAllArtifactDirs, cleanupOldArtifacts, getArtifactsDir } from "./artifacts.js";
import {
	type AgentProgress,
	type ArtifactConfig,
	type ArtifactPaths,
	type AsyncJobState,
	type Details,
	type ExtensionConfig,
	type SingleResult,
	ASYNC_DIR,
	DEFAULT_ARTIFACT_CONFIG,
	DEFAULT_MAX_OUTPUT,
	MAX_CONCURRENCY,
	MAX_PARALLEL,
	POLL_INTERVAL_MS,
	RESULTS_DIR,
	WIDGET_KEY,
	checkSubagentDepth,
} from "./types.js";
import { readStatus, findByPrefix, getFinalOutput, getOutputTail, mapConcurrent, getLastActivity } from "./utils.js";
import { buildCompletionKey, markSeenWithTtl } from "./completion-dedupe.js";
import { createFileCoalescer } from "./file-coalescer.js";
import { runSync } from "./execution.js";
import { renderWidget, renderSubagentResult } from "./render.js";
import { SubagentParams, StatusParams } from "./schemas.js";
import { executeChain } from "./chain-execution.js";
import { isAsyncAvailable, executeAsyncChain, executeAsyncSingle } from "./async-execution.js";
import { discoverAvailableSkills, normalizeSkillInput } from "./skills.js";
import { finalizeSingleOutput, injectSingleOutputInstruction, resolveSingleOutputPath } from "./single-output.js";
import { AgentManagerComponent, type ManagerResult } from "./agent-manager.js";
import { recordRun } from "./run-history.js";
import { handleManagementAction } from "./agent-management.js";

// ExtensionConfig is now imported from ./types.js

function loadConfig(): ExtensionConfig {
	const configPath = path.join(os.homedir(), ".pi", "agent", "extensions", "subagent", "config.json");
	try {
		if (fs.existsSync(configPath)) {
			return JSON.parse(fs.readFileSync(configPath, "utf-8")) as ExtensionConfig;
		}
	} catch {}
	return {};
}

/**
 * Create a directory and verify it is actually accessible.
 * On Windows with Azure AD/Entra ID, directories created shortly after
 * wake-from-sleep can end up with broken NTFS ACLs (null DACL) when the
 * cloud SID cannot be resolved without network connectivity. This leaves
 * the directory completely inaccessible to the creating user.
 */
function ensureAccessibleDir(dirPath: string): void {
	fs.mkdirSync(dirPath, { recursive: true });
	try {
		fs.accessSync(dirPath, fs.constants.R_OK | fs.constants.W_OK);
	} catch {
		// Directory exists but is inaccessible — remove and recreate
		try {
			fs.rmSync(dirPath, { recursive: true, force: true });
		} catch {}
		fs.mkdirSync(dirPath, { recursive: true });
		// Verify recovery succeeded
		fs.accessSync(dirPath, fs.constants.R_OK | fs.constants.W_OK);
	}
}

const ASYNC_SUCCESS_RETENTION_MS = 2 * 60 * 60 * 1000; // 2h
const ASYNC_FAILED_RETENTION_MS = 6 * 60 * 60 * 1000;  // 6h
const ASYNC_UNKNOWN_RETENTION_MS = 24 * 60 * 60 * 1000; // 24h fallback

function isPidAlive(pid: number | undefined): boolean {
	if (!pid || !Number.isFinite(pid)) return false;
	try {
		process.kill(pid, 0);
		return true;
	} catch {
		return false;
	}
}

function getOutputMtimeMs(filePath: string | undefined): number {
	if (!filePath) return 0;
	try {
		return fs.statSync(filePath).mtimeMs || 0;
	} catch {
		return 0;
	}
}

/**
 * Cleanup async run directories using state-aware retention:
 * - complete: delete after 2h
 * - failed: delete after 6h
 * - running/queued: keep while process is alive
 * - unknown/no status: delete after 24h
 */
function cleanupOldAsyncDirs(dir: string): void {
	if (!fs.existsSync(dir)) return;
	const now = Date.now();
	let entries: string[];
	try { entries = fs.readdirSync(dir); } catch { return; }
	for (const entry of entries) {
		try {
			const entryPath = path.join(dir, entry);
			const stat = fs.statSync(entryPath);
			if (!stat.isDirectory()) continue;

			const status = readStatus(entryPath);
			if (!status) {
				if (now - stat.mtimeMs > ASYNC_UNKNOWN_RETENTION_MS) {
					fs.rmSync(entryPath, { recursive: true, force: true });
				}
				continue;
			}

			const state = status.state;
			const statusPid = (status as unknown as { pid?: number }).pid;
			const statusTime = status.endedAt ?? status.lastUpdate ?? stat.mtimeMs;

			if (state === "running" || state === "queued") {
				if (isPidAlive(statusPid)) continue;
				// Stale running/queued with dead pid: treat like failed and keep a bit for triage.
				if (now - statusTime > ASYNC_FAILED_RETENTION_MS) {
					fs.rmSync(entryPath, { recursive: true, force: true });
				}
				continue;
			}

			const ttl = state === "complete"
				? ASYNC_SUCCESS_RETENTION_MS
				: state === "failed"
					? ASYNC_FAILED_RETENTION_MS
					: ASYNC_UNKNOWN_RETENTION_MS;

			if (now - statusTime > ttl) {
				fs.rmSync(entryPath, { recursive: true, force: true });
			}
		} catch {}
	}
}

/** Delete individual files older than maxAgeMs from a flat directory. */
function cleanupOldFiles(dir: string, maxAgeMs: number): void {
	if (!fs.existsSync(dir)) return;
	const now = Date.now();
	let entries: string[];
	try { entries = fs.readdirSync(dir); } catch { return; }
	for (const entry of entries) {
		try {
			const entryPath = path.join(dir, entry);
			const stat = fs.statSync(entryPath);
			if (stat.isFile() && now - stat.mtimeMs > maxAgeMs) {
				fs.unlinkSync(entryPath);
			}
		} catch {}
	}
}

/** Clean up orphaned /tmp/pi-subagent-XXXXXX dirs (prompt files from crashed processes). */
function cleanupOrphanedSubagentDirs(maxAgeMs: number): void {
	const tmpdir = os.tmpdir();
	let entries: string[];
	try { entries = fs.readdirSync(tmpdir); } catch { return; }
	const now = Date.now();
	for (const entry of entries) {
		if (!entry.startsWith("pi-subagent-") || entry.startsWith("pi-subagent-artifacts") || entry.startsWith("pi-subagent-session")) continue;
		try {
			const entryPath = path.join(tmpdir, entry);
			const stat = fs.statSync(entryPath);
			if (stat.isDirectory() && now - stat.mtimeMs > maxAgeMs) {
				fs.rmSync(entryPath, { recursive: true, force: true });
			}
		} catch {}
	}
}

export default function registerSubagentExtension(pi: ExtensionAPI): void {
	ensureAccessibleDir(RESULTS_DIR);
	ensureAccessibleDir(ASYNC_DIR);

	// Cleanup old chain directories on startup (after 24h)
	cleanupOldChainDirs();

	// Cleanup old async run directories on startup (state-aware retention)
	// These contain output-*.log files that can be 500MB+ each (--mode json thinking deltas)
	cleanupOldAsyncDirs(ASYNC_DIR);
	// Cleanup old result files (after 7 days)
	cleanupOldFiles(RESULTS_DIR, 7 * 24 * 60 * 60 * 1000);
	// Cleanup orphaned ephemeral prompt dirs (after 24h)
	cleanupOrphanedSubagentDirs(24 * 60 * 60 * 1000);

	const config = loadConfig();
	const asyncByDefault = config.asyncByDefault === true;
	const asyncLogNoisyEvents =
		config.asyncLogNoisyEvents === true || process.env.PI_SUBAGENT_LOG_NOISY_EVENTS === "1";

	const tempArtifactsDir = getArtifactsDir(null);
	cleanupAllArtifactDirs(DEFAULT_ARTIFACT_CONFIG.cleanupDays);
	let baseCwd = process.cwd();
	let currentSessionId: string | null = null;
	const asyncJobs = new Map<string, AsyncJobState>();
	const cleanupTimers = new Map<string, ReturnType<typeof setTimeout>>(); // Widget cleanup timers
	const asyncDirCleanupTimers = new Map<string, ReturnType<typeof setTimeout>>(); // Disk cleanup timers
	let lastUiContext: ExtensionContext | null = null;
	let poller: NodeJS.Timeout | null = null;

	const scheduleFinishedJobCleanup = (asyncId: string, status: "complete" | "failed", asyncDir?: string) => {
		const existingUiTimer = cleanupTimers.get(asyncId);
		if (existingUiTimer) clearTimeout(existingUiTimer);
		const uiTimer = setTimeout(() => {
			cleanupTimers.delete(asyncId);
			asyncJobs.delete(asyncId);
			if (lastUiContext) renderWidget(lastUiContext, Array.from(asyncJobs.values()));
		}, 10000);
		cleanupTimers.set(asyncId, uiTimer);

		if (!asyncDir) return;
		const existingDiskTimer = asyncDirCleanupTimers.get(asyncId);
		if (existingDiskTimer) clearTimeout(existingDiskTimer);
		const ttl = status === "complete" ? ASYNC_SUCCESS_RETENTION_MS : ASYNC_FAILED_RETENTION_MS;
		const diskTimer = setTimeout(() => {
			asyncDirCleanupTimers.delete(asyncId);
			try {
				const status = readStatus(asyncDir);
				if (status && (status.state === "running" || status.state === "queued")) {
					const pid = (status as unknown as { pid?: number }).pid;
					if (isPidAlive(pid)) return;
				}
				if (fs.existsSync(asyncDir)) {
					fs.rmSync(asyncDir, { recursive: true, force: true });
				}
			} catch {}
		}, ttl);
		asyncDirCleanupTimers.set(asyncId, diskTimer);
	};

	const ensurePoller = () => {
		if (poller) return;
		poller = setInterval(() => {
			if (!lastUiContext || !lastUiContext.hasUI) return;
			// Keep widget source aligned with /subagents overlay by continuously
			// pulling jobs from disk status files.
			rehydrateAsyncJobsFromDisk();
			if (asyncJobs.size === 0) {
				renderWidget(lastUiContext, []);
				return;
			}

			for (const job of asyncJobs.values()) {
				// Finished jobs should be auto-removed from widget shortly, even if
				// subagent:complete event was missed (e.g. hard kill/crash).
				if (job.status === "complete" || job.status === "failed") {
					if (!cleanupTimers.has(job.asyncId)) {
						scheduleFinishedJobCleanup(job.asyncId, job.status, job.asyncDir);
					}
					continue;
				}

				const status = readStatus(job.asyncDir);
				if (status) {
					let nextState = status.state;
					const statusPid = (status as unknown as { pid?: number }).pid;
					if ((nextState === "running" || nextState === "queued") && statusPid && !isPidAlive(statusPid)) {
						nextState = "failed";
						try {
							const statusPath = path.join(job.asyncDir, "status.json");
							const updated = {
								...(status as unknown as Record<string, unknown>),
								state: "failed",
								endedAt: (status as unknown as { endedAt?: number }).endedAt ?? Date.now(),
								lastUpdate: Date.now(),
								error: (status as unknown as { error?: string }).error ?? `Process ${statusPid} died (detected by widget poller)`,
							};
							fs.writeFileSync(statusPath, JSON.stringify(updated, null, 2));
						} catch {}
					}

					job.status = nextState;
					job.mode = status.mode;
					job.currentStep = status.currentStep ?? job.currentStep;
					job.stepsTotal = status.steps?.length ?? job.stepsTotal;
					job.startedAt = status.startedAt ?? job.startedAt;
					const observedUpdate = Math.max(
						status.lastUpdate ?? 0,
						status.endedAt ?? 0,
						getOutputMtimeMs(status.outputFile),
					);
					job.updatedAt = observedUpdate || Date.now();
					if (status.steps?.length) {
						job.agents = status.steps.map((step) => step.agent);
					}
					job.sessionDir = status.sessionDir ?? job.sessionDir;
					job.outputFile = status.outputFile ?? job.outputFile;
					job.totalTokens = status.totalTokens ?? job.totalTokens;
					// Pull live metrics from steps
					if (status.steps?.length) {
						const activeStep = status.steps[status.currentStep ?? 0] as Record<string, unknown>;
						if (!job.totalTokens) {
							const lt = activeStep?.liveTokens as { total?: number; input?: number; output?: number } | undefined;
							if (lt?.total) {
								job.totalTokens = { input: lt.input ?? 0, output: lt.output ?? 0, total: lt.total };
							}
						}
						job.currentTool = activeStep?.currentTool as string | undefined;
						job.currentToolArgs = activeStep?.currentToolArgs as string | undefined;
						job.activeChildren = typeof activeStep?.activeChildren === "number"
							? (activeStep.activeChildren as number)
							: undefined;

						// Model + cost + tool count from all steps
						const models: string[] = [];
						let totalCost = 0;
						let totalTools = 0;
						for (const s of status.steps as Record<string, unknown>[]) {
							models.push((s.resolvedModel ?? s.model ?? "") as string);
							if (typeof s.liveCost === "number") totalCost += s.liveCost;
							if (typeof s.toolCount === "number") totalTools += s.toolCount;
						}
						job.stepModels = models;
						if (totalCost > 0) job.liveCost = totalCost;
						if (totalTools > 0) job.toolCount = totalTools;
					}
					job.sessionFile = status.sessionFile ?? job.sessionFile;
					// job.shareUrl = status.shareUrl ?? job.shareUrl;
				} else {
					job.status = job.status === "queued" ? "running" : job.status;
					job.updatedAt = Date.now();
				}

				if ((job.status === "complete" || job.status === "failed") && !cleanupTimers.has(job.asyncId)) {
					scheduleFinishedJobCleanup(job.asyncId, job.status, job.asyncDir);
				}
			}

			renderWidget(lastUiContext, Array.from(asyncJobs.values()));
		}, POLL_INTERVAL_MS);
		poller.unref?.();
	};

	const rehydrateAsyncJobsFromDisk = () => {
		let dirs: string[] = [];
		try {
			dirs = fs.readdirSync(ASYNC_DIR).filter((d) => {
				try {
					return fs.statSync(path.join(ASYNC_DIR, d)).isDirectory();
				} catch {
					return false;
				}
			});
		} catch {
			return;
		}

		for (const dir of dirs) {
			const dirPath = path.join(ASYNC_DIR, dir);
			const status = readStatus(dirPath);
			if (!status) continue;

			let nextState = status.state ?? "failed";
			const statusPid = (status as unknown as { pid?: number }).pid;
			if ((nextState === "running" || nextState === "queued") && statusPid && !isPidAlive(statusPid)) {
				nextState = "failed";
				try {
					const statusPath = path.join(dirPath, "status.json");
					const updated = {
						...(status as unknown as Record<string, unknown>),
						state: "failed",
						endedAt: (status as unknown as { endedAt?: number }).endedAt ?? Date.now(),
						lastUpdate: Date.now(),
						error:
							(status as unknown as { error?: string }).error ??
							`Process ${statusPid} died (detected by rehydrate)`,
					};
					fs.writeFileSync(statusPath, JSON.stringify(updated, null, 2));
				} catch {
					/* ignore */
				}
			}

			const steps = (status.steps ?? []) as Record<string, unknown>[];
			const stepModels = steps.map((s) => ((s.resolvedModel ?? s.model ?? "") as string));
			let liveCost = 0;
			let toolCount = 0;
			for (const s of steps) {
				if (typeof s.liveCost === "number") liveCost += s.liveCost;
				if (typeof s.toolCount === "number") toolCount += s.toolCount;
			}

			const activeStep = steps[status.currentStep ?? 0];
			const observedUpdate = Math.max(
				status.lastUpdate ?? 0,
				status.endedAt ?? 0,
				getOutputMtimeMs(status.outputFile),
			);

			const asyncId = dir.slice(0, 36);
			asyncJobs.set(asyncId, {
				asyncId,
				asyncDir: dirPath,
				status: nextState,
				mode: status.mode ?? "single",
				agents: status.steps?.map((s: { agent: string }) => s.agent),
				currentStep: status.currentStep,
				stepsTotal: status.steps?.length,
				startedAt: status.startedAt,
				updatedAt: observedUpdate || Date.now(),
				sessionDir: status.sessionDir,
				outputFile: status.outputFile,
				totalTokens: status.totalTokens,
				currentTool: activeStep?.currentTool as string | undefined,
				currentToolArgs: activeStep?.currentToolArgs as string | undefined,
				activeChildren:
					typeof activeStep?.activeChildren === "number"
						? (activeStep.activeChildren as number)
						: undefined,
				stepModels,
				liveCost: liveCost > 0 ? liveCost : undefined,
				toolCount: toolCount > 0 ? toolCount : undefined,
				sessionFile: status.sessionFile,
			});

			if ((nextState === "complete" || nextState === "failed") && !cleanupTimers.has(asyncId)) {
				scheduleFinishedJobCleanup(asyncId, nextState, dirPath);
			}
		}
	};

	const completionSeen = new Map<string, number>();
	const completionTtlMs = 10 * 60 * 1000;
	const handleResult = (file: string) => {
		const p = path.join(RESULTS_DIR, file);
		if (!fs.existsSync(p)) return;
		try {
			const data = JSON.parse(fs.readFileSync(p, "utf-8"));
			if (data.sessionId && data.sessionId !== currentSessionId) return;
			if (!data.sessionId && data.cwd && data.cwd !== baseCwd) return;
			const now = Date.now();
			const completionKey = buildCompletionKey(data, `result:${file}`);
			if (markSeenWithTtl(completionSeen, completionKey, now, completionTtlMs)) {
				try {
					fs.unlinkSync(p);
				} catch {}
				return;
			}
			pi.events.emit("subagent:complete", data);
			fs.unlinkSync(p);
		} catch {}
	};

	const resultFileCoalescer = createFileCoalescer(handleResult, 50);
	let watcher: fs.FSWatcher | null = null;
	let watcherRestartTimer: ReturnType<typeof setTimeout> | null = null;

	function startResultWatcher(): void {
		watcherRestartTimer = null;
		try {
			watcher = fs.watch(RESULTS_DIR, (ev, file) => {
				if (ev !== "rename" || !file) return;
				const fileName = file.toString();
				if (!fileName.endsWith(".json")) return;
				resultFileCoalescer.schedule(fileName);
			});
			watcher.on("error", () => {
				// Watcher died (directory deleted, ACL change, etc.) — restart after delay
				watcher = null;
				watcherRestartTimer = setTimeout(() => {
					try {
						fs.mkdirSync(RESULTS_DIR, { recursive: true });
						startResultWatcher();
					} catch {}
				}, 3000);
			});
			watcher.unref?.();
		} catch {
			// fs.watch can throw if directory is inaccessible — retry after delay
			watcher = null;
			watcherRestartTimer = setTimeout(() => {
				try {
					fs.mkdirSync(RESULTS_DIR, { recursive: true });
					startResultWatcher();
				} catch {}
			}, 3000);
		}
	}

	startResultWatcher();
	fs.readdirSync(RESULTS_DIR)
		.filter((f) => f.endsWith(".json"))
		.forEach((file) => resultFileCoalescer.schedule(file, 0));

	const tool: ToolDefinition<typeof SubagentParams, Details> = {
		name: "subagent",
		label: "Subagent",
		description: `Delegate to subagents or manage agent definitions.

EXECUTION (use exactly ONE mode):
• SINGLE: { agent, task } - one task
• CHAIN: { chain: [{agent:"scout"}, {agent:"planner"}] } - sequential pipeline
• PARALLEL: { tasks: [{agent,task}, ...] } - concurrent execution

CHAIN TEMPLATE VARIABLES (use in task strings):
• {task} - The original task/request from the user
• {previous} - Text response from the previous step (empty for first step)
• {chain_dir} - Shared directory for chain files (e.g., <tmpdir>/pi-chain-runs/abc123/)

CHAIN DATA FLOW:
1. Each step's text response automatically becomes {previous} for the next step
2. Steps can also write files to {chain_dir} (via agent's "output" config)
3. Later steps can read those files (via agent's "reads" config)

Example: { chain: [{agent:"scout", task:"Analyze {task}"}, {agent:"planner", task:"Plan based on {previous}"}] }

MANAGEMENT (use action field — omit agent/task/chain/tasks):
• { action: "list" } - discover available agents and chains
• { action: "get", agent: "name" } - full agent detail with system prompt
• { action: "create", config: { name, description, systemPrompt, ... } } - create agent/chain
• { action: "update", agent: "name", config: { ... } } - modify fields (merge)
• { action: "delete", agent: "name" } - remove definition
• Use chainName instead of agent for chain operations`,
		parameters: SubagentParams,

		async execute(_id, params, signal, onUpdate, ctx) {
			baseCwd = ctx.cwd;
			if (params.action) {
				const validActions = ["list", "get", "create", "update", "delete"];
				if (!validActions.includes(params.action)) {
					return {
						content: [{ type: "text", text: `Unknown action: ${params.action}. Valid: ${validActions.join(", ")}` }],
						isError: true,
						details: { mode: "management" as const, results: [] },
					};
				}
				return handleManagementAction(params.action, params, ctx);
			}

			const { blocked, depth, maxDepth } = checkSubagentDepth();
			if (blocked) {
				return {
					content: [
						{
							type: "text",
							text:
								`Nested subagent call blocked (depth=${depth}, max=${maxDepth}). ` +
								"You are running at the maximum subagent nesting depth. " +
								"Complete your current task directly without delegating to further subagents.",
						},
					],
					isError: true,
					details: { mode: "single" as const, results: [] },
				};
			}

			const scope: AgentScope = resolveExecutionAgentScope(params.agentScope);
			currentSessionId = ctx.sessionManager.getSessionFile() ?? `session-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
			const agents = discoverAgents(ctx.cwd, scope).agents;
			const runId = randomUUID().slice(0, 8);
			const shareEnabled = params.share === true;
			const sessionEnabled = shareEnabled || Boolean(params.sessionDir);
			const sessionRoot = sessionEnabled
				? params.sessionDir
					? path.resolve(params.sessionDir)
					: fs.mkdtempSync(path.join(os.tmpdir(), "pi-subagent-session-"))
				: undefined;
			if (sessionRoot) {
				try {
					fs.mkdirSync(sessionRoot, { recursive: true });
				} catch {}
			}
			const sessionDirForIndex = (idx?: number) =>
				sessionRoot ? path.join(sessionRoot, `run-${idx ?? 0}`) : undefined;

			const hasChain = (params.chain?.length ?? 0) > 0;
			const hasTasks = (params.tasks?.length ?? 0) > 0;
			const hasSingle = Boolean(params.agent && params.task);

			const requestedAsync = params.async ?? asyncByDefault;
			// clarify implies sync mode (TUI is blocking)
			// - Chains default to TUI (clarify: true), so async requires explicit clarify: false
			// - Single/parallel default to no TUI, so async is allowed unless clarify: true is passed
			const effectiveAsync = requestedAsync && (
				hasChain 
					? params.clarify === false    // chains: only async if TUI explicitly disabled
					: params.clarify !== true     // single/parallel: async unless TUI explicitly enabled
			);
			const parallelDowngraded = hasTasks && requestedAsync && !effectiveAsync;
			const runLogNoisyEvents = params.debugNoisyEvents ?? asyncLogNoisyEvents;

			const artifactConfig: ArtifactConfig = {
				...DEFAULT_ARTIFACT_CONFIG,
				enabled: params.artifacts !== false,
			};

			const sessionFile = ctx.sessionManager.getSessionFile() ?? null;
			const artifactsDir = effectiveAsync ? tempArtifactsDir : getArtifactsDir(sessionFile);

			if (Number(hasChain) + Number(hasTasks) + Number(hasSingle) !== 1) {
				return {
					content: [
						{
							type: "text",
							text: `Provide exactly one mode. Agents: ${agents.map((a) => a.name).join(", ") || "none"}`,
						},
					],
					isError: true,
					details: { mode: "single" as const, results: [] },
				};
			}

			// Derive parent session model string (provider/id) for model inheritance.
			// When a subagent call omits `model:`, this is used as fallback before
			// the agent's frontmatter default — so children inherit the parent's model.
			const parentModel: string | undefined = (() => {
				const m = ctx.model;
				if (!m) return undefined;
				return m.provider ? `${m.provider}/${m.id}` : m.id;
			})();

			// Validate chain early (before async/sync branching)
			if (hasChain && params.chain) {
				if (params.chain.length === 0) {
					return {
						content: [{ type: "text", text: "Chain must have at least one step" }],
						isError: true,
						details: { mode: "chain" as const, results: [] },
					};
				}
				// First step must have a task
				const firstStep = params.chain[0] as ChainStep;
				if (isParallelStep(firstStep)) {
					// All tasks in the first parallel step must have tasks (no {previous} to reference)
					const missingTaskIndex = firstStep.parallel.findIndex((t) => !t.task);
					if (missingTaskIndex !== -1) {
						return {
							content: [{ type: "text", text: `First parallel step: task ${missingTaskIndex + 1} must have a task (no previous output to reference)` }],
							isError: true,
							details: { mode: "chain" as const, results: [] },
						};
					}
				} else if (!(firstStep as SequentialStep).task && !params.task) {
					return {
						content: [{ type: "text", text: "First step in chain must have a task" }],
						isError: true,
						details: { mode: "chain" as const, results: [] },
					};
				}
				// Validate all agents exist
				for (let i = 0; i < params.chain.length; i++) {
					const step = params.chain[i] as ChainStep;
					const stepAgents = getStepAgents(step);
					for (const agentName of stepAgents) {
						if (!agents.find((a) => a.name === agentName)) {
							return {
								content: [{ type: "text", text: `Unknown agent: ${agentName} (step ${i + 1})` }],
								isError: true,
								details: { mode: "chain" as const, results: [] },
							};
						}
					}
					// Validate parallel steps have at least one task
					if (isParallelStep(step) && step.parallel.length === 0) {
						return {
							content: [{ type: "text", text: `Parallel step ${i + 1} must have at least one task` }],
							isError: true,
							details: { mode: "chain" as const, results: [] },
						};
					}
				}
			}

			if (effectiveAsync) {
				if (!isAsyncAvailable()) {
					return {
						content: [{ type: "text", text: "Async mode requires jiti for TypeScript execution but it could not be found. Install globally: npm install -g jiti" }],
						isError: true,
						details: { mode: "single" as const, results: [] },
					};
				}
				const id = randomUUID();
				const asyncCtx = { pi, cwd: ctx.cwd, currentSessionId: currentSessionId!, parentModel };

				if (hasChain && params.chain) {
					const normalized = normalizeSkillInput(params.skill);
					const chainSkills = normalized === false ? [] : (normalized ?? []);
					return executeAsyncChain(id, {
						chain: params.chain as ChainStep[],
						agents,
						ctx: asyncCtx,
						cwd: params.cwd,
						maxOutput: params.maxOutput,
						artifactsDir: artifactConfig.enabled ? artifactsDir : undefined,
						artifactConfig,
						shareEnabled,
						sessionRoot,
						chainSkills,
						logNoisyEvents: runLogNoisyEvents,
					});
				}

				if (hasTasks && params.tasks) {
					const parallelTasks = params.tasks.map((t) => {
						const modelOverride = (t as { model?: string }).model;
						const skillOverride = normalizeSkillInput((t as { skill?: string | string[] | boolean }).skill);
						return {
							agent: t.agent,
							task: t.task,
							cwd: t.cwd,
							...(modelOverride ? { model: modelOverride } : {}),
							...(skillOverride !== undefined ? { skill: skillOverride } : {}),
						};
					});
					return executeAsyncChain(id, {
						chain: [{ parallel: parallelTasks }],
						agents,
						ctx: asyncCtx,
						cwd: params.cwd,
						maxOutput: params.maxOutput,
						artifactsDir: artifactConfig.enabled ? artifactsDir : undefined,
						artifactConfig,
						shareEnabled,
						sessionRoot,
						chainSkills: [],
						logNoisyEvents: runLogNoisyEvents,
					});
				}

				if (hasSingle) {
					const a = agents.find((x) => x.name === params.agent);
					if (!a) {
						return {
							content: [{ type: "text", text: `Unknown: ${params.agent}` }],
							isError: true,
							details: { mode: "single" as const, results: [] },
						};
					}
					const rawOutput = params.output !== undefined ? params.output : a.output;
					const effectiveOutput: string | false | undefined = rawOutput === true ? a.output : rawOutput;
					return executeAsyncSingle(id, {
						agent: params.agent!,
						task: params.task!,
						agentConfig: a,
						ctx: asyncCtx,
						cwd: params.cwd,
						maxOutput: params.maxOutput,
						artifactsDir: artifactConfig.enabled ? artifactsDir : undefined,
						artifactConfig,
						shareEnabled,
						sessionRoot,
						skills: (() => {
							const normalized = normalizeSkillInput(params.skill);
							if (normalized === false) return [];
							if (normalized === undefined) return undefined;
							return normalized;
						})(),
						output: effectiveOutput,
						modelOverride: params.model as string | undefined,
						logNoisyEvents: runLogNoisyEvents,
					});
				}
			}

			const allProgress: AgentProgress[] = [];
			const allArtifactPaths: ArtifactPaths[] = [];

			if (hasChain && params.chain) {
				const normalized = normalizeSkillInput(params.skill);
				const chainSkills = normalized === false ? [] : (normalized ?? []);
				// Use extracted chain execution module
				const chainResult = await executeChain({
					chain: params.chain as ChainStep[],
					task: params.task,
					agents,
					ctx,
					signal,
					runId,
					cwd: params.cwd,
					shareEnabled,
					sessionDirForIndex,
					artifactsDir,
					artifactConfig,
					includeProgress: params.includeProgress,
					clarify: params.clarify,
					onUpdate,
					chainSkills,
					chainDir: params.chainDir,
				});

				// User requested async via TUI - dispatch to async executor
				if (chainResult.requestedAsync) {
					if (!isAsyncAvailable()) {
						return {
							content: [{ type: "text", text: "Background mode requires jiti for TypeScript execution but it could not be found." }],
							isError: true,
							details: { mode: "chain" as const, results: [] },
						};
					}
					const id = randomUUID();
					const asyncCtx = { pi, cwd: ctx.cwd, currentSessionId: currentSessionId!, parentModel };
					return executeAsyncChain(id, {
						chain: chainResult.requestedAsync.chain,
						agents,
						ctx: asyncCtx,
						cwd: params.cwd,
						maxOutput: params.maxOutput,
						artifactsDir: artifactConfig.enabled ? artifactsDir : undefined,
						artifactConfig,
						shareEnabled,
						sessionRoot,
						chainSkills: chainResult.requestedAsync.chainSkills,
						logNoisyEvents: runLogNoisyEvents,
					});
				}

				return chainResult;
			}

			if (hasTasks && params.tasks) {
				// MAX_PARALLEL check first (fail fast before TUI)
				if (params.tasks.length > MAX_PARALLEL)
					return {
						content: [{ type: "text", text: `Max ${MAX_PARALLEL} tasks` }],
						isError: true,
						details: { mode: "parallel" as const, results: [] },
					};

				// Validate all agents exist
				const agentConfigs: AgentConfig[] = [];
				for (const t of params.tasks) {
					const config = agents.find(a => a.name === t.agent);
					if (!config) {
						return {
							content: [{ type: "text", text: `Unknown agent: ${t.agent}` }],
							isError: true,
							details: { mode: "parallel" as const, results: [] },
						};
					}
					agentConfigs.push(config);
				}

				// Mutable copies for TUI modifications
				let tasks = params.tasks.map(t => t.task);
				const modelOverrides: (string | undefined)[] = params.tasks.map(t => (t as { model?: string }).model);
				// Initialize skill overrides from task-level skill params (may be overridden by TUI)
				const skillOverrides: (string[] | false | undefined)[] = params.tasks.map(t => 
					normalizeSkillInput((t as { skill?: string | string[] | boolean }).skill)
				);

				// Show clarify TUI if requested
				if (params.clarify === true && ctx.hasUI) {
					// Get available models (same pattern as chain-execution.ts)
					const availableModels: ModelInfo[] = ctx.modelRegistry.getAvailable().map((m) => ({
						provider: m.provider,
						id: m.id,
						fullId: `${m.provider}/${m.id}`,
					}));

					// Resolve behaviors with task-level skill overrides for TUI display
					const behaviors = agentConfigs.map((c, i) => 
						resolveStepBehavior(c, { skills: skillOverrides[i] })
					);
					const availableSkills = discoverAvailableSkills(ctx.cwd);

					const result = await ctx.ui.custom<ChainClarifyResult>(
						(tui, theme, _kb, done) =>
							new ChainClarifyComponent(
								tui, theme,
								agentConfigs,
								tasks,
								'',          // no originalTask for parallel (each task is independent)
								undefined,   // no chainDir for parallel
								behaviors,
								availableModels,
								availableSkills,
								done,
								'parallel',  // mode
							),
						{ overlay: true, overlayOptions: { anchor: 'center', width: 84, maxHeight: '80%' } },
					);

					if (!result || !result.confirmed) {
						return { content: [{ type: 'text', text: 'Cancelled' }], details: { mode: 'parallel', results: [] } };
					}

					// Apply TUI overrides
					tasks = result.templates;
					for (let i = 0; i < result.behaviorOverrides.length; i++) {
						const override = result.behaviorOverrides[i];
						if (override?.model) modelOverrides[i] = override.model;
						if (override?.skills !== undefined) skillOverrides[i] = override.skills;
					}

					// User requested background execution
					if (result.runInBackground) {
						if (!isAsyncAvailable()) {
							return {
								content: [{ type: "text", text: "Background mode requires jiti for TypeScript execution but it could not be found." }],
								isError: true,
								details: { mode: "parallel" as const, results: [] },
							};
						}
						const id = randomUUID();
						const asyncCtx = { pi, cwd: ctx.cwd, currentSessionId: currentSessionId!, parentModel };
						// Convert parallel tasks to a chain with a single parallel step
						const parallelTasks = params.tasks!.map((t, i) => ({
							agent: t.agent,
							task: tasks[i],
							cwd: t.cwd,
							...(modelOverrides[i] ? { model: modelOverrides[i] } : {}),
							...(skillOverrides[i] !== undefined ? { skill: skillOverrides[i] } : {}),
						}));
						return executeAsyncChain(id, {
							chain: [{ parallel: parallelTasks }],
							agents,
							ctx: asyncCtx,
							cwd: params.cwd,
							maxOutput: params.maxOutput,
							artifactsDir: artifactConfig.enabled ? artifactsDir : undefined,
							artifactConfig,
							shareEnabled,
							sessionRoot,
							chainSkills: [],
							logNoisyEvents: runLogNoisyEvents,
						});
					}
				}

				// Execute with overrides (tasks array has same length as params.tasks)
				const behaviors = agentConfigs.map(c => resolveStepBehavior(c, {}));
				const liveResults: (SingleResult | undefined)[] = new Array(params.tasks.length).fill(undefined);
				const liveProgress: (AgentProgress | undefined)[] = new Array(params.tasks.length).fill(undefined);
				const results = await mapConcurrent(params.tasks, MAX_CONCURRENCY, async (t, i) => {
					const overrideSkills = skillOverrides[i];
					const effectiveSkills = overrideSkills === undefined ? behaviors[i]?.skills : overrideSkills;
					return runSync(ctx.cwd, agents, t.agent, tasks[i]!, {
						cwd: t.cwd ?? params.cwd,
						signal,
						runId,
						index: i,
						sessionDir: sessionDirForIndex(i),
						share: shareEnabled,
						artifactsDir: artifactConfig.enabled ? artifactsDir : undefined,
						artifactConfig,
						maxOutput: params.maxOutput,
						modelOverride: modelOverrides[i],
						parentModel,
						skills: effectiveSkills === false ? [] : effectiveSkills,
						onUpdate: onUpdate
							? (p) => {
									const stepResults = p.details?.results || [];
									const stepProgress = p.details?.progress || [];
									if (stepResults.length > 0) liveResults[i] = stepResults[0];
									if (stepProgress.length > 0) liveProgress[i] = stepProgress[0];
									const mergedResults = liveResults.filter((r): r is SingleResult => r !== undefined);
									const mergedProgress = liveProgress.filter((pg): pg is AgentProgress => pg !== undefined);
									onUpdate({
										content: p.content,
										details: {
											mode: "parallel",
											results: mergedResults,
											progress: mergedProgress,
											totalSteps: params.tasks!.length,
										},
									});
								}
							: undefined,
					});
				});
				for (let i = 0; i < results.length; i++) {
					const run = results[i]!;
					recordRun(run.agent, tasks[i]!, run.exitCode, run.progressSummary?.durationMs ?? 0);
				}

				for (const r of results) {
					if (r.progress) allProgress.push(r.progress);
					if (r.artifactPaths) allArtifactPaths.push(r.artifactPaths);
				}

				const ok = results.filter((r) => r.exitCode === 0).length;
				const downgradeNote = parallelDowngraded
					? " (async requested but clarify=true forced sync; use clarify background toggle)"
					: "";

				// Aggregate outputs from all parallel tasks
				const aggregatedOutput = results
					.map((r, i) => {
						const header = `=== Task ${i + 1}: ${r.agent} ===`;
						const output = r.truncation?.text || getFinalOutput(r.messages);
						const hasOutput = Boolean(output?.trim());
						const status = r.exitCode !== 0
							? `⚠️ FAILED (exit code ${r.exitCode})${r.error ? `: ${r.error}` : ""}`
							: r.error
								? `⚠️ WARNING: ${r.error}`
								: !hasOutput
									? "⚠️ EMPTY OUTPUT"
									: "";
						const body = status
							? (hasOutput ? `${status}\n${output}` : status)
							: output;
						return `${header}\n${body}`;
					})
					.join("\n\n");

				const summary = `${ok}/${results.length} succeeded${downgradeNote}`;
				const fullContent = `${summary}\n\n${aggregatedOutput}`;

				return {
					content: [{ type: "text", text: fullContent }],
					details: {
						mode: "parallel",
						results,
						progress: params.includeProgress ? allProgress : undefined,
						artifacts: allArtifactPaths.length ? { dir: artifactsDir, files: allArtifactPaths } : undefined,
					},
				};
			}

			if (hasSingle) {
				// Look up agent config for output handling
				const agentConfig = agents.find((a) => a.name === params.agent);
				if (!agentConfig) {
					return {
						content: [{ type: 'text', text: `Unknown agent: ${params.agent}` }],
						isError: true,
						details: { mode: 'single', results: [] },
					};
				}

				let task = params.task!;
				let modelOverride: string | undefined = params.model as string | undefined;
				let skillOverride: string[] | false | undefined = normalizeSkillInput(params.skill);
				// Normalize output: true means "use default" (same as undefined), false means disable
				const rawOutput = params.output !== undefined ? params.output : agentConfig.output;
				let effectiveOutput: string | false | undefined = rawOutput === true ? agentConfig.output : rawOutput;

				// Show clarify TUI if requested
				if (params.clarify === true && ctx.hasUI) {
					// Get available models (same pattern as chain-execution.ts)
					const availableModels: ModelInfo[] = ctx.modelRegistry.getAvailable().map((m) => ({
						provider: m.provider,
						id: m.id,
						fullId: `${m.provider}/${m.id}`,
					}));

					const behavior = resolveStepBehavior(agentConfig, { output: effectiveOutput, skills: skillOverride });
					const availableSkills = discoverAvailableSkills(ctx.cwd);

					const result = await ctx.ui.custom<ChainClarifyResult>(
						(tui, theme, _kb, done) =>
							new ChainClarifyComponent(
								tui, theme,
								[agentConfig],
								[task],
								task,
								undefined,  // no chainDir for single
								[behavior],
								availableModels,
								availableSkills,
								done,
								'single',   // mode
							),
						{ overlay: true, overlayOptions: { anchor: 'center', width: 84, maxHeight: '80%' } },
					);

					if (!result || !result.confirmed) {
						return { content: [{ type: 'text', text: 'Cancelled' }], details: { mode: 'single', results: [] } };
					}

					// Apply TUI overrides
					task = result.templates[0]!;
					const override = result.behaviorOverrides[0];
					if (override?.model) modelOverride = override.model;
					if (override?.output !== undefined) effectiveOutput = override.output;
					if (override?.skills !== undefined) skillOverride = override.skills;

					// User requested background execution
					if (result.runInBackground) {
						if (!isAsyncAvailable()) {
							return {
								content: [{ type: "text", text: "Background mode requires jiti for TypeScript execution but it could not be found." }],
								isError: true,
								details: { mode: "single" as const, results: [] },
							};
						}
						const id = randomUUID();
						const asyncCtx = { pi, cwd: ctx.cwd, currentSessionId: currentSessionId!, parentModel };
						return executeAsyncSingle(id, {
							agent: params.agent!,
							task,
							agentConfig,
							ctx: asyncCtx,
							cwd: params.cwd,
							maxOutput: params.maxOutput,
							artifactsDir: artifactConfig.enabled ? artifactsDir : undefined,
							artifactConfig,
							shareEnabled,
							sessionRoot,
							skills: skillOverride === false ? [] : skillOverride,
							output: effectiveOutput,
							modelOverride,
							logNoisyEvents: runLogNoisyEvents,
						});
					}
				}

				const cleanTask = task;
				const outputPath = resolveSingleOutputPath(effectiveOutput, ctx.cwd, params.cwd);
				task = injectSingleOutputInstruction(task, outputPath);

				const effectiveSkills = skillOverride === false
					? []
					: skillOverride === undefined
						? undefined
						: skillOverride;

				const r = await runSync(ctx.cwd, agents, params.agent!, task, {
					cwd: params.cwd,
					signal,
					runId,
					sessionDir: sessionDirForIndex(0),
					share: shareEnabled,
					artifactsDir: artifactConfig.enabled ? artifactsDir : undefined,
					artifactConfig,
					maxOutput: params.maxOutput,
					onUpdate,
					modelOverride,
					parentModel,
					skills: effectiveSkills,
				});
				recordRun(params.agent!, cleanTask, r.exitCode, r.progressSummary?.durationMs ?? 0);

				if (r.progress) allProgress.push(r.progress);
				if (r.artifactPaths) allArtifactPaths.push(r.artifactPaths);

				const fullOutput = getFinalOutput(r.messages);
				const finalizedOutput = finalizeSingleOutput({
					fullOutput,
					truncatedOutput: r.truncation?.text,
					outputPath,
					exitCode: r.exitCode,
				});

				if (r.exitCode !== 0)
					return {
						content: [{ type: "text", text: r.error || "Failed" }],
						details: {
							mode: "single",
							results: [r],
							progress: params.includeProgress ? allProgress : undefined,
							artifacts: allArtifactPaths.length ? { dir: artifactsDir, files: allArtifactPaths } : undefined,
							truncation: r.truncation,
						},
						isError: true,
					};
				return {
					content: [{ type: "text", text: finalizedOutput.displayOutput || "(no output)" }],
					details: {
						mode: "single",
						results: [r],
						progress: params.includeProgress ? allProgress : undefined,
						artifacts: allArtifactPaths.length ? { dir: artifactsDir, files: allArtifactPaths } : undefined,
						truncation: r.truncation,
					},
				};
			}

			return {
				content: [{ type: "text", text: "Invalid params" }],
				isError: true,
				details: { mode: "single" as const, results: [] },
			};
		},

		renderCall(args, theme) {
			if (args.action) {
				const target = args.agent || args.chainName || "";
				return new Text(
					`${theme.fg("toolTitle", theme.bold("subagent "))}${args.action}${target ? ` ${theme.fg("accent", target)}` : ""}`,
					0, 0,
				);
			}
			const isParallel = (args.tasks?.length ?? 0) > 0;
			const chainParallelCount = Array.isArray(args.chain) && args.chain.length === 1
				? (Array.isArray((args.chain[0] as { parallel?: unknown[] }).parallel)
					? ((args.chain[0] as { parallel?: unknown[] }).parallel?.length ?? 0)
					: 0)
				: 0;
			const asyncLabel = args.async === true ? theme.fg("warning", " [async]") : "";
			if (chainParallelCount > 0) {
				return new Text(
					`${theme.fg("toolTitle", theme.bold("subagent "))}parallel (${chainParallelCount})${asyncLabel}`,
					0,
					0,
				);
			}
			if (args.chain?.length)
				return new Text(
					`${theme.fg("toolTitle", theme.bold("subagent "))}chain (${args.chain.length})${asyncLabel}`,
					0,
					0,
				);
			if (isParallel)
				return new Text(
					`${theme.fg("toolTitle", theme.bold("subagent "))}parallel (${args.tasks!.length})`,
					0,
					0,
				);
			return new Text(
				`${theme.fg("toolTitle", theme.bold("subagent "))}${theme.fg("accent", args.agent || "?")}${asyncLabel}`,
				0,
				0,
			);
		},

		renderResult(result, options, theme) {
			return renderSubagentResult(result, options, theme);
		},

	};

	const statusTool: ToolDefinition<typeof StatusParams, Details> = {
		name: "subagent_status",
		label: "Subagent Status",
		description: "Inspect async subagent run status and artifacts",
		parameters: StatusParams,

		async execute(_id, params, _signal, _onUpdate, _ctx) {
			let asyncDir: string | null = null;
			let resolvedId = params.id;

			if (params.dir) {
				asyncDir = path.resolve(params.dir);
			} else if (params.id) {
				const direct = path.join(ASYNC_DIR, params.id);
				if (fs.existsSync(direct)) {
					asyncDir = direct;
				} else {
					const match = findByPrefix(ASYNC_DIR, params.id);
					if (match) {
						asyncDir = match;
						resolvedId = path.basename(match);
					}
				}
			}

			const resultPath =
				params.id && !asyncDir ? findByPrefix(RESULTS_DIR, params.id, ".json") : null;

			if (!asyncDir && !resultPath) {
				return {
					content: [{ type: "text", text: "Async run not found. Provide id or dir." }],
					isError: true,
					details: { mode: "single" as const, results: [] },
				};
			}

			if (asyncDir) {
				const status = readStatus(asyncDir);
				const logPath = path.join(asyncDir, `subagent-log-${resolvedId ?? "unknown"}.md`);
				const eventsPath = path.join(asyncDir, "events.jsonl");
				if (status) {
					const stepsTotal = status.steps?.length ?? 1;
					const current = status.currentStep !== undefined ? status.currentStep + 1 : undefined;
					const stepLine =
						current !== undefined ? `Step: ${current}/${stepsTotal}` : `Steps: ${stepsTotal}`;
					const started = new Date(status.startedAt).toISOString();
					const updated = status.lastUpdate ? new Date(status.lastUpdate).toISOString() : "n/a";
					const elapsed = status.endedAt
						? `${((status.endedAt - status.startedAt) / 1000).toFixed(0)}s`
						: `${((Date.now() - status.startedAt) / 1000).toFixed(0)}s (running)`;
					const tailLines = Math.max(1, Math.min(50, Math.floor(params.tailLines ?? 10)));
					const tailLineMaxChars = Math.max(80, Math.min(1000, Math.floor(params.tailLineMaxChars ?? 240)));
					const includeFailureTriage = params.includeFailureTriage !== false;

					const lines = [
						`Run: ${status.runId}`,
						`State: ${status.state}`,
						`Mode: ${status.mode}`,
						stepLine,
						`Elapsed: ${elapsed}`,
						`Started: ${started}`,
						`Updated: ${updated}`,
						`Dir: ${asyncDir}`,
					];

					// Show per-step details (model, status, tokens, errors)
					if (status.steps?.length) {
						lines.push("", "Steps:");
						for (let i = 0; i < status.steps.length; i++) {
							const s = status.steps[i] as { agent: string; model?: string; resolvedModel?: string; status: string; exitCode?: number | null; error?: string; tokens?: { input: number; output: number; total: number }; liveTokens?: { input: number; output: number; total: number }; liveCost?: number; turns?: number; durationMs?: number; outputBytes?: number; activeChildren?: number; currentTool?: string; currentToolArgs?: string; toolCount?: number };
							const dur = s.durationMs ? `${(s.durationMs / 1000).toFixed(1)}s` : "";
							const tokens = s.tokens ?? s.liveTokens;
							const tok = tokens ? `${tokens.total} tokens` : "";
							const cost = s.liveCost ? `$${s.liveCost.toFixed(3)}` : "";
							const modelDisplay = s.resolvedModel ?? s.model;
							const modelTag = modelDisplay ? ` [${modelDisplay}]` : "";
							const outSize = s.outputBytes ? `${(s.outputBytes / 1024).toFixed(0)}KB output` : "";
							const children = s.activeChildren ? `${s.activeChildren} subagent${s.activeChildren > 1 ? "s" : ""} active` : "";
							const toolInfo = s.currentTool ? `→ ${s.currentTool}${s.currentToolArgs ? `(${s.currentToolArgs})` : ""}` : "";
							const toolCount = s.toolCount ? `${s.toolCount} calls` : "";
							const info = [dur, tok, cost, outSize, children, toolInfo, toolCount].filter(Boolean).join(", ");
							lines.push(`  ${i + 1}. ${s.agent}${modelTag}: ${s.status}${info ? ` (${info})` : ""}`);
							if (s.error) {
								const shortStepError = s.error.length > 400 ? `${s.error.slice(0, 397)}…` : s.error;
								lines.push(`     ⚠️ ${shortStepError}`);
							}
						}
					}

					// Quick failure triage for failed runs
					if (status.state === "failed" || status.error) {
						if (includeFailureTriage) {
							lines.push("", "--- Failure Triage ---");
							// Check all output logs (output-0.log, output-1.log, etc.)
							const outputFiles = fs.readdirSync(asyncDir).filter((f: string) => f.match(/^output-\d+\.log$/)).sort();
							for (const of2 of outputFiles) {
								const outputLogPath = path.join(asyncDir, of2);
								const logSize = fs.statSync(outputLogPath).size;
								if (logSize === 0) {
									lines.push(`⚠️ ${of2}: BLANK — likely provider 429/quota/auth error.`);
								} else {
									const tail = getOutputTail(outputLogPath, tailLines, tailLineMaxChars);
									const tailText = tail.join("\n");
									// Pattern-match known failures from recent tail only (safe + bounded)
									const has429 = /429|rate.limit|quota.*exceed|resource.*exhaust/i.test(tailText);
									const hasAuth = /401|403|auth.*fail|invalid.*key|unauthorized/i.test(tailText);
									const hasTimeout = /timeout|timed.out|ETIMEDOUT/i.test(tailText);
									const hasOOM = /heap|memory|ENOMEM|killed/i.test(tailText);
									if (has429) lines.push(`⚠️ ${of2}: PROVIDER QUOTA/RATE LIMIT (429) — cool down or switch model.`);
									else if (hasAuth) lines.push(`⚠️ ${of2}: AUTH ERROR — check API key / credentials.`);
									else if (hasTimeout) lines.push(`⚠️ ${of2}: TIMEOUT — request took too long.`);
									else if (hasOOM) lines.push(`⚠️ ${of2}: MEMORY — process killed or OOM.`);

									if (tail.length > 0) {
										lines.push(`${of2} tail(last ${tail.length}, ${(logSize / 1024).toFixed(0)}KB):`);
										for (const line of tail) {
											lines.push(`  ${line}`);
										}
									}
								}
							}
							if (outputFiles.length === 0) {
								lines.push("⚠️ No output logs found — process may not have started.");
							}
							lines.push(`(Tip: set includeFailureTriage=false for short status; tailLines=${tailLines})`);
						} else {
							lines.push("", "--- Failure Triage ---", "(disabled; pass includeFailureTriage=true to include log tails)");
						}
						if (status.error) {
							const shortStatusError = status.error.length > 600 ? `${status.error.slice(0, 597)}…` : status.error;
							lines.push(`Error: ${shortStatusError}`);
						}
					}

					if (status.totalTokens) {
						lines.push(`Tokens: ${status.totalTokens.input}in + ${status.totalTokens.output}out = ${status.totalTokens.total} total`);
					}
					if (status.sessionFile) lines.push(`Session: ${status.sessionFile}`);
					if (fs.existsSync(logPath)) lines.push(`Log: ${logPath}`);
					if (fs.existsSync(eventsPath)) lines.push(`Events: ${eventsPath}`);

					return { content: [{ type: "text", text: lines.join("\n") }], details: { mode: "single", results: [] } };
				}
			}

			if (resultPath) {
				try {
					const raw = fs.readFileSync(resultPath, "utf-8");
					const data = JSON.parse(raw) as { id?: string; success?: boolean; summary?: string };
					const status = data.success ? "complete" : "failed";
					const lines = [`Run: ${data.id ?? params.id}`, `State: ${status}`, `Result: ${resultPath}`];
					if (data.summary) lines.push("", data.summary);
					return { content: [{ type: "text", text: lines.join("\n") }], details: { mode: "single", results: [] } };
				} catch {}
			}

			return {
				content: [{ type: "text", text: "Status file not found." }],
				isError: true,
				details: { mode: "single" as const, results: [] },
			};
		},
	};

	pi.registerTool(tool);
	pi.registerTool(statusTool);

	interface InlineConfig {
		output?: string | false;
		reads?: string[] | false;
		model?: string;
		skill?: string[] | false;
		progress?: boolean;
	}

	const parseInlineConfig = (raw: string): InlineConfig => {
		const config: InlineConfig = {};
		for (const part of raw.split(",")) {
			const trimmed = part.trim();
			if (!trimmed) continue;
			const eq = trimmed.indexOf("=");
			if (eq === -1) {
				if (trimmed === "progress") config.progress = true;
				continue;
			}
			const key = trimmed.slice(0, eq).trim();
			const val = trimmed.slice(eq + 1).trim();
			switch (key) {
				case "output": config.output = val === "false" ? false : val; break;
				case "reads": config.reads = val === "false" ? false : val.split("+").filter(Boolean); break;
				case "model": config.model = val || undefined; break;
				case "skill": case "skills": config.skill = val === "false" ? false : val.split("+").filter(Boolean); break;
				case "progress": config.progress = val !== "false"; break;
			}
		}
		return config;
	};

	const parseAgentToken = (token: string): { name: string; config: InlineConfig } => {
		const bracket = token.indexOf("[");
		if (bracket === -1) return { name: token, config: {} };
		const end = token.lastIndexOf("]");
		return { name: token.slice(0, bracket), config: parseInlineConfig(token.slice(bracket + 1, end !== -1 ? end : undefined)) };
	};

	/** Extract --bg flag from end of args, return cleaned args and whether flag was present */
	const extractBgFlag = (args: string): { args: string; bg: boolean } => {
		// Only match --bg at the very end to avoid false positives in quoted strings
		if (args.endsWith(" --bg") || args === "--bg") {
			return { args: args.slice(0, args.length - (args === "--bg" ? 4 : 5)).trim(), bg: true };
		}
		return { args, bg: false };
	};

	const setupDirectRun = (ctx: ExtensionContext) => {
		const runId = randomUUID().slice(0, 8);
		const sessionRoot = fs.mkdtempSync(path.join(os.tmpdir(), "pi-subagent-session-"));
		return {
			runId,
			shareEnabled: false,
			sessionDirForIndex: (idx?: number) => path.join(sessionRoot, `run-${idx ?? 0}`),
			artifactsDir: getArtifactsDir(ctx.sessionManager.getSessionFile() ?? null),
			artifactConfig: { ...DEFAULT_ARTIFACT_CONFIG } as ArtifactConfig,
		};
	};

	const makeAgentCompletions = (multiAgent: boolean) => (prefix: string) => {
		const agents = discoverAgents(baseCwd, "both").agents;
		if (!multiAgent) {
			if (prefix.includes(" ")) return null;
			return agents.filter((a) => a.name.startsWith(prefix)).map((a) => ({ value: a.name, label: a.name }));
		}

		const lastArrow = prefix.lastIndexOf(" -> ");
		const segment = lastArrow !== -1 ? prefix.slice(lastArrow + 4) : prefix;
		if (segment.includes(" -- ") || segment.includes('"') || segment.includes("'")) return null;

		const lastWord = (prefix.match(/(\S*)$/) || ["", ""])[1];
		const beforeLastWord = prefix.slice(0, prefix.length - lastWord.length);

		if (lastWord === "->") {
			return agents.map((a) => ({ value: `${prefix} ${a.name}`, label: a.name }));
		}

		return agents.filter((a) => a.name.startsWith(lastWord)).map((a) => ({ value: `${beforeLastWord}${a.name}`, label: a.name }));
	};

	const openAgentManager = async (ctx: ExtensionContext) => {
		const agentData = { ...discoverAgentsAll(ctx.cwd), cwd: ctx.cwd };
		const models = ctx.modelRegistry.getAvailable().map((m) => ({
			provider: m.provider,
			id: m.id,
			fullId: `${m.provider}/${m.id}`,
		}));
		const skills = discoverAvailableSkills(ctx.cwd);

		const result = await ctx.ui.custom<ManagerResult>(
			(tui, theme, _kb, done) => new AgentManagerComponent(tui, theme, agentData, models, skills, done),
			{ overlay: true, overlayOptions: { anchor: "center", width: 84, maxHeight: "80%" } },
		);
		if (!result) return;

		// Ad-hoc chains from the overlay use direct execution for the chain-clarify TUI.
		// All other paths (single, saved-chain, parallel launches, slash commands)
		// route through sendToolCall → LLM → tool handler to get live progress.
		if (result.action === "chain") {
			const agents = discoverAgents(baseCwd, "both").agents;
			const exec = setupDirectRun(ctx);
			const chain: SequentialStep[] = result.agents.map((name, i) => ({
				agent: name,
				...(i === 0 ? { task: result.task } : {}),
			}));
			executeChain({ chain, task: result.task, agents, ctx, ...exec, clarify: true })
				.then((r) => {
					// User requested async via TUI - dispatch to async executor
					if (r.requestedAsync) {
						if (!isAsyncAvailable()) {
							pi.sendUserMessage("Background mode requires jiti for TypeScript execution but it could not be found.");
							return;
						}
						const id = randomUUID();
						const parentModelStr: string | undefined = (() => { const m = ctx.model; if (!m) return undefined; return m.provider ? `${m.provider}/${m.id}` : m.id; })();
						const asyncCtx = { pi, cwd: ctx.cwd, currentSessionId: ctx.sessionManager.getSessionId() ?? id, parentModel: parentModelStr };
						const sessionRoot = fs.mkdtempSync(path.join(os.tmpdir(), "pi-subagent-session-"));
						executeAsyncChain(id, {
							chain: r.requestedAsync.chain,
							agents,
							ctx: asyncCtx,
							maxOutput: undefined,
							artifactsDir: exec.artifactsDir,
							artifactConfig: exec.artifactConfig,
							shareEnabled: false,
							sessionRoot,
							chainSkills: r.requestedAsync.chainSkills,
							logNoisyEvents: asyncLogNoisyEvents,
						}).then((asyncResult) => {
							pi.sendUserMessage(asyncResult.content[0]?.text || "(launched in background)");
						}).catch((err) => {
							pi.sendUserMessage(`Async launch failed: ${err instanceof Error ? err.message : String(err)}`);
						});
						return;
					}
					pi.sendUserMessage(r.content[0]?.text || "(no output)");
				})
				.catch((err) => pi.sendUserMessage(`Chain failed: ${err instanceof Error ? err.message : String(err)}`));
			return;
		}

		const sendToolCall = (params: Record<string, unknown>) => {
			pi.sendUserMessage(
				`Call the subagent tool with these exact parameters: ${JSON.stringify({ ...params, agentScope: "both" })}`,
			);
		};

		if (result.action === "launch") {
			sendToolCall({ agent: result.agent, task: result.task, clarify: !result.skipClarify });
		} else if (result.action === "launch-chain") {
			const chainParam = result.chain.steps.map((step) => ({
				agent: step.agent,
				task: step.task || undefined,
				output: step.output,
				reads: step.reads,
				progress: step.progress,
				skill: step.skills,
				model: step.model,
			}));
			sendToolCall({ chain: chainParam, task: result.task, clarify: !result.skipClarify });
		} else if (result.action === "parallel") {
			sendToolCall({ tasks: result.tasks, clarify: !result.skipClarify });
		}
	};

	pi.registerCommand("agents", {
		description: "Open the Agents Manager",
		handler: async (_args, ctx) => {
			await openAgentManager(ctx);
		},
	});

	pi.registerCommand("run", {
		description: "Run a subagent directly: /run agent[output=file] task [--bg]",
		getArgumentCompletions: makeAgentCompletions(false),
		handler: async (args, ctx) => {
			const { args: cleanedArgs, bg } = extractBgFlag(args);
			const input = cleanedArgs.trim();
			const firstSpace = input.indexOf(" ");
			if (firstSpace === -1) { ctx.ui.notify("Usage: /run <agent> <task> [--bg]", "error"); return; }
			const { name: agentName, config: inline } = parseAgentToken(input.slice(0, firstSpace));
			const task = input.slice(firstSpace + 1).trim();
			if (!task) { ctx.ui.notify("Usage: /run <agent> <task> [--bg]", "error"); return; }

			const agents = discoverAgents(baseCwd, "both").agents;
			if (!agents.find((a) => a.name === agentName)) { ctx.ui.notify(`Unknown agent: ${agentName}`, "error"); return; }

			let finalTask = task;
			if (inline.reads && Array.isArray(inline.reads) && inline.reads.length > 0) {
				finalTask = `[Read from: ${inline.reads.join(", ")}]\n\n${finalTask}`;
			}
			const params: Record<string, unknown> = { agent: agentName, task: finalTask, clarify: false };
			if (inline.output !== undefined) params.output = inline.output;
			if (inline.skill !== undefined) params.skill = inline.skill;
			if (inline.model) params.model = inline.model;
			if (bg) params.async = true;
			pi.sendUserMessage(`Call the subagent tool with these exact parameters: ${JSON.stringify({ ...params, agentScope: "both" })}`);
		},
	});

	interface ParsedStep { name: string; config: InlineConfig; task?: string }

	const parseAgentArgs = (args: string, command: string, ctx: ExtensionContext): { steps: ParsedStep[]; task: string } | null => {
		const input = args.trim();
		const usage = `Usage: /${command} agent1 "task1" -> agent2 "task2"`;
		let steps: ParsedStep[];
		let sharedTask: string;
		let perStep = false;

		if (input.includes(" -> ")) {
			perStep = true;
			const segments = input.split(" -> ");
			steps = [];
			for (const seg of segments) {
				const trimmed = seg.trim();
				if (!trimmed) continue;
				let agentPart: string;
				let task: string | undefined;
				const qMatch = trimmed.match(/^(\S+(?:\[[^\]]*\])?)\s+(?:"([^"]*)"|'([^']*)')$/);
				if (qMatch) {
					agentPart = qMatch[1]!;
					task = (qMatch[2] ?? qMatch[3]) || undefined;
				} else {
					const dashIdx = trimmed.indexOf(" -- ");
					if (dashIdx !== -1) {
						agentPart = trimmed.slice(0, dashIdx).trim();
						task = trimmed.slice(dashIdx + 4).trim() || undefined;
					} else {
						agentPart = trimmed;
					}
				}
				const parsed = parseAgentToken(agentPart);
				steps.push({ ...parsed, task });
			}
			sharedTask = steps.find((s) => s.task)?.task ?? "";
		} else {
			const delimiterIndex = input.indexOf(" -- ");
			if (delimiterIndex === -1) {
				ctx.ui.notify(usage, "error");
				return null;
			}
			const agentsPart = input.slice(0, delimiterIndex).trim();
			sharedTask = input.slice(delimiterIndex + 4).trim();
			if (!agentsPart || !sharedTask) {
				ctx.ui.notify(usage, "error");
				return null;
			}
			steps = agentsPart.split(/\s+/).filter(Boolean).map((t) => parseAgentToken(t));
		}

		if (steps.length === 0) {
			ctx.ui.notify(usage, "error");
			return null;
		}
		const agents = discoverAgents(baseCwd, "both").agents;
		for (const step of steps) {
			if (!agents.find((a) => a.name === step.name)) {
				ctx.ui.notify(`Unknown agent: ${step.name}`, "error");
				return null;
			}
		}
		if (command === "chain" && !steps[0]?.task && (perStep || !sharedTask)) {
			ctx.ui.notify(`First step must have a task: /chain agent "task" -> agent2`, "error");
			return null;
		}
		if (command === "parallel" && !steps.some((s) => s.task) && !sharedTask) {
			ctx.ui.notify("At least one step must have a task", "error");
			return null;
		}
		return { steps, task: sharedTask };
	};

	pi.registerCommand("chain", {
		description: "Run agents in sequence: /chain scout \"task\" -> planner [--bg]",
		getArgumentCompletions: makeAgentCompletions(true),
		handler: async (args, ctx) => {
			const { args: cleanedArgs, bg } = extractBgFlag(args);
			const parsed = parseAgentArgs(cleanedArgs, "chain", ctx);
			if (!parsed) return;
			const chain = parsed.steps.map(({ name, config, task: stepTask }, i) => ({
				agent: name,
				...(stepTask ? { task: stepTask } : i === 0 && parsed.task ? { task: parsed.task } : {}),
				...(config.output !== undefined ? { output: config.output } : {}),
				...(config.reads !== undefined ? { reads: config.reads } : {}),
				...(config.model ? { model: config.model } : {}),
				...(config.skill !== undefined ? { skill: config.skill } : {}),
				...(config.progress !== undefined ? { progress: config.progress } : {}),
			}));
			const params: Record<string, unknown> = { chain, task: parsed.task, clarify: false, agentScope: "both" };
			if (bg) params.async = true;
			pi.sendUserMessage(`Call the subagent tool with these exact parameters: ${JSON.stringify(params)}`);
		},
	});

	pi.registerCommand("parallel", {
		description: "Run agents in parallel: /parallel scout \"task1\" -> reviewer \"task2\" [--bg]",
		getArgumentCompletions: makeAgentCompletions(true),
		handler: async (args, ctx) => {
			const { args: cleanedArgs, bg } = extractBgFlag(args);
			const parsed = parseAgentArgs(cleanedArgs, "parallel", ctx);
			if (!parsed) return;
			if (parsed.steps.length > MAX_PARALLEL) { ctx.ui.notify(`Max ${MAX_PARALLEL} parallel tasks`, "error"); return; }
			const tasks = parsed.steps.map(({ name, config, task: stepTask }) => ({
				agent: name,
				task: stepTask ?? parsed.task,
				...(config.output !== undefined ? { output: config.output } : {}),
				...(config.reads !== undefined ? { reads: config.reads } : {}),
				...(config.model ? { model: config.model } : {}),
				...(config.skill !== undefined ? { skill: config.skill } : {}),
				...(config.progress !== undefined ? { progress: config.progress } : {}),
			}));
			const params: Record<string, unknown> = { chain: [{ parallel: tasks }], task: parsed.task, clarify: false, agentScope: "both" };
			if (bg) params.async = true;
			pi.sendUserMessage(`Call the subagent tool with these exact parameters: ${JSON.stringify(params)}`);
		},
	});

	pi.registerCommand("subagents", {
		description: "Show async runs. Usage: /subagents [all] | /subagents clear",
		handler: async (args, ctx) => {
			const arg = args.trim().toLowerCase();
			const showAll = arg === "all";
			const MAX_RECENT = 30;

			if (arg === "clear") {
				let removed = 0;
				let skippedActive = 0;
				let skippedUnknown = 0;
				let failed = 0;

				try {
					const dirs = fs.readdirSync(ASYNC_DIR).filter((d) => {
						try { return fs.statSync(path.join(ASYNC_DIR, d)).isDirectory(); } catch { return false; }
					});

					for (const dir of dirs) {
						const dirPath = path.join(ASYNC_DIR, dir);
						const status = readStatus(dirPath);
						if (!status) {
							skippedUnknown++;
							continue;
						}

						const state = status.state;
						const pid = (status as unknown as { pid?: number }).pid;
						const alive = isPidAlive(pid);
						const active = (state === "running" || state === "queued") && alive;
						if (active) {
							skippedActive++;
							continue;
						}

						const clearable = state === "complete" || state === "failed" || ((state === "running" || state === "queued") && !alive);
						if (!clearable) {
							skippedUnknown++;
							continue;
						}

						try {
							fs.rmSync(dirPath, { recursive: true, force: true });
							removed++;
							asyncJobs.delete(dir);

							const uiTimer = cleanupTimers.get(dir);
							if (uiTimer) {
								clearTimeout(uiTimer);
								cleanupTimers.delete(dir);
							}
							const diskTimer = asyncDirCleanupTimers.get(dir);
							if (diskTimer) {
								clearTimeout(diskTimer);
								asyncDirCleanupTimers.delete(dir);
							}

							// Remove tiny async result summary file too
							const resultFile = path.join(RESULTS_DIR, `${dir}.json`);
							try {
								if (fs.existsSync(resultFile)) fs.unlinkSync(resultFile);
							} catch {}
						} catch {
							failed++;
						}
					}
				} catch (e) {
					ctx.ui.notify(`subagents clear failed: ${(e as Error).message}`, "error");
					return;
				}

				if (lastUiContext) {
					renderWidget(lastUiContext, Array.from(asyncJobs.values()));
				}

				ctx.ui.notify(
					`Cleared ${removed} finished/failed runs. Skipped active: ${skippedActive}, unknown: ${skippedUnknown}${failed ? `, errors: ${failed}` : ""}`,
					"info",
				);
				return;
			}

			// ── Collect jobs from memory + disk ──
			const jobs: AsyncJobState[] = Array.from(asyncJobs.values());
			const existingIds = new Set(jobs.map(j => j.asyncId));

			try {
				const dirs = fs.readdirSync(ASYNC_DIR).filter(d => {
					try { return fs.statSync(path.join(ASYNC_DIR, d)).isDirectory(); } catch { return false; }
				});
				for (const dir of dirs) {
					if (existingIds.has(dir)) continue;
					const dirPath = path.join(ASYNC_DIR, dir);
					const status = readStatus(dirPath);
					if (!status) continue;

					const steps = (status.steps ?? []) as Record<string, unknown>[];
					const stepModels = steps.map(s => ((s.resolvedModel ?? s.model ?? "") as string));
					let liveCost = 0, toolCount = 0;
					for (const s of steps) {
						if (typeof s.liveCost === "number") liveCost += s.liveCost;
						if (typeof s.toolCount === "number") toolCount += s.toolCount;
					}

					jobs.push({
						asyncId: dir.slice(0, 36),
						asyncDir: dirPath,
						status: status.state ?? "failed",
						mode: status.mode ?? "single",
						agents: status.steps?.map((s: { agent: string }) => s.agent),
						stepsTotal: status.steps?.length,
						startedAt: status.startedAt,
						updatedAt: status.lastUpdate ?? status.endedAt,
						outputFile: status.outputFile,
						stepModels,
						liveCost: liveCost > 0 ? liveCost : undefined,
						toolCount: toolCount > 0 ? toolCount : undefined,
					});
				}
			} catch {}

			// ── Detect dead processes ──
			for (const job of jobs) {
				if (job.status !== "running") continue;
				try {
					const statusPath = path.join(job.asyncDir, "status.json");
					const raw = fs.readFileSync(statusPath, "utf-8");
					const sj = JSON.parse(raw);
					if (sj.pid) {
						try { process.kill(sj.pid, 0); } catch {
							job.status = "failed";
							sj.state = "failed";
							sj.endedAt = sj.endedAt ?? Date.now();
							sj.lastUpdate = Date.now();
							sj.error = sj.error ?? `Process ${sj.pid} died (detected by /subagents)`;
							try { fs.writeFileSync(statusPath, JSON.stringify(sj, null, 2)); } catch {}
						}
					}
				} catch {}
			}

			if (jobs.length === 0) {
				ctx.ui.notify("No async subagent runs found", "info");
				return;
			}

			// ── Sort ──
			jobs.sort((a, b) => {
				const rank = (s: string) => s === "running" ? 0 : s === "queued" ? 1 : s === "failed" ? 2 : 3;
				const r = rank(a.status) - rank(b.status);
				if (r !== 0) return r;
				return (b.startedAt ?? 0) - (a.startedAt ?? 0);
			});

			const activeJobs = jobs.filter(j => j.status === "running" || j.status === "queued");
			const finishedJobs = jobs.filter(j => j.status !== "running" && j.status !== "queued");
			const displayJobs = showAll
				? jobs
				: [...activeJobs, ...finishedJobs.slice(0, Math.max(0, MAX_RECENT - activeJobs.length))];
			const hiddenCount = jobs.length - displayJobs.length;

			// ── Shared helpers ──
			const fmtElapsed = (startMs: number, endMs: number): string => {
				const s = Math.floor((endMs - startMs) / 1000);
				if (s >= 3600) return `${Math.floor(s / 3600)}h${String(Math.floor((s % 3600) / 60)).padStart(2, "0")}m`;
				if (s >= 60) return `${Math.floor(s / 60)}m${String(s % 60).padStart(2, "0")}s`;
				return `${s}s`;
			};
			const shortModel = (m: string): string => {
				if (!m) return "";
				const idx = m.indexOf("/");
				let name = idx >= 0 ? m.slice(idx + 1) : m;
				const c = name.indexOf(":");
				if (c >= 0) name = name.slice(0, c);
				return name;
			};
			const fmtCost = (c: number): string => c >= 0.01 ? `$${c.toFixed(2)}` : `$${c.toFixed(3)}`;

			// ── Parse output JSONL into readable timeline ──
			const parseOutputLog = (logPath: string, maxEntries = 200): string[] => {
				const entries: string[] = [];
				try {
					const content = fs.readFileSync(logPath, "utf-8");
					const lines = content.split("\n");
					for (const line of lines) {
						if (!line.startsWith("{")) continue;
						let evt: Record<string, unknown>;
						try { evt = JSON.parse(line); } catch { continue; }

						const type = evt.type as string;
						if (type === "tool_execution_start") {
							const name = evt.toolName as string ?? "?";
							const a = evt.args as Record<string, unknown> | undefined;
							let argStr = "";
							if (a) {
								const parts = Object.entries(a).slice(0, 3).map(([k, v]) => {
									const vs = String(v);
									return `${k}=${vs.length > 40 ? vs.slice(0, 37) + "..." : vs}`;
								});
								argStr = parts.join(", ");
							}
							entries.push(`🔧 ${name}(${argStr})`);
						} else if (type === "message_end") {
							const msg = evt.message as Record<string, unknown> | undefined;
							const content = msg?.content as Array<Record<string, unknown>> | undefined;
							const usage = msg?.usage as Record<string, unknown> | undefined;
							const model = (msg?.model ?? "") as string;
							if (content) {
								for (const part of content) {
									if (part.type === "text" && typeof part.text === "string") {
										const text = (part.text as string).trim();
										if (text) {
											// Show first ~3 lines of assistant text
											const preview = text.split("\n").slice(0, 3).join(" ").slice(0, 200);
											const modelTag = model ? ` [${shortModel(model)}]` : "";
											const costTag = usage ? ` (${usage.input_tokens ?? "?"}→${usage.output_tokens ?? "?"} tok)` : "";
											entries.push(`💬${modelTag}${costTag} ${preview}`);
										}
									}
								}
							}
						} else if (type === "error") {
							const msg = (evt.message ?? evt.error ?? "unknown error") as string;
							entries.push(`❌ ${msg.slice(0, 200)}`);
						}
						if (entries.length >= maxEntries) break;
					}
				} catch (e) {
					entries.push(`(could not read log: ${(e as Error).message})`);
				}
				return entries;
			};

			// ── Detail overlay for a single job ──
			const showDetail = async (job: AsyncJobState): Promise<void> => {
				// Gather all readable info
				const logPath = job.outputFile ?? path.join(job.asyncDir, "output-0.log");
				const timeline = parseOutputLog(logPath);

				// Read status.json for step details
				let statusInfo: string[] = [];
				try {
					const sj = JSON.parse(fs.readFileSync(path.join(job.asyncDir, "status.json"), "utf-8"));
					if (sj.error) statusInfo.push(`Error: ${sj.error}`);
					if (sj.steps?.length) {
						for (let si = 0; si < sj.steps.length; si++) {
							const step = sj.steps[si];
							const m = shortModel(step.resolvedModel ?? step.model ?? "");
							const dur = step.durationMs ? fmtElapsed(0, step.durationMs) : "—";
							const cost = typeof step.liveCost === "number" ? fmtCost(step.liveCost) : "";
							const tok = step.liveTokens?.total ? `${step.liveTokens.total} tok` : (step.tokens?.total ? `${step.tokens.total} tok` : "");
							const tools = typeof step.toolCount === "number" ? `${step.toolCount} calls` : "";
							const info = [m, dur, cost, tok, tools].filter(Boolean).join("  ·  ");
							statusInfo.push(`Step ${si + 1}: ${step.agent} [${step.status}]  ${info}`);
						}
					}
				} catch {}

				// Check for artifacts (subagent log, output markdown)
				let artifactContent = "";
				const mdLog = path.join(job.asyncDir, `subagent-log-${job.asyncId}.md`);
				if (fs.existsSync(mdLog)) {
					try { artifactContent = fs.readFileSync(mdLog, "utf-8"); } catch {}
				}
				// Also check for _output.md artifact
				try {
					const artDir = tempArtifactsDir ?? "/tmp/pi-subagent-artifacts";
					const outputs = fs.readdirSync(artDir)
						.filter(f => f.startsWith(job.asyncId.slice(0, 8)) && f.endsWith("_output.md"));
					for (const o of outputs) {
						const content = fs.readFileSync(path.join(artDir, o), "utf-8");
						if (content.trim()) {
							artifactContent += `\n\n--- ${o} ---\n${content.slice(0, 3000)}`;
						}
					}
				} catch {}

				// Build detail lines
				const allLines: string[] = [];
				if (statusInfo.length) {
					allLines.push("STEPS");
					allLines.push(...statusInfo);
					allLines.push("");
				}
				if (timeline.length) {
					allLines.push(`ACTIVITY LOG (${timeline.length} events)`);
					allLines.push(...timeline);
					allLines.push("");
				}
				if (artifactContent.trim()) {
					allLines.push("OUTPUT");
					allLines.push(...artifactContent.trim().split("\n").slice(0, 100));
				}
				if (allLines.length === 0) {
					allLines.push("(no output data available)");
				}

				await ctx.ui.custom((tui, theme, _kb, done) => {
					let scroll = 0;
					let cW: number | undefined;
					let cL: string[] | undefined;
					const dp = "  "; // detail padding

					const inv = () => { cW = undefined; cL = undefined; tui.requestRender(); };

					return {
						handleInput(data: string) {
							if (matchesKey(data, Key.escape) || matchesKey(data, "q") || matchesKey(data, Key.backspace)) {
								done(null);
							} else if (matchesKey(data, Key.up) || matchesKey(data, "k")) {
								if (scroll > 0) { scroll--; inv(); }
							} else if (matchesKey(data, Key.down) || matchesKey(data, "j")) {
								if (scroll < allLines.length - 1) { scroll++; inv(); }
							} else if (matchesKey(data, Key.home) || matchesKey(data, "g")) {
								scroll = 0; inv();
							} else if (matchesKey(data, Key.end) || matchesKey(data, "G")) {
								scroll = Math.max(0, allLines.length - 5); inv();
							}
						},
						render(width: number): string[] {
							if (cL && cW === width) return cL;
							const now = Date.now();
							const w = Math.min(width, 100);
							const sep = theme.fg("dim", dp + "─".repeat(w - 4));
							const out: string[] = [];

							// Header
							const id = job.asyncId.slice(0, 8);
							const icon = job.status === "complete" ? theme.fg("success", "✓")
								: job.status === "failed" ? theme.fg("error", "✗")
								: job.status === "running" ? theme.fg("warning", "●")
								: theme.fg("dim", "○");
							const agents = job.agents?.join(" → ") ?? "?";
							const end = (job.status === "complete" || job.status === "failed") ? (job.updatedAt ?? now) : now;
							const elapsed = job.startedAt ? fmtElapsed(job.startedAt, end) : "—";
							const models = [...new Set((job.stepModels ?? []).map(shortModel).filter(Boolean))];

							out.push("");
							out.push(`${dp} ${icon} ${theme.fg("dim", id)}  ${agents}  ${theme.fg("muted", elapsed)}`);
							const meta: string[] = [];
							if (models.length) meta.push(theme.fg("accent", models.join(", ")));
							if (job.liveCost) meta.push(fmtCost(job.liveCost));
							if (job.toolCount) meta.push(`${job.toolCount} calls`);
							if (meta.length) out.push(theme.fg("muted", `${dp}    ${meta.join("  ·  ")}`));
							out.push("");
							out.push(sep);

							// Scrollable content
							const termH = process.stdout.rows ?? 24;
							const maxVisible = Math.max(5, Math.floor(termH * 0.75) - 8);
							const clamped = Math.min(scroll, Math.max(0, allLines.length - maxVisible));
							const visible = allLines.slice(clamped, clamped + maxVisible);

							out.push(""); // breathing room
							for (const line of visible) {
								let styled = line;
								if (line.startsWith("🔧")) styled = theme.fg("muted", line);
								else if (line.startsWith("💬")) styled = line;
								else if (line.startsWith("❌")) styled = theme.fg("error", line);
								else if (line === line.toUpperCase() && line.length > 2 && !line.startsWith("(")) styled = theme.fg("accent", line);
								out.push(truncateToWidth(`${dp}  ${styled}`, width));
							}
							out.push("");

							if (clamped > 0) out.push(theme.fg("dim", `${dp}    ↑ ${clamped} more`));
							const below = allLines.length - clamped - maxVisible;
							if (below > 0) out.push(theme.fg("dim", `${dp}    ↓ ${below} more`));

							out.push(sep);
							out.push(theme.fg("dim", `${dp}↑↓ scroll  g/G jump  esc/q back`));
							out.push("");

							cW = width;
							cL = out;
							return out;
						},
						invalidate() { cW = undefined; cL = undefined; },
					};
				}, { overlay: true, overlayOptions: { anchor: "center", width: "90%", maxHeight: "90%" } });
			};

			// ── Tab-filtered job lists ──
			const pad = "  "; // left padding for content
			const tabDefs = [
				{ key: "running", label: "Running", icon: "●", filter: (j: AsyncJobState) => j.status === "running" || j.status === "queued" },
				{ key: "done",    label: "Done",    icon: "✓", filter: (j: AsyncJobState) => j.status === "complete" },
				{ key: "failed",  label: "Failed",  icon: "✗", filter: (j: AsyncJobState) => j.status === "failed" },
			] as const;
			const tabJobs = tabDefs.map(t => displayJobs.filter(t.filter));

			// ── List overlay (returns selected job or null) ──
			const showList = async (): Promise<AsyncJobState | null> => {
				return ctx.ui.custom<AsyncJobState | null>((tui, theme, _kb, done) => {
					let activeTab = tabJobs[0].length > 0 ? 0 : tabJobs[2].length > 0 ? 2 : 1;
					let cursor = 0;
					let cachedWidth: number | undefined;
					let cachedLines: string[] | undefined;

					const inv = () => { cachedWidth = undefined; cachedLines = undefined; tui.requestRender(); };
					const switchTab = (idx: number) => { activeTab = idx; cursor = 0; inv(); };
					const currentList = () => tabJobs[activeTab];

					return {
						handleInput(data: string) {
							if (matchesKey(data, Key.escape) || matchesKey(data, "q")) {
								done(null);
							} else if (matchesKey(data, Key.enter)) {
								const list = currentList();
								if (list[cursor]) done(list[cursor]);
							} else if (matchesKey(data, Key.up) || matchesKey(data, "k")) {
								if (cursor > 0) { cursor--; inv(); }
							} else if (matchesKey(data, Key.down) || matchesKey(data, "j")) {
								if (cursor < currentList().length - 1) { cursor++; inv(); }
							} else if (matchesKey(data, Key.home) || matchesKey(data, "g")) {
								cursor = 0; inv();
							} else if (matchesKey(data, Key.end) || matchesKey(data, "G")) {
								cursor = Math.max(0, currentList().length - 1); inv();
							} else if (matchesKey(data, Key.tab) || matchesKey(data, Key.right) || matchesKey(data, "l")) {
								switchTab((activeTab + 1) % 3);
							} else if (matchesKey(data, Key.shift("tab")) || matchesKey(data, Key.left) || matchesKey(data, "h")) {
								switchTab((activeTab + 2) % 3);
							} else if (data === "1") { switchTab(0); }
							else if (data === "2") { switchTab(1); }
							else if (data === "3") { switchTab(2); }
						},
						render(width: number): string[] {
							if (cachedLines && cachedWidth === width) return cachedLines;

							const lines: string[] = [];
							const now = Date.now();
							const w = Math.min(width, 100);
							const sep = theme.fg("dim", pad + "─".repeat(w - 4));

							// ── Title ──
							lines.push("");
							lines.push(pad + theme.fg("accent", "Subagents") +
								(hiddenCount > 0 ? theme.fg("dim", `  (${hiddenCount} older — /subagents all)`) : ""));
							lines.push("");

							// ── Tabs ──
							const tabParts: string[] = [];
							for (let t = 0; t < tabDefs.length; t++) {
								const def = tabDefs[t];
								const count = tabJobs[t].length;
								const label = `${def.icon} ${def.label} (${count})`;
								if (t === activeTab) {
									tabParts.push(theme.fg("accent", `[${label}]`));
								} else if (count === 0) {
									tabParts.push(theme.fg("dim", ` ${label} `));
								} else {
									tabParts.push(theme.fg("muted", ` ${label} `));
								}
							}
							lines.push(pad + tabParts.join(theme.fg("dim", "  ")));
							lines.push(sep);

							// ── Job list for active tab ──
							const list = currentList();

							if (list.length === 0) {
								lines.push("");
								lines.push(pad + theme.fg("dim", `  No ${tabDefs[activeTab].label.toLowerCase()} runs`));
								lines.push("");
							} else {
								const termHeight = process.stdout.rows ?? 24;
								const maxLines = Math.max(6, Math.floor(termHeight * 0.70) - 8);
								const maxJobs = Math.floor(maxLines / 3);
								let scrollStart = 0;
								if (cursor >= scrollStart + maxJobs) scrollStart = cursor - maxJobs + 1;
								if (cursor < scrollStart) scrollStart = cursor;
								const visible = list.slice(scrollStart, scrollStart + maxJobs);

								lines.push(""); // breathing room after tabs

								for (let i = 0; i < visible.length; i++) {
									const job = visible[i];
									const jobIdx = scrollStart + i;
									const selected = jobIdx === cursor;
									const id = job.asyncId.slice(0, 8);

									const icon =
										job.status === "complete" ? theme.fg("success", "✓")
										: job.status === "failed" ? theme.fg("error", "✗")
										: job.status === "running" ? theme.fg("warning", "●")
										: theme.fg("dim", "○");

									const end = (job.status === "complete" || job.status === "failed")
										? (job.updatedAt ?? now) : now;
									const elapsed = job.startedAt ? fmtElapsed(job.startedAt, end) : "—";
									const agents = job.agents?.length
										? job.agents.join(theme.fg("dim", " → "))
										: (job.mode ?? "?");

									const pointer = selected ? theme.fg("accent", "▸") : " ";
									lines.push(truncateToWidth(`${pad}${pointer} ${icon} ${theme.fg("dim", id)}  ${agents}  ${theme.fg("muted", elapsed)}`, width));

									// Detail line
									const parts: string[] = [];
									const models = [...new Set((job.stepModels ?? []).map(shortModel).filter(Boolean))];
									if (models.length) parts.push(theme.fg("accent", models.join(theme.fg("dim", ", "))));
									if (job.status === "running") {
										const activity = getLastActivity(job.outputFile);
										if (activity && activity !== "DEAD") parts.push(theme.fg("muted", activity));
									} else {
										if (job.liveCost) parts.push(theme.fg("muted", fmtCost(job.liveCost)));
										if (job.toolCount) parts.push(theme.fg("dim", `${job.toolCount} calls`));
									}
									lines.push(truncateToWidth(`${pad}     ${parts.length ? parts.join(theme.fg("dim", "  ·  ")) : theme.fg("dim", "—")}`, width));

									if (i < visible.length - 1) lines.push(""); // spacer between jobs
								}

								// Scroll indicators
								lines.push("");
								if (scrollStart > 0) lines.push(theme.fg("dim", `${pad}  ↑ ${scrollStart} more`));
								const below = list.length - scrollStart - maxJobs;
								if (below > 0) lines.push(theme.fg("dim", `${pad}  ↓ ${below} more`));
							}

							lines.push(sep);
							lines.push(theme.fg("dim", `${pad}↑↓ navigate  tab/←→ switch  enter inspect  q close`));
							lines.push("");

							cachedWidth = width;
							cachedLines = lines;
							return lines;
						},
						invalidate() { cachedWidth = undefined; cachedLines = undefined; },
					};
				}, { overlay: true, overlayOptions: { anchor: "center", width: "90%", maxHeight: "90%" } });
			};

			// ── Main loop: list ↔ detail ──
			let selected: AsyncJobState | null;
			do {
				selected = await showList();
				if (selected) await showDetail(selected);
			} while (selected !== null);
		},
	});

	pi.registerShortcut("ctrl+shift+a", {
		handler: async (ctx) => {
			await openAgentManager(ctx);
		},
	});

	pi.events.on("subagent:started", (data) => {
		const info = data as {
			id?: string;
			asyncDir?: string;
			agent?: string;
			chain?: string[];
		};
		if (!info.id) return;
		const asyncDir = info.asyncDir ?? path.join(ASYNC_DIR, info.id);
		const agents = info.chain && info.chain.length > 0 ? info.chain : info.agent ? [info.agent] : undefined;
		const now = Date.now();
		asyncJobs.set(info.id, {
			asyncId: info.id,
			asyncDir,
			status: "queued",
			mode: info.chain ? "chain" : "single",
			agents,
			stepsTotal: agents?.length,
			startedAt: now,
			updatedAt: now,
		});
		if (lastUiContext) {
			renderWidget(lastUiContext, Array.from(asyncJobs.values()));
			ensurePoller();
		}
	});

	pi.events.on("subagent:complete", (data) => {
		const result = data as { id?: string; success?: boolean; asyncDir?: string };
		const asyncId = result.id;
		if (!asyncId) return;
		const job = asyncJobs.get(asyncId);
		if (job) {
			job.status = result.success ? "complete" : "failed";
			job.updatedAt = Date.now();
			if (result.asyncDir) job.asyncDir = result.asyncDir;
		}
		if (lastUiContext) {
			renderWidget(lastUiContext, Array.from(asyncJobs.values()));
		}

		const finalStatus: "complete" | "failed" = result.success ? "complete" : "failed";
		scheduleFinishedJobCleanup(asyncId, finalStatus, result.asyncDir ?? job?.asyncDir);
	});

	pi.on("tool_result", (event, ctx) => {
		if (event.toolName !== "subagent") return;
		if (!ctx.hasUI) return;
		lastUiContext = ctx;
		if (asyncJobs.size > 0) {
			renderWidget(ctx, Array.from(asyncJobs.values()));
			ensurePoller();
		}
	});

	const cleanupSessionArtifacts = (ctx: ExtensionContext) => {
		try {
			const sessionFile = ctx.sessionManager.getSessionFile();
			if (sessionFile) {
				cleanupOldArtifacts(getArtifactsDir(sessionFile), DEFAULT_ARTIFACT_CONFIG.cleanupDays);
			}
		} catch {}
	};

	pi.on("session_start", (_event, ctx) => {
		baseCwd = ctx.cwd;
		currentSessionId = ctx.sessionManager.getSessionFile() ?? `session-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
		cleanupSessionArtifacts(ctx);
		cleanupOldAsyncDirs(ASYNC_DIR);
		for (const timer of cleanupTimers.values()) clearTimeout(timer);
		cleanupTimers.clear();
		asyncJobs.clear();
		resultFileCoalescer.clear();
		rehydrateAsyncJobsFromDisk();
		if (ctx.hasUI) {
			lastUiContext = ctx;
			renderWidget(ctx, Array.from(asyncJobs.values()));
			ensurePoller();
		}
	});
	pi.on("session_switch", (_event, ctx) => {
		baseCwd = ctx.cwd;
		currentSessionId = ctx.sessionManager.getSessionFile() ?? `session-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
		cleanupSessionArtifacts(ctx);
		cleanupOldAsyncDirs(ASYNC_DIR);
		for (const timer of cleanupTimers.values()) clearTimeout(timer);
		cleanupTimers.clear();
		asyncJobs.clear();
		resultFileCoalescer.clear();
		rehydrateAsyncJobsFromDisk();
		if (ctx.hasUI) {
			lastUiContext = ctx;
			renderWidget(ctx, Array.from(asyncJobs.values()));
			ensurePoller();
		}
	});
	pi.on("session_branch", (_event, ctx) => {
		baseCwd = ctx.cwd;
		currentSessionId = ctx.sessionManager.getSessionFile() ?? `session-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
		cleanupSessionArtifacts(ctx);
		cleanupOldAsyncDirs(ASYNC_DIR);
		for (const timer of cleanupTimers.values()) clearTimeout(timer);
		cleanupTimers.clear();
		asyncJobs.clear();
		resultFileCoalescer.clear();
		rehydrateAsyncJobsFromDisk();
		if (ctx.hasUI) {
			lastUiContext = ctx;
			renderWidget(ctx, Array.from(asyncJobs.values()));
			ensurePoller();
		}
	});
	pi.on("session_shutdown", () => {
		watcher?.close();
		if (watcherRestartTimer) clearTimeout(watcherRestartTimer);
		watcherRestartTimer = null;
		if (poller) clearInterval(poller);
		poller = null;
		// Clear all pending cleanup timers
		for (const timer of cleanupTimers.values()) {
			clearTimeout(timer);
		}
		cleanupTimers.clear();
		for (const timer of asyncDirCleanupTimers.values()) {
			clearTimeout(timer);
		}
		asyncDirCleanupTimers.clear();
		asyncJobs.clear();
		resultFileCoalescer.clear();
		if (lastUiContext?.hasUI) {
			lastUiContext.ui.setWidget(WIDGET_KEY, undefined);
		}
	});
}
