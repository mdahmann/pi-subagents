/**
 * Subagent completion notifications (extension)
 */

import type { ExtensionAPI } from "@mariozechner/pi-coding-agent";
import { buildCompletionKey, getGlobalSeenMap, markSeenWithTtl } from "./completion-dedupe.js";

interface ChainStepResult {
	agent: string;
	output: string;
	success: boolean;
}

function sanitizeForNotify(text: string, opts?: { maxLines?: number; maxLineChars?: number }): string {
	const maxLines = opts?.maxLines ?? 80;
	const maxLineChars = opts?.maxLineChars ?? 160;

	const withoutOsc = text.replace(/\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)/g, "");
	const withoutAnsi = withoutOsc
		.replace(/\x1b\[[0-?]*[ -/]*[@-~]/g, "")
		.replace(/\x1b[@-_]/g, "");
	const withoutControls = withoutAnsi.replace(/[\u0000-\u0008\u000B-\u001A\u001C-\u001F\u007F]/g, "");

	const lines = withoutControls
		.split("\n")
		.map((line) => line.trimEnd());

	const clamped = lines.slice(0, maxLines).map((line) =>
		line.length > maxLineChars ? `${line.slice(0, Math.max(1, maxLineChars - 1))}…` : line,
	);

	if (lines.length > maxLines) {
		clamped.push(`… (${lines.length - maxLines} more lines)`);
	}

	return clamped.join("\n");
}

interface SubagentResult {
	id: string | null;
	agent: string | null;
	success: boolean;
	summary: string;
	exitCode: number;
	timestamp: number;
	sessionFile?: string;
	shareUrl?: string;
	gistUrl?: string;
	shareError?: string;
	results?: ChainStepResult[];
	taskIndex?: number;
	totalTasks?: number;
}

export default function registerSubagentNotify(pi: ExtensionAPI): void {
	const seen = getGlobalSeenMap("__pi_subagents_notify_seen__");
	const ttlMs = 10 * 60 * 1000;

	const handleComplete = (data: unknown) => {
		const result = data as SubagentResult;
		const now = Date.now();
		const key = buildCompletionKey(result, "notify");
		if (markSeenWithTtl(seen, key, now, ttlMs)) return;

		const agent = result.agent ?? "unknown";
		const status = result.success ? "completed" : "failed";

		const taskInfo =
			result.taskIndex !== undefined && result.totalTasks !== undefined
				? ` (${result.taskIndex + 1}/${result.totalTasks})`
				: "";

		const extra: string[] = [];
		if (result.shareUrl) {
			extra.push(`Session: ${result.shareUrl}`);
		} else if (result.shareError) {
			extra.push(`Session share error: ${result.shareError}`);
		} else if (result.sessionFile) {
			extra.push(`Session file: ${result.sessionFile}`);
		}

		const safeSummary = sanitizeForNotify(result.summary, { maxLines: 80, maxLineChars: 160 });
		const content = [
			`Background task ${status}: **${agent}**${taskInfo}`,
			"",
			safeSummary,
			extra.length ? "" : undefined,
			extra.length ? extra.join("\n") : undefined,
		]
			.filter((line) => line !== undefined)
			.join("\n");

		pi.sendMessage(
			{
				customType: "subagent-notify",
				content,
				display: true,
			},
			{ triggerTurn: true },
		);
	};

	pi.events.on("subagent:complete", handleComplete);
}
