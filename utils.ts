/**
 * General utility functions for the subagent extension
 */

import * as fs from "node:fs";
import * as path from "node:path";
import * as os from "node:os";
import * as path from "node:path";
import type { Message } from "@mariozechner/pi-ai";
import type { AsyncStatus, DisplayItem, ErrorInfo } from "./types.js";

// ============================================================================
// File System Utilities
// ============================================================================

// Cache for status file reads - avoid re-reading unchanged files
const statusCache = new Map<string, { mtime: number; status: AsyncStatus }>();

/**
 * Read async job status from disk (with mtime-based caching)
 */
export function readStatus(asyncDir: string): AsyncStatus | null {
	const statusPath = path.join(asyncDir, "status.json");
	try {
		const stat = fs.statSync(statusPath);
		const cached = statusCache.get(statusPath);
		if (cached && cached.mtime === stat.mtimeMs) {
			return cached.status;
		}
		const content = fs.readFileSync(statusPath, "utf-8");
		const status = JSON.parse(content) as AsyncStatus;
		statusCache.set(statusPath, { mtime: stat.mtimeMs, status });
		// Limit cache size to prevent memory leaks
		if (statusCache.size > 50) {
			const firstKey = statusCache.keys().next().value;
			if (firstKey) statusCache.delete(firstKey);
		}
		return status;
	} catch {
		return null;
	}
}

// Cache for output tail reads - avoid re-reading unchanged files
const outputTailCache = new Map<string, { mtime: number; size: number; lines: string[] }>();

/**
 * Get the last N lines from an output file (with mtime/size-based caching)
 */
/**
 * Extract human-readable text from JSONL event lines.
 * Filters out raw JSON, extracts text deltas and tool activity.
 */
function extractReadableLines(rawLines: string[]): string[] {
	const readable: string[] = [];
	for (let i = rawLines.length - 1; i >= 0 && readable.length < 10; i--) {
		const line = rawLines[i].trim();
		if (!line) continue;

		// Plain text lines (warnings, errors) — keep as-is
		if (!line.startsWith("{")) {
			readable.unshift(line);
			continue;
		}

		// JSONL event — extract meaningful content
		try {
			const evt = JSON.parse(line);

			if (evt.type === "tool_execution_start" && evt.toolName) {
				const argsPreview = evt.args
					? Object.entries(evt.args as Record<string, unknown>)
						.slice(0, 2)
						.map(([k, v]) => `${k}=${typeof v === "string" ? v.slice(0, 20) : v}`)
						.join(", ")
					: "";
				readable.unshift(`→ ${evt.toolName}(${argsPreview})`);
			} else if (evt.type === "message_update" && evt.assistantMessageEvent) {
				const ae = evt.assistantMessageEvent;
				if (ae.type === "text_delta" && ae.delta) {
					readable.unshift(ae.delta.slice(0, 120));
				}
			} else if (evt.type === "message_end" && evt.message?.role === "assistant") {
				// Extract final text from content array
				const content = evt.message.content;
				if (Array.isArray(content)) {
					for (const part of content) {
						if (part.type === "text" && part.text) {
							const preview = part.text.split("\n").filter((l: string) => l.trim()).slice(-2).join(" | ");
							readable.unshift(preview.slice(0, 120));
						}
					}
				}
			}
			// Skip other JSONL event types (session, turn_start, message_start, etc.)
		} catch {
			// Not valid JSON — show as-is
			readable.unshift(line);
		}
	}
	return readable;
}

export function getOutputTail(outputFile: string | undefined, maxLines: number = 3): string[] {
	if (!outputFile) return [];
	let fd: number | null = null;
	try {
		const stat = fs.statSync(outputFile);
		if (stat.size === 0) return [];

		// Check cache using both mtime and size (size changes more frequently during writes)
		const cached = outputTailCache.get(outputFile);
		if (cached && cached.mtime === stat.mtimeMs && cached.size === stat.size) {
			return cached.lines;
		}

		const tailBytes = 8192; // Read more to find readable content in JSONL streams
		const start = Math.max(0, stat.size - tailBytes);
		fd = fs.openSync(outputFile, "r");
		const buffer = Buffer.alloc(Math.min(tailBytes, stat.size));
		fs.readSync(fd, buffer, 0, buffer.length, start);
		const content = buffer.toString("utf-8");
		const allLines = content.split("\n").filter((l) => l.trim());

		// Extract readable content from JSONL events (or pass through plain text)
		const readable = extractReadableLines(allLines);
		const lines = readable.slice(-maxLines).map((l) => l.slice(0, 120) + (l.length > 120 ? "…" : ""));

		// Cache the result
		outputTailCache.set(outputFile, { mtime: stat.mtimeMs, size: stat.size, lines });
		// Limit cache size
		if (outputTailCache.size > 20) {
			const firstKey = outputTailCache.keys().next().value;
			if (firstKey) outputTailCache.delete(firstKey);
		}

		return lines;
	} catch {
		return [];
	} finally {
		if (fd !== null) {
			try {
				fs.closeSync(fd);
			} catch {}
		}
	}
}

/**
 * Get human-readable last activity time for a file
 */
export function getLastActivity(outputFile: string | undefined): string {
	if (!outputFile) return "";
	try {
		// Single stat call - throws if file doesn't exist
		const stat = fs.statSync(outputFile);
		const ago = Date.now() - stat.mtimeMs;
		if (ago < 1000) return "active now";
		if (ago < 60000) return `active ${Math.floor(ago / 1000)}s ago`;

		const minAgo = Math.floor(ago / 60000);

		// If stale for 5+ min, check if process is actually alive
		if (minAgo >= 5) {
			try {
				const statusPath = path.join(path.dirname(outputFile), "status.json");
				const status = JSON.parse(fs.readFileSync(statusPath, "utf-8"));
				const pid = status.pid;
				if (pid) {
					try {
						process.kill(pid, 0); // signal 0 = check existence
					} catch {
						// Process is dead — update status.json
						status.state = "failed";
						status.endedAt = Date.now();
						status.lastUpdate = Date.now();
						status.error = `Process ${pid} died without completion (stale ${minAgo}m)`;
						fs.writeFileSync(statusPath, JSON.stringify(status, null, 2));
						return "DEAD";
					}
				}
			} catch {}
		}

		return `active ${minAgo}m ago`;
	} catch {
		return "";
	}
}

/**
 * Find a file/directory by prefix in a directory
 */
export function findByPrefix(dir: string, prefix: string, suffix?: string): string | null {
	if (!fs.existsSync(dir)) return null;
	const entries = fs.readdirSync(dir).filter((entry) => entry.startsWith(prefix));
	if (suffix) {
		const withSuffix = entries.filter((entry) => entry.endsWith(suffix));
		return withSuffix.length > 0 ? path.join(dir, withSuffix.sort()[0]) : null;
	}
	if (entries.length === 0) return null;
	return path.join(dir, entries.sort()[0]);
}

/**
 * Find the latest session file in a directory
 */
export function findLatestSessionFile(sessionDir: string): string | null {
	if (!fs.existsSync(sessionDir)) return null;
	const files = fs.readdirSync(sessionDir)
		.filter((f) => f.endsWith(".jsonl"))
		.map((f) => {
			const filePath = path.join(sessionDir, f);
			return {
				path: filePath,
				mtime: fs.statSync(filePath).mtimeMs,
			};
		})
		.sort((a, b) => b.mtime - a.mtime);
	return files.length > 0 ? files[0].path : null;
}

/**
 * Write a prompt to a temporary file
 */
export function writePrompt(agent: string, prompt: string): { dir: string; path: string } {
	const dir = fs.mkdtempSync(path.join(os.tmpdir(), "pi-subagent-"));
	const p = path.join(dir, `${agent.replace(/[^\w.-]/g, "_")}.md`);
	fs.writeFileSync(p, prompt, { mode: 0o600 });
	return { dir, path: p };
}

// ============================================================================
// Message Parsing Utilities
// ============================================================================

/**
 * Get the final text output from a list of messages
 */
export function getFinalOutput(messages: Message[]): string {
	for (let i = messages.length - 1; i >= 0; i--) {
		const msg = messages[i];
		if (msg.role === "assistant") {
			for (const part of msg.content) {
				if (part.type === "text") return part.text;
			}
		}
	}
	return "";
}

/**
 * Extract display items (text and tool calls) from messages
 */
export function getDisplayItems(messages: Message[]): DisplayItem[] {
	const items: DisplayItem[] = [];
	for (const msg of messages) {
		if (msg.role === "assistant") {
			for (const part of msg.content) {
				if (part.type === "text") items.push({ type: "text", text: part.text });
				else if (part.type === "toolCall") items.push({ type: "tool", name: part.name, args: part.arguments });
			}
		}
	}
	return items;
}

/**
 * Detect errors in subagent execution from messages (only errors with no subsequent success)
 */
export function detectSubagentError(messages: Message[]): ErrorInfo {
	// Step 1: Find the last assistant message with text content.
	// If the agent produced a text response after encountering errors,
	// it had a chance to recover — only errors AFTER this point matter.
	let lastAssistantTextIndex = -1;
	for (let i = messages.length - 1; i >= 0; i--) {
		const msg = messages[i];
		if (msg.role === "assistant") {
			const hasText = Array.isArray(msg.content) && msg.content.some(
				(c) => c.type === "text" && "text" in c && (c.text as string).trim().length > 0,
			);
			if (hasText) {
				lastAssistantTextIndex = i;
				break;
			}
		}
	}

	// Step 2: Only scan tool results AFTER the last assistant text message.
	// Errors before the agent's final response are implicitly recovered.
	const scanStart = lastAssistantTextIndex >= 0 ? lastAssistantTextIndex + 1 : 0;

	// Step 3: Check tool results in the post-response window
	for (let i = messages.length - 1; i >= scanStart; i--) {
		const msg = messages[i];
		if (msg.role !== "toolResult") continue;

		if ((msg as any).isError) {
			const text = msg.content.find((c) => c.type === "text");
			const details = text && "text" in text ? text.text : undefined;
			const exitMatch = details?.match(/exit(?:ed)?\s*(?:with\s*)?(?:code|status)?\s*[:\s]?\s*(\d+)/i);
			return {
				hasError: true,
				exitCode: exitMatch ? parseInt(exitMatch[1], 10) : 1,
				errorType: (msg as any).toolName || "tool",
				details: details?.slice(0, 200),
			};
		}

		const toolName = (msg as any).toolName;
		if (toolName !== "bash") continue;

		const text = msg.content.find((c) => c.type === "text");
		if (!text || !("text" in text)) continue;
		const output = text.text;

		const exitMatch = output.match(/exit(?:ed)?\s*(?:with\s*)?(?:code|status)?\s*[:\s]?\s*(\d+)/i);
		if (exitMatch) {
			const code = parseInt(exitMatch[1], 10);
			if (code !== 0) {
				return { hasError: true, exitCode: code, errorType: "bash", details: output.slice(0, 200) };
			}
		}

		// NOTE: These patterns can match legitimate output (grep results, logs,
		// testing). With the assistant-message check above, most false positives
		// are mitigated since the agent will have responded after routine errors.
		const fatalPatterns = [
			/command not found/i,
			/permission denied/i,
			/no such file or directory/i,
			/segmentation fault/i,
			/killed|terminated/i,
			/out of memory/i,
			/connection refused/i,
			/timeout/i,
		];
		for (const pattern of fatalPatterns) {
			if (pattern.test(output)) {
				return { hasError: true, exitCode: 1, errorType: "bash", details: output.slice(0, 200) };
			}
		}
	}

	return { hasError: false };
}

/**
 * Extract a preview of tool arguments for display
 */
export function extractToolArgsPreview(args: Record<string, unknown>): string {
	// Handle MCP tool calls - show server/tool info
	if (args.tool && typeof args.tool === "string") {
		const server = args.server && typeof args.server === "string" ? `${args.server}/` : "";
		const toolArgs = args.args && typeof args.args === "string" ? ` ${args.args.slice(0, 40)}` : "";
		return `${server}${args.tool}${toolArgs}`;
	}
	
	const previewKeys = ["command", "path", "file_path", "pattern", "query", "url", "task", "describe", "search"];
	for (const key of previewKeys) {
		if (args[key] && typeof args[key] === "string") {
			const value = args[key] as string;
			return value.length > 60 ? `${value.slice(0, 57)}...` : value;
		}
	}
	
	// Fallback: show first string value found
	for (const [key, value] of Object.entries(args)) {
		if (typeof value === "string" && value.length > 0) {
			const preview = value.length > 50 ? `${value.slice(0, 47)}...` : value;
			return `${key}=${preview}`;
		}
	}
	return "";
}

/**
 * Extract text content from various message content formats
 */
export function extractTextFromContent(content: unknown): string {
	if (!content) return "";
	// Handle string content directly
	if (typeof content === "string") return content;
	// Handle array content
	if (!Array.isArray(content)) return "";
	const texts: string[] = [];
	for (const part of content) {
		if (part && typeof part === "object") {
			// Handle { type: "text", text: "..." }
			if ("type" in part && part.type === "text" && "text" in part) {
				texts.push(String(part.text));
			}
			// Handle { type: "tool_result", content: "..." }
			else if ("type" in part && part.type === "tool_result" && "content" in part) {
				const inner = extractTextFromContent(part.content);
				if (inner) texts.push(inner);
			}
			// Handle { text: "..." } without type
			else if ("text" in part) {
				texts.push(String(part.text));
			}
		}
	}
	return texts.join("\n");
}

// ============================================================================
// Concurrency Utilities
// ============================================================================

/**
 * Map over items with limited concurrency
 */
export async function mapConcurrent<T, R>(
	items: T[],
	limit: number,
	fn: (item: T, i: number) => Promise<R>,
): Promise<R[]> {
	// Clamp to at least 1; NaN/undefined/0/negative all become 1
	const safeLimit = Math.max(1, Math.floor(limit) || 1);
	const results: R[] = new Array(items.length);
	let next = 0;

	async function worker(): Promise<void> {
		while (next < items.length) {
			const i = next++;
			results[i] = await fn(items[i], i);
		}
	}

	const workers = Array.from({ length: Math.min(safeLimit, items.length) }, () => worker());
	await Promise.all(workers);
	return results;
}
