import childProcess from "node:child_process";
import { appendFileSync } from "node:fs";
import { syncBuiltinESMExports } from "node:module";
import { basename } from "node:path";

const script = process.env.MOCK_PI_SCRIPT;
if (process.argv[1] === script) {
	appendFileSync(process.env.MOCK_PI_ARGS_PATH, JSON.stringify(process.argv.slice(2)) + "\n");
}

// On Windows the detached runner cannot use the test process's argv[1] or
// spawn the harness's .cmd shim directly. Route its Pi invocation to the mock.
if (process.platform === "win32" && basename(process.argv[2] ?? "") === "subagent-runner.ts") {
	const spawn = childProcess.spawn;
	childProcess.spawn = (command, args, options) => {
		if (command === "pi") return spawn(process.execPath, [script, ...args], options);
		if (command === process.execPath && basename(args[0] ?? "") === "cli.js") {
			return spawn(command, [script, ...args.slice(1)], options);
		}
		return spawn(command, args, options);
	};
	syncBuiltinESMExports();
}
