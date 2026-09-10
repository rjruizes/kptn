/**
 * Thin launcher for the shared `kptn ui` FastAPI server.
 *
 * The extension owns no protocol of its own any more: it starts the same
 * server a developer would start from a terminal, hosts it in one webview,
 * and validates the handful of messages the page sends back.
 *
 * Nothing in this module imports `vscode`, so every rule it enforces --
 * argv, port retries, process reuse, disposal, bridge-token and workspace
 * containment checks -- is testable without an editor host.
 */

import { spawn } from 'child_process';
import * as crypto from 'crypto';
import * as fs from 'fs';
import * as http from 'http';
import * as net from 'net';
import * as path from 'path';

/** The loopback interface is the only address the UI server ever binds. */
export const LOOPBACK_HOST = '127.0.0.1';

/** Query parameter that carries the short-lived bridge token into the page. */
export const BRIDGE_TOKEN_PARAM = 'bridge_token';

/** How many times a lost port reservation is retried before giving up. */
export const PORT_RESERVATION_ATTEMPTS = 3;

const HEALTH_PROBE_ATTEMPTS = 100;
const HEALTH_PROBE_INTERVAL_MS = 100;
const HEALTH_PROBE_TIMEOUT_MS = 1000;

export interface ProcessStream {
	setEncoding?(encoding: string): void;
	on(event: 'data', listener: (chunk: string | Buffer) => void): void;
}

export interface SpawnOptions {
	cwd: string;
	env?: NodeJS.ProcessEnv;
	stdio?: ('pipe' | 'ignore' | 'inherit')[];
	/**
	 * Always absent/false. The `kptn ui` child stays in the extension's own
	 * process group precisely so that terminating it can never widen into a
	 * process-group signal that would reach the detached run workers.
	 */
	detached?: boolean;
}

export interface SpawnedProcess {
	readonly pid?: number;
	readonly stdout?: ProcessStream | null;
	readonly stderr?: ProcessStream | null;
	on(event: 'exit', listener: (code: number | null, signal: string | null) => void): void;
	on(event: 'error', listener: (error: Error) => void): void;
	kill(signal?: string): boolean;
}

export type Spawner = (command: string, argv: string[], options: SpawnOptions) => SpawnedProcess;

/**
 * The single network boundary: it hands out a free loopback port and reports
 * when the server behind that port answers `/healthz`.
 */
export interface LoopbackGate {
	reservePort(): Promise<number>;
	/** One quick probe -- used to decide whether a cached server is reusable. */
	isHealthy(url: URL): Promise<boolean>;
	/** Patient polling -- used while a freshly spawned server boots. */
	waitUntilHealthy(url: URL): Promise<boolean>;
}

/**
 * The filesystem reads the containment check needs. `fs` satisfies it; a test
 * can substitute one to drive the symlink branches deterministically.
 */
export interface PathProbe {
	realpathSync(target: string): string;
	lstatSync(target: string): { isSymbolicLink(): boolean };
}

/** Anything with an `fsPath` -- `vscode.Uri` satisfies this. */
export interface WorkspaceLocation {
	readonly fsPath: string;
}

export interface LaunchLog {
	appendLine(line: string): void;
}

export const defaultSpawner: Spawner = (command, argv, options) =>
	spawn(command, argv, options) as unknown as SpawnedProcess;

/**
 * Ask the OS for a free loopback port, then release it.
 *
 * There is no way to hand an already-bound socket to a Python child, so the
 * reservation has to be closed before the server can take the port. That race
 * is why `KptnServer.start` retries with a freshly reserved port when the
 * child dies reporting the address was already in use.
 */
export function reserveLoopbackPort(): Promise<number> {
	return new Promise((resolve, reject) => {
		const server = net.createServer();
		server.unref();
		server.on('error', reject);
		server.listen({ host: LOOPBACK_HOST, port: 0 }, () => {
			const address = server.address();
			if (address === null || typeof address === 'string') {
				server.close(() => reject(new Error('Could not reserve a loopback port')));
				return;
			}
			const { port } = address;
			server.close((error) => (error ? reject(error) : resolve(port)));
		});
	});
}

function probeHealth(healthUrl: URL): Promise<boolean> {
	return new Promise((resolve) => {
		const request = http.get(
			{
				host: healthUrl.hostname,
				port: healthUrl.port,
				path: healthUrl.pathname,
				timeout: HEALTH_PROBE_TIMEOUT_MS,
			},
			(response) => {
				const status = response.statusCode ?? 0;
				response.resume();
				resolve(status >= 200 && status < 300);
			},
		);
		request.on('timeout', () => request.destroy());
		request.on('error', () => resolve(false));
	});
}

/** The production gate: real ports, real `/healthz` polling. */
export function createLoopbackGate(
	options: {
		attempts?: number;
		intervalMs?: number;
		probe?: (healthUrl: URL) => Promise<boolean>;
		sleep?: (ms: number) => Promise<void>;
		reserve?: () => Promise<number>;
	} = {},
): LoopbackGate {
	const attempts = Math.max(1, options.attempts ?? HEALTH_PROBE_ATTEMPTS);
	const intervalMs = options.intervalMs ?? HEALTH_PROBE_INTERVAL_MS;
	const probe = options.probe ?? probeHealth;
	const sleep =
		options.sleep ?? ((ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms)));
	const reserve = options.reserve ?? reserveLoopbackPort;

	return {
		reservePort: reserve,
		async isHealthy(url: URL): Promise<boolean> {
			return probe(new URL('healthz', url));
		},
		async waitUntilHealthy(url: URL): Promise<boolean> {
			const healthUrl = new URL('healthz', url);
			for (let attempt = 0; attempt < attempts; attempt += 1) {
				if (await probe(healthUrl)) {
					return true;
				}
				if (attempt < attempts - 1) {
					await sleep(intervalMs);
				}
			}
			return false;
		},
	};
}

/** Does this stderr text say the port we reserved was taken before we got there? */
export function isAddressInUse(stderr: string): boolean {
	const text = stderr.toLowerCase();
	return (
		text.includes('address already in use') ||
		text.includes('eaddrinuse') ||
		text.includes('errno 48') ||
		text.includes('errno 98') ||
		text.includes('only one usage of each socket address')
	);
}

function disposedError(): Error {
	return new Error('The kptn UI launcher was disposed.');
}

interface RunningServer {
	url: URL;
	child: SpawnedProcess;
	alive: boolean;
	/** Set once this child has been signalled, so it is signalled exactly once. */
	terminated?: boolean;
}

export class KptnServer {
	private running?: RunningServer;
	private starting?: Promise<URL>;
	private disposed = false;
	/**
	 * Every child this launcher has spawned and not yet reaped. `running` is
	 * only set once a child answers `/healthz`, so this is what makes an
	 * in-flight or already-replaced child terminable by `dispose()`.
	 */
	private readonly live = new Set<RunningServer>();

	constructor(
		private readonly spawner: Spawner = defaultSpawner,
		private readonly gate: LoopbackGate = createLoopbackGate(),
		private readonly log: LaunchLog = { appendLine: () => { } },
	) { }

	/**
	 * Start (or reuse) the shared UI server for *workspace*.
	 *
	 * A healthy child is reused across command invocations so that the run
	 * workers it supervises keep running: reopening the panel must never mean
	 * a second server, and never mean a restarted one.
	 */
	async start(
		workspace: WorkspaceLocation,
		pythonPath: string,
		options: { extraPythonPath?: string } = {},
	): Promise<URL> {
		this.refuseIfDisposed();
		if (this.starting) {
			return this.starting;
		}

		// The memoized promise covers the *whole* resolution -- the health probe
		// of a cached server included -- and is assigned before this method
		// awaits anything. If the probe were awaited out here, two concurrent
		// invocations that both find a wedged-but-alive server would both get
		// past the guard, both relaunch, and one of the two children would
		// never be recorded in `this.running` and so could never be terminated.
		this.starting = this.resolve(workspace, pythonPath, options).finally(() => {
			this.starting = undefined;
		});
		return this.starting;
	}

	/**
	 * Refuse to go any further once disposed.
	 *
	 * Called before every spawn and after every await that a `dispose()` could
	 * have interleaved with, so a launcher that is shutting down can neither
	 * create a child that outlives it nor hand back a URL for one it killed.
	 */
	private refuseIfDisposed(): void {
		if (this.disposed) {
			throw disposedError();
		}
	}

	/** Reuse a healthy cached server, or replace it. Only ever one at a time. */
	private async resolve(
		workspace: WorkspaceLocation,
		pythonPath: string,
		options: { extraPythonPath?: string },
	): Promise<URL> {
		const current = this.running;
		if (current?.alive) {
			const healthy = await this.gate.isHealthy(current.url);
			// Disposal can land while that probe is in flight. Returning here
			// would hand back a URL for a child `dispose()` has already killed;
			// falling through would spawn a replacement after the live set was
			// swept, leaving a child nothing will ever signal.
			this.refuseIfDisposed();
			if (healthy) {
				this.log.appendLine(`Reusing the running kptn UI server at ${current.url.toString()}`);
				return current.url;
			}
			this.log.appendLine('The running kptn UI server stopped answering; relaunching.');
			this.terminate(current);
		}

		return this.launch(workspace, pythonPath, options);
	}

	private async launch(
		workspace: WorkspaceLocation,
		pythonPath: string,
		options: { extraPythonPath?: string },
	): Promise<URL> {
		let lastFailure = 'the server never answered /healthz';

		for (let attempt = 1; attempt <= PORT_RESERVATION_ATTEMPTS; attempt += 1) {
			// Checked before *every* attempt, not just the first: disposal can
			// land during attempt N's health wait, and attempt N+1 would then
			// add to a live set that has already been swept.
			this.refuseIfDisposed();
			const port = await this.gate.reservePort();
			const url = new URL(`http://${LOOPBACK_HOST}:${port}/`);
			const argv = ['-m', 'kptn', 'ui', '--no-open', '--port', String(port)];

			this.log.appendLine(`Starting kptn UI: ${pythonPath} ${argv.join(' ')}`);
			const child = this.spawner(pythonPath, argv, {
				cwd: workspace.fsPath,
				env: {
					...process.env,
					PYTHONUNBUFFERED: '1',
					...(options.extraPythonPath ? { PYTHONPATH: options.extraPythonPath } : {}),
				},
				stdio: ['ignore', 'pipe', 'pipe'],
				// Deliberately attached: see SpawnOptions.detached.
				detached: false,
			});

			const state: RunningServer = { url, child, alive: true };
			this.live.add(state);
			let stderr = '';
			let spawnError: Error | undefined;

			child.stderr?.setEncoding?.('utf8');
			child.stderr?.on('data', (chunk) => {
				const text = chunk.toString();
				stderr += text;
				this.log.appendLine(`[kptn ui] ${text.trimEnd()}`);
			});
			child.stdout?.setEncoding?.('utf8');
			child.stdout?.on('data', (chunk) => this.log.appendLine(`[kptn ui] ${chunk.toString().trimEnd()}`));
			child.on('exit', (code, signal) => {
				state.alive = false;
				this.live.delete(state);
				this.log.appendLine(`kptn UI exited (${signal ? `signal ${signal}` : `code ${code}`}).`);
			});
			child.on('error', (error) => {
				state.alive = false;
				this.live.delete(state);
				spawnError = error;
				this.log.appendLine(`kptn UI failed to launch: ${error.message}`);
			});

			if (await this.gate.waitUntilHealthy(url)) {
				if (this.disposed) {
					// Disposed while this child was booting: it must not outlive us.
					this.terminate(state);
					throw disposedError();
				}
				this.running = state;
				return url;
			}

			this.terminate(state);

			if (spawnError) {
				throw new Error(`The kptn UI server failed to start: ${spawnError.message}`);
			}
			if (!isAddressInUse(stderr)) {
				const detail = stderr.trim() ? ` ${stderr.trim().split('\n').slice(-3).join(' ')}` : '';
				throw new Error(
					`The kptn UI server failed to start on ${url.toString()}.${detail}`,
				);
			}

			lastFailure = `port ${port} was taken between reservation and launch`;
			this.log.appendLine(`Retrying on a new port: ${lastFailure}.`);
		}

		throw new Error(
			`The kptn UI server failed to start after ${PORT_RESERVATION_ATTEMPTS} port reservations (${lastFailure}).`,
		);
	}

	/**
	 * Stop the UI server -- and nothing else.
	 *
	 * The child is signalled through its own handle with SIGTERM. No
	 * process-group signal (`process.kill(-pid)`) is ever sent, because the
	 * run workers the server supervises are detached on purpose so they
	 * survive editor and server restarts.
	 */
	dispose(): void {
		this.disposed = true;
		// Every spawned child, not just the one that became `running`: a child
		// still booting, or one already replaced, is ours to clean up too.
		for (const state of [...this.live]) {
			this.terminate(state);
		}
		this.running = undefined;
	}

	private terminate(state: RunningServer): void {
		state.alive = false;
		this.live.delete(state);
		if (this.running === state) {
			this.running = undefined;
		}
		if (state.terminated) {
			return;
		}
		state.terminated = true;
		try {
			state.child.kill('SIGTERM');
		} catch (error) {
			this.log.appendLine(
				`Could not terminate the kptn UI server: ${error instanceof Error ? error.message : String(error)}`,
			);
		}
	}
}

/* -------------------------------------------------------------------------- */
/*  Webview bridge authorization                                              */
/* -------------------------------------------------------------------------- */

export interface OpenSourceApproval {
	ok: true;
	fsPath: string;
	zeroBasedLine: number;
}

export type BridgeRejectionReason =
	| 'not-open-source'
	| 'bad-token'
	| 'invalid-path'
	| 'outside-workspace';

export interface OpenSourceRejection {
	ok: false;
	reason: BridgeRejectionReason;
}

export type OpenSourceDecision = OpenSourceApproval | OpenSourceRejection;

/** Mint the short-lived token that is injected into the hosted page's URL. */
export function createBridgeToken(): string {
	return crypto.randomBytes(24).toString('hex');
}

function tokensMatch(expected: string, provided: unknown): boolean {
	if (typeof provided !== 'string' || expected.length === 0) {
		return false;
	}
	// `.length` counts UTF-16 code units, `timingSafeEqual` compares bytes and
	// throws when they differ, so the guard has to be on byte length: a
	// multibyte string of the same `.length` (`'é'.repeat(24)`) is 96 bytes.
	const mine = Buffer.from(expected, 'utf8');
	const theirs = Buffer.from(provided, 'utf8');
	if (mine.length !== theirs.length) {
		return false;
	}
	try {
		return crypto.timingSafeEqual(mine, theirs);
	} catch {
		// The security decision function never throws; an unusable comparison
		// is a rejection.
		return false;
	}
}

/** Convert a one-based editor line into a zero-based `vscode.Position` line. */
export function zeroBasedLine(line: unknown): number {
	if (typeof line !== 'number' || !Number.isFinite(line)) {
		return 0;
	}
	return Math.max(0, Math.floor(line) - 1);
}

/**
 * Resolve *requested* against *workspaceRoot*, following symlinks, and return
 * it only if it really lands inside the workspace.
 *
 * `path.resolve` alone is not enough: it normalizes `..` but says nothing
 * about a symlink inside the workspace that points out of it, so both the
 * root and the candidate are realpath-ed before they are compared.
 */
export function resolveInsideWorkspace(
	workspaceRoot: string,
	requested: unknown,
	probe: PathProbe = fs,
): string | undefined {
	if (typeof requested !== 'string' || requested.trim() === '') {
		return undefined;
	}
	if (requested.indexOf('\0') !== -1) {
		return undefined;
	}

	const realRoot = realOrRefuse(workspaceRoot, probe);
	if (realRoot === undefined) {
		return undefined;
	}
	const absolute = path.resolve(realRoot, requested);
	const candidate = realOrRefuse(absolute, probe);
	if (candidate === undefined) {
		return undefined;
	}

	const relative = path.relative(realRoot, candidate);
	if (relative === '' || relative.startsWith('..') || path.isAbsolute(relative)) {
		return undefined;
	}
	return candidate;
}

/**
 * Realpath *target*, or -- when it does not exist yet -- the realpath of its
 * nearest existing ancestor with the missing tail appended.
 *
 * Refuses rather than synthesizing whenever the missing component is itself a
 * symlink: a dangling link inside the workspace resolves to nothing, and a
 * path this function cannot verify must not be handed back as if it had been.
 * That closes the TOCTOU seam where the link is repointed outside the
 * workspace between this check and the open.
 */
function realOrRefuse(target: string, probe: PathProbe): string | undefined {
	try {
		return probe.realpathSync(target);
	} catch {
		// Falls through: either the path does not exist, or it is a link we
		// cannot follow.
	}

	try {
		if (probe.lstatSync(target).isSymbolicLink()) {
			return undefined;
		}
	} catch {
		// Nothing at this path at all: a not-yet-created leaf is fine.
	}

	const parent = path.dirname(target);
	if (parent === target) {
		// Not even the filesystem root resolved; there is nothing to trust.
		return undefined;
	}
	const realParent = realOrRefuse(parent, probe);
	if (realParent === undefined) {
		return undefined;
	}
	return path.join(realParent, path.basename(target));
}

/**
 * Decide whether an `openSource` webview message may open a document.
 *
 * Both guards must pass: the message has to carry the bridge token minted for
 * this panel *and* name a path inside the workspace. Either one alone is not
 * enough -- the token lives in a page URL, so it is not a secret strong enough
 * to authorize opening arbitrary files.
 */
export function authorizeOpenSource(
	message: unknown,
	options: {
		token: string;
		workspaceRoot: string;
		probe?: PathProbe;
	},
): OpenSourceDecision {
	if (typeof message !== 'object' || message === null) {
		return { ok: false, reason: 'not-open-source' };
	}
	const request = message as Record<string, unknown>;
	if (request.type !== 'openSource') {
		return { ok: false, reason: 'not-open-source' };
	}
	if (!tokensMatch(options.token, request.token)) {
		return { ok: false, reason: 'bad-token' };
	}
	if (typeof request.path !== 'string' || request.path.trim() === '') {
		return { ok: false, reason: 'invalid-path' };
	}

	const fsPath = resolveInsideWorkspace(options.workspaceRoot, request.path, options.probe);
	if (!fsPath) {
		return { ok: false, reason: 'outside-workspace' };
	}

	return { ok: true, fsPath, zeroBasedLine: zeroBasedLine(request.line) };
}

/* -------------------------------------------------------------------------- */
/*  Webview host page                                                         */
/* -------------------------------------------------------------------------- */

export function withBridgeToken(pageUrl: string, token: string): string {
	const url = new URL(pageUrl);
	url.searchParams.set(BRIDGE_TOKEN_PARAM, token);
	return url.toString();
}

/**
 * The host page: one iframe, one relay script, no assets from anywhere else.
 *
 * The CSP names the single loopback (or forwarded) origin as the only frame
 * source, blocks everything else outright, and allows exactly one nonce'd
 * inline script -- the relay that forwards the page's messages to the
 * extension host.
 */
export function buildHostHtml(pageUrl: string, token: string): string {
	const nonce = createBridgeToken();
	const origin = new URL(pageUrl).origin;
	const frameSrc = escapeHtml(origin);
	const src = escapeHtml(pageUrl);

	return `<!DOCTYPE html>
<html lang="en">
<head>
	<meta charset="UTF-8">
	<meta http-equiv="Content-Security-Policy" content="default-src 'none'; frame-src ${frameSrc}; style-src 'unsafe-inline'; script-src 'nonce-${nonce}';">
	<meta name="viewport" content="width=device-width, initial-scale=1.0">
	<title>kptn Pipeline</title>
	<style>
		html, body { margin: 0; padding: 0; height: 100%; overflow: hidden; }
		iframe { border: 0; width: 100%; height: 100vh; display: block; }
	</style>
</head>
<body>
	<iframe id="kptn-ui" src="${src}" title="kptn Pipeline UI"></iframe>
	<script nonce="${nonce}">
		(function () {
			const vscodeApi = acquireVsCodeApi();
			const origin = ${JSON.stringify(origin)};
			const token = ${JSON.stringify(token)};
			window.addEventListener('message', function (event) {
				if (event.origin !== origin) { return; }
				const data = event.data;
				if (!data || data.type !== 'openSource') { return; }
				vscodeApi.postMessage({
					type: 'openSource',
					path: data.path,
					line: data.line,
					token: token,
				});
			});
		})();
	</script>
</body>
</html>`;
}

function escapeHtml(value: string): string {
	return value
		.replace(/&/g, '&amp;')
		.replace(/</g, '&lt;')
		.replace(/>/g, '&gt;')
		.replace(/"/g, '&quot;')
		.replace(/'/g, '&#39;');
}

