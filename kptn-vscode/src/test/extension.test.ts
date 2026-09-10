import * as assert from 'assert';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import {
	KptnServer,
	LoopbackGate,
	SpawnOptions,
	SpawnedProcess,
	Spawner,
	authorizeOpenSource,
	buildHostHtml,
	createBridgeToken,
	isAddressInUse,
	reserveLoopbackPort,
	withBridgeToken,
	zeroBasedLine,
} from '../server';

/**
 * A stand-in for the `kptn ui` child process.
 *
 * The tests never touch a real interpreter, a real socket or a real clock, so
 * every assertion below runs in milliseconds and none of them can pass because
 * something on the developer's machine happened to answer.
 */
class FakeProcess implements SpawnedProcess {
	public argv: string[] = [];
	public command = '';
	public options?: SpawnOptions;
	public readonly killSignals: (string | undefined)[] = [];
	public exited = false;
	public readonly pid = 4242;

	private exitListeners: ((code: number | null, signal: string | null) => void)[] = [];
	private errorListeners: ((error: Error) => void)[] = [];
	private stderrListeners: ((chunk: string | Buffer) => void)[] = [];

	constructor(private readonly stderrText: string) { }

	public readonly stderr = {
		setEncoding: (): void => { },
		on: (_event: 'data', listener: (chunk: string | Buffer) => void): void => {
			this.stderrListeners.push(listener);
			if (this.stderrText) {
				listener(this.stderrText);
			}
		},
	};

	on(event: 'exit', listener: (code: number | null, signal: string | null) => void): void;
	on(event: 'error', listener: (error: Error) => void): void;
	on(event: 'exit' | 'error', listener: (...args: never[]) => void): void {
		if (event === 'exit') {
			this.exitListeners.push(listener as unknown as (code: number | null, signal: string | null) => void);
		} else {
			this.errorListeners.push(listener as unknown as (error: Error) => void);
		}
	}

	kill(signal?: string): boolean {
		this.killSignals.push(signal);
		return true;
	}

	emitExit(code: number | null): void {
		this.exited = true;
		for (const listener of this.exitListeners) {
			listener(code, null);
		}
	}

	emitError(error: Error): void {
		for (const listener of this.errorListeners) {
			listener(error);
		}
	}
}

interface SpawnRecord {
	command: string;
	argv: string[];
	options: SpawnOptions;
}

/**
 * Hands out the supplied processes in order (repeating the last one), while
 * recording every launch so a test can assert how many spawns happened.
 */
function fakeSpawner(...processes: FakeProcess[]): Spawner & { calls: SpawnRecord[] } {
	const calls: SpawnRecord[] = [];
	const spawner = ((command: string, argv: string[], options: SpawnOptions): SpawnedProcess => {
		const chosen = processes[Math.min(calls.length, processes.length - 1)];
		chosen.command = command;
		chosen.argv = argv;
		chosen.options = options;
		calls.push({ command, argv, options });
		return chosen;
	}) as Spawner & { calls: SpawnRecord[] };
	spawner.calls = calls;
	return spawner;
}

const ADDRESS_IN_USE_STDERR = '[Errno 48] error while attempting to bind on address: address already in use';

/**
 * A loopback gate that hands out a known port and reports the server healthy.
 *
 * `fakeHealth.port` is the port the launcher is expected to pass through to the
 * CLI, so the argv assertion is checking a value the test chose rather than one
 * the implementation invented.
 */
const fakeHealth = Object.assign(
	function fakeHealthFactory(): LoopbackGate {
		return {
			reservePort: async () => fakeHealth.port,
			isHealthy: async () => true,
			waitUntilHealthy: async () => true,
		};
	},
	{ port: 51987 },
);

/** A gate that reserves a fresh port each time and never reports health. */
function unhealthyGate(firstPort = 40100): LoopbackGate & { reserved: number[] } {
	const reserved: number[] = [];
	return {
		reserved,
		reservePort: async () => {
			const port = firstPort + reserved.length;
			reserved.push(port);
			return port;
		},
		isHealthy: async () => false,
		waitUntilHealthy: async () => false,
	};
}

/** A gate that stays unhealthy until `healthyOnAttempt` reservations have happened. */
function flakyGate(healthyOnAttempt: number, firstPort = 40200): LoopbackGate & { reserved: number[] } {
	const reserved: number[] = [];
	return {
		reserved,
		reservePort: async () => {
			const port = firstPort + reserved.length;
			reserved.push(port);
			return port;
		},
		isHealthy: async () => false,
		waitUntilHealthy: async () => reserved.length >= healthyOnAttempt,
	};
}

const workspaceUri = { fsPath: path.join(path.sep, 'work', 'project') };

suite('shared UI launcher', () => {
	test('starts the shared UI with the selected interpreter', async () => {
		const process = new FakeProcess('');
		const server = new KptnServer(fakeSpawner(process), fakeHealth());
		const url = await server.start(workspaceUri, '/venv/bin/python');
		assert.strictEqual(url.hostname, '127.0.0.1');
		assert.deepStrictEqual(process.argv, [
			'-m', 'kptn', 'ui', '--no-open', '--port', String(fakeHealth.port),
		]);
	});

	test('runs the interpreter it was given, from the workspace directory', async () => {
		const process = new FakeProcess('');
		const server = new KptnServer(fakeSpawner(process), fakeHealth());
		await server.start(workspaceUri, '/venv/bin/python');
		assert.strictEqual(process.command, '/venv/bin/python');
		assert.strictEqual(process.options?.cwd, workspaceUri.fsPath);
	});

	test('serves the started port on the returned url', async () => {
		const process = new FakeProcess('');
		const server = new KptnServer(fakeSpawner(process), fakeHealth());
		const url = await server.start(workspaceUri, '/venv/bin/python');
		assert.strictEqual(url.port, String(fakeHealth.port));
		assert.strictEqual(url.protocol, 'http:');
	});

	test('reuses a healthy server process across command invocations', async () => {
		const process = new FakeProcess('');
		const spawner = fakeSpawner(process);
		const server = new KptnServer(spawner, fakeHealth());

		const first = await server.start(workspaceUri, '/venv/bin/python');
		const second = await server.start(workspaceUri, '/venv/bin/python');

		assert.strictEqual(spawner.calls.length, 1, 'a second invocation must not spawn a second server');
		assert.strictEqual(second.toString(), first.toString());
	});

	test('reuses one process for concurrent invocations', async () => {
		const process = new FakeProcess('');
		const spawner = fakeSpawner(process);
		const server = new KptnServer(spawner, fakeHealth());

		const [first, second] = await Promise.all([
			server.start(workspaceUri, '/venv/bin/python'),
			server.start(workspaceUri, '/venv/bin/python'),
		]);

		assert.strictEqual(spawner.calls.length, 1, 'overlapping invocations must share one launch');
		assert.strictEqual(second.toString(), first.toString());
	});

	test('relaunches after the cached process exits', async () => {
		const first = new FakeProcess('');
		const second = new FakeProcess('');
		const spawner = fakeSpawner(first, second);
		const server = new KptnServer(spawner, fakeHealth());

		await server.start(workspaceUri, '/venv/bin/python');
		first.emitExit(1);
		await server.start(workspaceUri, '/venv/bin/python');

		assert.strictEqual(spawner.calls.length, 2, 'a dead server must be replaced, not reused');
	});

	test('does not race into extra servers when the cached server is wedged', async () => {
		// The regression this pins: the in-flight guard used to be checked
		// before the cached server's health probe was awaited, so two
		// concurrent invocations both got past it, both relaunched, and the
		// loser of the `starting` assignment was never recorded -- an orphan
		// holding a port that `dispose()` could not reach.
		const children = [new FakeProcess(''), new FakeProcess(''), new FakeProcess('')];
		const spawner = fakeSpawner(...children);
		let cachedIsHealthy = true;
		let reserved = 0;
		const gate: LoopbackGate = {
			reservePort: async () => 50000 + reserved++,
			isHealthy: async () => cachedIsHealthy,
			waitUntilHealthy: async () => true,
		};
		const server = new KptnServer(spawner, gate);

		await server.start(workspaceUri, '/venv/bin/python');
		assert.strictEqual(spawner.calls.length, 1);

		cachedIsHealthy = false;
		const [first, second] = await Promise.all([
			server.start(workspaceUri, '/venv/bin/python'),
			server.start(workspaceUri, '/venv/bin/python'),
		]);

		assert.strictEqual(
			spawner.calls.length,
			2,
			'a wedged cached server must be replaced exactly once, however many callers noticed',
		);
		assert.strictEqual(first.toString(), second.toString(), 'both callers must get the same server');

		server.dispose();
		for (let index = 0; index < spawner.calls.length; index += 1) {
			assert.ok(
				children[index].killSignals.includes('SIGTERM'),
				`child ${index} was spawned but never terminated -- it would outlive the extension`,
			);
		}
	});

	test('terminates a child that was still booting when disposal happened', async () => {
		const child = new FakeProcess('');
		let release: ((healthy: boolean) => void) | undefined;
		const gate: LoopbackGate = {
			reservePort: async () => 50100,
			isHealthy: async () => false,
			waitUntilHealthy: () =>
				new Promise<boolean>((resolve) => {
					release = resolve;
				}),
		};
		const server = new KptnServer(fakeSpawner(child), gate);

		const started = server.start(workspaceUri, '/venv/bin/python');
		await new Promise<void>((resolve) => setImmediate(resolve));
		assert.strictEqual(child.killSignals.length, 0, 'the child must still be booting');

		server.dispose();
		// Immediately, without waiting for the health probe: a `kptn ui` that
		// hangs before it ever answers would otherwise be left running with no
		// handle to it, since it is not yet recorded as the running server.
		assert.deepStrictEqual(
			child.killSignals,
			['SIGTERM'],
			'disposal must terminate a booting child without waiting on its health probe',
		);

		release?.(true);
		await assert.rejects(() => started, /disposed/i);
		assert.deepStrictEqual(
			child.killSignals,
			['SIGTERM'],
			'a child still booting at disposal must be terminated exactly once',
		);
	});

	test('relaunches when the cached server stops answering', async () => {
		const first = new FakeProcess('');
		const second = new FakeProcess('');
		const spawner = fakeSpawner(first, second);
		let cachedIsHealthy = true;
		const gate: LoopbackGate = {
			reservePort: async () => fakeHealth.port,
			isHealthy: async () => cachedIsHealthy,
			waitUntilHealthy: async () => true,
		};
		const server = new KptnServer(spawner, gate);

		await server.start(workspaceUri, '/venv/bin/python');
		cachedIsHealthy = false;
		await server.start(workspaceUri, '/venv/bin/python');

		assert.strictEqual(spawner.calls.length, 2, 'a wedged server must be replaced, not reused');
		assert.deepStrictEqual(first.killSignals, ['SIGTERM'], 'the wedged server must be terminated');
	});

	test('fails the launch when the server never becomes healthy', async () => {
		const process = new FakeProcess('Traceback: ModuleNotFoundError: No module named kptn');
		const spawner = fakeSpawner(process);
		const server = new KptnServer(spawner, unhealthyGate());

		await assert.rejects(
			() => server.start(workspaceUri, '/venv/bin/python'),
			/failed to start/i,
		);
		assert.strictEqual(spawner.calls.length, 1, 'a plain startup failure must not be retried');
		assert.ok(process.killSignals.length > 0, 'the failed child must be terminated');
	});

	test('retries on a newly reserved port when the reserved port was taken', async () => {
		const first = new FakeProcess(ADDRESS_IN_USE_STDERR);
		const second = new FakeProcess('');
		const spawner = fakeSpawner(first, second);
		const gate = flakyGate(2);
		const server = new KptnServer(spawner, gate);

		const url = await server.start(workspaceUri, '/venv/bin/python');

		assert.strictEqual(spawner.calls.length, 2, 'an address-in-use loss must be retried');
		assert.strictEqual(gate.reserved.length, 2, 'the retry must reserve a fresh port');
		assert.notStrictEqual(gate.reserved[0], gate.reserved[1]);
		assert.strictEqual(url.port, String(gate.reserved[1]));
		assert.deepStrictEqual(second.argv.slice(-2), ['--port', String(gate.reserved[1])]);
	});

	test('gives up after three port reservations', async () => {
		const process = new FakeProcess(ADDRESS_IN_USE_STDERR);
		const spawner = fakeSpawner(process);
		const gate = unhealthyGate();
		const server = new KptnServer(spawner, gate);

		await assert.rejects(
			() => server.start(workspaceUri, '/venv/bin/python'),
			/failed to start/i,
		);
		assert.strictEqual(spawner.calls.length, 3, 'the retry budget is three attempts');
		assert.strictEqual(gate.reserved.length, 3);
	});

	test('terminates only the ui child on disposal', async () => {
		const process = new FakeProcess('');
		const server = new KptnServer(fakeSpawner(process), fakeHealth());
		await server.start(workspaceUri, '/venv/bin/python');

		server.dispose();

		assert.deepStrictEqual(process.killSignals, ['SIGTERM']);
		for (const signal of process.killSignals) {
			assert.notStrictEqual(signal, 'SIGKILL');
		}
	});

	test('never detaches the ui child, so termination cannot reach a run worker group', async () => {
		const process = new FakeProcess('');
		const server = new KptnServer(fakeSpawner(process), fakeHealth());
		await server.start(workspaceUri, '/venv/bin/python');

		assert.notStrictEqual(process.options?.detached, true);
		server.dispose();
		// Disposal signals the child handle only. A negative pid (process-group
		// kill) would reach the deliberately detached run workers, so the child
		// must never be placed in its own group in the first place.
		assert.deepStrictEqual(process.killSignals, ['SIGTERM']);
	});

	test('a disposed launcher refuses to start again', async () => {
		const process = new FakeProcess('');
		const spawner = fakeSpawner(process);
		const server = new KptnServer(spawner, fakeHealth());
		server.dispose();

		await assert.rejects(() => server.start(workspaceUri, '/venv/bin/python'), /disposed/i);
		assert.strictEqual(spawner.calls.length, 0);
	});
});

suite('shared UI bridge authorization', () => {
	const token = 'a'.repeat(32);
	let root: string;
	let outside: string;
	let linkedRoot: string;

	suiteSetup(() => {
		const base = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'kptn-bridge-')));
		root = path.join(base, 'workspace');
		outside = path.join(base, 'elsewhere');
		fs.mkdirSync(path.join(root, 'models'), { recursive: true });
		fs.mkdirSync(outside, { recursive: true });
		fs.writeFileSync(path.join(root, 'models', 'orders.sql'), 'select 1');
		fs.writeFileSync(path.join(outside, 'secrets.env'), 'TOKEN=1');
		linkedRoot = path.join(base, 'workspace-link');
		try {
			fs.symlinkSync(outside, path.join(root, 'escape'), 'dir');
			fs.symlinkSync(root, linkedRoot, 'dir');
		} catch {
			// A platform without symlink permission simply skips those assertions.
		}
	});

	const approve = (message: unknown) => authorizeOpenSource(message, { token, workspaceRoot: root });

	test('opens a source file inside the workspace', () => {
		const result = approve({ type: 'openSource', path: 'models/orders.sql', token });
		assert.strictEqual(result.ok, true);
		assert.ok(result.ok && result.fsPath === path.join(root, 'models', 'orders.sql'));
	});

	test('rejects a path that traverses out of the workspace', () => {
		const result = approve({ type: 'openSource', path: '../elsewhere/secrets.env', token });
		assert.strictEqual(result.ok, false);
		assert.ok(!result.ok && result.reason === 'outside-workspace');
	});

	test('rejects a deeply nested traversal that lands outside the workspace', () => {
		const result = approve({
			type: 'openSource',
			path: 'models/../../elsewhere/secrets.env',
			token,
		});
		assert.strictEqual(result.ok, false);
	});

	test('rejects an absolute path outside the workspace', () => {
		const result = approve({ type: 'openSource', path: path.join(outside, 'secrets.env'), token });
		assert.strictEqual(result.ok, false);
		assert.ok(!result.ok && result.reason === 'outside-workspace');
	});

	test('rejects an absolute system path outside the workspace', () => {
		const result = approve({ type: 'openSource', path: '/etc/passwd', token });
		assert.strictEqual(result.ok, false);
	});

	test('rejects a symlink inside the workspace that escapes it', function () {
		if (!fs.existsSync(path.join(root, 'escape'))) {
			this.skip();
		}
		const result = approve({ type: 'openSource', path: 'escape/secrets.env', token });
		assert.strictEqual(result.ok, false);
		assert.ok(!result.ok && result.reason === 'outside-workspace');
	});

	test('accepts a file inside a workspace whose root is itself a symlink', function () {
		if (!fs.existsSync(linkedRoot)) {
			this.skip();
		}
		const result = authorizeOpenSource(
			{ type: 'openSource', path: 'models/orders.sql', token },
			{ token, workspaceRoot: linkedRoot },
		);
		assert.strictEqual(result.ok, true, 'a symlinked workspace root must not make its own files unreachable');
		assert.strictEqual(result.ok && result.fsPath, path.join(root, 'models', 'orders.sql'));
	});

	test('rejects the workspace root itself', () => {
		const result = approve({ type: 'openSource', path: '.', token });
		assert.strictEqual(result.ok, false);
	});

	test('rejects an empty or non-string path', () => {
		assert.strictEqual(approve({ type: 'openSource', path: '', token }).ok, false);
		assert.strictEqual(approve({ type: 'openSource', path: 42, token }).ok, false);
		assert.strictEqual(approve({ type: 'openSource', token }).ok, false);
	});

	test('rejects a multibyte token of the same string length without throwing', () => {
		// 'é'.repeat(n) has the same .length as an n-character ASCII token but
		// twice the bytes; comparing byte-unequal buffers throws, and the
		// security decision function must reject rather than throw.
		const multibyte = '\u00e9'.repeat(token.length);
		assert.strictEqual(multibyte.length, token.length);
		assert.notStrictEqual(Buffer.byteLength(multibyte, 'utf8'), Buffer.byteLength(token, 'utf8'));
		const result = approve({ type: 'openSource', path: 'models/orders.sql', token: multibyte });
		assert.strictEqual(result.ok, false);
		assert.ok(!result.ok && result.reason === 'bad-token');
	});

	test('rejects a dangling symlink inside the workspace', function () {
		const link = path.join(root, 'dangling');
		if (!fs.existsSync(path.join(root, 'escape'))) {
			this.skip();
		}
		if (!fs.lstatSync(link, { throwIfNoEntry: false })) {
			fs.symlinkSync(path.join(root, 'models', 'does-not-exist.sql'), link);
		}
		const result = approve({ type: 'openSource', path: 'dangling', token });
		assert.strictEqual(
			result.ok,
			false,
			'a link that resolves to nothing must not be synthesized into a path',
		);
	});

	test('still accepts a file that does not exist yet under a real directory', () => {
		const result = approve({ type: 'openSource', path: 'models/not-created-yet.sql', token });
		assert.strictEqual(result.ok, true, 'a plain missing leaf is not a symlink and stays resolvable');
	});

	test('rejects a mismatched bridge token', () => {
		const result = approve({ type: 'openSource', path: 'models/orders.sql', token: 'b'.repeat(32) });
		assert.strictEqual(result.ok, false);
		assert.ok(!result.ok && result.reason === 'bad-token');
	});

	test('rejects a missing bridge token', () => {
		const result = approve({ type: 'openSource', path: 'models/orders.sql' });
		assert.strictEqual(result.ok, false);
		assert.ok(!result.ok && result.reason === 'bad-token');
	});

	test('rejects a token that is only a prefix of the real one', () => {
		const result = approve({ type: 'openSource', path: 'models/orders.sql', token: token.slice(0, 8) });
		assert.strictEqual(result.ok, false);
		assert.ok(!result.ok && result.reason === 'bad-token');
	});

	test('requires both checks: a good token cannot open an outside path', () => {
		assert.strictEqual(approve({ type: 'openSource', path: '/etc/passwd', token }).ok, false);
	});

	test('requires both checks: an inside path cannot be opened with a bad token', () => {
		assert.strictEqual(
			approve({ type: 'openSource', path: 'models/orders.sql', token: 'nope' }).ok,
			false,
		);
	});

	test('rejects a message that is not an openSource request', () => {
		assert.strictEqual(approve({ type: 'runPipeline', path: 'models/orders.sql', token }).ok, false);
		assert.strictEqual(approve(null).ok, false);
		assert.strictEqual(approve('openSource').ok, false);
	});

	test('navigates to the requested one-based line', () => {
		const result = approve({ type: 'openSource', path: 'models/orders.sql', line: 42, token });
		assert.ok(result.ok);
		assert.strictEqual(result.ok && result.zeroBasedLine, 41);
	});

	test('maps line one to the first line of the document', () => {
		const result = approve({ type: 'openSource', path: 'models/orders.sql', line: 1, token });
		assert.strictEqual(result.ok && result.zeroBasedLine, 0);
	});

	test('treats a missing or out-of-range line as the first line', () => {
		assert.strictEqual(zeroBasedLine(undefined), 0);
		assert.strictEqual(zeroBasedLine(0), 0);
		assert.strictEqual(zeroBasedLine(-5), 0);
		assert.strictEqual(zeroBasedLine(Number.NaN), 0);
		assert.strictEqual(zeroBasedLine('7' as unknown as number), 0);
	});

	test('does not shift lines beyond the first by more than one', () => {
		assert.strictEqual(zeroBasedLine(2), 1);
		assert.strictEqual(zeroBasedLine(1000), 999);
	});
});

suite('shared UI host page', () => {
	const token = 'c'.repeat(48);
	const pageUrl = withBridgeToken('http://127.0.0.1:51987/', token);

	test('injects the bridge token into the hosted page url', () => {
		assert.strictEqual(new URL(pageUrl).searchParams.get('bridge_token'), token);
	});

	test('keeps the loopback origin when injecting the token', () => {
		assert.strictEqual(new URL(pageUrl).origin, 'http://127.0.0.1:51987');
	});

	test('preserves an existing query string', () => {
		const withPath = withBridgeToken('http://127.0.0.1:51987/plan?profile=nightly', token);
		const parsed = new URL(withPath);
		assert.strictEqual(parsed.searchParams.get('profile'), 'nightly');
		assert.strictEqual(parsed.searchParams.get('bridge_token'), token);
		assert.strictEqual(parsed.pathname, '/plan');
	});

	test('frames the complete loopback url', () => {
		const html = buildHostHtml(pageUrl, token);
		assert.ok(html.includes(`src="${pageUrl.replace(/&/g, '&amp;')}"`), html);
	});

	test('the csp allows only the one loopback origin as a frame source', () => {
		const html = buildHostHtml(pageUrl, token);
		const csp = /content="([^"]+)"/.exec(html)?.[1] ?? '';
		assert.ok(csp.includes("default-src 'none'"), csp);
		assert.ok(csp.includes('frame-src http://127.0.0.1:51987;'), csp);
		assert.ok(!csp.includes('*'), `the CSP must not use a wildcard source: ${csp}`);
		assert.ok(!/https?:\/\/(?!127\.0\.0\.1:51987)[a-z0-9.-]/i.test(csp), csp);
	});

	test('loads no script or style from anywhere but the page itself', () => {
		const html = buildHostHtml(pageUrl, token);
		assert.ok(!/<script[^>]+src=/.test(html), 'no external script may be loaded');
		assert.ok(!/<link[^>]+href=/.test(html), 'no external stylesheet may be loaded');
		assert.ok(!html.includes('cdn.'), 'no CDN asset may be referenced');
	});

	test('the relay script runs under a nonce the csp names', () => {
		const html = buildHostHtml(pageUrl, token);
		const nonce = /script-src 'nonce-([^']+)'/.exec(html)?.[1];
		assert.ok(nonce, 'the CSP must name a script nonce');
		assert.ok(html.includes(`<script nonce="${nonce}">`), 'the inline script must carry that nonce');
	});

	test('the relay drops messages from any other origin', () => {
		const html = buildHostHtml(pageUrl, token);
		assert.ok(
			html.includes("if (event.origin !== origin) { return; }"),
			'the relay must check the message origin',
		);
	});

	test('mints a distinct token per page', () => {
		const first = createBridgeToken();
		const second = createBridgeToken();
		assert.notStrictEqual(first, second);
		assert.ok(first.length >= 32, `a bridge token must be long enough to be unguessable: ${first}`);
	});
});

suite('shared UI loopback plumbing', () => {
	test('recognizes the address-in-use failures the retry exists for', () => {
		assert.strictEqual(isAddressInUse('[Errno 48] address already in use'), true);
		assert.strictEqual(isAddressInUse('OSError: [Errno 98] Address already in use'), true);
		assert.strictEqual(isAddressInUse('Error: listen EADDRINUSE'), true);
		assert.strictEqual(isAddressInUse('ModuleNotFoundError: No module named kptn'), false);
		assert.strictEqual(isAddressInUse(''), false);
	});

	test('reserves a usable loopback port and releases it', async () => {
		const port = await reserveLoopbackPort();
		assert.ok(port > 1024 && port < 65536, `unexpected port ${port}`);
		const again = await reserveLoopbackPort();
		assert.ok(again > 1024, `unexpected port ${again}`);
	});
});

suite('extension contributions', () => {
	const manifestPath = path.join(__dirname, '..', '..', 'package.json');
	const manifest = JSON.parse(fs.readFileSync(manifestPath, 'utf8')) as {
		activationEvents: string[];
		contributes: Record<string, unknown> & { commands: { command: string; title: string }[] };
	};

	test('activates on the single open-UI command', () => {
		assert.deepStrictEqual(manifest.activationEvents, ['onCommand:kptn.openUI']);
	});

	test('contributes exactly one command', () => {
		assert.deepStrictEqual(manifest.contributes.commands, [
			{ command: 'kptn.openUI', title: 'kptn: Open Pipeline UI' },
		]);
	});

	test('contributes no views, view containers or menus', () => {
		assert.deepStrictEqual(Object.keys(manifest.contributes), ['commands']);
	});

	test('no longer ships the JSON-RPC backend shim', () => {
		assert.strictEqual(fs.existsSync(path.join(__dirname, '..', '..', 'backend.py')), false);
	});
});
