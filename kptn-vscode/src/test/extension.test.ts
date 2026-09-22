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
	INSTALL_MARKER_SEGMENTS,
	consumeInstallMarker,
	decideStartupAction,
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

/**
 * A gate whose health answers are controlled by the test.
 *
 * `settleIsHealthy` releases a pending `isHealthy`, `settleWait` a pending
 * `waitUntilHealthy`, so a test can land `dispose()` in the exact window
 * between a probe starting and answering.
 */
function deferredGate(firstPort = 40300): LoopbackGate & {
	reserved: number[];
	settleIsHealthy(healthy: boolean): void;
	settleWait(healthy: boolean): void;
	waitStarted(): boolean;
} {
	const reserved: number[] = [];
	let releaseIsHealthy: ((healthy: boolean) => void) | undefined;
	let releaseWait: ((healthy: boolean) => void) | undefined;
	return {
		reserved,
		reservePort: async () => {
			const port = firstPort + reserved.length;
			reserved.push(port);
			return port;
		},
		isHealthy: () =>
			new Promise<boolean>((resolve) => {
				releaseIsHealthy = resolve;
			}),
		waitUntilHealthy: () =>
			new Promise<boolean>((resolve) => {
				releaseWait = resolve;
			}),
		settleIsHealthy: (healthy: boolean) => releaseIsHealthy?.(healthy),
		settleWait: (healthy: boolean) => releaseWait?.(healthy),
		waitStarted: () => releaseWait !== undefined,
	};
}

/**
 * Capture how a start settles without leaving a floating rejection, so a test
 * can assert the spawn count *first* and report the count -- not a mocha
 * timeout -- as the failure when a post-disposal child hangs forever.
 */
function outcomeOf(started: Promise<URL>): Promise<string> {
	return started.then(
		(url) => `resolved:${url.toString()}`,
		(error: unknown) => `rejected:${error instanceof Error ? error.message : String(error)}`,
	);
}

/**
 * A gate that can hold **any one** await in the start path.
 *
 * `hold` names a stage and an occurrence -- `reservePort:1`, `wait:2`,
 * `isHealthy:1` -- and that call returns a promise the test releases by hand.
 * Holds are ignored until `arm()` is called, so a scenario can seed a running
 * server first and still label stages by their position in the start under
 * test. Any health wait whose answer is not scripted **stalls forever**, which
 * is what a `kptn ui` child that never answers `/healthz` really does: an
 * unterminated child in that state is an orphan nothing will ever signal.
 */
function scriptedGate(options: {
	hold: string;
	waits?: boolean[];
	cachedHealthy?: boolean;
	firstPort?: number;
}): LoopbackGate & { reserved: number[]; reachedHold(): boolean; release(): void } {
	const reserved: number[] = [];
	const counts: Record<string, number> = { reservePort: 0, isHealthy: 0, wait: 0 };
	let armed = false;
	let reached = false;
	let release: (() => void) | undefined;
	const firstPort = options.firstPort ?? 40500;

	const stall = (): Promise<never> => new Promise<never>(() => { });
	const maybeHold = (stage: string): Promise<void> => {
		if (!armed || `${stage}:${counts[stage]}` !== options.hold) {
			return Promise.resolve();
		}
		reached = true;
		return new Promise<void>((resolve) => {
			release = resolve;
		});
	};

	return {
		reserved,
		reachedHold: () => reached,
		release: () => release?.(),
		arm: () => {
			armed = true;
			counts.reservePort = 0;
			counts.isHealthy = 0;
			counts.wait = 0;
		},
		reservePort: async () => {
			counts.reservePort += 1;
			await maybeHold('reservePort');
			const port = firstPort + reserved.length;
			reserved.push(port);
			return port;
		},
		isHealthy: async () => {
			counts.isHealthy += 1;
			await maybeHold('isHealthy');
			return options.cachedHealthy ?? false;
		},
		waitUntilHealthy: async () => {
			counts.wait += 1;
			await maybeHold('wait');
			const scripted = options.waits?.[counts.wait - 1];
			return scripted === undefined ? stall() : scripted;
		},
	} as LoopbackGate & {
		reserved: number[];
		reachedHold(): boolean;
		release(): void;
		arm(): void;
	};
}

/** Run the event loop until the scripted gate is parked on its held stage. */
async function reachHold(gate: { reachedHold(): boolean }): Promise<void> {
	for (let tick = 0; tick < 200 && !gate.reachedHold(); tick += 1) {
		await settle();
	}
	assert.ok(gate.reachedHold(), 'the scripted gate never reached the stage it was told to hold');
}

/** Let queued microtasks and immediates run, without waiting on a clock. */
function settle(): Promise<void> {
	return new Promise<void>((resolve) => setImmediate(resolve));
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

	test('passes the proxy path prefix through to the CLI', async () => {
		// VS Code for the Web forwards the loopback port at a path, not a
		// host, and the served page's own URLs have to carry that path or the
		// browser resolves them against the proxy's host instead. The prefix
		// is resolvable only after the port is known, which is why the
		// launcher takes a callback rather than a value.
		const process = new FakeProcess('');
		const ports: number[] = [];
		const server = new KptnServer(fakeSpawner(process), fakeHealth());

		await server.start(workspaceUri, '/venv/bin/python', {
			resolveRootPath: async (port: number) => {
				ports.push(port);
				return '/notebook/user/me/vscode/proxy/' + port;
			},
		});

		assert.deepStrictEqual(ports, [fakeHealth.port], 'resolved for the reserved port');
		assert.deepStrictEqual(process.argv, [
			'-m', 'kptn', 'ui', '--no-open', '--port', String(fakeHealth.port),
			'--root-path', '/notebook/user/me/vscode/proxy/' + fakeHealth.port,
		]);
	});

	test('omits --root-path when the UI is served at the root', async () => {
		// Loopback is the common case: no proxy, no prefix, and the CLI must
		// not be handed an empty flag value to parse.
		const process = new FakeProcess('');
		const server = new KptnServer(fakeSpawner(process), fakeHealth());

		await server.start(workspaceUri, '/venv/bin/python', {
			resolveRootPath: async () => '/',
		});

		assert.deepStrictEqual(process.argv, [
			'-m', 'kptn', 'ui', '--no-open', '--port', String(fakeHealth.port),
		]);
	});

	test('starts without a prefix when resolving one fails', async () => {
		// asExternalUri talks to the host; a failure there must not be the
		// difference between a working UI and no UI at all.
		const process = new FakeProcess('');
		const server = new KptnServer(fakeSpawner(process), fakeHealth());

		const url = await server.start(workspaceUri, '/venv/bin/python', {
			resolveRootPath: async () => {
				throw new Error('no port forwarding here');
			},
		});

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

	test('does not spawn a replacement after disposal, and rejects', async () => {
		const children = [new FakeProcess(''), new FakeProcess('')];
		const spawner = fakeSpawner(...children);
		const gate = deferredGate();
		const server = new KptnServer(spawner, gate);

		// Seed a live cached server: first start, healthy.
		const first = server.start(workspaceUri, '/venv/bin/python');
		await settle();
		gate.settleWait(true);
		await first;
		assert.strictEqual(spawner.calls.length, 1);

		// Second start: the reuse probe hangs, disposal lands, probe says dead.
		const second = outcomeOf(server.start(workspaceUri, '/venv/bin/python'));
		await settle();
		server.dispose();
		gate.settleIsHealthy(false);
		await settle();

		assert.strictEqual(
			spawner.calls.length,
			1,
			'a post-disposal relaunch must not happen: nothing would ever terminate it',
		);
		assert.match(await second, /rejected:.*disposed/i);
		for (const [index, child] of children.slice(0, spawner.calls.length).entries()) {
			assert.ok(child.killSignals.length > 0, `child ${index} was never signalled`);
		}
	});

	test('does not return a URL for a cached child disposal already killed', async () => {
		const child = new FakeProcess('');
		const spawner = fakeSpawner(child);
		const gate = deferredGate();
		const server = new KptnServer(spawner, gate);

		const first = server.start(workspaceUri, '/venv/bin/python');
		await settle();
		gate.settleWait(true);
		const url = await first;

		const second = outcomeOf(server.start(workspaceUri, '/venv/bin/python'));
		await settle();
		server.dispose();
		gate.settleIsHealthy(true);
		await settle();

		assert.match(
			await second,
			/rejected:.*disposed/i,
			'reusing a child that disposal just terminated would frame a webview on a dead port',
		);
		assert.deepStrictEqual(child.killSignals, ['SIGTERM']);
		assert.strictEqual(url.port, String(gate.reserved[0]));
	});

	test('does not spawn a retry attempt after disposal', async () => {
		const children = [new FakeProcess(ADDRESS_IN_USE_STDERR), new FakeProcess('')];
		const spawner = fakeSpawner(...children);
		const gate = deferredGate();
		const server = new KptnServer(spawner, gate);

		const started = outcomeOf(server.start(workspaceUri, '/venv/bin/python'));
		await settle();
		assert.strictEqual(spawner.calls.length, 1, 'attempt 1 must have spawned');

		server.dispose();
		gate.settleWait(false);
		await settle();

		assert.strictEqual(
			spawner.calls.length,
			1,
			'the address-in-use retry must not spawn attempt 2 after disposal',
		);
		assert.match(await started, /rejected:.*disposed/i);
	});

	test('every child the spawner ever created is signalled, even around disposal', async () => {
		// Deliberately asserted against the spawner's own creation log rather
		// than the launcher's `live` set: a set that is missing an entry cannot
		// reveal the entry it is missing.
		const children = [new FakeProcess(ADDRESS_IN_USE_STDERR), new FakeProcess(''), new FakeProcess('')];
		const spawner = fakeSpawner(...children);
		const gate = deferredGate();
		const server = new KptnServer(spawner, gate);

		const started = outcomeOf(server.start(workspaceUri, '/venv/bin/python'));
		await settle();
		server.dispose();
		gate.settleWait(false);
		await settle();

		for (let index = 0; index < spawner.calls.length; index += 1) {
			assert.ok(
				children[index].killSignals.length > 0,
				`child ${index} was created by the spawner but never signalled -- it would outlive the extension`,
			);
		}
		assert.match(await started, /rejected:.*disposed/i);
	});

	test('every child of a retrying launch is signalled by disposal', async () => {
		const children = [
			new FakeProcess(ADDRESS_IN_USE_STDERR),
			new FakeProcess(ADDRESS_IN_USE_STDERR),
			new FakeProcess(''),
		];
		const spawner = fakeSpawner(...children);
		const gate = flakyGate(3);
		const server = new KptnServer(spawner, gate);

		await server.start(workspaceUri, '/venv/bin/python');
		assert.strictEqual(spawner.calls.length, 3, 'two lost ports then a healthy start');

		server.dispose();

		for (const [index, child] of children.entries()) {
			assert.deepStrictEqual(
				child.killSignals,
				['SIGTERM'],
				`child ${index} (port ${gate.reserved[index]}) must be signalled exactly once`,
			);
		}
	});

	test('does not leave an unsignalled child when disposal lands during port reservation', async () => {
		const child = new FakeProcess('');
		const spawner = fakeSpawner(child);
		const gate = scriptedGate({ hold: 'reservePort:1', waits: [] });
		(gate as unknown as { arm(): void }).arm();
		const server = new KptnServer(spawner, gate);

		const started = outcomeOf(server.start(workspaceUri, '/venv/bin/python'));
		await reachHold(gate);
		server.dispose();
		gate.release();
		await settle();
		await settle();

		for (let index = 0; index < spawner.calls.length; index += 1) {
			assert.ok(
				child.killSignals.length > 0,
				`child ${index} was spawned after dispose() swept the live set and never signalled`,
			);
		}
		assert.match(await started, /rejected:.*disposed/i);
	});

	test('does not leave an unsignalled child when disposal lands during a retry reservation', async () => {
		const children = [new FakeProcess(ADDRESS_IN_USE_STDERR), new FakeProcess('')];
		const spawner = fakeSpawner(...children);
		const gate = scriptedGate({ hold: 'reservePort:2', waits: [false] });
		(gate as unknown as { arm(): void }).arm();
		const server = new KptnServer(spawner, gate);

		const started = outcomeOf(server.start(workspaceUri, '/venv/bin/python'));
		await reachHold(gate);
		assert.strictEqual(spawner.calls.length, 1, 'attempt 1 must have spawned and lost its port');
		server.dispose();
		gate.release();
		await settle();
		await settle();

		for (let index = 0; index < spawner.calls.length; index += 1) {
			assert.ok(
				children[index].killSignals.length > 0,
				`child ${index} was spawned during a retry after disposal and never signalled`,
			);
		}
		assert.match(await started, /rejected:.*disposed/i);
	});

	// The sweep. One case per deferrable await in the start path, on the first
	// attempt and on a retry. Whichever await a future change adds, the shape
	// of this table is what makes the next missed window fail loudly instead of
	// shipping: the invariant asserted is always the same, and it is asserted
	// against the spawner's own creation log, never against the launcher's
	// `live` set -- a set that is missing an entry cannot reveal that entry.
	const disposalWindows: {
		name: string;
		hold: string;
		waits?: boolean[];
		cachedHealthy?: boolean;
		seed?: boolean;
		stderr?: string[];
	}[] = [
		{ name: 'the cached-server health probe (reports dead)', hold: 'isHealthy:1', seed: true, cachedHealthy: false },
		{ name: 'the cached-server health probe (reports alive)', hold: 'isHealthy:1', seed: true, cachedHealthy: true },
		{ name: 'the first port reservation', hold: 'reservePort:1', waits: [] },
		{ name: 'the first health wait', hold: 'wait:1', waits: [false] },
		{ name: 'a retry port reservation', hold: 'reservePort:2', waits: [false], stderr: [ADDRESS_IN_USE_STDERR, ''] },
		{ name: 'a retry health wait', hold: 'wait:2', waits: [false, false], stderr: [ADDRESS_IN_USE_STDERR, ''] },
	];

	for (const window of disposalWindows) {
		test(`disposal during ${window.name} leaves no unsignalled child`, async () => {
			const stderrs = window.stderr ?? ['', '', ''];
			const children = [0, 1, 2].map((index) => new FakeProcess(stderrs[index] ?? ''));
			const spawner = fakeSpawner(...children);
			const seedWaits = window.seed ? [true] : [];
			const gate = scriptedGate({
				hold: window.hold,
				waits: [...seedWaits, ...(window.waits ?? [])],
				cachedHealthy: window.cachedHealthy,
			});
			const server = new KptnServer(spawner, gate);

			if (window.seed) {
				await server.start(workspaceUri, '/venv/bin/python');
				assert.strictEqual(spawner.calls.length, 1, 'the cached server must be running');
			}
			(gate as unknown as { arm(): void }).arm();

			const started = outcomeOf(server.start(workspaceUri, '/venv/bin/python'));
			await reachHold(gate);
			server.dispose();
			gate.release();
			await settle();
			await settle();

			for (let index = 0; index < spawner.calls.length; index += 1) {
				assert.ok(
					children[index].killSignals.length > 0,
					`child ${index} of ${spawner.calls.length} exists after disposal with no signal -- ` +
					`an orphan holding port ${gate.reserved[index]}`,
				);
			}
			assert.match(await started, /rejected:.*disposed/i);
		});
	}

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

suite('startup behaviour', () => {
	/*
	 * Starting `kptn ui` costs the interpreter plus every import the served
	 * project pulls in -- seconds, all of it after the reader has asked for
	 * the UI and is watching an empty panel. Prewarming moves that cost to
	 * window startup, where nobody is waiting on it.
	 *
	 * The decision is kept pure, and here rather than in extension.ts,
	 * because extension.ts cannot be imported without a live `vscode`.
	 */

	function markerPath(root: string): string {
		return path.join(root, ...INSTALL_MARKER_SEGMENTS);
	}

	function workspaceWithMarker(): string {
		const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kptn-startup-'));
		fs.mkdirSync(path.dirname(markerPath(root)), { recursive: true });
		fs.writeFileSync(markerPath(root), '');
		return root;
	}

	test('does nothing when prewarming is off', () => {
		// The default everywhere but prod: opening a folder must not spawn a
		// server the reader never asked for.
		assert.strictEqual(
			decideStartupAction({ enabled: false, justInstalled: false }),
			'none',
		);
	});

	test('does nothing when prewarming is off, even just after an install', () => {
		assert.strictEqual(
			decideStartupAction({ enabled: false, justInstalled: true }),
			'none',
		);
	});

	test('prewarms silently on an ordinary window', () => {
		assert.strictEqual(
			decideStartupAction({ enabled: true, justInstalled: false }),
			'prewarm',
		);
	});

	test('opens the panel on the first window after an install', () => {
		// The extension host keeps the old build live until the window is
		// reloaded, so this reload is the earliest the new one can show
		// itself -- and the reader reloaded *because* they were told to.
		assert.strictEqual(
			decideStartupAction({ enabled: true, justInstalled: true }),
			'open',
		);
	});

	test('reports no marker in a workspace that never had one', () => {
		const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kptn-startup-'));

		assert.strictEqual(consumeInstallMarker(root), false);
		assert.strictEqual(fs.existsSync(markerPath(root)), false, 'created nothing');
	});

	test('reports the marker and removes it', () => {
		const root = workspaceWithMarker();

		assert.strictEqual(consumeInstallMarker(root), true);
		assert.strictEqual(fs.existsSync(markerPath(root)), false);
	});

	test('the marker fires exactly once', () => {
		// Left behind, it would reopen the panel on every window open for
		// the rest of the deployment.
		const root = workspaceWithMarker();

		consumeInstallMarker(root);

		assert.strictEqual(consumeInstallMarker(root), false);
	});

	test('an unreadable workspace is not an activation failure', () => {
		// Activation runs this before anything else works; a surprise here
		// must not be the difference between a usable extension and none.
		assert.strictEqual(
			consumeInstallMarker(path.join(path.sep, 'no', 'such', 'workspace')),
			false,
		);
	});
});

suite('extension contributions', () => {
	const manifestPath = path.join(__dirname, '..', '..', 'package.json');
	const manifest = JSON.parse(fs.readFileSync(manifestPath, 'utf8')) as {
		activationEvents: string[];
		contributes: Record<string, unknown> & {
			commands: { command: string; title: string }[];
			configuration: { properties: Record<string, { type: string; default: unknown }> };
		};
	};

	test('activates at startup, so it can prewarm before it is asked', () => {
		assert.deepStrictEqual(manifest.activationEvents, ['onStartupFinished']);
	});

	test('contributes the prewarm setting, off by default', () => {
		const setting = manifest.contributes.configuration.properties['kptn.prewarmOnStartup'];
		assert.strictEqual(setting.type, 'boolean');
		assert.strictEqual(setting.default, false);
	});

	test('contributes exactly one command', () => {
		assert.deepStrictEqual(manifest.contributes.commands, [
			{ command: 'kptn.openUI', title: 'kptn: Open Pipeline UI' },
		]);
	});

	test('contributes no views, view containers or menus', () => {
		assert.deepStrictEqual(Object.keys(manifest.contributes), ['commands', 'configuration']);
	});

	test('no longer ships the JSON-RPC backend shim', () => {
		assert.strictEqual(fs.existsSync(path.join(__dirname, '..', '..', 'backend.py')), false);
	});
});
