// The module 'vscode' contains the VS Code extensibility API
// Import the module and reference it with the alias vscode in your code below
import * as vscode from 'vscode';
import { spawn, ChildProcessWithoutNullStreams } from 'child_process';
import * as fs from 'fs';
import * as path from 'path';

type JsonRpcResponse = {
	jsonrpc: '2.0';
	id: number;
	result?: unknown;
	error?: { code: number; message: string };
};

class BackendClient implements vscode.Disposable {
	private child?: ChildProcessWithoutNullStreams;
	private pending = new Map<number, { resolve: (value: unknown) => void; reject: (error: Error) => void }>();
	private stdoutBuffer = '';
	private nextId = 1;
	private disposed = false;
	private readonly output: vscode.OutputChannel;

	constructor(private readonly context: vscode.ExtensionContext) {
		this.output = vscode.window.createOutputChannel('kptn backend');
	}

	start(): void {
		if (this.child || this.disposed) {
			return;
		}

		const pythonExecutable = process.env.KPTN_VSCODE_PYTHON ?? 'python';
		const scriptPath = path.join(this.context.extensionPath, 'backend.py');
		const { pythonPath, sources } = this.resolvePythonPath();
		const env = { ...process.env };

		if (pythonPath) {
			env.PYTHONPATH = pythonPath;
			this.output.appendLine(`Using PYTHONPATH from: ${sources.join(' -> ')}`);
		} else {
			this.output.appendLine('No PYTHONPATH set (sibling ../kptn and python_libs missing).');
		}

		this.child = spawn(pythonExecutable, [scriptPath], {
			cwd: this.context.extensionPath,
			stdio: ['pipe', 'pipe', 'pipe'],
			env,
		});

		this.child.stdout?.setEncoding('utf8');
		this.child.stdout?.on('data', (data: string | Buffer) => this.handleStdout(data.toString()));
		this.child.stderr?.on('data', (data: string | Buffer) => this.output.appendLine(`[stderr] ${data.toString().trimEnd()}`));
		this.child.on('exit', (code, signal) => {
			const reason = signal ? `signal ${signal}` : `code ${code}`;
			this.output.appendLine(`Backend exited (${reason}).`);
			this.rejectAllPending(new Error(`Backend exited (${reason})`));
			this.child = undefined;
		});
		this.child.on('error', (error: Error) => {
			this.output.appendLine(`Backend failed to start: ${error.message}`);
			this.rejectAllPending(error);
			this.child = undefined;
		});
	}

	async getMessage(): Promise<string> {
		const result = await this.sendRequest('getMessage');
		const payload = result as { message?: unknown };
		if (typeof payload?.message === 'string') {
			return payload.message;
		}

		throw new Error('Backend response missing message');
	}

	dispose(): void {
		this.disposed = true;
		this.rejectAllPending(new Error('Backend disposed'));
		this.child?.kill();
		this.child = undefined;
		this.output.dispose();
	}

	private ensureStarted(): void {
		if (!this.child && !this.disposed) {
			this.start();
		}
	}

	private sendRequest(method: string, params: Record<string, unknown> = {}): Promise<unknown> {
		this.ensureStarted();

		if (!this.child?.stdin || !this.child.stdin.writable) {
			return Promise.reject(new Error('Backend process is not running'));
		}

		const id = this.nextId++;
		const payload = JSON.stringify({ jsonrpc: '2.0', id, method, params });

		return new Promise((resolve, reject) => {
			this.pending.set(id, { resolve, reject });

			try {
				this.child?.stdin.write(`${payload}\n`);
			} catch (error) {
				this.pending.delete(id);
				const wrappedError = error instanceof Error ? error : new Error(String(error));
				reject(wrappedError);
			}
		});
	}

	private handleStdout(chunk: string): void {
		this.stdoutBuffer += chunk;
		const lines = this.stdoutBuffer.split(/\r?\n/);
		this.stdoutBuffer = lines.pop() ?? '';

		for (const line of lines) {
			const trimmed = line.trim();
			if (!trimmed) {
				continue;
			}

			let response: JsonRpcResponse;
			try {
				response = JSON.parse(trimmed) as JsonRpcResponse;
			} catch (error) {
				this.output.appendLine(`Failed to parse backend response: ${error}`);
				continue;
			}

			const pending = this.pending.get(response.id);
			if (!pending) {
				this.output.appendLine(`No pending request for response id ${response.id}`);
				continue;
			}

			this.pending.delete(response.id);
			if (response.error) {
				pending.reject(new Error(response.error.message));
			} else {
				pending.resolve(response.result);
			}
		}
	}

	private rejectAllPending(error: Error): void {
		this.pending.forEach(({ reject }) => reject(error));
		this.pending.clear();
	}

	private resolvePythonPath(): { pythonPath?: string; sources: string[] } {
		const sources: string[] = [];
		const segments: string[] = [];
		const siblingPath = path.resolve(this.context.extensionPath, '..', 'kptn');
		const vendoredPath = path.join(this.context.extensionPath, 'python_libs');

		if (fs.existsSync(siblingPath)) {
			segments.push(siblingPath);
			sources.push('sibling ../kptn');
		}

		if (fs.existsSync(vendoredPath)) {
			segments.push(vendoredPath);
			sources.push('vendored python_libs');
		}

		if (process.env.PYTHONPATH) {
			segments.push(process.env.PYTHONPATH);
			sources.push('existing PYTHONPATH');
		}

		if (!segments.length) {
			return { pythonPath: undefined, sources };
		}

		return { pythonPath: segments.join(path.delimiter), sources };
	}
}

// This method is called when your extension is activated
// Your extension is activated the very first time the command is executed
export function activate(context: vscode.ExtensionContext) {
	const backend = new BackendClient(context);
	backend.start();

	// Use the console to output diagnostic information (console.log) and errors (console.error)
	// This line of code will only be executed once when your extension is activated
	console.log('Congratulations, your extension "kptn" is now active!');

	// The command has been defined in the package.json file
	// Now provide the implementation of the command with registerCommand
	// The commandId parameter must match the command field in package.json
	const disposable = vscode.commands.registerCommand('kptn.helloWorld', async () => {
		try {
			const backendMessage = await backend.getMessage();
			vscode.window.showInformationMessage(backendMessage);
		} catch (error) {
			const errorMessage = error instanceof Error ? error.message : String(error);
			vscode.window.showErrorMessage(`Could not reach backend: ${errorMessage}`);
		}
	});

	context.subscriptions.push(disposable);
	context.subscriptions.push(backend);
}

// This method is called when your extension is deactivated
export function deactivate() {}
