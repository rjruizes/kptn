/**
 * kptn VS Code extension: a thin launcher for the shared pipeline UI.
 *
 * There is exactly one supported frontend -- the FastAPI app served by
 * `kptn ui` -- and this extension does nothing but start it on a loopback
 * port, host it in a single webview, and open source files when the hosted
 * page asks. It speaks no protocol of its own.
 */

import * as fs from 'fs';
import * as path from 'path';
import * as vscode from 'vscode';

import {
	KptnServer,
	authorizeOpenSource,
	buildHostHtml,
	createBridgeToken,
	createLoopbackGate,
	defaultSpawner,
	withBridgeToken,
} from './server';

const VIEW_TYPE = 'kptn.ui';

interface HostedPanel {
	panel: vscode.WebviewPanel;
	/** The token the currently rendered page was given. Rotated on each render. */
	token: string;
	workspaceRoot: string;
}

let server: KptnServer | undefined;
let hosted: HostedPanel | undefined;

export function activate(context: vscode.ExtensionContext): void {
	const output = vscode.window.createOutputChannel('kptn');
	context.subscriptions.push(output);

	server = new KptnServer(defaultSpawner, createLoopbackGate(), output);
	context.subscriptions.push({ dispose: () => disposeServer() });

	context.subscriptions.push(
		vscode.commands.registerCommand('kptn.openUI', async () => {
			try {
				await openPipelineUI(context, output);
			} catch (error) {
				const message = error instanceof Error ? error.message : String(error);
				output.appendLine(`Could not open the pipeline UI: ${message}`);
				vscode.window.showErrorMessage(`Could not open the kptn pipeline UI: ${message}`);
			}
		}),
	);
}

export function deactivate(): void {
	disposeServer();
}

function disposeServer(): void {
	hosted?.panel.dispose();
	hosted = undefined;
	// Terminates the `kptn ui` child only. Run workers are detached on
	// purpose so that they outlive both the editor and the server.
	server?.dispose();
	server = undefined;
}

async function openPipelineUI(
	context: vscode.ExtensionContext,
	output: vscode.OutputChannel,
): Promise<void> {
	const workspace = vscode.workspace.workspaceFolders?.[0];
	if (!workspace) {
		throw new Error('Open a folder containing a kptn project first.');
	}
	if (!server) {
		throw new Error('The kptn extension is not active.');
	}

	const { executable, source } = await resolvePythonExecutable(output);
	output.appendLine(`Using Python interpreter from ${source}: ${executable}`);

	const extraPythonPath = resolveExtraPythonPath(context, output);
	const url = await server.start(workspace.uri, executable, { extraPythonPath });
	const externalUri = await vscode.env.asExternalUri(vscode.Uri.parse(url.toString()));

	// The token is minted per render and lives only as long as the rendered
	// page: it is a page-URL parameter, not a credential, which is why the
	// extension also checks workspace containment on every request it
	// authorizes.
	const token = createBridgeToken();
	const pageUrl = withBridgeToken(externalUri.toString(), token);
	const workspaceRoot = workspace.uri.fsPath;

	if (!hosted) {
		const panel = vscode.window.createWebviewPanel(
			VIEW_TYPE,
			'kptn Pipeline',
			vscode.ViewColumn.One,
			{ enableScripts: true, retainContextWhenHidden: true },
		);
		hosted = { panel, token, workspaceRoot };
		panel.onDidDispose(() => {
			// The server keeps running: closing the panel must not stop a run.
			hosted = undefined;
		});
		// Registered once per panel, so reopening the command cannot pile up
		// listeners holding stale tokens.
		panel.webview.onDidReceiveMessage((message: unknown) =>
			handleBridgeMessage(message, hosted, output),
		);
	} else {
		hosted.token = token;
		hosted.workspaceRoot = workspaceRoot;
		hosted.panel.reveal(vscode.ViewColumn.One);
	}

	hosted.panel.webview.html = buildHostHtml(pageUrl, token);
	output.appendLine(`Hosting the kptn pipeline UI from ${externalUri.toString()}`);
}

/**
 * Act on one message from the hosted page.
 *
 * Exported so the real-editor test can drive it: the token and containment
 * checks are the only thing standing between a page message and an arbitrary
 * file being opened, so they are checked here, once, for every message.
 */
export async function handleBridgeMessage(
	message: unknown,
	target: { token: string; workspaceRoot: string } | undefined,
	output: Pick<vscode.OutputChannel, 'appendLine'>,
): Promise<boolean> {
	if (!target) {
		return false;
	}

	const decision = authorizeOpenSource(message, {
		token: target.token,
		workspaceRoot: target.workspaceRoot,
	});

	if (!decision.ok) {
		if (decision.reason !== 'not-open-source') {
			output.appendLine(`Refused an openSource request: ${decision.reason}.`);
		}
		return false;
	}

	const document = await vscode.workspace.openTextDocument(vscode.Uri.file(decision.fsPath));
	const editor = await vscode.window.showTextDocument(document, {
		preview: false,
		viewColumn: vscode.ViewColumn.Beside,
	});
	const position = new vscode.Position(decision.zeroBasedLine, 0);
	editor.selection = new vscode.Selection(position, position);
	editor.revealRange(new vscode.Range(position, position), vscode.TextEditorRevealType.InCenter);
	return true;
}

/**
 * PYTHONPATH additions so `python -m kptn` resolves in a source checkout or a
 * packaged extension that vendored the Python package.
 */
function resolveExtraPythonPath(
	context: vscode.ExtensionContext,
	output: vscode.OutputChannel,
): string | undefined {
	const segments: string[] = [];
	const vendored = path.join(context.extensionPath, 'python_libs');
	if (fs.existsSync(vendored)) {
		segments.push(vendored);
	}
	const sibling = path.resolve(context.extensionPath, '..');
	if (fs.existsSync(path.join(sibling, 'kptn', '__init__.py'))) {
		segments.push(sibling);
	}
	if (process.env.PYTHONPATH) {
		segments.push(process.env.PYTHONPATH);
	}
	if (!segments.length) {
		output.appendLine('No PYTHONPATH additions; relying on the selected interpreter having kptn installed.');
		return undefined;
	}
	return segments.join(path.delimiter);
}

/* -------------------------------------------------------------------------- */
/*  Python interpreter resolution                                             */
/* -------------------------------------------------------------------------- */

export async function resolvePythonExecutable(
	output: vscode.OutputChannel,
): Promise<{ executable: string; source: string }> {
	const envOverride = process.env.KPTN_VSCODE_PYTHON;
	if (envOverride && envOverride.trim()) {
		return { executable: envOverride, source: 'KPTN_VSCODE_PYTHON' };
	}

	const active = await getActiveInterpreterFromPythonExtension(output);
	if (active?.executable) {
		return active as { executable: string; source: string };
	}

	const pythonConfig = vscode.workspace.getConfiguration('python');
	const defaultInterpreter = pythonConfig.get<string>('defaultInterpreterPath');
	if (defaultInterpreter && defaultInterpreter.trim()) {
		return { executable: defaultInterpreter, source: 'python.defaultInterpreterPath' };
	}

	const legacyInterpreter = pythonConfig.get<string>('pythonPath');
	if (legacyInterpreter && legacyInterpreter.trim()) {
		return { executable: legacyInterpreter, source: 'python.pythonPath' };
	}

	const venv = process.env.VIRTUAL_ENV;
	if (venv && venv.trim()) {
		const binDir = process.platform === 'win32' ? 'Scripts' : 'bin';
		const exeName = process.platform === 'win32' ? 'python.exe' : 'python';
		const candidate = path.join(venv, binDir, exeName);
		if (fs.existsSync(candidate)) {
			return { executable: candidate, source: 'VIRTUAL_ENV' };
		}
	}

	return { executable: 'python', source: 'PATH default' };
}

async function getActiveInterpreterFromPythonExtension(
	output: vscode.OutputChannel,
): Promise<{ executable?: string; source?: string }> {
	const resource = vscode.workspace.workspaceFolders?.[0]?.uri;

	try {
		const envPath = await vscode.commands.executeCommand<unknown>(
			'python.environment.getActiveEnvironmentPath',
			resource,
		);
		const executable = extractInterpreterPath(envPath);
		if (executable) {
			return { executable, source: 'python.environment.getActiveEnvironmentPath' };
		}
	} catch (error) {
		output.appendLine(
			`Unable to resolve the active environment via python.environment.getActiveEnvironmentPath: ${describe(error)}`,
		);
	}

	const pythonExt = vscode.extensions.getExtension('ms-python.python');
	if (pythonExt) {
		try {
			const api = (pythonExt.isActive ? pythonExt.exports : await pythonExt.activate()) as
				| { environments?: { getActiveEnvironmentPath?: (resource?: vscode.Uri) => unknown } }
				| undefined;
			const envPath = await api?.environments?.getActiveEnvironmentPath?.(resource);
			const executable = extractInterpreterPath(envPath);
			if (executable) {
				return { executable, source: 'ms-python.python active interpreter' };
			}
		} catch (error) {
			output.appendLine(`Unable to resolve the active environment via ms-python.python: ${describe(error)}`);
		}
	}

	return {};
}

function extractInterpreterPath(candidate: unknown): string | undefined {
	if (!candidate) {
		return undefined;
	}
	if (typeof candidate === 'string') {
		return candidate;
	}
	if (typeof candidate !== 'object') {
		return undefined;
	}

	const data = candidate as Record<string, unknown>;
	const executable = data.executable;
	if (typeof executable === 'string' && executable.trim()) {
		return executable;
	}
	if (executable && typeof executable === 'object') {
		const execObj = executable as Record<string, unknown>;
		if (typeof execObj.path === 'string' && execObj.path.trim()) {
			return execObj.path;
		}
		const execUri = execObj.uri as vscode.Uri | undefined;
		if (execUri && typeof execUri.fsPath === 'string') {
			return execUri.fsPath;
		}
	}
	if (typeof data.path === 'string' && data.path.trim()) {
		return data.path;
	}
	const dataUri = data.uri as vscode.Uri | undefined;
	if (dataUri && typeof dataUri.fsPath === 'string') {
		return dataUri.fsPath;
	}

	return undefined;
}

function describe(error: unknown): string {
	return error instanceof Error ? error.message : String(error);
}
