// The module 'vscode' contains the VS Code extensibility API
// Import the module and reference it with the alias vscode in your code below
import * as vscode from 'vscode';
import { spawn, ChildProcessWithoutNullStreams } from 'child_process';
import * as fs from 'fs';
import * as path from 'path';
import * as yaml from 'js-yaml';

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
	private startPromise?: Promise<void>;

	constructor(private readonly context: vscode.ExtensionContext) {
		this.output = vscode.window.createOutputChannel('kptn backend');
	}

	async start(): Promise<void> {
		if (this.child || this.disposed) {
			return;
		}

		const { executable: pythonExecutable, source: pythonSource } = await this.resolvePythonExecutable();
		const scriptPath = path.join(this.context.extensionPath, 'backend.py');
		const useVendored = pythonSource === 'PATH default';
		const { pythonPath, sources } = this.resolvePythonPath(useVendored);
		const env = { ...process.env };

		this.output.appendLine(`Using Python executable from ${pythonSource}: ${pythonExecutable}`);
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
			this.startPromise = undefined;
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

	async getLineageHtml(configUri: vscode.Uri, graph?: string): Promise<{ html: string; tables?: number; edges?: number }> {
		const result = await this.sendRequest('generateLineageHtml', {
			configPath: configUri.fsPath,
			graph,
		});

		const payload = result as { html?: unknown; tables?: unknown; edges?: unknown };
		if (typeof payload?.html === 'string') {
			const tables = typeof payload.tables === 'number' ? payload.tables : undefined;
			const edges = typeof payload.edges === 'number' ? payload.edges : undefined;
			return { html: payload.html, tables, edges };
		}

		throw new Error('Backend response missing lineage HTML');
	}

	async getLineageServerBaseUrl(): Promise<string> {
		const result = await this.sendRequest('getLineageServer');
		const payload = result as { baseUrl?: unknown };
		if (typeof payload?.baseUrl === 'string') {
			return payload.baseUrl;
		}

		throw new Error('Lineage server is not available');
	}

	async getTablePreview(configUri: vscode.Uri, table: string): Promise<{ columns?: string[]; row?: unknown[]; message?: string; resolvedTable?: string }> {
		const result = await this.sendRequest('getTablePreview', {
			configPath: configUri.fsPath,
			table,
		});

		const payload = result as { columns?: unknown; row?: unknown; message?: unknown; resolvedTable?: unknown };
		const columns = Array.isArray(payload?.columns) ? payload.columns.map((entry) => String(entry)) : undefined;
		const row = Array.isArray(payload?.row) ? payload.row : undefined;
		const message = typeof payload?.message === 'string' ? payload.message : undefined;
		const resolvedTable = typeof payload?.resolvedTable === 'string' ? payload.resolvedTable : undefined;
		return { columns, row, message, resolvedTable };
	}

	private async ensureStarted(): Promise<void> {
		if (!this.child && !this.disposed) {
			this.startPromise = this.startPromise ?? this.start();
		}
		await this.startPromise;
	}

	private async sendRequest(method: string, params: Record<string, unknown> = {}): Promise<unknown> {
		await this.ensureStarted();
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

	private resolvePythonPath(includeVendored: boolean): { pythonPath?: string; sources: string[] } {
		const sources: string[] = [];
		const segments: string[] = [];
		const siblingPath = path.resolve(this.context.extensionPath, '..', 'kptn');
		const vendoredPath = path.join(this.context.extensionPath, 'python_libs');

		if (fs.existsSync(siblingPath)) {
			segments.push(siblingPath);
			sources.push('sibling ../kptn');
		}

		if (includeVendored && fs.existsSync(vendoredPath)) {
			segments.push(vendoredPath);
			sources.push('vendored python_libs');
		} else if (!includeVendored) {
			this.output.appendLine('Skipping vendored python_libs in favor of workspace/active environment.');
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

	private async resolvePythonExecutable(): Promise<{ executable: string; source: string }> {
		const envOverride = process.env.KPTN_VSCODE_PYTHON;
		if (envOverride && envOverride.trim()) {
			return { executable: envOverride, source: 'KPTN_VSCODE_PYTHON' };
		}

		const active = await this.getActiveInterpreterFromPythonExtension();
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
			return { executable: candidate, source: 'VIRTUAL_ENV' };
		}

		return { executable: 'python', source: 'PATH default' };
	}

	private async getActiveInterpreterFromPythonExtension(): Promise<{ executable?: string; source?: string }> {
		const resource = vscode.workspace.workspaceFolders?.[0]?.uri;

		try {
			const envPath = await vscode.commands.executeCommand<unknown>('python.environment.getActiveEnvironmentPath', resource);
			const executable = this.extractInterpreterPath(envPath);
			if (executable) {
				return { executable, source: 'python.environment.getActiveEnvironmentPath' };
			}
		} catch (error) {
			this.output.appendLine(`Unable to resolve active environment via python.environment.getActiveEnvironmentPath: ${error instanceof Error ? error.message : String(error)}`);
		}

		const pythonExt = vscode.extensions.getExtension<any>('ms-python.python');
		if (pythonExt) {
			try {
				const api = pythonExt.isActive ? pythonExt.exports : await pythonExt.activate();
				const envPath = await api?.environments?.getActiveEnvironmentPath?.(resource);
				const executable = this.extractInterpreterPath(envPath);
				if (executable) {
					return { executable, source: 'ms-python.python active interpreter' };
				}
			} catch (error) {
				this.output.appendLine(`Unable to resolve active environment via ms-python.python: ${error instanceof Error ? error.message : String(error)}`);
			}
		}

		return {};
	}

	private extractInterpreterPath(candidate: unknown): string | undefined {
		if (!candidate) {
			return undefined;
		}
		if (typeof candidate === 'string') {
			return candidate;
		}

		if (typeof candidate === 'object') {
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
		}

		return undefined;
	}
}

async function fetchHelloMessage(backend: BackendClient): Promise<string> {
	try {
		return await backend.getMessage();
	} catch (error) {
		const errorMessage = error instanceof Error ? error.message : String(error);
		return `Hello from kptn — backend unavailable (${errorMessage}).`;
	}
}

function escapeHtml(value: string): string {
	return value
		.replace(/&/g, '&amp;')
		.replace(/</g, '&lt;')
		.replace(/>/g, '&gt;')
		.replace(/"/g, '&quot;')
		.replace(/'/g, '&#39;');
}

function buildHelloHtml(message: string): string {
	const safeMessage = escapeHtml(message);

	return `<!DOCTYPE html>
<html lang="en">
<head>
	<meta charset="UTF-8">
	<meta name="viewport" content="width=device-width, initial-scale=1.0">
	<title>kptn Hello World</title>
	<style>
		:root {
			color-scheme: light dark;
			font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
		}

		body {
			margin: 0;
			min-height: 100vh;
			display: grid;
			place-items: center;
			background: radial-gradient(circle at 25% 25%, rgba(59, 130, 246, 0.12), transparent 30%),
			            radial-gradient(circle at 75% 35%, rgba(16, 185, 129, 0.12), transparent 25%),
			            radial-gradient(circle at 50% 80%, rgba(236, 72, 153, 0.1), transparent 20%),
			            #0f172a;
			color: #e2e8f0;
			text-align: center;
		}

		.card {
			padding: 32px 40px;
			border-radius: 16px;
			backdrop-filter: blur(10px);
			background: rgba(15, 23, 42, 0.7);
			box-shadow: 0 25px 50px rgba(0, 0, 0, 0.35);
			border: 1px solid rgba(226, 232, 240, 0.08);
		}

		h1 {
			margin: 0 0 12px;
			font-size: 26px;
			letter-spacing: 0.5px;
		}

		p {
			margin: 0;
			font-size: 16px;
			opacity: 0.9;
		}
	</style>
</head>
<body>
	<div class="card">
		<h1>Welcome to kptn</h1>
		<p>${safeMessage}</p>
	</div>
</body>
</html>`;
}

class HelloWorldPanel implements vscode.Disposable {
	private static currentPanel: HelloWorldPanel | undefined;
	private readonly panel: vscode.WebviewPanel;
	private readonly disposables: vscode.Disposable[] = [];
	private disposed = false;

	private constructor(panel: vscode.WebviewPanel, private readonly backend: BackendClient) {
		this.panel = panel;
		this.panel.onDidDispose(() => this.dispose(), null, this.disposables);
	}

	static async render(context: vscode.ExtensionContext, backend: BackendClient): Promise<void> {
		const column = vscode.window.activeTextEditor?.viewColumn;

		if (HelloWorldPanel.currentPanel) {
			HelloWorldPanel.currentPanel.panel.reveal(column);
			await HelloWorldPanel.currentPanel.update();
			return;
		}

		const panel = vscode.window.createWebviewPanel(
			'kptnHelloWorld',
			'kptn: Hello World',
			column ?? vscode.ViewColumn.One,
			{
				enableScripts: false,
				retainContextWhenHidden: true,
			},
		);

		const instance = new HelloWorldPanel(panel, backend);
		HelloWorldPanel.currentPanel = instance;
		context.subscriptions.push(instance);
		context.subscriptions.push(panel);

		await instance.update();
	}

	dispose(): void {
		if (this.disposed) {
			return;
		}

		this.disposed = true;
		HelloWorldPanel.currentPanel = undefined;

		while (this.disposables.length) {
			const disposable = this.disposables.pop();
			disposable?.dispose();
		}

		this.panel.dispose();
	}

	private async update(): Promise<void> {
		const message = await fetchHelloMessage(this.backend);
		this.panel.webview.html = buildHelloHtml(message);
	}
}

class KptnTreeItem extends vscode.TreeItem {
	constructor(label: string, collapsibleState: vscode.TreeItemCollapsibleState) {
		super(label, collapsibleState);
	}
}

class KptnConfigItem extends KptnTreeItem {
	constructor(readonly uri: vscode.Uri, label: string) {
		super(label, vscode.TreeItemCollapsibleState.Collapsed);
		this.resourceUri = uri;
		this.tooltip = uri.fsPath;
	}
}

class KptnGraphItem extends KptnTreeItem {
	constructor(readonly graphName: string, readonly tasks: string[]) {
		super(graphName, vscode.TreeItemCollapsibleState.Collapsed);
		this.description = `${tasks.length} task${tasks.length === 1 ? '' : 's'}`;
		this.contextValue = 'kptnGraph';
	}
}

const KPTN_CONFIG_GLOB = '**/kptn.yaml';
const KPTN_CONFIG_EXCLUDE = '**/{.git,node_modules,dist,out,.venv,.mypy_cache,.pytest_cache,.ruff_cache}/**';

class KptnTreeDataProvider implements vscode.TreeDataProvider<KptnTreeItem>, vscode.Disposable {
	private readonly emitter = new vscode.EventEmitter<KptnTreeItem | void>();
	readonly onDidChangeTreeData = this.emitter.event;
	private disposed = false;

	constructor(private readonly backend: BackendClient) {}

	refresh(): void {
		this.emitter.fire();
	}

	dispose(): void {
		if (this.disposed) {
			return;
		}
		this.disposed = true;
		this.emitter.dispose();
	}

	getTreeItem(element: KptnTreeItem): vscode.TreeItem {
		return element;
	}

	async getChildren(element?: KptnTreeItem): Promise<KptnTreeItem[]> {
		if (!vscode.workspace.workspaceFolders?.length) {
			return [new KptnTreeItem('Open a folder to view kptn configs', vscode.TreeItemCollapsibleState.None)];
		}

		if (!element) {
			return this.buildRootItems();
		}

		if (element instanceof KptnConfigItem) {
			return this.buildConfigChildren(element);
		}

		if (element instanceof KptnGraphItem) {
			if (!element.tasks.length) {
				return [new KptnTreeItem('No tasks in graph', vscode.TreeItemCollapsibleState.None)];
			}

			return element.tasks.map((taskName) => {
				const taskItem = new KptnTreeItem(taskName, vscode.TreeItemCollapsibleState.None);
				taskItem.description = 'Task';
				return taskItem;
			});
		}

		return [];
	}

	private async buildRootItems(): Promise<KptnTreeItem[]> {
		const configs = await vscode.workspace.findFiles(KPTN_CONFIG_GLOB, KPTN_CONFIG_EXCLUDE, 50);
		if (!configs.length) {
			return [new KptnTreeItem('No kptn.yaml found in workspace', vscode.TreeItemCollapsibleState.None)];
		}

		return configs.map((uri) => {
			const folder = vscode.workspace.getWorkspaceFolder(uri);
			const label = folder ? path.relative(folder.uri.fsPath, uri.fsPath) || folder.name : uri.fsPath;
			const item = new KptnConfigItem(uri, label);
			item.description = folder ? folder.name : undefined;
			item.contextValue = 'kptnConfig';
			return item;
		});
	}

	private async buildConfigChildren(config: KptnConfigItem): Promise<KptnTreeItem[]> {
		const parsed = await this.parseConfig(config.uri);
		if ('error' in parsed) {
			const errorItem = new KptnTreeItem(parsed.error, vscode.TreeItemCollapsibleState.None);
			errorItem.tooltip = parsed.details;
			return [errorItem];
		}

		if (!parsed.graphs.length) {
			return [new KptnTreeItem('No graphs defined in kptn.yaml', vscode.TreeItemCollapsibleState.None)];
		}

		return parsed.graphs.map((graph) => new KptnGraphItem(graph.name, graph.tasks));
	}

	private async parseConfig(uri: vscode.Uri): Promise<{ graphs: { name: string; tasks: string[] }[] } | { error: string; details?: string }> {
		let raw: string;
		try {
			raw = await fs.promises.readFile(uri.fsPath, 'utf8');
		} catch (error) {
			const message = error instanceof Error ? error.message : String(error);
			return { error: 'Unable to read kptn.yaml', details: message };
		}

		let doc: unknown;
		try {
			doc = yaml.load(raw);
		} catch (error) {
			const message = error instanceof Error ? error.message : String(error);
			return { error: 'Invalid YAML', details: message };
		}

		if (!doc || typeof doc !== 'object' || Array.isArray(doc)) {
			return { error: 'Unexpected kptn.yaml shape', details: 'Expected a mapping at the top level' };
		}

		const graphs = (doc as Record<string, unknown>).graphs;
		if (!graphs || typeof graphs !== 'object') {
			return { error: 'No graphs section found', details: 'Add a top-level "graphs" mapping' };
		}

		const parsedGraphs: { name: string; tasks: string[] }[] = [];
		for (const [graphName, value] of Object.entries(graphs as Record<string, unknown>)) {
			if (!value || typeof value !== 'object') {
				continue;
			}

			const tasks = (value as Record<string, unknown>).tasks;
			const taskNames = tasks && typeof tasks === 'object' ? Object.keys(tasks as Record<string, unknown>) : [];
			parsedGraphs.push({ name: graphName, tasks: taskNames });
		}

		return { graphs: parsedGraphs };
	}
}

class HelloWorldViewProvider implements vscode.WebviewViewProvider, vscode.Disposable {
	private disposed = false;

	constructor(private readonly backend: BackendClient) {}

	async resolveWebviewView(webviewView: vscode.WebviewView): Promise<void> {
		webviewView.webview.options = { enableScripts: false };
		const message = await fetchHelloMessage(this.backend);
		webviewView.webview.html = buildHelloHtml(message);
	}

	dispose(): void {
		this.disposed = true;
	}
}

class LineagePanel implements vscode.Disposable {
	private static panels = new Map<string, LineagePanel>();
	private disposed = false;
	private readonly messageSubscription: vscode.Disposable;
	private readonly hoverDecoration: vscode.TextEditorDecorationType;
	private currentHoverPath?: string;

	private constructor(
		private readonly panel: vscode.WebviewPanel,
		private readonly backend: BackendClient,
		private readonly configUri: vscode.Uri,
	) {
		this.panel.onDidDispose(() => this.dispose());
		this.hoverDecoration = vscode.window.createTextEditorDecorationType({
			backgroundColor: new vscode.ThemeColor('editor.hoverHighlightBackground'),
			isWholeLine: true,
			});
			this.messageSubscription = this.panel.webview.onDidReceiveMessage(async (event) => {
				if (!event || typeof event?.type !== 'string') {
					return;
				}

				if (event.type === 'tableMeta' && typeof event.table === 'string') {
					await this.handleTablePreview(event.table);
					return;
				}

			if (event.type === 'openFile' && typeof event.path === 'string') {
				try {
					const uri = vscode.Uri.file(event.path);
					const doc = await vscode.workspace.openTextDocument(uri);
					const targetColumn = this.getAlternateViewColumn();
					const showOptions: vscode.TextDocumentShowOptions = { preview: false };
					if (targetColumn) {
						showOptions.viewColumn = targetColumn;
					}
					await vscode.window.showTextDocument(doc, showOptions);
				} catch (error) {
					const message = error instanceof Error ? error.message : String(error);
					vscode.window.showErrorMessage(`Could not open file: ${message}`);
				}
				return;
			}

			if (event.type === 'hoverFile' && typeof event.path === 'string') {
				this.applyHoverHighlight(event.path);
				return;
			}

			if (event.type === 'hoverExit') {
				this.clearHoverHighlight();
			}
		});
	}

	static async show(context: vscode.ExtensionContext, backend: BackendClient, configUri: vscode.Uri, graph?: string): Promise<void> {
		const key = configUri.fsPath;
		const existing = LineagePanel.panels.get(key);
		if (existing) {
			existing.panel.reveal();
			await existing.update(graph);
			return;
		}

		const panel = vscode.window.createWebviewPanel(
			'kptnLineage',
			`SQL Graph: ${path.basename(configUri.fsPath)}`,
			vscode.ViewColumn.Active,
			{
				enableScripts: true,
				retainContextWhenHidden: true,
			},
		);

		const instance = new LineagePanel(panel, backend, configUri);
		LineagePanel.panels.set(key, instance);
		context.subscriptions.push(instance);
		context.subscriptions.push(panel);

		await instance.update(graph);
	}

	dispose(): void {
		if (this.disposed) {
			return;
		}

		this.disposed = true;
		LineagePanel.panels.delete(this.configUri.fsPath);
		this.messageSubscription.dispose();
		this.clearHoverHighlight();
		this.hoverDecoration.dispose();
		this.panel.dispose();
	}

	private async update(graph?: string): Promise<void> {
		const { html, tables, edges } = await this.backend.getLineageHtml(this.configUri, graph);
		if (tables === 0 || edges === 0) {
			this.panel.webview.html = this.buildFallbackHtml('No SQL lineage data found for this kptn.yaml. Ensure SQL tasks are present and configured.');
			return;
		}

		const tableMap = await this.buildTableFileMap();
		this.panel.webview.html = this.injectClickHandlers(html, tableMap);
	}

	private async handleTablePreview(tableName: string): Promise<void> {
		try {
		const preview = await this.backend.getTablePreview(this.configUri, tableName);
		await this.panel.webview.postMessage({
			type: 'tablePreview',
			table: tableName,
			columns: preview.columns,
			row: preview.row,
			message: preview.message,
			resolvedTable: preview.resolvedTable,
		});
	} catch (error) {
		const message = error instanceof Error ? error.message : String(error);
		vscode.window.showErrorMessage(`Unable to load table details: ${message}`);
		await this.panel.webview.postMessage({
				type: 'tablePreview',
				table: tableName,
				message,
			});
		}
	}

	private getAlternateViewColumn(): vscode.ViewColumn | undefined {
		const current = this.panel.viewColumn;
		if (current) {
			for (const group of vscode.window.tabGroups.all) {
				if (group.viewColumn && group.viewColumn !== current) {
					return group.viewColumn;
				}
			}
		}

		for (const editor of vscode.window.visibleTextEditors) {
			if (editor.viewColumn && editor.viewColumn !== current) {
				return editor.viewColumn;
			}
		}

		return undefined;
	}

	private async buildTableFileMap(): Promise<Record<string, string>> {
		try {
			const raw = await fs.promises.readFile(this.configUri.fsPath, 'utf8');
			const doc = yaml.load(raw) as unknown;
			if (!doc || typeof doc !== 'object' || Array.isArray(doc)) {
				return {};
			}

			const tasks = (doc as Record<string, unknown>).tasks;
			if (!tasks || typeof tasks !== 'object') {
				return {};
			}

			const map: Record<string, string> = {};
			for (const [taskName, value] of Object.entries(tasks as Record<string, unknown>)) {
				if (!value || typeof value !== 'object') {
					continue;
				}

				const spec = value as Record<string, unknown>;
				const fileEntry = typeof spec.file === 'string' ? spec.file : undefined;
				const file = fileEntry ? fileEntry.split(':')[0] : undefined;
				const outputs = Array.isArray(spec.outputs) ? spec.outputs.filter((o) => typeof o === 'string') as string[] : [];
				if (!file || !outputs.length) {
					continue;
				}

				for (const output of outputs) {
					const normalized = this.normalizeTableName(output);
					if (normalized) {
						const absolute = path.isAbsolute(file) ? file : path.join(path.dirname(this.configUri.fsPath), file);
						map[normalized] = absolute;
					}
				}

				const normalizedTaskName = this.normalizeTableName(taskName);
				if (normalizedTaskName && file) {
					const absolute = path.isAbsolute(file) ? file : path.join(path.dirname(this.configUri.fsPath), file);
					map[normalizedTaskName] = absolute;
				}
			}

			return map;
		} catch {
			return {};
		}
	}

	private buildFallbackHtml(message: string): string {
		const escaped = message.replace(/</g, '&lt;').replace(/>/g, '&gt;');
		return `<!DOCTYPE html>
<html><body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; padding: 16px;">
<h2>SQL lineage not available</h2>
<p>${escaped}</p>
</body></html>`;
	}

	private injectClickHandlers(html: string, tableMap: Record<string, string>): string {
		const script = `
<script>
(function() {
	const vscode = acquireVsCodeApi();
	const tableMap = ${JSON.stringify(tableMap)};
	const openSvg = '<svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 2.5h7l3 3v8H3z"/><path d="M10 2.5v3h3"/><path d="M7 6 5 8l2 2"/><path d="M9 10l2-2-2-2"/></svg>';
	const tableSvg = '<svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.2" stroke-linecap="round" stroke-linejoin="round"><rect x="2.5" y="3.5" width="11" height="9" rx="1"/><path d="M2.5 6.5h11"/><path d="M2.5 9.5h11"/><path d="M6 3.5v9"/><path d="M10 3.5v9"/></svg>';

	const style = document.createElement('style');
	style.textContent = [
		'.table-name { display: flex; align-items: center; gap: 6px; }',
		'.kptn-open-file, .kptn-open-table { display: inline-flex; align-items: center; justify-content: center; width: 22px; height: 22px; border-radius: 6px; border: none; background: rgba(255,255,255,0.04); color: #e2e8f0; cursor: pointer; padding: 2px; transition: background 0.15s ease, color 0.15s ease; }',
		'.kptn-open-file:hover, .kptn-open-table:hover { background: rgba(255,255,255,0.1); color: #fbbf24; }',
		'.kptn-open-file svg, .kptn-open-table svg { width: 16px; height: 16px; }',
		'.table-preview { margin-top: 6px; }',
		'.preview-table { width: 100%; border-collapse: collapse; table-layout: fixed; }',
		'.preview-table th, .preview-table td { border: 1px solid rgba(226,232,240,0.2); padding: 6px 8px; font-size: 12px; color: #e2e8f0; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }',
		'.preview-table th { background: rgba(255,255,255,0.05); color: #cbd5e1; text-align: left; }',
		'.preview-status { font-size: 13px; color: #cbd5e1; }',
	].join('');
	document.head.appendChild(style);

	function normalize(name) {
		return (name || '').trim().replace(/^[^:]*:\\/\\//, '').split('.').slice(-1)[0]?.toLowerCase() || '';
	}
	function findTableElement(tableName) {
		const target = (tableName || '').trim().toLowerCase();
		if (!target) return null;
		const tables = document.querySelectorAll('.table');
		for (const table of tables) {
			if (!(table instanceof HTMLElement)) continue;
			const nameEl = table.querySelector('.table-name');
			if (!(nameEl instanceof HTMLElement)) continue;
			const textNode = Array.from(nameEl.childNodes).find((node) => node.nodeType === Node.TEXT_NODE);
			const label = (textNode?.textContent || nameEl.textContent || '').trim().toLowerCase();
			if (label === target) {
				return table;
			}
		}
		return null;
	}
	function formatCellValue(value) {
		if (value === null || value === undefined) return 'null';
		if (typeof value === 'object') {
			try {
				return JSON.stringify(value);
			} catch {
				return String(value);
			}
		}
		return String(value);
	}
	function renderPreview(tableName, payload) {
		const tableEl = findTableElement(tableName);
		if (!tableEl) return;
		const columns = Array.isArray(payload?.columns) ? payload.columns : [];
		const row = Array.isArray(payload?.row) ? payload.row : [];
		if (!columns.length || !row.length) {
			refreshConnections();
			return;
		}

		let previewTable = tableEl.querySelector('table.columns');
		if (!(previewTable instanceof HTMLTableElement)) {
			previewTable = document.createElement('table');
			previewTable.className = 'columns preview-table';
			const thead = document.createElement('thead');
			const headerRow = document.createElement('tr');
			columns.forEach((column) => {
				const th = document.createElement('th');
				th.className = 'column';
				th.textContent = String(column);
				headerRow.appendChild(th);
			});
			thead.appendChild(headerRow);
			previewTable.appendChild(thead);
			tableEl.appendChild(previewTable);
		}

		let tbody = previewTable.querySelector('tbody');
		if (!(tbody instanceof HTMLTableSectionElement)) {
			tbody = document.createElement('tbody');
			previewTable.appendChild(tbody);
		}
		tbody.innerHTML = '';
		tbody.className = 'preview-body';
		const dataRow = document.createElement('tr');
		columns.forEach((_, index) => {
			const td = document.createElement('td');
			td.textContent = formatCellValue(index < row.length ? row[index] : null);
			dataRow.appendChild(td);
		});
		tbody.appendChild(dataRow);

		refreshConnections();
	}
	function refreshConnections() {
		if (typeof updateConnectionPaths === 'function') {
			updateConnectionPaths();
		} else {
			window.dispatchEvent(new Event('resize'));
		}
	}
	function ensureButtons() {
		document.querySelectorAll('.table-name').forEach((nameEl) => {
			if (!(nameEl instanceof HTMLElement)) return;
			const hasOpen = nameEl.querySelector('.kptn-open-file');
			const hasMeta = nameEl.querySelector('.kptn-open-table');
			const textNode = Array.from(nameEl.childNodes).find((node) => node.nodeType === Node.TEXT_NODE);
			const name = (textNode?.textContent || nameEl.textContent || '').trim();
			const key = normalize(name);
			const path = tableMap[key];

			if (!hasOpen) {
				const openBtn = document.createElement('button');
				openBtn.className = 'kptn-open-file';
				openBtn.type = 'button';
				openBtn.title = 'Open source';
				openBtn.innerHTML = openSvg;
				openBtn.addEventListener('click', (event) => {
					event.stopPropagation();
					if (path) {
						vscode.postMessage({ type: 'openFile', path });
					} else {
						vscode.postMessage({ type: 'openFileMissing', table: name });
					}
				});
				nameEl.appendChild(openBtn);
			}

			if (!hasMeta) {
				const metaBtn = document.createElement('button');
				metaBtn.className = 'kptn-open-table';
				metaBtn.type = 'button';
				metaBtn.title = 'Table details';
				metaBtn.innerHTML = tableSvg;
				metaBtn.addEventListener('click', (event) => {
					event.stopPropagation();
					renderPreview(name, { table: name, message: 'Loading table details...' });
					vscode.postMessage({ type: 'tableMeta', table: name, path: path || null });
				});
				nameEl.appendChild(metaBtn);
			}
		});
		refreshConnections();
	}

	ensureButtons();

	window.addEventListener('message', (event) => {
		const payload = event.data;
		if (!payload || payload.type !== 'tablePreview') {
			return;
		}
		renderPreview(payload.table, payload);
	});

	document.addEventListener('mouseover', (event) => {
		const target = event.target;
		if (!(target instanceof HTMLElement)) return;
		const tableEl = target.closest('.table');
		if (!tableEl) return;
		const nameEl = tableEl.querySelector('.table-name');
		const name = nameEl ? nameEl.textContent || '' : '';
		const key = normalize(name);
		const path = tableMap[key];
		if (path) {
			vscode.postMessage({ type: 'hoverFile', path });
		}
	});

	document.addEventListener('mouseout', (event) => {
		const target = event.target;
		if (!(target instanceof HTMLElement)) return;
		if (!event.relatedTarget || !(event.relatedTarget instanceof HTMLElement)) {
			return;
		}
		const leavingTable = target.closest('.table') && !event.relatedTarget.closest('.table');
		if (leavingTable) {
			vscode.postMessage({ type: 'hoverExit' });
		}
	});
})();
</script>`;

		if (html.includes('</body>')) {
			return html.replace('</body>', `${script}</body>`);
		}

		return `${html}${script}`;
	}

	private normalizeTableName(value: string): string | undefined {
		const cleaned = value.replace(/^[^:]+:\/\//, '').split('.').slice(-1)[0];
		if (!cleaned) {
			return undefined;
		}
		return cleaned.trim().toLowerCase();
	}

	private applyHoverHighlight(filePath: string): void {
		this.currentHoverPath = filePath;
		const editors = vscode.window.visibleTextEditors.filter((editor) => editor.document.uri.fsPath === filePath);
		const decorationRange = new vscode.Range(0, 0, Number.MAX_SAFE_INTEGER, 0);
		for (const editor of editors) {
			editor.setDecorations(this.hoverDecoration, [decorationRange]);
		}
	}

	private clearHoverHighlight(): void {
		if (!this.currentHoverPath) {
			return;
		}

		for (const editor of vscode.window.visibleTextEditors) {
			editor.setDecorations(this.hoverDecoration, []);
		}

		this.currentHoverPath = undefined;
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
			await HelloWorldPanel.render(context, backend);
		} catch (error) {
			const errorMessage = error instanceof Error ? error.message : String(error);
			vscode.window.showErrorMessage(`Could not open kptn hello view: ${errorMessage}`);
		}
	});

	const viewProvider = new HelloWorldViewProvider(backend);
	context.subscriptions.push(vscode.window.registerWebviewViewProvider('kptn.helloView', viewProvider));

	const treeDataProvider = new KptnTreeDataProvider(backend);
	context.subscriptions.push(vscode.window.createTreeView('kptn.tree', { treeDataProvider }));
	context.subscriptions.push(vscode.workspace.onDidChangeWorkspaceFolders(() => treeDataProvider.refresh()));

	const viewLineageCommand = vscode.commands.registerCommand('kptn.viewLineage', async (item?: KptnConfigItem) => {
		if (!(item instanceof KptnConfigItem)) {
			vscode.window.showErrorMessage('Select a kptn.yaml to view the SQL graph');
			return;
		}

		try {
			await LineagePanel.show(context, backend, item.uri);
		} catch (error) {
			const errorMessage = error instanceof Error ? error.message : String(error);
			vscode.window.showErrorMessage(`Could not render SQL graph: ${errorMessage}`);
		}
	});

	const watcher = vscode.workspace.createFileSystemWatcher(KPTN_CONFIG_GLOB);
	watcher.onDidCreate(() => treeDataProvider.refresh());
	watcher.onDidChange(() => treeDataProvider.refresh());
	watcher.onDidDelete(() => treeDataProvider.refresh());
	context.subscriptions.push(watcher);

	context.subscriptions.push(disposable);
	context.subscriptions.push(backend);
	context.subscriptions.push(viewProvider);
	context.subscriptions.push(treeDataProvider);
	context.subscriptions.push(viewLineageCommand);
}

// This method is called when your extension is deactivated
export function deactivate() {}
