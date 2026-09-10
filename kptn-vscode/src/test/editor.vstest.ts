/**
 * Suites that need a real VS Code host.
 *
 * `npm test` runs the host-free suites under plain mocha; this file runs only
 * under `npm run test:vscode`, which downloads and launches a real editor.
 * It exists so the two claims that cannot be checked without an editor -- that
 * an authorized message really opens the document at the requested one-based
 * line, and that a refused message opens nothing at all -- are actually
 * verified rather than asserted.
 */

import * as assert from 'assert';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import * as vscode from 'vscode';

import { dispatchBridgeMessage, handleBridgeMessage } from '../extension';

const token = 'd'.repeat(48);
const silentOutput = { appendLine: (): void => { } };

suite('shared UI bridge in a real editor', () => {
	let root: string;
	let outside: string;
	let sourcePath: string;

	suiteSetup(() => {
		const base = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'kptn-editor-')));
		root = path.join(base, 'workspace');
		outside = path.join(base, 'elsewhere');
		fs.mkdirSync(path.join(root, 'models'), { recursive: true });
		fs.mkdirSync(outside, { recursive: true });
		sourcePath = path.join(root, 'models', 'orders.sql');
		fs.writeFileSync(sourcePath, Array.from({ length: 80 }, (_, i) => `-- line ${i + 1}`).join('\n'));
		fs.writeFileSync(path.join(outside, 'secrets.env'), 'TOKEN=1');
	});

	teardown(async () => {
		await vscode.commands.executeCommand('workbench.action.closeAllEditors');
	});

	test('opens the requested source at the one-based line', async () => {
		const opened = await handleBridgeMessage(
			{ type: 'openSource', path: 'models/orders.sql', line: 42, token },
			{ token, workspaceRoot: root },
			silentOutput,
		);

		assert.strictEqual(opened, true);
		const editor = vscode.window.activeTextEditor;
		assert.ok(editor, 'a document must be open');
		assert.strictEqual(editor.document.uri.fsPath, sourcePath);
		assert.strictEqual(editor.selection.active.line, 41, 'line 42 is index 41');
		assert.strictEqual(editor.document.lineAt(editor.selection.active.line).text, '-- line 42');
	});

	test('opens the first line when no line is requested', async () => {
		const opened = await handleBridgeMessage(
			{ type: 'openSource', path: 'models/orders.sql', token },
			{ token, workspaceRoot: root },
			silentOutput,
		);

		assert.strictEqual(opened, true);
		assert.strictEqual(vscode.window.activeTextEditor?.selection.active.line, 0);
	});

	test('opens nothing when the bridge token does not match', async () => {
		const opened = await handleBridgeMessage(
			{ type: 'openSource', path: 'models/orders.sql', line: 3, token: 'e'.repeat(48) },
			{ token, workspaceRoot: root },
			silentOutput,
		);

		assert.strictEqual(opened, false);
		assert.strictEqual(vscode.window.activeTextEditor, undefined, 'no editor may be opened');
	});

	test('opens nothing for a path outside the workspace', async () => {
		const opened = await handleBridgeMessage(
			{ type: 'openSource', path: path.join(outside, 'secrets.env'), token },
			{ token, workspaceRoot: root },
			silentOutput,
		);

		assert.strictEqual(opened, false);
		assert.strictEqual(vscode.window.activeTextEditor, undefined, 'no editor may be opened');
	});

	test('opens nothing for a traversal out of the workspace', async () => {
		const opened = await handleBridgeMessage(
			{ type: 'openSource', path: '../elsewhere/secrets.env', token },
			{ token, workspaceRoot: root },
			silentOutput,
		);

		assert.strictEqual(opened, false);
		assert.strictEqual(vscode.window.activeTextEditor, undefined, 'no editor may be opened');
	});

	test('reports an authorized path that no longer exists instead of failing silently', async () => {
		const notified: string[] = [];
		const logged: string[] = [];

		const opened = await dispatchBridgeMessage(
			{ type: 'openSource', path: 'models/deleted.sql', line: 2, token },
			{ token, workspaceRoot: root },
			{ appendLine: (line: string) => logged.push(line) },
			(text: string) => notified.push(text),
		);

		assert.strictEqual(opened, false, 'a missing file opens nothing');
		assert.strictEqual(notified.length, 1, 'the user must be told, not left with a silent failure');
		assert.ok(/could not open the requested source/i.test(notified[0]), notified[0]);
		assert.strictEqual(logged.length, 1, 'the failure must reach the output channel');
		assert.strictEqual(vscode.window.activeTextEditor, undefined);
	});

	test('a refused message is not reported as an error to the user', async () => {
		const notified: string[] = [];
		const opened = await dispatchBridgeMessage(
			{ type: 'openSource', path: '/etc/passwd', token },
			{ token, workspaceRoot: root },
			{ appendLine: (): void => { } },
			(text: string) => notified.push(text),
		);

		assert.strictEqual(opened, false);
		assert.deepStrictEqual(notified, [], 'a refusal is logged, not raised as a notification');
	});
});
