import { defineConfig } from '@vscode/test-cli';

export default defineConfig({
	// `*.test.js` holds the host-free unit suites (also run by `npm test` under
	// plain mocha); `*.vstest.js` holds the suites that need a real editor.
	files: ['out/test/**/*.test.js', 'out/test/**/*.vstest.js'],
});
