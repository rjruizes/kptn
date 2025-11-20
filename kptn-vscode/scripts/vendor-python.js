"use strict";

/**
 * Vendor the sibling ../kptn Python package into ./python_libs for packaging.
 */

const { execSync } = require("child_process");
const fs = require("fs");
const path = require("path");

const repoRoot = path.resolve(__dirname, "..");
const monorepoRoot = path.resolve(repoRoot, "..");
const siblingPackage = path.resolve(repoRoot, "..", "kptn");
const targetPath = path.join(repoRoot, "python_libs");
const pythonExe = process.env.KPTN_VSCODE_PYTHON || "python";

function removeIfExists(target) {
    if (fs.existsSync(target)) {
        fs.rmSync(target, { recursive: true, force: true });
    }
}

function pruneCacheDirs(basedir) {
    const walk = (dir) => {
        const entries = fs.readdirSync(dir, { withFileTypes: true });
        for (const entry of entries) {
            const fullPath = path.join(dir, entry.name);
            if (entry.isDirectory()) {
                if (entry.name === "__pycache__") {
                    fs.rmSync(fullPath, { recursive: true, force: true });
                    continue;
                }
                walk(fullPath);
            }
        }
    };
    walk(basedir);
}

function main() {
    const candidates = [
        { name: "monorepo root", path: monorepoRoot },
        { name: "sibling kptn", path: siblingPackage },
    ];

    const chosen = candidates.find(({ path: p }) =>
        fs.existsSync(p) && (fs.existsSync(path.join(p, "pyproject.toml")) || fs.existsSync(path.join(p, "setup.py")))
    );

    if (!chosen) {
        const expected = candidates.map(({ name, path: p }) => `${name}: ${p}`).join("; ");
        throw new Error(`No installable kptn source found. Expected pyproject.toml or setup.py in one of: ${expected}`);
    }

    const sourcePath = chosen.path;

    console.log(`Vendoring Python package from ${sourcePath} (${chosen.name})`);
    removeIfExists(targetPath);

    const cmd = `${pythonExe} -m pip install "${sourcePath}" --target "${targetPath}"`;
    console.log(`Running: ${cmd}`);
    execSync(cmd, { stdio: "inherit" });

    pruneCacheDirs(targetPath);
    console.log(`Vendored Python libs to ${targetPath}`);
}

if (require.main === module) {
    try {
        main();
    } catch (error) {
        console.error(String(error));
        process.exit(1);
    }
}
