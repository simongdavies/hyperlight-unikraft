import { createHash } from "node:crypto";
import { access, readFile, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const runtimeFiles = [
  "component.wasm",
  "component.wat",
  "component.wit",
  "package.json",
  "policy.json",
  "worker.mjs",
  "workerd.capnp"
];
const generatedFiles = [
  "generated/component.js",
  "generated/component.d.ts",
  "generated/component.core.wasm"
];
const packageJson = JSON.parse(await readFile(join(root, "package.json"), "utf8"));

const artifacts = {};
for (const file of [...runtimeFiles, ...generatedFiles]) {
  try {
    await access(join(root, file));
  } catch {
    if (generatedFiles.includes(file)) {
      continue;
    }
    throw new Error(`required identity input is missing: ${file}`);
  }
  const bytes = await readFile(join(root, file));
  artifacts[file] = {
    bytes: bytes.byteLength,
    sha256: createHash("sha256").update(bytes).digest("hex")
  };
}

const lock = {
  schemaVersion: 1,
  identityAlgorithm: "sha256",
  toolchain: {
    wasmTools: "1.252.0",
    jco: packageJson.devDependencies["@bytecodealliance/jco"],
    preview2Shim: packageJson.devDependencies["@bytecodealliance/preview2-shim"],
    workerd: packageJson.devDependencies.workerd,
    compatibilityDate: "2026-09-25"
  },
  artifacts
};
const canonical = `${JSON.stringify(lock, null, 2)}\n`;
await writeFile(join(root, "bundle.lock.json"), canonical);
process.stdout.write(canonical);
