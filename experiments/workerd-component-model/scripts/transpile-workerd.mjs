import { spawnSync } from "node:child_process";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const args = [
  "exec",
  "--yes",
  "--package",
  "@bytecodealliance/jco@1.35.0",
  "--",
  "jco",
  "transpile",
  "component.wasm",
  "-o",
  "generated",
  "--name",
  "component",
  "--minify",
  "--instantiation",
  "sync",
  "--no-nodejs-compat",
  "--no-wasi-shim",
  "--no-namespaced-exports"
];

const result = spawnSync("npm", args, {
  cwd: root,
  stdio: "inherit",
  shell: process.platform === "win32"
});
if (result.error) {
  throw result.error;
}
process.exitCode = result.status ?? 1;
