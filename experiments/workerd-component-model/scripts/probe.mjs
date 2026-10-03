import { createHash } from "node:crypto";
import { constants } from "node:fs";
import { access, readFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const EXPECTED_PINS = {
  "@bytecodealliance/jco": "1.35.0",
  "@bytecodealliance/preview2-shim": "0.26.0",
  "workerd": "1.20260925.1"
};

async function exists(path) {
  try {
    await access(path, constants.F_OK);
    return true;
  } catch {
    return false;
  }
}

export async function evaluate(root) {
  const packageJson = JSON.parse(await readFile(join(root, "package.json"), "utf8"));
  const policy = JSON.parse(await readFile(join(root, "policy.json"), "utf8"));
  const lockBytes = await readFile(join(root, "bundle.lock.json"));
  const lock = JSON.parse(lockBytes);
  const evidence = JSON.parse(await readFile(join(root, "evidence.json"), "utf8"));
  const config = await readFile(join(root, "workerd.capnp"), "utf8");
  const component = await readFile(join(root, "component.wasm"));

  const failures = [];
  for (const [name, version] of Object.entries(EXPECTED_PINS)) {
    if (packageJson.devDependencies?.[name] !== version) {
      failures.push(`${name} must be pinned to ${version}`);
    }
    if (lock.toolchain?.wasmTools !== "1.252.0" ||
        lock.toolchain?.compatibilityDate !== "2026-09-25") {
      failures.push("bundle lock toolchain metadata is not pinned");
    }
  }

  if (policy.ambientNetwork !== false || policy.storageBindings.length !== 0) {
    failures.push("policy must deny ambient network and storage bindings");
  }
  if (policy.componentImports.length !== 0) {
    failures.push("the decision fixture must remain import-free");
  }
  if (!config.includes('(name = "internet", network = (allow = []))')) {
    failures.push("workerd must override the implicit internet service with deny-all");
  }
  if (config.includes("disk =")) {
    failures.push("workerd fixture must not introduce a storage lane");
  }

  for (const [file, expected] of Object.entries(lock.artifacts)) {
    const bytes = await readFile(join(root, file));
    const sha256 = createHash("sha256").update(bytes).digest("hex");
    if (sha256 !== expected.sha256 || bytes.byteLength !== expected.bytes) {
      failures.push(`${file} does not match bundle.lock.json`);
    }
  }
  const bundleIdentity = createHash("sha256").update(lockBytes).digest("hex");
  if (evidence.bundleIdentity !== bundleIdentity) {
    failures.push("evidence.json references a stale bundle identity");
  }
  for (const file of [
    "component.js",
    "component.core.wasm",
    "component.d.ts"
  ]) {
    const locked = lock.artifacts[`generated/${file}`];
    const measured = evidence.lowering?.artifacts?.[file];
    if (!locked || locked.sha256 !== measured?.sha256 ||
        locked.bytes !== measured?.bytes) {
      failures.push(`evidence for generated/${file} does not match bundle lock`);
    }
  }

  const generated = {
    javascript: await exists(join(root, "generated", "component.js")),
    declarations: await exists(join(root, "generated", "component.d.ts")),
    splitCoreWasm: await exists(join(root, "generated", "component.core.wasm"))
  };

  return {
    recommendation: "pinned-jco",
    nativeComponentAcceptedByV8: WebAssembly.validate(component),
    generated,
    policy,
    bundleIdentity,
    failures
  };
}

const isMain = process.argv[1] &&
  fileURLToPath(import.meta.url) === process.argv[1];
if (isMain) {
  const root = join(dirname(fileURLToPath(import.meta.url)), "..");
  const report = await evaluate(root);
  process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
  if (report.nativeComponentAcceptedByV8) {
    process.stderr.write("unexpected: V8 accepted a Component Model binary natively\n");
    process.exitCode = 1;
  } else if (report.failures.length > 0) {
    process.exitCode = 1;
  }
}
