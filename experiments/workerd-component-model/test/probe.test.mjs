import assert from "node:assert/strict";
import test from "node:test";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { evaluate } from "../scripts/probe.mjs";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");

test("component decision fixture is pinned, hermetic, and identity-locked", async () => {
  const report = await evaluate(root);

  assert.equal(report.nativeComponentAcceptedByV8, false);
  assert.equal(report.policy.ambientNetwork, false);
  assert.deepEqual(report.policy.storageBindings, []);
  assert.deepEqual(report.policy.componentImports, []);
  assert.match(report.bundleIdentity, /^[0-9a-f]{64}$/);
  assert.deepEqual(report.failures, []);
});
