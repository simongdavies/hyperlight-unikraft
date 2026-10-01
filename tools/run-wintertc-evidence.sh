#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 The Hyperlight Authors.
set -euo pipefail
export HOME="${HOME:-/root}"
export PATH="${HOME}/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

if [[ $# -lt 1 || $# -gt 3 ]]; then
    echo "usage: $0 PACKAGE_DIR [SCRATCH_MIB] [OUTPUT_TAG]" >&2
    exit 2
fi

package_dir="$(realpath "$1")"
scratch_mib="${2:-344}"
output_tag="${3:-${scratch_mib}}"
minimum_free_kib="${WINTERTC_MINIMUM_FREE_KIB:-20971520}"
executor="$package_dir/workerd-sandbox-executor"
handoff="$package_dir/hyperlight-handoff.json"
package_bundle="$package_dir/wintertc-evidence.bundle.json"
repository_bundle="examples/workerd-bundles/wintertc-evidence.json"
workerd_demo_bundle="examples/workerd-bundles/workerd-wintertc-demo.json"
artifact_dir="build-elfloader/workerd-executor"
evidence="build-elfloader/wintertc-evidence-${output_tag}.json"
performance="build-elfloader/wintertc-performance-${output_tag}.json"
stdout_log="build-elfloader/wintertc-evidence-${output_tag}.stdout.json"
stderr_log="build-elfloader/wintertc-evidence-${output_tag}.stderr"

for command in cargo file git jq python3 readelf sha256sum; do
    command -v "$command" >/dev/null || {
        echo "missing required command: $command" >&2
        exit 2
    }
done
available_kib="$(df -Pk . | awk 'NR == 2 { print $4 }')"
test "$available_kib" -ge "$minimum_free_kib" || {
    echo "insufficient free space: ${available_kib} KiB available, ${minimum_free_kib} required" >&2
    exit 2
}
write_probe="$(mktemp .wintertc-write-probe.XXXXXX)"
printf 'writable\n' >"$write_probe"
rm -f "$write_probe"
test -x "$executor"
test -f "$handoff"
test -f "$package_bundle"
test -f "$workerd_demo_bundle"

test "$(sha256sum "$executor" | cut -d' ' -f1)" = \
    "$(jq -r '.executor.sha256' "$handoff")"
test "$(readelf -n "$executor" | sed -n 's/.*Build ID: //p')" = \
    "$(jq -r '.executor.build_id' "$handoff")"
test "$(jq -r '.schema_version' "$handoff")" = 2
build_head="$(jq -r '.build_source.head' "$handoff")"
build_patch="$(jq -r '.build_source.tracked_binary_patch.path' "$handoff")"
build_patch_sha="$(jq -r '.build_source.tracked_binary_patch.sha256' "$handoff")"
build_identity="$(jq -r '.build_source.head_and_patch_identity_sha256' "$handoff")"
test -n "$build_head"
test -f "$package_dir/$(basename "$build_patch")"
test "$(sha256sum "$package_dir/$(basename "$build_patch")" | cut -d' ' -f1)" = \
    "$build_patch_sha"
test "$(
    printf '%s\n%s\n' "$build_head" "$build_patch_sha" | sha256sum | cut -d' ' -f1
)" = "$build_identity"
executor_revision="workerd-head:${build_head},source-identity:${build_identity}"
bundle_sha="$(jq -r '.evidence_bundle.sha256' "$handoff")"
test "$(sha256sum "$package_bundle" | cut -d' ' -f1)" = "$bundle_sha"
test "$(sha256sum "$workerd_demo_bundle" | cut -d' ' -f1)" = "$bundle_sha"

python3 - "$executor" "$handoff" <<'PY'
import json
import subprocess
import sys

executor, handoff_path = sys.argv[1:]
handoff = json.load(open(handoff_path, encoding="utf-8"))
text = subprocess.check_output(["readelf", "-W", "-l", executor], text=True)
segments = []
for line in text.splitlines():
    fields = line.split()
    if fields and fields[0] == "LOAD":
        segments.append((int(fields[2], 16), int(fields[5], 16)))
if not segments:
    raise SystemExit("executor has no PT_LOAD segments")
page_size = 4096
start = min(address for address, _ in segments) // page_size * page_size
end = (
    max(address + size for address, size in segments) + page_size - 1
) // page_size * page_size
span = end - start
expected = handoff["executor"]["pt_load_page_span_bytes"]
print(f"pt_load_span_bytes={span}")
if span != expected:
    raise SystemExit(f"PT_LOAD span mismatch: actual={span} expected={expected}")
PY

bash examples/workerd-executor/build-rootfs.sh "$executor"
sha256sum \
    kernel/workerd_hyperlight-x86_64 \
    "$artifact_dir/executor" \
    "$artifact_dir/rootfs.img" \
    "$repository_bundle" \
    "$workerd_demo_bundle" \
    examples/workerd-bundles/wintertc-evidence-manifest.json \
    >"build-elfloader/wintertc-artifact-sha256-${output_tag}.txt"

export HYPERLIGHT_MAX_SURROGATES="${HYPERLIGHT_MAX_SURROGATES:-4}"
export HYPERLIGHT_INITIAL_SURROGATES="${HYPERLIGHT_INITIAL_SURROGATES:-0}"

cargo run --release --locked --example workerd-memory-probe -- \
    "$scratch_mib" \
    --bundle examples/workerd-bundles/acceptance.json \
    >"build-elfloader/workerd-memory-${output_tag}.json" \
    2>"build-elfloader/workerd-memory-${output_tag}.stderr"
jq -e '.result == "passed"' "build-elfloader/workerd-memory-${output_tag}.json"

set +e
cargo run --release --locked --example wintertc-vm-evidence -- \
    --executor-revision "$executor_revision" \
    --artifact-dir "$artifact_dir" \
    --scratch-mib "$scratch_mib" \
    --output "$evidence" \
    --performance-output "$performance" \
    >"$stdout_log" \
    2>"$stderr_log"
run_status=$?
set -e

test -f "$evidence"
test -f "$performance"
jq -e '.accepted == true' "$evidence"
jq -e '
  ["fetch-policy-denial", "fetch-dns-denial", "fetch-timeout", "fetch-overload"]
  as $required
  | [.matrix[]
      | select(.id as $id | $required | index($id))
      | select(
          .accepted != true
          or (.detail // "" | contains("internal error; reference = "))
          or ([.observations.errors[]?]
              | any(contains("internal error; reference = ")))
        )]
  | length == 0
' "$evidence"
exit "$run_status"
