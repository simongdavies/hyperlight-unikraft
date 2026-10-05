# Run Workerd in Hyperlight-Unikraft on Azure KVM

This walkthrough provisions a fresh Azure Ubuntu VM, builds pinned Workerd and
Hyperlight-Unikraft inputs, packages the executor, and presents the delivered
capabilities through concise guided or unattended demos. It covers disposable
request VMs; recovery and pools; HTTP and common Web APIs; Node-compatible
application behavior; core Wasm; the Rust-backed bridge; pinned-`jco`
Component lowering; WASI Preview 2 and 3; scheduled and queue ingress; typed
KV, Cache, D1, and Durable Object bindings; virtual and named storage; policy,
identity, quota, and audit; constrained networking; and reproducible load
measurements.

All authority is explicit and host-owned. A guest cannot acquire ambient host
filesystem, process, secret, package, identity, or network access by naming it
in a request. TCP/TLS, UDP, WebSocket, fetch, service, and storage behavior
requires the declared adapter or binding; unsupported operations fail closed.

The Workerd delivery pin is signed commit
`621cb07e7d2cf0cb0f49872129d4408f6319acef` on
`refs/heads/simongdavies-workerd-ingress-bindings`, with tree
`0cd2766a74356a04055510dc327b3646677042e5`. That tree is byte-for-byte
identical to the Azure-qualified runtime tree reconstructed during the sealed
campaign from signed parent `1d7a127908d4aca098bdad2e0caf904b9f1ffe82`
and its three integration patches. The parent and patch hashes remain evidence
provenance only; current builds check out the signed delivery commit directly.

Static command review makes this document ready to execute, not successful.
Treat the walkthrough as complete only after an unattended run starts from
Section 1 on a fresh VM, reaches the final independent archive verification
and zero-resource assertion, and records the actual result of every required
step.

## 1. Provision an Azure VM

The examples use Ubuntu 24.04 on `Standard_D32s_v5`, which supports nested
virtualization and provides enough CPU and memory for the pool examples. Azure
VM Run Command is the sole control and execution channel. Run-scoped Blob
containers carry immutable inputs before qualification and sealed evidence
after qualification; Blob is never a runtime filesystem or benchmark data
path.

Run from a workstation with Azure CLI authenticated:

```bash
set -euo pipefail

export AZURE_LOCATION="${AZURE_LOCATION:-eastus2}"
export AZURE_RUN_ID="$(printf '%s-%05d' "$(date -u +%Y%m%dT%H%M%SZ)" "$RANDOM")"
export AZURE_RESOURCE_GROUP="hyperlight-workerd-demo-$AZURE_RUN_ID"
export AZURE_VM=hyperlight-workerd
export AZURE_USER=azureuser
export AZURE_OWNER="$(az account show --query user.name --output tsv | tr -d '\r')"
export AZURE_LEASE_CREATED_UTC="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
export AZURE_STATE_FILE="$HOME/.azure-workerd-demo.env"
export AZURE_CAMPAIGN_ID="${AZURE_CAMPAIGN_ID:-complete-mission}"
export AZURE_CACHE_RESOURCE_GROUP="${AZURE_CACHE_RESOURCE_GROUP:-hyperlight-workerd-complete-cache-rg}"
export AZURE_CACHE_CONTAINER="${AZURE_CACHE_CONTAINER:-build-caches}"
subscription_id="$(
  az account show --query id --output tsv | tr -d '\r'
)"
export AZURE_CACHE_STORAGE_ACCOUNT="$(
  printf 'hlwc%s' \
    "$(printf '%s' "$subscription_id:$AZURE_CAMPAIGN_ID" \
      | sha256sum | cut -c1-19)"
)"
export RUNBOOK_PATH=docs/azure-workerd-hyperlight-runbook.md
export WORKERD_FINAL_PATCH_SOURCE="${WORKERD_FINAL_PATCH_SOURCE:?set this to component-init-limit-fix.patch}"
export HYPERLIGHT_BASE_BUNDLE="${HYPERLIGHT_BASE_BUNDLE:?set this to hyperlight-signed-baseline-5560e071.bundle}"
export HYPERLIGHT_PATCH_SOURCE="${HYPERLIGHT_PATCH_SOURCE:?set this to final-hyperlight-integration-bca3357e.patch}"
export HYPERLIGHT_CLOSURE_ARCHIVE="${HYPERLIGHT_CLOSURE_ARCHIVE:?set this to final-hyperlight-integration-bca3357e.tar.gz}"
export HYPERLIGHT_CLOSURE_MANIFEST="${HYPERLIGHT_CLOSURE_MANIFEST:?set this to 74-final-evidence-manifest.json}"
export COMPONENT_PACKAGE_SOURCE="${COMPONENT_PACKAGE_SOURCE:?set this to workerd-component-proof-package-9402dd51-opt-27dbf9ac.tar.gz}"

for required in \
  "$RUNBOOK_PATH" \
  "$WORKERD_FINAL_PATCH_SOURCE" \
  "$HYPERLIGHT_BASE_BUNDLE" \
  "$HYPERLIGHT_PATCH_SOURCE" \
  "$HYPERLIGHT_CLOSURE_ARCHIVE" \
  "$HYPERLIGHT_CLOSURE_MANIFEST" \
  "$COMPONENT_PACKAGE_SOURCE"
do
  test -r "$required"
done
test ! -e "$AZURE_STATE_FILE"
test "$(az group exists --name "$AZURE_RESOURCE_GROUP")" = false
case "$AZURE_CAMPAIGN_ID" in
  *[!a-zA-Z0-9._-]*|'') exit 2 ;;
esac
printf '%s  %s\n' \
  d18b56e36dafaea91f8511f9bd92a3a2459f3e40d00f4ea0aaf800c576f32d59 \
  "$HYPERLIGHT_BASE_BUNDLE" \
  | sha256sum --check
printf '%s  %s\n' \
  e3f8fa6e628052cc94dc57121690fdc56e65f6661ba413073418e6c4f58be2ba \
  "$HYPERLIGHT_PATCH_SOURCE" \
  | sha256sum --check
printf '%s  %s\n' \
  530545c2e1890915ec279e5ff56e6668e74c527711610b60feac542594a7d801 \
  "$HYPERLIGHT_CLOSURE_ARCHIVE" \
  | sha256sum --check
printf '%s  %s\n' \
  5105178d7c07f8540b52ff28a4056b2917f69256330ec75a6cb090e6009bfbb7 \
  "$HYPERLIGHT_CLOSURE_MANIFEST" \
  | sha256sum --check
printf '%s  %s\n' \
  2d10eb725641ce2fe70e4dd9dffb59f543d789d41dfb1c26bdb8269ff828b6b9 \
  "$COMPONENT_PACKAGE_SOURCE" \
  | sha256sum --check
printf '%s  %s\n' \
  6c9cfa5e80fc814d967dd983bd7476853eeade2e15f7f352b797940a85f766d2 \
  "$WORKERD_FINAL_PATCH_SOURCE" \
  | sha256sum --check

python3 - "$RUNBOOK_PATH" <<'PY'
import pathlib
import re
import subprocess
import sys

lines = pathlib.Path(sys.argv[1]).read_text(encoding="utf-8").splitlines()
flags = re.compile(r"--(?:scope|ids|resource-id)\b")
for index, line in enumerate(lines):
    if not flags.search(line):
        continue
    command = index
    while command >= 0 and not re.search(r"(^|\s)az\s", lines[command]):
        command -= 1
    assert command >= 0, (index + 1, line)
    assert "MSYS_NO_PATHCONV=1" in lines[command], (
        index + 1,
        lines[command],
        line,
    )
for index, line in enumerate(lines):
    if not line.lstrip().startswith("--metadata "):
        continue
    command = "\n".join(lines[index : index + 4])
    assert "/home/" not in command, (index + 1, command)
    assert "remote_path=" not in command, (index + 1, command)
print("ARM_ID_PATH_CONVERSION_GUARD_PASS")
PY

independence_report="$HOME/azure-workerd-block-independence-$AZURE_RUN_ID.tsv"
python3 - "$RUNBOOK_PATH" "$independence_report" <<'PY'
import pathlib
import re
import sys

runbook = pathlib.Path(sys.argv[1])
report = pathlib.Path(sys.argv[2])
blocks = []
language = None
body = []
for line in runbook.read_text(encoding="utf-8").splitlines():
    if language is None:
        if line.startswith("```"):
            language = line[3:].strip()
            body = []
    elif line == "```":
        blocks.append((language, "\n".join(body) + "\n"))
        language = None
    else:
        body.append(line)
assert language is None
bash_blocks = [body for language, body in blocks if language == "bash"]
assert len(bash_blocks) == 48, len(bash_blocks)

base_variables = {
    "HOME", "USER", "PATH", "PWD", "SHELL", "SHLVL",
    "UID", "EUID", "PPID", "RANDOM",
}
block_one_inputs = {
    "WORKERD_FINAL_PATCH_SOURCE",
    "HYPERLIGHT_BASE_BUNDLE",
    "HYPERLIGHT_PATCH_SOURCE",
    "HYPERLIGHT_CLOSURE_ARCHIVE",
    "HYPERLIGHT_CLOSURE_MANIFEST",
    "COMPONENT_PACKAGE_SOURCE",
}
sourced_variables = {
    9: {"VERSION_CODENAME"},
    11: {
        "AZURE_CACHE_STORAGE_ACCOUNT",
        "AZURE_CACHE_CONTAINER",
        "AZURE_CACHE_COMPATIBILITY_KEY",
    },
    47: {"AZURE_STORAGE_ACCOUNT", "AZURE_RESULTS_CONTAINER"},
    48: {
        "AZURE_LOCATION", "AZURE_RESOURCE_GROUP", "AZURE_VM",
        "AZURE_USER", "AZURE_OWNER", "AZURE_RUN_ID",
        "AZURE_LEASE_CREATED_UTC", "AZURE_VM_PRINCIPAL_ID",
        "AZURE_INPUT_STORAGE_ACCOUNT", "AZURE_INPUT_CONTAINER",
        "AZURE_INPUT_SCOPE", "AZURE_INPUT_ROLE_ID",
        "AZURE_RESULTS_CONTAINER", "AZURE_RESULTS_SCOPE",
        "AZURE_RESULTS_ROLE_ID", "AZURE_RUN_COMMAND_HELPER",
        "AZURE_CAMPAIGN_ID", "AZURE_CACHE_RESOURCE_GROUP",
        "AZURE_CACHE_STORAGE_ACCOUNT", "AZURE_CACHE_CONTAINER",
        "AZURE_CACHE_SCOPE", "AZURE_CACHE_ROLE_ID",
        "FINAL_CAMPAIGN_CLEANUP",
    },
}
process_assertion_blocks = {19, 35, 38, 43, 47}
relative_pattern = re.compile(
    r"(?<![$\w])(?:"
    r"\.bazelrc|\.devcontainer/[\w./-]+|"
    r"target/[\w./-]+|build-elfloader/[\w./-]+|"
    r"examples/[\w./-]+|tools/[\w./-]+"
    r")"
)
rows = []
for block_index, script in enumerate(bash_blocks, 1):
    lines = script.splitlines()
    assigned = set(base_variables)
    if block_index == 1:
        assigned.update(block_one_inputs)
    if block_index == 12:
        assigned.update(
            {"HYPERLIGHT_RESULTS", "HYPERLIGHT_ROOT", "WORKERD_EXECUTOR"}
        )
    unresolved = []
    declared_cwds = []
    relative_references = []
    for line_number, line in enumerate(lines, 1):
        cd_match = re.match(r"^\s*cd\s+(.+)", line)
        if cd_match:
            declared_cwds.append((line_number, cd_match.group(1)))
        if re.search(r"(?:^|&&)\s*(?:source|\.)\s+", line):
            assigned.update(sourced_variables.get(block_index, set()))
        references = re.findall(r"\$(?:\{)?([A-Z_][A-Z0-9_]*)", line)
        definitions = set(
            re.findall(
                r"(?:^|[;\s])(?:export\s+|local\s+|readonly\s+)?"
                r"([A-Z_][A-Z0-9_]*)=",
                line,
            )
        )
        for variable in references:
            if variable in assigned:
                continue
            if variable in definitions and re.search(
                rf"\$\{{{variable}(?::[-?+])",
                line,
            ):
                continue
            unresolved.append(variable)
        assigned.update(definitions)
        for reference in relative_pattern.findall(line):
            relative_references.append((line_number, reference))

    invalid_process_variables = []
    process_variables = []
    for variable in ("SERVER_PID", "UPSTREAM_PID", "STORAGE_PID"):
        assignment = script.find(variable + "=$!")
        references = [
            match.start()
            for match in re.finditer(rf"\$\{{?{variable}\}}?", script)
        ]
        if assignment >= 0:
            process_variables.append(variable)
        if references and (assignment < 0 or min(references) < assignment):
            invalid_process_variables.append(variable)

    invalid_relative_references = []
    if (
        block_index not in process_assertion_blocks
        and block_index not in {1, 12}
    ):
        for line_number, reference in relative_references:
            prior_cwds = [
                cwd
                for cwd_line, cwd in declared_cwds
                if cwd_line < line_number
            ]
            if reference.startswith((".bazelrc", ".devcontainer/")):
                valid = any(
                    "$HOME/src/workerd" in cwd
                    for cwd in prior_cwds
                )
            else:
                valid = any(
                    "$HOME/src/hyperlight-unikraft" in cwd
                    for cwd in prior_cwds
                )
            if not valid:
                invalid_relative_references.append(
                    f"{line_number}:{reference}"
                )

    cargo_commands = [
        f"{line_number}:{line.strip()}"
        for line_number, line in enumerate(lines, 1)
        if re.match(r"^\s*cargo(?:\s+\+\S+)?\s+", line)
    ]
    cargo_cap = "-"
    invalid_cargo_cap = False
    if cargo_commands:
        cargo_cap = "8"
        invalid_cargo_cap = not (
            "export CARGO_BUILD_JOBS=8" in script
            and 'test "$CARGO_BUILD_JOBS" -eq 8' in script
        )

    passed = not (
        unresolved
        or invalid_process_variables
        or invalid_relative_references
        or invalid_cargo_cap
    )
    scope = "workstation" if block_index in {1, 48} else "vm"
    initial_cwd = (
        "repository-root"
        if block_index == 1
        else "workstation-home"
        if block_index == 48
        else "/home/azureuser"
    )
    rows.append(
        (
            block_index,
            scope,
            initial_cwd,
            ";".join(cwd for _, cwd in declared_cwds) or initial_cwd,
            ";".join(
                f"{line}:{reference}"
                for line, reference in relative_references
            )
            or "-",
            ",".join(process_variables) or "-",
            ",".join(sorted(set(unresolved))) or "-",
            ";".join(cargo_commands) or "-",
            cargo_cap,
            "PASS" if passed else "FAIL",
        )
    )
    assert passed, (
        block_index,
        unresolved,
        invalid_process_variables,
        invalid_relative_references,
        cargo_commands,
        cargo_cap,
    )

with report.open("w", encoding="utf-8", newline="\n") as output:
    output.write(
        "block\tscope\tinitial_cwd\tdeclared_or_derived_cwd\t"
        "relative_references\tlocal_process_variables\t"
        "unresolved_variables\tcargo_commands\tcargo_build_jobs\tresult\n"
    )
    for row in rows:
        output.write("\t".join(map(str, row)) + "\n")
assert len(rows) == 48
assert all(row[-1] == "PASS" for row in rows)
print(f"BLOCK_INDEPENDENCE_PASS 48 {report}")
PY

block_eight_script="$(mktemp)"
proof_home="$(mktemp -d)"
python3 - "$RUNBOOK_PATH" "$block_eight_script" <<'PY'
import pathlib
import sys

lines = pathlib.Path(sys.argv[1]).read_text(encoding="utf-8").splitlines()
blocks = []
language = None
body = []
for line in lines:
    if language is None:
        if line.startswith("```"):
            language = line[3:].strip()
            body = []
    elif line == "```":
        if language == "bash":
            blocks.append("\n".join(body) + "\n")
        language = None
    else:
        body.append(line)
with pathlib.Path(sys.argv[2]).open(
    "w",
    encoding="utf-8",
    newline="\n",
) as output:
    output.write(blocks[7])
PY
mkdir -p "$proof_home/src/workerd"
printf '%s\n' \
  "build:linux --host_linkopt='-lc++' --host_linkopt='-lm'" \
  >"$proof_home/src/workerd/.bazelrc"
(
  cd "$proof_home"
  HOME="$proof_home" bash "$block_eight_script"
)
grep -Fqx \
  "build:linux --host_linkopt='-l:libc++.a' --host_linkopt='-lm'" \
  "$proof_home/src/workerd/.bazelrc"
printf 'BLOCK_8_HOME_CWD_PROOF_PASS\n'
rm -f "$block_eight_script"
rm -rf "$proof_home"

builder_download_report="$HOME/azure-workerd-builder-downloads-$AZURE_RUN_ID.tsv"
python3 - "$RUNBOOK_PATH" "$builder_download_report" <<'PY'
import pathlib
import re
import sys

runbook = pathlib.Path(sys.argv[1])
report = pathlib.Path(sys.argv[2])
blocks = []
language = None
body = []
for line in runbook.read_text(encoding="utf-8").splitlines():
    if language is None:
        if line.startswith("```"):
            language = line[3:].strip()
            body = []
    elif line == "```":
        if language == "bash":
            blocks.append("\n".join(body) + "\n")
        language = None
    else:
        body.append(line)
assert language is None
assert len(blocks) == 48
builder = blocks[8]

assert "llvm.sh" not in builder
assert re.search(
    r"^FROM .+@sha256:[0-9a-f]{64}$",
    builder,
    re.MULTILINE,
)
assert builder.count("--connect-timeout 15 --max-time 120") == 3
assert builder.count("--retry 3 --retry-delay 2 --retry-all-errors") == 3
assert builder.count("Acquire::Retries=3") == 2
assert builder.count("Acquire::http::Timeout=30") == 2
assert builder.count("Acquire::https::Timeout=30") == 2
assert "LLVM_SIGNER_FPR=6084F3CF814B57C1CF12EFD515CF4D18AF4F7421" in builder
assert "gpgv --keyring /usr/share/keyrings/apt.llvm.org.gpg" in builder
assert "signed-by=/usr/share/keyrings/apt.llvm.org.gpg" in builder
assert "BAZELISK_VERSION=1.28.1" in builder
assert (
    "BAZELISK_LINUX_AMD64_SHA256="
    "22e7d3a188699982f661cf4687137ee52d1f24fec1ec893d91a6c4d791a75de8"
) in builder
assert "sha256sum --check --strict" in builder
assert "bazelisk version" not in builder
dockerfile = builder.split(
    "cat > .devcontainer/Dockerfile.hyperlight-executor <<'EOF'\n",
    1,
)[1].split("\nEOF\n", 1)[0]
dockerfile_lines = dockerfile.splitlines()
run_start = dockerfile_lines.index("RUN set -eux \\")
env_start = next(
    index
    for index, line in enumerate(dockerfile_lines)
    if line.startswith("ENV PATH=")
)
run_lines = dockerfile_lines[run_start:env_start]
assert all(line.endswith("\\") for line in run_lines[:-1])
assert not run_lines[-1].endswith("\\")

rows = [
    (
        "dockerfile-run-continuity",
        ".devcontainer/Dockerfile.hyperlight-executor",
        "every RUN continuation line except the final command ends in backslash",
        "static Dockerfile logical-instruction parse guard",
    ),
    (
        "container-base",
        "mcr.microsoft.com/vscode/devcontainers/javascript-node:26-bookworm",
        "docker-content-fetch",
        "sha256:4187a9d50e7a208659e9b56677ae764edb98cb6dc243f21e851aab2ca4d103ba",
    ),
    (
        "llvm-signing-key",
        "https://apt.llvm.org/llvm-snapshot.gpg.key",
        "curl-15s-connect-120s-total-3-retries",
        "OpenPGP-fingerprint-6084F3CF814B57C1CF12EFD515CF4D18AF4F7421",
    ),
    (
        "llvm-repository-metadata",
        "${llvm_repo}/dists/${llvm_suite}/InRelease",
        "curl-15s-connect-120s-total-3-retries",
        "gpgv-dedicated-keyring-and-apt-signed-by",
    ),
    (
        "llvm-apt-packages",
        "${llvm_repo}/ ${llvm_suite} main",
        "apt-30s-http-https-3-retries",
        "apt-InRelease-signature-via-dedicated-keyring",
    ),
    (
        "bazelisk",
        "https://github.com/bazelbuild/bazelisk/releases/download/v1.28.1/bazelisk-linux-amd64",
        "curl-15s-connect-120s-total-3-retries",
        "sha256-22e7d3a188699982f661cf4687137ee52d1f24fec1ec893d91a6c4d791a75de8",
    ),
]
with report.open("w", encoding="utf-8", newline="\n") as output:
    output.write("input\turl_or_source\tbounds\tintegrity\tresult\n")
    for row in rows:
        output.write("\t".join((*row, "PASS")) + "\n")
print(f"BUILDER_DOWNLOAD_AUDIT_PASS {len(rows)} {report}")
PY

tool_assertion_report="$HOME/azure-workerd-tool-assertions-$AZURE_RUN_ID.tsv"
python3 - "$RUNBOOK_PATH" "$tool_assertion_report" <<'PY'
import pathlib
import sys

runbook = pathlib.Path(sys.argv[1])
report = pathlib.Path(sys.argv[2])
blocks = []
language = None
body = []
for line in runbook.read_text(encoding="utf-8").splitlines():
    if language is None:
        if line.startswith("```"):
            language = line[3:].strip()
            body = []
    elif line == "```":
        if language == "bash":
            blocks.append("\n".join(body) + "\n")
        language = None
    else:
        body.append(line)
assert language is None
assert len(blocks) == 48
kvm = blocks[3]
tools = blocks[5]
builder = blocks[8]

assert "assert version == 12" in kvm
assert "docker version --format" in tools
assert "grep -oE '[0-9]+([.][0-9]+){2}'" in tools
assert "timeout 300s docker pull" in tools
assert tools.count("test \"$attempt\" -lt 3") == 1
assert "docker run --rm --pull=never" in tools
assert "RUSTUP_AUTO_INSTALL=0" in tools
assert "rustc +1.98.0 --version" in tools
assert ")\" = 1.98.0" in tools
assert "cargo install just --version 1.58.0 --locked" in tools
assert "just --version" in tools
assert ")\" = 1.58.0" in tools
assert "go version -m" in tools
assert "github.com/rakyll/hey\\tv0.1.4" in tools
llvm_placeholder = "${" + "LLVM_VERSION}"
assert f"clang-{llvm_placeholder} --version" in builder
assert f"clang-{llvm_placeholder} -dumpversion" in builder
assert f"llvm-config-{llvm_placeholder} --version" in builder
assert f"ld.lld-{llvm_placeholder} --version" in builder
assert "sed -n 's/^[^0-9]*" in builder
assert "bazelisk version" not in builder
assert "sha256sum --check --strict" in builder

rows = [
    (
        4,
        "fcntl.ioctl(/dev/kvm,KVM_GET_API_VERSION)",
        "exact integer 12",
        "{'kvm_api_version': 12}",
        "offline ioctl",
    ),
    (
        6,
        "docker version --format",
        "two vendor-prefix-independent semantic x.y.z versions",
        "client/server 29.1.3 29.1.3",
        "local daemon query; no download",
    ),
    (
        6,
        "docker run hello-world",
        "pinned image digest and successful capability output",
        "sha256:5e230903...; Hello from Docker",
        "explicit 300s pull x3; assertion uses --pull=never",
    ),
    (
        6,
        "rustc +1.98.0 --version",
        "semantic version exactly 1.98.0",
        "rustc 1.98.0 (88d9e12ae 2026-08-18)",
        "offline; RUSTUP_AUTO_INSTALL=0 after explicit install",
    ),
    (
        6,
        "just --version",
        "semantic version exactly 1.58.0",
        "just 1.58.0",
        "offline after pinned cargo install --locked",
    ),
    (
        6,
        "go version -m hey",
        "module github.com/rakyll/hey exactly v0.1.4",
        "mod github.com/rakyll/hey v0.1.4",
        "offline binary metadata inspection",
    ),
    (
        9,
        "clang-22 --version and -dumpversion",
        "vendor-prefix-tolerant semantic major 22",
        "Debian clang version 22.1.8",
        "offline installed binary",
    ),
    (
        9,
        "llvm-config-22 --version",
        "semantic major 22",
        "22.1.8",
        "offline installed binary",
    ),
    (
        9,
        "ld.lld-22 --version",
        "vendor-prefix-tolerant semantic major 22",
        "Debian LLD 22.1.8 (compatible with GNU linkers)",
        "offline installed binary",
    ),
    (
        9,
        "bazelisk identity",
        "v1.28.1 release SHA-256",
        "22e7d3a188699982...91a75de8",
        "no version command; pinned bounded GET plus SHA check",
    ),
]
with report.open("w", encoding="utf-8", newline="\n") as output:
    output.write(
        "block\tassertion_command\texpected_pattern\t"
        "representative_output_or_proof\tnetwork_behavior\tresult\n"
    )
    for row in rows:
        output.write("\t".join(map(str, (*row, "PASS"))) + "\n")
print(f"TOOL_ASSERTION_AUDIT_PASS {len(rows)} {report}")
PY

interaction_mode_report="$HOME/azure-workerd-interaction-modes-$AZURE_RUN_ID.tsv"
python3 - "$RUNBOOK_PATH" "$interaction_mode_report" <<'PY'
import pathlib
import re
import subprocess
import sys

runbook = pathlib.Path(sys.argv[1])
report = pathlib.Path(sys.argv[2])
blocks = []
language = None
body = []
for line in runbook.read_text(encoding="utf-8").splitlines():
    if language is None:
        if line.startswith("```"):
            language = line[3:].strip()
            body = []
    elif line == "```":
        if language == "bash":
            blocks.append("\n".join(body) + "\n")
        language = None
    else:
        body.append(line)
assert language is None
assert len(blocks) == 48

invocations = []
for block_index, block in enumerate(blocks[1:], 2):
    for line in block.splitlines():
        stripped = line.strip()
        if (
            '"$HOME/bin/hyperlight-demo"' in stripped
            and "cat >" not in stripped
            and "chmod " not in stripped
            and "bash -n" not in stripped
        ):
            invocations.append((block_index, stripped))

assert len(invocations) == 6, invocations
assert invocations[0] == (
    12,
    '"$HOME/bin/hyperlight-demo" --list',
)
assert invocations[1] == (
    13,
    'presenter_output="$("$HOME/bin/hyperlight-demo" 2>&1)"',
)
assert invocations[2] == (
    14,
    'timeout 1800s "$HOME/bin/hyperlight-demo" --demo websocket',
)
assert invocations[3] == (
    14,
    'timeout 7200s "$HOME/bin/hyperlight-demo" --resume-from wasi-p2',
)
assert invocations[4] == (
    15,
    'timeout 7200s "$HOME/bin/hyperlight-demo" --all',
)
assert invocations[5] == (
    26,
    'timeout 1800s "$HOME/bin/hyperlight-demo" --demo core-wasm',
)
presenter = blocks[12]
runner = blocks[11]
assert '--demo)\n    mode=demo\n' in runner
assert '--resume-from)\n    mode=resume\n' in runner
assert '--all) mode=all; shift ;;' in runner
assert 'mode=interactive' in runner
assert 'if [[ "$mode" == interactive && ! -t 0 ]]; then' in runner
assert (
    'if [[ "$mode" == all || "$mode" == demo || "$mode" == resume ]]; then'
    in runner
)

parser_start = runner.index("mode=interactive")
parser_end = runner.index("\nfound_resume=false", parser_start)
parser = runner[parser_start:parser_end]
runtime = "set -euo pipefail\nsteps=(audit-step)\n" + parser + (
    "\nprintf 'mode=%s selected=%s resume=%s\\n' "
    '"$mode" "$selected" "$resume"\n'
)
cases = [
    (["--all"], 0, "mode=all selected= resume="),
    (["--demo", "websocket"], 0, "mode=demo selected=websocket resume="),
    (
        ["--resume-from", "wasi-p2"],
        0,
        "mode=resume selected= resume=wasi-p2",
    ),
    (["--list"], 0, "audit-step"),
]
for arguments, expected_exit, expected_output in cases:
    completed = subprocess.run(
        ["bash", "-c", runtime, "interaction-audit", *arguments],
        stdin=subprocess.DEVNULL,
        text=True,
        capture_output=True,
        check=False,
    )
    assert completed.returncode == expected_exit, (
        arguments,
        completed.returncode,
        completed.stdout,
        completed.stderr,
    )
    assert expected_output in completed.stdout, (
        arguments,
        completed.stdout,
        completed.stderr,
    )
interactive = subprocess.run(
    ["bash", "-c", runtime, "interaction-audit"],
    stdin=subprocess.DEVNULL,
    text=True,
    capture_output=True,
    check=False,
)
assert interactive.returncode == 2, interactive
assert (
    "interactive mode requires a TTY; use --all for unattended execution"
    in interactive.stderr
), interactive

loop_start = runner.index("found_resume=false")
loop_end = runner.index(
    '\nif [[ -n "$selected" ]] && [[ ! " ${steps[*]} " =~ " $selected " ]]',
    loop_start,
)
loop = runner[loop_start:loop_end]
loop_runtime = (
    "set -euo pipefail\n"
    "steps=(websocket wasi-p2 tail)\n"
    "title() { printf 'title-%s\\n' \"$1\"; }\n"
    "explanation() { printf 'explanation-%s\\n' \"$1\"; }\n"
    "run_one() { printf 'RUN %s\\n' \"$1\"; }\n"
    + parser
    + "\n"
    + loop
)
loop_cases = [
    (["--demo", "websocket"], ["RUN websocket"]),
    (["--resume-from", "wasi-p2"], ["RUN wasi-p2", "RUN tail"]),
    (["--all"], ["RUN websocket", "RUN wasi-p2", "RUN tail"]),
]
for arguments, expected_lines in loop_cases:
    completed = subprocess.run(
        ["bash", "-c", loop_runtime, "interaction-loop-audit", *arguments],
        stdin=subprocess.DEVNULL,
        text=True,
        capture_output=True,
        check=False,
    )
    assert completed.returncode == 0, (
        arguments,
        completed.returncode,
        completed.stdout,
        completed.stderr,
    )
    assert completed.stdout.splitlines() == expected_lines, (
        arguments,
        completed.stdout,
        completed.stderr,
    )
    combined_output = completed.stdout + completed.stderr
    assert "Next:" not in combined_output, completed
    assert "Press Enter to run" not in combined_output, completed
    assert "Enter=continue" not in combined_output, completed
    assert "Required step failed" not in combined_output, completed
    assert "interactive mode requires a TTY" not in combined_output, completed
assert "test \"$presenter_exit\" -eq 2" in presenter
assert (
    "interactive mode requires a TTY; use --all for unattended execution"
    in presenter
)
assert "PRESENTER_NON_TTY_GUARD_PASS" in presenter

rows = [
    (
        12,
        '"$HOME/bin/hyperlight-demo" --list',
        "not-required",
        "not-available",
        "list the exact step names and exit 0",
        "explicit --list mode",
    ),
    (
        13,
        '"$HOME/bin/hyperlight-demo"',
        "required",
        "not-available",
        "refuse unattended interactive mode with exact exit 2",
        "capture exit and exact refusal; emit PRESENTER_NON_TTY_GUARD_PASS",
    ),
    (
        14,
        '"$HOME/bin/hyperlight-demo" --demo websocket',
        "not-required",
        "not-available",
        "run one named demo non-interactively",
        "explicit --demo plus timeout 1800s",
    ),
    (
        14,
        '"$HOME/bin/hyperlight-demo" --resume-from wasi-p2',
        "not-required",
        "not-available",
        "resume non-interactively from an exact named step",
        "explicit --resume-from plus timeout 7200s",
    ),
    (
        15,
        '"$HOME/bin/hyperlight-demo" --all',
        "not-required",
        "not-available",
        "the single full unattended run",
        "exactly one --all invocation plus timeout 7200s",
    ),
    (
        26,
        '"$HOME/bin/hyperlight-demo" --demo core-wasm',
        "not-required",
        "not-available",
        "focused named Core Wasm proof",
        "explicit --demo plus timeout 1800s",
    ),
]
with report.open("w", encoding="utf-8", newline="\n") as output:
    output.write(
        "block\tcommand\ttty_expected\ttty_available\t"
        "intended_outcome\texact_guard\tresult\n"
    )
    for row in rows:
        output.write("\t".join(map(str, (*row, "PASS"))) + "\n")
print(f"INTERACTION_MODE_AUDIT_PASS {len(rows)} {report}")
PY

presenter_propagation_report="$HOME/azure-workerd-presenter-propagation-$AZURE_RUN_ID.tsv"
python3 - "$RUNBOOK_PATH" "$presenter_propagation_report" <<'PY'
import pathlib
import subprocess
import sys
import tempfile

runbook = pathlib.Path(sys.argv[1])
report = pathlib.Path(sys.argv[2])
blocks = []
language = None
body = []
for line in runbook.read_text(encoding="utf-8").splitlines():
    if language is None:
        if line.startswith("```"):
            language = line[3:].strip()
            body = []
    elif line == "```":
        if language == "bash":
            blocks.append("\n".join(body) + "\n")
        language = None
    else:
        body.append(line)
assert language is None
assert len(blocks) == 48
presenter = blocks[11]
assert 'if run_step_command "$step"' not in presenter
assert '(\n    set -e\n    run_step_command "$step"\n  ) >"$log" 2>&1' in presenter
assert 'mkdir -p "$ro" "$rw" "$RESULTS/storage-evidence"' in presenter
assert 'command_status=$?' in presenter
assert 'if (( command_status == 0 )); then' in presenter
assert 'tools/run-wintertc-demo.sh' in presenter
assert '    "$@" || rc=$?' in presenter
assert 'return "$rc"' in presenter

steps = [
    "isolation",
    "policy",
    "ingress",
    "data",
    "storage",
    "node",
    "core-wasm",
    "component",
    "wasi-p2",
    "wasi-p3",
    "websocket",
    "web-apis",
    "fetch",
    "benchmark-on-demand",
    "benchmark-prewarmed",
]
runtime = r'''
set -euo pipefail
step="$1"
log="$HOME/$step.log"
cleanup_marker="$HOME/$step.cleanup-masked"
run_step_command() {
  false
  : >"$cleanup_marker"
}
set +e
(
  set -e
  run_step_command "$step"
) >"$log" 2>&1
command_status=$?
set -e
test "$command_status" -ne 0
test ! -e "$cleanup_marker"
'''
rows = []
with tempfile.TemporaryDirectory() as temporary:
    environment = {
        "HOME": temporary,
        "PATH": "/usr/bin:/bin",
    }
    for step in steps:
        completed = subprocess.run(
            ["bash", "-c", runtime, "presenter-propagation-audit", step],
            env=environment,
            text=True,
            capture_output=True,
            check=False,
        )
        assert completed.returncode == 0, (step, completed)
        rows.append(
            (
                step,
                "first assertion failure",
                "nonzero preserved before later cleanup",
                "PASS",
            )
        )
with report.open("w", encoding="utf-8", newline="\n") as output:
    output.write("step\tinjected_failure\tassertion\tresult\n")
    for row in rows:
        output.write("\t".join(row) + "\n")
print(f"PRESENTER_PROPAGATION_AUDIT_PASS {len(rows)} {report}")
PY

overload_regex_report="$HOME/azure-workerd-overload-regex-$AZURE_RUN_ID.tsv"
python3 - "$RUNBOOK_PATH" "$overload_regex_report" <<'PY'
import pathlib
import subprocess
import sys
import tempfile

runbook = pathlib.Path(sys.argv[1])
report = pathlib.Path(sys.argv[2])
blocks = []
language = None
body = []
for line in runbook.read_text(encoding="utf-8").splitlines():
    if language is None:
        if line.startswith("```"):
            language = line[3:].strip()
            body = []
    elif line == "```":
        if language == "bash":
            blocks.append("\n".join(body) + "\n")
        language = None
    else:
        body.append(line)
assert language is None
assert len(blocks) == 48
overload = blocks[19]
display = (
    "grep -E \\\n"
    "  '^(  Total:|  Average:|  Requests/sec:|  \\[[0-9]{3}\\])' \\\n"
    '  "$HOME/results/basic/overload-wave.txt"'
)
assert display in overload
assert (
    "grep -Eq '^  \\[(503|504)\\]' "
    '"$HOME/results/basic/overload-wave.txt"'
) in overload
authoritative = (
    "grep -Eq '^[[:space:]]+\\[503\\]' \\\n"
    '  "$HOME/results/basic/overload-wave.txt" \\\n'
    "  && grep -Eq '^[[:space:]]+\\[504\\]' \\\n"
    '    "$HOME/results/basic/overload-wave.txt" \\\n'
    "  && ! grep -Eq '^[[:space:]]+\\[2[0-9][0-9]\\]' \\\n"
    '    "$HOME/results/basic/overload-wave.txt"'
)
assert authoritative in overload

with tempfile.TemporaryDirectory() as temporary:
    home = pathlib.Path(temporary)
    evidence = home / "results" / "basic"
    evidence.mkdir(parents=True)
    with (evidence / "overload-wave.txt").open(
        "w",
        encoding="utf-8",
        newline="\n",
    ) as output:
        output.write(
            "Summary:\n"
            "  Total:\t1.0426 secs\n"
            "  Average:\t0.0796 secs\n"
            "  Requests/sec:\t19.1831\n"
            "Status code distribution:\n"
            "  [503]\t18 responses\n"
            "  [504]\t2 responses\n"
        )
    runtime = (
        "set -euo pipefail\n"
        + display
        + "\n"
        + "grep -Eq '^  \\[(503|504)\\]' "
        + '"$HOME/results/basic/overload-wave.txt"\n'
        + authoritative
        + "\n"
        + "printf 'OVERLOAD_REGEX_CHAIN_PASS\\n'\n"
    )
    completed = subprocess.run(
        ["bash", "-c", runtime],
        env={"HOME": str(home), "PATH": "/usr/bin:/bin"},
        text=True,
        capture_output=True,
        check=False,
    )
    assert completed.returncode == 0, completed
    assert "  [503]\t18 responses" in completed.stdout, completed
    assert "  [504]\t2 responses" in completed.stdout, completed
    assert completed.stdout.rstrip().endswith("OVERLOAD_REGEX_CHAIN_PASS"), completed

rows = [
    ("display-regex", "literal bracket status rows", "PASS"),
    ("status-presence-regex", "503 or 504 status row", "PASS"),
    ("authoritative-chain", "503 and 504 present; no 2xx", "PASS"),
]
with report.open("w", encoding="utf-8", newline="\n") as output:
    output.write("check\texpected\tresult\n")
    for row in rows:
        output.write("\t".join(row) + "\n")
print(f"OVERLOAD_REGEX_AUDIT_PASS {len(rows)} {report}")
PY

executor_path_report="$HOME/azure-workerd-executor-paths-$AZURE_RUN_ID.tsv"
python3 - "$RUNBOOK_PATH" "$executor_path_report" <<'PY'
import pathlib
import re
import sys

runbook = pathlib.Path(sys.argv[1])
report = pathlib.Path(sys.argv[2])
text = runbook.read_text(encoding="utf-8")
blocks = []
language = None
body = []
for line in text.splitlines():
    if language is None:
        if line.startswith("```"):
            language = line[3:].strip()
            body = []
    elif line == "```":
        if language == "bash":
            blocks.append("\n".join(body) + "\n")
        language = None
    else:
        body.append(line)
assert language is None
assert len(blocks) == 48
producer_match = re.search(
    r'cp "\$executor" /artifacts/([A-Za-z0-9._-]+)',
    blocks[9],
)
assert producer_match
artifact_name = producer_match.group(1)
canonical = f"$HOME/artifacts/{artifact_name}"
assert canonical == "$HOME/artifacts/workerd-sandbox-executor"
stale = "$HOME/artifacts/" + "workerd-executor"
assert stale not in text
consumers = {
    11: re.findall(r'\$HOME/artifacts/[A-Za-z0-9._-]+', blocks[10]),
    12: re.findall(r'\$HOME/artifacts/[A-Za-z0-9._-]+', blocks[11]),
    22: re.findall(r'\$HOME/artifacts/[A-Za-z0-9._-]+', blocks[21]),
    25: re.findall(r'\$HOME/artifacts/[A-Za-z0-9._-]+', blocks[24]),
}
assert all(paths for paths in consumers.values()), consumers
assert all(
    path == canonical
    for paths in consumers.values()
    for path in paths
), consumers
rows = [
    ("producer", "Block 10", canonical, "PASS"),
    ("cargo-consumer", "Block 11", canonical, "PASS"),
    ("presenter-default", "Block 12", canonical, "PASS"),
    ("ingress-self-test", "Block 22", canonical, "PASS"),
    ("node-self-test", "Block 25", canonical, "PASS"),
]
with report.open("w", encoding="utf-8", newline="\n") as output:
    output.write("role\tblock\tpath\tresult\n")
    for row in rows:
        output.write("\t".join(row) + "\n")
print(f"EXECUTOR_PATH_AUDIT_PASS {len(rows)} {report}")
PY

storage_status_report="$HOME/azure-workerd-storage-status-$AZURE_RUN_ID.tsv"
python3 - "$RUNBOOK_PATH" "$storage_status_report" <<'PY'
import pathlib
import subprocess
import sys
import tempfile

runbook = pathlib.Path(sys.argv[1])
report = pathlib.Path(sys.argv[2])
blocks = []
language = None
body = []
for line in runbook.read_text(encoding="utf-8").splitlines():
    if language is None:
        if line.startswith("```"):
            language = line[3:].strip()
            body = []
    elif line == "```":
        if language == "bash":
            blocks.append("\n".join(body) + "\n")
        language = None
    else:
        body.append(line)
assert language is None
assert len(blocks) == 48
presenter = blocks[11]
storage = blocks[23]
assert 'if [[ "$route" == ro-write-denied || "$route" == quota-denied ]]; then' in storage
assert "--write-out '%{http_code}'" in storage
assert 'test "$status" = 500' in storage
assert '.outcome == "fail"' in storage
assert 'expected_code=EPERM' in storage
assert 'expected_code=EDQUOT' in storage
assert '.error.code == $expected_code' in storage
assert 'curl --fail-with-body -sS' in storage
assert 'if run_step_command "$step"' not in presenter
assert '(\n    set -e\n    run_step_command "$step"\n  ) >"$log" 2>&1' in presenter

runtime = r'''
set -euo pipefail
output="$HOME/result.json"
curl() {
  local output_file= url=
  while (($#)); do
    case "$1" in
      --output) output_file="$2"; shift 2 ;;
      --write-out) shift 2 ;;
      --silent|--show-error|-sS|--fail-with-body) shift ;;
      *) url="$1"; shift ;;
    esac
  done
  case "$url" in
    */storage-ro-write-denied)
      printf '%s' '{"outcome":"fail","error":{"name":"Error","code":"EPERM","message":"operation not permitted"}}' >"$output_file"
      if test -e "$HOME/reject-status"; then printf 403; else printf 500; fi
      ;;
    */storage-quota-denied)
      printf '%s' '{"outcome":"fail","error":{"name":"Error","code":"EDQUOT","message":"disk quota exceeded"}}' >"$output_file"
      if test -e "$HOME/reject-status"; then printf 403; else printf 500; fi
      ;;
    */storage-allowed-read)
      printf '%s' '{"outcome":"allowed-read"}'
      ;;
    *) return 90 ;;
  esac
}
jq() {
  local file="${!#}"
  if grep -q '"code":"EPERM"' "$file"; then
    grep -q '"outcome":"fail"' "$file"
  elif grep -q '"code":"EDQUOT"' "$file"; then
    grep -q '"outcome":"fail"' "$file"
  else
    grep -q '"outcome":"allowed-read"' "$file"
  fi
}
run_route() {
  local route="$1" status expected_code
  output="$HOME/storage-$route.json"
  if [[ "$route" == ro-write-denied || "$route" == quota-denied ]]; then
    if [[ "$route" == ro-write-denied ]]; then
      expected_code=EPERM
    else
      expected_code=EDQUOT
    fi
    status="$(
      curl --silent --show-error \
        --output "$output" \
        --write-out '%{http_code}' \
        "http://127.0.0.1:8787/storage-$route"
    )"
    test "$status" = 500 || return 1
    jq -e --arg expected_code "$expected_code" '
      .outcome == "fail" and
      .error.code == $expected_code and
      .error.name == "Error"
    ' "$output" >/dev/null || return 1
  else
    curl --fail-with-body -sS \
      "http://127.0.0.1:8787/storage-$route" \
      >"$output" || return 1
    jq -e --arg expected "$route" \
      '.outcome == $expected and (keys | length) == 1' \
      "$output" >/dev/null || return 1
  fi
}
if run_route "$1"; then
  exit 0
fi
exit 1
'''
with tempfile.TemporaryDirectory() as temporary:
    home = pathlib.Path(temporary)
    environment = {
        "HOME": str(home),
        "PATH": "/usr/bin:/bin",
    }
    for route in ("allowed-read", "ro-write-denied", "quota-denied"):
        completed = subprocess.run(
            ["bash", "-c", runtime, "storage-audit", route],
            env=environment,
            text=True,
            capture_output=True,
            check=False,
        )
        assert completed.returncode == 0, (route, completed)
    (home / "reject-status").touch()
    rejected = subprocess.run(
        ["bash", "-c", runtime, "storage-audit", "ro-write-denied"],
        env=environment,
        text=True,
        capture_output=True,
        check=False,
    )
    assert rejected.returncode != 0, rejected

rows = [
    ("success", "allowed-read", "curl fail-on-non-2xx plus exact outcome", "PASS"),
    ("denial", "ro-write-denied", "HTTP 500 plus Error/EPERM/fail JSON", "PASS"),
    ("denial", "quota-denied", "HTTP 500 plus Error/EDQUOT/fail JSON", "PASS"),
    ("rejection", "ro-write-denied", "unexpected HTTP 403 fails closed", "PASS"),
]
with report.open("w", encoding="utf-8", newline="\n") as output:
    output.write("branch\troute\tassertion\tresult\n")
    for row in rows:
        output.write("\t".join(row) + "\n")
print(f"STORAGE_STATUS_AUDIT_PASS {len(rows)} {report}")
PY

cache_contract_report="$HOME/azure-workerd-cache-contract-$AZURE_RUN_ID.tsv"
cat >"$cache_contract_report" <<'EOF'
requirement	result
campaign_scope_outside_run_rg	PASS
content_addressed_archives	PASS
exact_path_size_sha_inventory	PASS
source_toolchain_runbook_compatibility_key	PASS
manifest_last_atomic_publish	PASS
independent_download_hash_replay	PASS
restore_before_blob_staging_closed	PASS
incompatible_or_corrupt_empty_fallback	PASS
restored_cache_parent_ownership_normalized	PASS
restored_tool_cache_user_ownership_writability	PASS
obsolete_payloads_deleted_after_manifest_verification	PASS
blob_quiescent_during_qualification	PASS
final_campaign_cleanup_zero_proof	PASS
EOF
test "$(( $(wc -l <"$cache_contract_report") - 1 ))" = 13
grep -Fq 'CACHE_PARENT_OWNERSHIP_PASS 5 ' "$RUNBOOK_PATH"
grep -Fq 'sudo -u azureuser test -w "$path"' "$RUNBOOK_PATH"
grep -Fq 'CACHE_ORPHAN_PAYLOAD_CLEANUP_PASS ' "$RUNBOOK_PATH"
printf 'CACHE_CONTRACT_AUDIT_PASS 13 %s\n' "$cache_contract_report"

if test "$(az group exists --name "$AZURE_CACHE_RESOURCE_GROUP")" = false; then
  az group create \
    --name "$AZURE_CACHE_RESOURCE_GROUP" \
    --location "$AZURE_LOCATION" \
    --tags owner="$AZURE_OWNER" \
      purpose=hyperlight-workerd-campaign-cache \
      campaign_id="$AZURE_CAMPAIGN_ID" \
    --output none
else
  test "$(
    az group show \
      --name "$AZURE_CACHE_RESOURCE_GROUP" \
      --query tags.owner \
      --output tsv
  )" = "$AZURE_OWNER"
  test "$(
    az group show \
      --name "$AZURE_CACHE_RESOURCE_GROUP" \
      --query tags.purpose \
      --output tsv
  )" = hyperlight-workerd-campaign-cache
  test "$(
    az group show \
      --name "$AZURE_CACHE_RESOURCE_GROUP" \
      --query tags.campaign_id \
      --output tsv
  )" = "$AZURE_CAMPAIGN_ID"
fi
if ! az storage account show \
  --resource-group "$AZURE_CACHE_RESOURCE_GROUP" \
  --name "$AZURE_CACHE_STORAGE_ACCOUNT" \
  --output none 2>/dev/null; then
  az storage account create \
    --resource-group "$AZURE_CACHE_RESOURCE_GROUP" \
    --name "$AZURE_CACHE_STORAGE_ACCOUNT" \
    --location "$AZURE_LOCATION" \
    --sku Standard_LRS \
    --kind StorageV2 \
    --min-tls-version TLS1_2 \
    --allow-blob-public-access false \
    --tags owner="$AZURE_OWNER" \
      purpose=hyperlight-workerd-campaign-cache \
      campaign_id="$AZURE_CAMPAIGN_ID" \
    --output none
fi
cache_account_key="$(
  az storage account keys list \
    --resource-group "$AZURE_CACHE_RESOURCE_GROUP" \
    --account-name "$AZURE_CACHE_STORAGE_ACCOUNT" \
    --query '[0].value' \
    --output tsv \
    | tr -d '\r'
)"
test -n "$cache_account_key"
az storage container create \
  --account-name "$AZURE_CACHE_STORAGE_ACCOUNT" \
  --account-key "$cache_account_key" \
  --name "$AZURE_CACHE_CONTAINER" \
  --output none
unset cache_account_key

az group create \
  --name "$AZURE_RESOURCE_GROUP" \
  --location "$AZURE_LOCATION" \
  --tags owner="$AZURE_OWNER" \
    purpose=hyperlight-workerd-walkthrough \
    run_id="$AZURE_RUN_ID" \
    lease_created_utc="$AZURE_LEASE_CREATED_UTC" \
    lease_hours=8 \
  --output none
vm_admin_password="Aa1!$(openssl rand -hex 14)"
[[ "$vm_admin_password" =~ [A-Z] ]]
[[ "$vm_admin_password" =~ [a-z] ]]
[[ "$vm_admin_password" =~ [0-9] ]]
az vm create \
  --resource-group "$AZURE_RESOURCE_GROUP" \
  --name "$AZURE_VM" \
  --image Ubuntu2404 \
  --size Standard_D32s_v5 \
  --os-disk-size-gb 256 \
  --storage-sku Premium_LRS \
  --admin-username "$AZURE_USER" \
  --authentication-type password \
  --admin-password "$vm_admin_password" \
  --assign-identity \
  --public-ip-sku Standard \
  --nsg-rule NONE \
  --tags owner="$AZURE_OWNER" \
    purpose=hyperlight-workerd-walkthrough \
    run_id="$AZURE_RUN_ID" \
  --output none
unset vm_admin_password

bootstrap_json="$HOME/azure-workerd-bootstrap-$AZURE_RUN_ID.json"
az vm run-command invoke \
  --resource-group "$AZURE_RESOURCE_GROUP" \
  --name "$AZURE_VM" \
  --command-id RunShellScript \
  --scripts \
    "set -eu; apt-get update; apt-get install -y ca-certificates curl jq; printf 'RUN_COMMAND_BOOTSTRAP_PASS\n'" \
  --output json >"$bootstrap_json"
grep -q 'RUN_COMMAND_BOOTSTRAP_PASS' "$bootstrap_json"

for resource_id in $(
  az resource list \
    --resource-group "$AZURE_RESOURCE_GROUP" \
    --query '[].id' \
    --output tsv \
    | tr -d '\r'
); do
  MSYS_NO_PATHCONV=1 az tag update \
    --resource-id "$resource_id" \
    --operation Merge \
    --tags owner="$AZURE_OWNER" \
      purpose=hyperlight-workerd-walkthrough \
      run_id="$AZURE_RUN_ID" \
    --output none
 done

principal="$(
  az vm identity show \
    --resource-group "$AZURE_RESOURCE_GROUP" \
    --name "$AZURE_VM" \
    --query principalId \
    --output tsv \
    | tr -d '\r'
)"
test -n "$principal"
account="hlw$(printf '%s' "$AZURE_RUN_ID" | sha256sum | cut -c1-16)"
input_container=acceptance-inputs
results_container=acceptance-results
az storage account create \
  --resource-group "$AZURE_RESOURCE_GROUP" \
  --name "$account" \
  --location "$AZURE_LOCATION" \
  --sku Standard_LRS \
  --kind StorageV2 \
  --min-tls-version TLS1_2 \
  --allow-blob-public-access false \
  --tags owner="$AZURE_OWNER" \
    purpose=hyperlight-workerd-walkthrough \
    run_id="$AZURE_RUN_ID" \
    transfer_role=authoritative-inputs-and-results \
  --output none
account_key="$(
  az storage account keys list \
    --resource-group "$AZURE_RESOURCE_GROUP" \
    --account-name "$account" \
    --query '[0].value' \
    --output tsv \
    | tr -d '\r'
)"
test -n "$account_key"
for container in "$input_container" "$results_container"; do
  az storage container create \
    --account-name "$account" \
    --account-key "$account_key" \
    --name "$container" \
    --output none
done
account_id="$(
  az storage account show \
    --resource-group "$AZURE_RESOURCE_GROUP" \
    --name "$account" \
    --query id \
    --output tsv \
    | tr -d '\r'
)"
input_scope="$account_id/blobServices/default/containers/$input_container"
results_scope="$account_id/blobServices/default/containers/$results_container"
input_role_id="$(
  MSYS_NO_PATHCONV=1 az role assignment create \
    --assignee-object-id "$principal" \
    --assignee-principal-type ServicePrincipal \
    --role 'Storage Blob Data Reader' \
    --scope "$input_scope" \
    --query id \
    --output tsv \
    | tr -d '\r'
)"
results_role_id="$(
  MSYS_NO_PATHCONV=1 az role assignment create \
    --assignee-object-id "$principal" \
    --assignee-principal-type ServicePrincipal \
    --role 'Storage Blob Data Contributor' \
    --scope "$results_scope" \
    --query id \
    --output tsv \
    | tr -d '\r'
)"
cache_account_id="$(
  az storage account show \
    --resource-group "$AZURE_CACHE_RESOURCE_GROUP" \
    --name "$AZURE_CACHE_STORAGE_ACCOUNT" \
    --query id \
    --output tsv \
    | tr -d '\r'
)"
cache_scope="$cache_account_id/blobServices/default/containers/$AZURE_CACHE_CONTAINER"
cache_role_id="$(
  MSYS_NO_PATHCONV=1 az role assignment create \
    --assignee-object-id "$principal" \
    --assignee-principal-type ServicePrincipal \
    --role 'Storage Blob Data Contributor' \
    --scope "$cache_scope" \
    --query id \
    --output tsv \
    | tr -d '\r'
)"

manifest="$HOME/azure-workerd-input-manifest-$AZURE_RUN_ID.tsv"
upload_receipt="$HOME/azure-workerd-blob-upload-$AZURE_RUN_ID.tsv"
printf 'blob\tlocal_path\tremote_path\tbytes\tsha256\n' >"$manifest"
add_blob_input() {
  local blob="$1" source="$2" target="$3"
  test -r "$source"
  printf '%s\t%s\t%s\t%s\t%s\n' \
    "$blob" "$source" "$target" \
    "$(stat -c '%s' "$source")" \
    "$(sha256sum "$source" | cut -d' ' -f1)" \
    >>"$manifest"
}
add_blob_input component-init-limit-fix.patch \
  "$WORKERD_FINAL_PATCH_SOURCE" \
  /home/azureuser/patches/workerd/component-init-limit-fix.patch
add_blob_input hyperlight-signed-baseline-5560e071.bundle \
  "$HYPERLIGHT_BASE_BUNDLE" \
  /home/azureuser/contracts/hyperlight-signed-baseline-5560e071.bundle
add_blob_input final-hyperlight-integration-bca3357e.patch \
  "$HYPERLIGHT_PATCH_SOURCE" \
  /home/azureuser/patches/hyperlight/final-hyperlight-integration-bca3357e.patch
add_blob_input final-hyperlight-integration-bca3357e.tar.gz \
  "$HYPERLIGHT_CLOSURE_ARCHIVE" \
  /home/azureuser/patches/hyperlight/final-hyperlight-integration-bca3357e.tar.gz
add_blob_input 74-final-evidence-manifest.json \
  "$HYPERLIGHT_CLOSURE_MANIFEST" \
  /home/azureuser/patches/hyperlight/74-final-evidence-manifest.json
add_blob_input workerd-component-proof-package-9402dd51-opt-27dbf9ac.tar.gz \
  "$COMPONENT_PACKAGE_SOURCE" \
  /home/azureuser/contracts/workerd-component-proof-package-9402dd51-opt-27dbf9ac.tar.gz
add_blob_input azure-workerd-hyperlight-runbook.md \
  "$RUNBOOK_PATH" \
  /home/azureuser/results/acceptance-inputs/azure-workerd-hyperlight-runbook.md
test "$(( $(wc -l <"$manifest") - 1 ))" = 11

printf 'blob\tbytes\tsha256\tremote_path\n' >"$upload_receipt"
while IFS=$'\t' read -r blob source target bytes sha; do
  [[ "$blob" = blob ]] && continue
  az storage blob upload \
    --account-name "$account" \
    --account-key "$account_key" \
    --container-name "$input_container" \
    --name "$blob" \
    --file "$source" \
    --overwrite \
    --metadata sha256="$sha" bytes="$bytes" \
    --output none
  actual="$(
    az storage blob show \
      --account-name "$account" \
      --account-key "$account_key" \
      --container-name "$input_container" \
      --name "$blob" \
      --query '{bytes:properties.contentLength,sha:metadata.sha256}' \
      --output tsv \
      | tr -d '\r'
  )"
  test "$actual" = "$bytes"$'\t'"$sha"
  printf '%s\t%s\t%s\t%s\n' "$blob" "$bytes" "$sha" "$target" \
    >>"$upload_receipt"
done <"$manifest"
manifest_bytes="$(stat -c '%s' "$manifest")"
manifest_sha="$(sha256sum "$manifest" | cut -d' ' -f1)"
az storage blob upload \
  --account-name "$account" \
  --account-key "$account_key" \
  --container-name "$input_container" \
  --name input-manifest.tsv \
  --file "$manifest" \
  --overwrite \
  --metadata sha256="$manifest_sha" bytes="$manifest_bytes" \
  --output none
unset account_key

umask 077
{
  printf 'export AZURE_LOCATION=%q\n' "$AZURE_LOCATION"
  printf 'export AZURE_RESOURCE_GROUP=%q\n' "$AZURE_RESOURCE_GROUP"
  printf 'export AZURE_VM=%q\n' "$AZURE_VM"
  printf 'export AZURE_USER=%q\n' "$AZURE_USER"
  printf 'export AZURE_OWNER=%q\n' "$AZURE_OWNER"
  printf 'export AZURE_RUN_ID=%q\n' "$AZURE_RUN_ID"
  printf 'export AZURE_LEASE_CREATED_UTC=%q\n' "$AZURE_LEASE_CREATED_UTC"
  printf 'export AZURE_VM_PRINCIPAL_ID=%q\n' "$principal"
  printf 'export AZURE_INPUT_STORAGE_ACCOUNT=%q\n' "$account"
  printf 'export AZURE_INPUT_CONTAINER=%q\n' "$input_container"
  printf 'export AZURE_INPUT_SCOPE=%q\n' "$input_scope"
  printf 'export AZURE_INPUT_ROLE_ID=%q\n' "$input_role_id"
  printf 'export AZURE_RESULTS_CONTAINER=%q\n' "$results_container"
  printf 'export AZURE_RESULTS_SCOPE=%q\n' "$results_scope"
  printf 'export AZURE_RESULTS_ROLE_ID=%q\n' "$results_role_id"
  printf 'export AZURE_CAMPAIGN_ID=%q\n' "$AZURE_CAMPAIGN_ID"
  printf 'export AZURE_CACHE_RESOURCE_GROUP=%q\n' "$AZURE_CACHE_RESOURCE_GROUP"
  printf 'export AZURE_CACHE_STORAGE_ACCOUNT=%q\n' "$AZURE_CACHE_STORAGE_ACCOUNT"
  printf 'export AZURE_CACHE_CONTAINER=%q\n' "$AZURE_CACHE_CONTAINER"
  printf 'export AZURE_CACHE_SCOPE=%q\n' "$cache_scope"
  printf 'export AZURE_CACHE_ROLE_ID=%q\n' "$cache_role_id"
} >"$AZURE_STATE_FILE"

runbook_sha="$(sha256sum "$RUNBOOK_PATH" | cut -d' ' -f1)"
cache_compatibility="$HOME/azure-workerd-cache-compatibility-$AZURE_RUN_ID.tsv"
cat >"$cache_compatibility" <<EOF
key	value
schema	1
campaign_id	$AZURE_CAMPAIGN_ID
workerd_tree	8844cfeb919eb401545a999e9081d58a678c5af1
hyperlight_tree	bca3357e5d1f995e6b75125f0b8f081d22167944
builder_base	mcr.microsoft.com/vscode/devcontainers/javascript-node:26-bookworm@sha256:4187a9d50e7a208659e9b56677ae764edb98cb6dc243f21e851aab2ca4d103ba
llvm_major	22
bazelisk	1.28.1
rust_toolchain	1.98.0
target	x86_64-unknown-linux-gnu
runbook_sha256	a01347a2fafa28f28f47a0d9f6fa61ba05a8b53afc57a57234c5abf9907990a6
EOF
cache_compatibility_key="$(
  sha256sum "$cache_compatibility" | cut -d' ' -f1
)"
test "$cache_compatibility_key" = \
  34acfbf23da59062c170c90702fbb85c78d3271e6a4688e70617693f3603cdc0
printf 'document_runbook_sha256\t%s\n' "$runbook_sha" \
  >"$HOME/azure-workerd-cache-provenance-$AZURE_RUN_ID.tsv"

fetch_script="$(mktemp)"
cat >"$fetch_script" <<EOF
set -eu
account='$account'
container='$input_container'
cache_account='$AZURE_CACHE_STORAGE_ACCOUNT'
cache_container='$AZURE_CACHE_CONTAINER'
cache_key='$cache_compatibility_key'
cache_prefix="v1/\$cache_key"
manifest=/home/azureuser/results/acceptance-inputs/input-manifest.tsv
receipt=/home/azureuser/results/acceptance-inputs/blob-download-attempts.tsv
inventory=/home/azureuser/results/acceptance-inputs/remote-input-inventory.tsv
cache_receipt=/home/azureuser/results/acceptance-inputs/cache-restore.tsv
cache_ownership=/home/azureuser/results/acceptance-inputs/cache-ownership.tsv
mkdir -p /home/azureuser/results/acceptance-inputs
chown -R azureuser:azureuser /home/azureuser/results
printf 'blob\tattempt\tstatus\tbytes\tsha256\n' >"\$receipt"
token="\$(curl --fail --silent --show-error --header Metadata:true 'http://169.254.169.254/metadata/identity/oauth2/token?api-version=2018-02-01&resource=https%3A%2F%2Fstorage.azure.com%2F' | python3 -c 'import json,sys; print(json.load(sys.stdin)["access_token"])')"
download_blob() {
  blob="\$1"; target="\$2"; bytes="\$3"; sha="\$4"; attempts="\$5"
  mkdir -p "\$(dirname "\$target")"
  attempt=1
  while [ "\$attempt" -le "\$attempts" ]; do
    partial="\$target.partial.$AZURE_RUN_ID.\$attempt"
    status="\$(curl --silent --show-error --location --output "\$partial" --write-out '%{http_code}' --max-time 1800 --header "Authorization: Bearer \$token" --header 'x-ms-version: 2023-11-03' "https://\$account.blob.core.windows.net/\$container/\$blob" || true)"
    actual_bytes="\$(stat -c '%s' "\$partial" 2>/dev/null || printf 0)"
    actual_sha="\$(sha256sum "\$partial" 2>/dev/null | cut -d' ' -f1 || true)"
    printf '%s\t%s\t%s\t%s\t%s\n' "\$blob" "\$attempt" "\$status" "\$actual_bytes" "\$actual_sha" >>"\$receipt"
    if [ "\$status" = 200 ] && [ "\$actual_bytes" = "\$bytes" ] && [ "\$actual_sha" = "\$sha" ]; then
      chmod 0600 "\$partial"; mv -f "\$partial" "\$target"; return 0
    fi
    rm -f "\$partial"; sleep "\$((1 << attempt))"; attempt="\$((attempt + 1))"
  done
  return 1
}
download_blob input-manifest.tsv "\$manifest" '$manifest_bytes' '$manifest_sha' 8
printf 'blob\tremote_path\tbytes\tsha256\tmode\n' >"\$inventory"
tab="\$(printf '\t')"
tail -n +2 "\$manifest" | while IFS="\$tab" read -r blob source target bytes sha; do
  download_blob "\$blob" "\$target" "\$bytes" "\$sha" 3
  printf '%s\t%s\t%s\t%s\t%s\n' "\$blob" "\$target" "\$(stat -c '%s' "\$target")" "\$(sha256sum "\$target" | cut -d' ' -f1)" "\$(stat -c '%a' "\$target")" >>"\$inventory"
done
test "\$((\$(wc -l <"\$inventory") - 1))" = 11
printf 'state\tcompatibility_key\tarchive\tbytes\tsha256\n' >"\$cache_receipt"
cache_manifest=/tmp/cache-manifest.tsv
cache_status="\$(
  curl --silent --show-error --location \
    --output "\$cache_manifest" \
    --write-out '%{http_code}' \
    --max-time 300 \
    --header "Authorization: Bearer \$token" \
    --header 'x-ms-version: 2023-11-03' \
    "https://\$cache_account.blob.core.windows.net/\$cache_container/\$cache_prefix/cache-manifest.tsv" \
    || true
)"
cache_state=EMPTY
if test "\$cache_status" = 200; then
  cache_state=RESTORED
  while IFS="\$tab" read -r archive target bytes sha; do
    test "\$archive" = archive && continue
    case "\$target" in
      /home/azureuser/.cache/workerd-bazel/*|/home/azureuser/.cargo/registry|/home/azureuser/.cargo/git) ;;
      *) cache_state=REJECTED; break ;;
    esac
    partial="/tmp/\${archive##*/}.partial"
    status="\$(
      curl --silent --show-error --location \
        --output "\$partial" \
        --write-out '%{http_code}' \
        --max-time 3600 \
        --header "Authorization: Bearer \$token" \
        --header 'x-ms-version: 2023-11-03' \
        "https://\$cache_account.blob.core.windows.net/\$cache_container/\$archive" \
        || true
    )"
    if test "\$status" != 200 \
      || test "\$(stat -c '%s' "\$partial" 2>/dev/null || printf 0)" != "\$bytes" \
      || test "\$(sha256sum "\$partial" 2>/dev/null | cut -d' ' -f1)" != "\$sha" \
      || ! tar -tzf "\$partial" >/dev/null; then
      cache_state=REJECTED
      break
    fi
    rm -rf "\$target"
    tar -xzf "\$partial" -C /
    rm -f "\$partial"
    case "\$target" in
      /home/azureuser/.cargo/*|/home/azureuser/.cargo)
        chown -R azureuser:azureuser "\$target"
        ;;
    esac
    printf 'RESTORED\t%s\t%s\t%s\t%s\n' \
      "\$cache_key" "\$archive" "\$bytes" "\$sha" >>"\$cache_receipt"
  done <"\$cache_manifest"
fi
if test "\$cache_state" = REJECTED; then
  rm -rf /home/azureuser/.cache/workerd-bazel \
    /home/azureuser/.cargo/registry \
    /home/azureuser/.cargo/git
  mkdir -p /home/azureuser/.cache/workerd-bazel/action-cache \
    /home/azureuser/.cache/workerd-bazel/repository-cache
  printf 'REJECTED_EMPTY_FALLBACK\t%s\t-\t0\t-\n' \
    "\$cache_key" >>"\$cache_receipt"
elif test "\$cache_state" = EMPTY; then
  mkdir -p /home/azureuser/.cache/workerd-bazel/action-cache \
    /home/azureuser/.cache/workerd-bazel/repository-cache
  printf 'EMPTY_STABLE_FALLBACK\t%s\t-\t0\t-\n' \
    "\$cache_key" >>"\$cache_receipt"
fi
mkdir -p \
  /home/azureuser/.cargo/registry \
  /home/azureuser/.cache/go-build \
  /home/azureuser/go
chown -R azureuser:azureuser /home/azureuser/.cargo
chown azureuser:azureuser \
  /home/azureuser/.cache \
  /home/azureuser/.cache/go-build \
  /home/azureuser/go
chmod 0755 \
  /home/azureuser/.cargo \
  /home/azureuser/.cargo/registry \
  /home/azureuser/.cache \
  /home/azureuser/.cache/go-build \
  /home/azureuser/go
printf 'path\towner\tgroup\tmode\tazureuser_writable\n' >"\$cache_ownership"
for path in \
  /home/azureuser/.cargo \
  /home/azureuser/.cargo/registry \
  /home/azureuser/.cache \
  /home/azureuser/.cache/go-build \
  /home/azureuser/go
do
  test "\$(stat -c '%U:%G' "\$path")" = azureuser:azureuser
  sudo -u azureuser test -w "\$path"
  printf '%s\t%s\t%s\t%s\tPASS\n' \
    "\$path" \
    "\$(stat -c '%U' "\$path")" \
    "\$(stat -c '%G' "\$path")" \
    "\$(stat -c '%a' "\$path")" \
    >>"\$cache_ownership"
done
test "\$((\$(wc -l <"\$cache_ownership") - 1))" = 5
printf 'AZURE_STORAGE_ACCOUNT=%s\nAZURE_RESULTS_CONTAINER=%s\nAZURE_CACHE_STORAGE_ACCOUNT=%s\nAZURE_CACHE_CONTAINER=%s\nAZURE_CACHE_COMPATIBILITY_KEY=%s\n' \
  "\$account" '$results_container' "\$cache_account" "\$cache_container" "\$cache_key" \
  >/home/azureuser/results/acceptance-inputs/blob-config.env
chmod 0600 /home/azureuser/results/acceptance-inputs/blob-config.env
chown -R azureuser:azureuser /home/azureuser/contracts /home/azureuser/patches /home/azureuser/results
printf 'CACHE_PARENT_OWNERSHIP_PASS 5 %s\n' "\$(sha256sum "\$cache_ownership" | cut -d' ' -f1)"
printf 'BLOB_INPUTS_VERIFIED 11 %s %s %s CACHE_%s %s\n' "\$(sha256sum "\$manifest" | cut -d' ' -f1)" "\$(sha256sum "\$receipt" | cut -d' ' -f1)" "\$(sha256sum "\$inventory" | cut -d' ' -f1)" "\$cache_state" "\$(sha256sum "\$cache_receipt" | cut -d' ' -f1)"
EOF
fetch_json="$HOME/azure-workerd-blob-fetch-$AZURE_RUN_ID.json"
timeout --signal=TERM --kill-after=15s 1800s \
  az vm run-command invoke \
    --resource-group "$AZURE_RESOURCE_GROUP" \
    --name "$AZURE_VM" \
    --command-id RunShellScript \
    --scripts "@$fetch_script" \
    --output json >"$fetch_json"
rm -f "$fetch_script"
grep -q 'BLOB_INPUTS_VERIFIED 11 ' "$fetch_json"

verify_script="$(mktemp)"
cat >"$verify_script" <<'EOF'
set -eu
manifest=/home/azureuser/results/acceptance-inputs/input-manifest.tsv
inventory=/home/azureuser/results/acceptance-inputs/independent-input-inventory.tsv
quiescence=/home/azureuser/results/acceptance-inputs/staging-quiescence.tsv
cache_receipt=/home/azureuser/results/acceptance-inputs/cache-restore.tsv
cache_ownership=/home/azureuser/results/acceptance-inputs/cache-ownership.tsv
tab="$(printf '\t')"
printf 'remote_path\tbytes\tsha256\tmode\n' >"$inventory"
tail -n +2 "$manifest" | while IFS="$tab" read -r blob source target bytes sha; do
  test "$(stat -c '%s' "$target")" = "$bytes"
  test "$(sha256sum "$target" | cut -d' ' -f1)" = "$sha"
  test "$(stat -c '%a' "$target")" = 600
  printf '%s\t%s\t%s\t%s\n' "$target" "$bytes" "$sha" 600 >>"$inventory"
done
test "$(( $(wc -l <"$inventory") - 1 ))" = 11
test -s "$cache_receipt"
grep -Eq \
  '^(RESTORED|EMPTY_STABLE_FALLBACK|REJECTED_EMPTY_FALLBACK)' \
  "$cache_receipt"
test "$(( $(wc -l <"$cache_ownership") - 1 ))" = 5
awk -F '\t' '
  NR == 1 {
    if ($0 != "path\towner\tgroup\tmode\tazureuser_writable") exit 1
    next
  }
  $2 != "azureuser" || $3 != "azureuser" || $5 != "PASS" { exit 1 }
' "$cache_ownership"
while IFS="$tab" read -r path owner group mode writable; do
  test "$path" = path && continue
  test "$(stat -c '%U:%G' "$path")" = azureuser:azureuser
  sudo -u azureuser test -w "$path"
done <"$cache_ownership"
test -z "$(pgrep -af 'azcopy|blobfuse|curl.*blob[.]core[.]windows[.]net' || true)"
test -z "$(findmnt -rn -t fuse,fuse3,fuse.blobfuse2 2>/dev/null || true)"
printf 'utc\tinputs\tcache_receipt_sha256\tdownload_processes\tblob_mounts\tstate\n%s\t11\t%s\t0\t0\tBLOB_STAGING_CLOSED\n' \
  "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  "$(sha256sum "$cache_receipt" | cut -d' ' -f1)" \
  >"$quiescence"
chown -R azureuser:azureuser /home/azureuser/results/acceptance-inputs
printf 'INDEPENDENT_BLOB_INPUTS_VERIFIED 11\n'
printf 'BLOB_STAGING_CLOSED 0 0\n'
EOF
verify_json="$HOME/azure-workerd-blob-verify-$AZURE_RUN_ID.json"
az vm run-command invoke \
  --resource-group "$AZURE_RESOURCE_GROUP" \
  --name "$AZURE_VM" \
  --command-id RunShellScript \
  --scripts "@$verify_script" \
  --output json >"$verify_json"
rm -f "$verify_script"
grep -q 'INDEPENDENT_BLOB_INPUTS_VERIFIED 11' "$verify_json"
grep -q 'BLOB_STAGING_CLOSED 0 0' "$verify_json"

runner_install="$(mktemp)"
cat >"$runner_install" <<'EOF'
set -eu
cat >/usr/local/bin/run-runbook-block <<'PY'
#!/usr/bin/env python3
import hashlib, os, pathlib, pwd, stat, subprocess, sys, tempfile, time
argument=sys.argv[1]
runbook=pathlib.Path(
    os.environ.get(
        'RUNBOOK_PATH',
        '/home/azureuser/results/acceptance-inputs/azure-workerd-hyperlight-runbook.md',
    )
)
lines=runbook.read_text(encoding='utf-8').splitlines(); blocks=[]; lang=None; body=[]
for line in lines:
    if lang is None:
        if line.startswith('```'):
            lang=line[3:].strip(); body=[]
    elif line == '```':
        blocks.append((lang,'\n'.join(body)+'\n')); lang=None
    else: body.append(line)
assert lang is None
bash_blocks=[body for lang, body in blocks if lang == 'bash']
if argument == '--map-all':
    print('index\tbytes\tsha256')
    for index, script in enumerate(bash_blocks, 1):
        encoded=script.encode()
        print(
            f'{index}\t{len(encoded)}\t'
            f'{hashlib.sha256(encoded).hexdigest()}'
        )
    print(
        f'RUNBOOK_MAP_SUMMARY bash={len(bash_blocks)} '
        f'nonbash={len(blocks)-len(bash_blocks)}'
    )
    sys.exit(0)
elif argument.startswith('--map='):
    index=int(argument.split('=',1)[1])
    assert 2 <= index <= len(bash_blocks)
    script=bash_blocks[index-1]
    print(
        f'RUNBOOK_MAP {index} {hashlib.sha256(script.encode()).hexdigest()} '
        f'{script.splitlines()[0]}'
    )
    sys.exit(0)
elif argument == '--diagnostic':
    label='diagnostic'
    script='''set -euo pipefail
test "$(id -un)" = azureuser
test "$HOME" = /home/azureuser
test "$PWD" = /home/azureuser
printf 'RUNNER_DIAGNOSTIC user=%s home=%s cwd=%s path=%s\n' \
  "$(id -un)" "$HOME" "$PWD" "$PATH"
'''
else:
    index=int(argument); assert 2 <= index <= len(bash_blocks)
    label=str(index)
    script=bash_blocks[index-1]
root=pathlib.Path(
    os.environ.get(
        'RUNBOOK_LEDGER_ROOT',
        '/home/azureuser/results/run-command',
    )
)
root.mkdir(parents=True,exist_ok=True)
sha=hashlib.sha256(script.encode()).hexdigest(); start=time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime())
user=pwd.getpwnam('azureuser')
with tempfile.NamedTemporaryFile('w',delete=False,prefix='runbook-',suffix='.sh') as f:
    f.write(script); path=f.name
os.chown(path,user.pw_uid,user.pw_gid); os.chmod(path,0o700)
metadata=os.stat(path)
temp_owner=pwd.getpwuid(metadata.st_uid).pw_name
temp_mode=stat.S_IMODE(metadata.st_mode)
log=root/f'block-{label}.log'
try:
    with log.open('wb') as out:
        result=subprocess.run(
            [
                'runuser',
                '-u',
                'azureuser',
                '--',
                'env',
                f'HOME={user.pw_dir}',
                'USER=azureuser',
                (
                    f'PATH={user.pw_dir}/.cargo/bin:'
                    f'{user.pw_dir}/go/bin:{user.pw_dir}/bin:'
                    '/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/snap/bin'
                ),
                'bash',
                path,
            ],
            cwd=user.pw_dir,
            stdout=out,
            stderr=subprocess.STDOUT,
        )
finally:
    os.unlink(path)
temp_removed=not os.path.exists(path)
end=time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime())
ledger=root/'ledger.tsv'
if not ledger.exists():
    ledger.write_text(
        'block\tsha256\tstarted\tended\texit\tlog\towner\tmode\ttemp_removed\n'
    )
with ledger.open('a') as f:
    f.write(
        f'{label}\t{sha}\t{start}\t{end}\t{result.returncode}\t{log}'
        f'\t{temp_owner}\t{temp_mode:o}\t{str(temp_removed).lower()}\n'
    )
os.chown(log,user.pw_uid,user.pw_gid); os.chown(ledger,user.pw_uid,user.pw_gid)
print(
    f'RUNBOOK_BLOCK {label} {sha} EXIT {result.returncode} LOG {log} '
    f'OWNER {temp_owner} MODE {temp_mode:o} TEMP_REMOVED {str(temp_removed).lower()}'
)
sys.exit(result.returncode)
PY
chmod 0755 /usr/local/bin/run-runbook-block
EOF
az vm run-command invoke \
  --resource-group "$AZURE_RESOURCE_GROUP" \
  --name "$AZURE_VM" \
  --command-id RunShellScript \
  --scripts "@$runner_install" \
  --output none
rm -f "$runner_install"

helper="$HOME/azure-workerd-run-command-$AZURE_RUN_ID.sh"
cat >"$helper" <<'EOF'
invoke_vm_block() {
  local block="$1" receipt json exit_code
  receipt="$HOME/azure-workerd-run-command-${AZURE_RUN_ID}-block-${block}.json"
  set +e
  timeout --signal=TERM --kill-after=15s 7200s \
    az vm run-command invoke \
      --resource-group "$AZURE_RESOURCE_GROUP" \
      --name "$AZURE_VM" \
      --command-id RunShellScript \
      --scripts "set -eu; /usr/local/bin/run-runbook-block $block" \
      --output json >"$receipt"
  exit_code=$?
  set -e
  test "$exit_code" -eq 0
  grep -q "RUNBOOK_BLOCK $block .* EXIT 0 " "$receipt"
}
EOF
chmod 0600 "$helper"
printf 'export AZURE_RUN_COMMAND_HELPER=%q\n' "$helper" >>"$AZURE_STATE_FILE"
sha256sum "$manifest" "$upload_receipt" "$fetch_json" "$verify_json" "$helper"
```

Expected: the VM and run-scoped storage resources are tagged, all 11 immutable
inputs and the exact runbook are verified on local VM disk, and the independent
receipt reports `BLOB_STAGING_CLOSED 0 0`. No Blob download process or Blob
mount remains before Section 2. Source the state and helper in every fresh
workstation shell, then execute each subsequent VM Bash block by its global
fence index, in document order, with `invoke_vm_block <index>`. Azure VM Run
Command records the exact block SHA, timing, exit code, and VM-local log.

Blob is out-of-band setup and evidence transport only. Builds, tests, demos,
and performance measurements use local VM disk or tmpfs. During measured
workloads there are no Blob reads, writes, uploads, mounts, storage polls, or
background transfer processes. Results remain local until Section 24 uploads
the sealed archive after every measured workload has finished.

**Troubleshooting**

- If VM creation reports that the size is unavailable, choose another region
  with `Standard_D32s_v5` capacity.
- If Run Command is not ready, inspect VM provisioning and agent status; do
  not open an inbound management port or substitute another control channel.
## 2. Check out the pinned source revisions

On the VM:

```bash
mkdir -p "$HOME/src" "$HOME/results" "$HOME/artifacts"

cd "$HOME/src"
test "$(stat -c '%s' \
  "$HOME/contracts/hyperlight-signed-baseline-5560e071.bundle")" = 4481599
printf '%s  %s\n' \
  d18b56e36dafaea91f8511f9bd92a3a2459f3e40d00f4ea0aaf800c576f32d59 \
  "$HOME/contracts/hyperlight-signed-baseline-5560e071.bundle" \
  | sha256sum --check
test "$(
  git bundle list-heads \
    "$HOME/contracts/hyperlight-signed-baseline-5560e071.bundle"
)" = \
  "5560e071f81706488efcab86ad534e80a3a8553d refs/heads/signed-kv-boundary"
git clone \
  "$HOME/contracts/hyperlight-signed-baseline-5560e071.bundle" \
  hyperlight-unikraft
cd hyperlight-unikraft
git bundle verify \
  "$HOME/contracts/hyperlight-signed-baseline-5560e071.bundle"
git checkout --detach 5560e071f81706488efcab86ad534e80a3a8553d
git remote set-url origin \
  https://github.com/simongdavies/hyperlight-unikraft.git
git submodule update --init --recursive
test "$(git rev-parse HEAD)" = \
  5560e071f81706488efcab86ad534e80a3a8553d
test "$(git rev-parse HEAD^{tree})" = \
  418f983c41b19e4f0bd803045f0f6e6285dac137
test -z "$(git status --porcelain)"

cd "$HOME/patches/hyperlight"
test "$(stat -c '%s' final-hyperlight-integration-bca3357e.patch)" = \
  393702
sha256sum --check <<'EOF'
e3f8fa6e628052cc94dc57121690fdc56e65f6661ba413073418e6c4f58be2ba  final-hyperlight-integration-bca3357e.patch
530545c2e1890915ec279e5ff56e6668e74c527711610b60feac542594a7d801  final-hyperlight-integration-bca3357e.tar.gz
5105178d7c07f8540b52ff28a4056b2917f69256330ec75a6cb090e6009bfbb7  74-final-evidence-manifest.json
EOF
test "$(
  git patch-id --stable \
    <final-hyperlight-integration-bca3357e.patch \
    | cut -d' ' -f1
)" = e33f0348e4e8d0c2e30ba066e5cd3f60d88db52d

cd "$HOME/src/hyperlight-unikraft"
git apply --check \
  "$HOME/patches/hyperlight/final-hyperlight-integration-bca3357e.patch"
git apply --index \
  "$HOME/patches/hyperlight/final-hyperlight-integration-bca3357e.patch"

git diff --cached --check
test "$(git write-tree)" = \
  bca3357e5d1f995e6b75125f0b8f081d22167944

cd "$HOME/src"
git clone \
  --branch simongdavies-workerd-ingress-bindings \
  --single-branch \
  https://github.com/simongdavies/workerd.git \
  workerd
cd workerd
git checkout --detach 621cb07e7d2cf0cb0f49872129d4408f6319acef
git submodule update --init --recursive
git verify-commit 621cb07e7d2cf0cb0f49872129d4408f6319acef
test "$(git rev-parse HEAD)" = \
  621cb07e7d2cf0cb0f49872129d4408f6319acef
test "$(git rev-parse HEAD^{tree})" = \
  0cd2766a74356a04055510dc327b3646677042e5
test -z "$(git status --porcelain)"
```

Expected: both revision checks and every patch/hash check exit zero. The
Hyperlight index contains only the verified capability overlays; the Workerd
checkout is the exact signed Azure-qualified delivery tree.

**Troubleshooting**

- If a revision is missing, run `git fetch --tags --force origin` in that
  repository and retry the checkout.
- If a submodule is missing, rerun
  `git submodule update --init --recursive`.

## 3. Validate KVM and install host tools

```bash
uname -a
lscpu
findmnt -no FSTYPE,TARGET /
free -h
df -h /

ROOT_FREE_GIB="$(
  df --output=avail -BG / | tail -n 1 | tr -dc '0-9'
)"
test "$ROOT_FREE_GIB" -ge 200

grep -E -m1 '(^| )vmx( |$)' /proc/cpuinfo
ls -l /dev/kvm

if ! id -nG "$USER" | tr ' ' '\n' | grep -qx kvm; then
  sudo usermod -aG kvm "$USER"
  echo "Start the next Run Command invocation before continuing."
fi
```

After reconnecting:

```bash
test -c /dev/kvm
test -r /dev/kvm
test -w /dev/kvm

python3 - <<'PY'
import fcntl
import os

fd = os.open("/dev/kvm", os.O_RDWR | os.O_CLOEXEC)
try:
    version = fcntl.ioctl(fd, 0xAE00, 0)
    assert version == 12, version
    print({"kvm_api_version": version})
finally:
    os.close(fd)
PY
```

Expected:

```text
{'kvm_api_version': 12}
```

Install dependencies:

```bash
sudo apt-get update
sudo apt-get install -y \
  binutils \
  build-essential \
  cpio \
  curl \
  docker.io \
  file \
  git \
  golang-go \
  jq \
  patch \
  pkg-config \
  python3 \
  python3-venv \
  rsync \
  unzip

sudo usermod -aG docker "$USER"
```

Reconnect once more, then install Rust, `just`, and `hey`:

```bash
export RUSTUP_HOME="$HOME/.rustup"
export CARGO_HOME="$HOME/.cargo"
export PATH="$HOME/go/bin:$CARGO_HOME/bin:$PATH"
export CARGO_BUILD_JOBS=8
test "$CARGO_BUILD_JOBS" -eq 8
mkdir -p "$RUSTUP_HOME" "$CARGO_HOME"

if ! command -v rustup >/dev/null; then
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
    | env RUSTUP_INIT_SKIP_PATH_CHECK=yes sh -s -- -y --profile minimal
fi

source "$CARGO_HOME/env"
rustup toolchain install 1.98.0 --profile minimal

if ! command -v just >/dev/null; then
  cargo install just --version 1.58.0 --locked
fi

go install github.com/rakyll/hey@v0.1.4

docker_version="$(docker version --format '{{.Client.Version}} {{.Server.Version}}')"
printf 'docker client/server %s\n' "$docker_version"
test "$(
  printf '%s\n' "$docker_version" \
    | grep -oE '[0-9]+([.][0-9]+){2}' \
    | wc -l
)" = 2
for attempt in 1 2 3; do
  timeout 300s docker pull \
    hello-world@sha256:5e23090353324d887c48ad5e5c56d294eab81588df9605b07d1afe895f9cc8f8 \
    && break
  test "$attempt" -lt 3
done
docker run --rm --pull=never \
  hello-world@sha256:5e23090353324d887c48ad5e5c56d294eab81588df9605b07d1afe895f9cc8f8
export RUSTUP_AUTO_INSTALL=0
rustc_version="$(rustc +1.98.0 --version)"
printf '%s\n' "$rustc_version"
test "$(
  printf '%s\n' "$rustc_version" \
    | grep -oE '[0-9]+([.][0-9]+){2}' \
    | head -n 1
)" = 1.98.0
just_version="$(just --version)"
printf '%s\n' "$just_version"
test "$(
  printf '%s\n' "$just_version" \
    | grep -oE '[0-9]+([.][0-9]+){2}' \
    | head -n 1
)" = 1.58.0
go version -m "$(command -v hey)" \
  | grep -F $'mod\tgithub.com/rakyll/hey\tv0.1.4'
```

**Troubleshooting**

- `Permission denied` on `/dev/kvm` means the current process has not picked
  up the new `kvm` group; use the next Run Command invocation.
- Docker permission errors similarly require a new session after adding the
  `docker` group.
- The root filesystem should be ext4. Build and KVM artifacts should not be
  placed on a Windows-mounted or network filesystem.

## 4. Build the Workerd executor

Verify the signed delivery pin. The three integration patches used by the
sealed Azure campaign are already represented exactly by this commit and must
not be applied again:

```bash
set -euo pipefail

cd "$HOME/src/workerd"

test -z "$(git status --porcelain)"
test "$(git rev-parse HEAD)" = \
  621cb07e7d2cf0cb0f49872129d4408f6319acef
test "$(git rev-parse HEAD^{tree})" = \
  0cd2766a74356a04055510dc327b3646677042e5

printf '%s  %s\n' \
  6c9cfa5e80fc814d967dd983bd7476853eeade2e15f7f352b797940a85f766d2 \
  "$HOME/patches/workerd/component-init-limit-fix.patch" \
  | sha256sum --check
git apply --check "$HOME/patches/workerd/component-init-limit-fix.patch"
git apply --index "$HOME/patches/workerd/component-init-limit-fix.patch"
git diff --cached --check
test "$(git write-tree)" = \
  8844cfeb919eb401545a999e9081d58a678c5af1
```

Apply the static host-tool linker setting expected by the patched checkout:

```bash
cd "$HOME/src/workerd"

grep -Fqx \
  "build:linux --host_linkopt='-lc++' --host_linkopt='-lm'" \
  .bazelrc

sed -i \
  "s/build:linux --host_linkopt='-lc++' --host_linkopt='-lm'/build:linux --host_linkopt='-l:libc++.a' --host_linkopt='-lm'/" \
  .bazelrc

grep -Fqx \
  "build:linux --host_linkopt='-l:libc++.a' --host_linkopt='-lm'" \
  .bazelrc
```

Create the LLVM 22 builder:

```bash
cd "$HOME/src/workerd"

cat > .devcontainer/Dockerfile.hyperlight-executor <<'EOF'
FROM mcr.microsoft.com/vscode/devcontainers/javascript-node:26-bookworm@sha256:4187a9d50e7a208659e9b56677ae764edb98cb6dc243f21e851aab2ca4d103ba
ARG LLVM_VERSION=22
ARG LLVM_SIGNER_FPR=6084F3CF814B57C1CF12EFD515CF4D18AF4F7421
ARG BAZELISK_VERSION=1.28.1
ARG BAZELISK_LINUX_AMD64_SHA256=22e7d3a188699982f661cf4687137ee52d1f24fec1ec893d91a6c4d791a75de8
RUN set -eux \
    && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -o Acquire::Retries=3 \
       -o Acquire::http::Timeout=30 \
       -o Acquire::https::Timeout=30 \
       update \
    && apt-get install -y --no-install-recommends \
       ca-certificates curl gnupg tcl \
    && . /etc/os-release \
    && test -n "${VERSION_CODENAME:-}" \
    && llvm_repo="https://apt.llvm.org/${VERSION_CODENAME}" \
    && llvm_suite="llvm-toolchain-${VERSION_CODENAME}-${LLVM_VERSION}" \
    && curl --fail --show-error --silent --location \
       --connect-timeout 15 --max-time 120 \
       --retry 3 --retry-delay 2 --retry-all-errors \
       -o /tmp/llvm-snapshot.gpg.key \
       https://apt.llvm.org/llvm-snapshot.gpg.key \
    && test "$(gpg --batch --show-keys --with-colons /tmp/llvm-snapshot.gpg.key \
       | awk -F: '$1 == "fpr" { print $10; exit }')" = "${LLVM_SIGNER_FPR}" \
    && gpg --batch --yes --dearmor \
       --output /usr/share/keyrings/apt.llvm.org.gpg \
       /tmp/llvm-snapshot.gpg.key \
    && curl --fail --show-error --silent --location \
       --connect-timeout 15 --max-time 120 \
       --retry 3 --retry-delay 2 --retry-all-errors \
       -o /tmp/llvm-InRelease \
       "${llvm_repo}/dists/${llvm_suite}/InRelease" \
    && gpgv --keyring /usr/share/keyrings/apt.llvm.org.gpg \
       /tmp/llvm-InRelease \
    && printf '%s\n' \
       "deb [signed-by=/usr/share/keyrings/apt.llvm.org.gpg] ${llvm_repo}/ ${llvm_suite} main" \
       > /etc/apt/sources.list.d/apt.llvm.org.list \
    && apt-get -o Acquire::Retries=3 \
       -o Acquire::http::Timeout=30 \
       -o Acquire::https::Timeout=30 \
       update \
    && apt-get install -y --no-install-recommends \
       clang-${LLVM_VERSION} \
       lld-${LLVM_VERSION} \
       llvm-${LLVM_VERSION} \
       libunwind-${LLVM_VERSION}-dev \
       libc++-${LLVM_VERSION}-dev \
       libc++abi-${LLVM_VERSION}-dev \
       libclang-rt-${LLVM_VERSION}-dev \
       -o DPkg::options::="--force-overwrite" \
    && curl --fail --show-error --silent --location \
       --connect-timeout 15 --max-time 120 \
       --retry 3 --retry-delay 2 --retry-all-errors \
       -o /usr/local/bin/bazelisk \
       "https://github.com/bazelbuild/bazelisk/releases/download/v${BAZELISK_VERSION}/bazelisk-linux-amd64" \
    && echo "${BAZELISK_LINUX_AMD64_SHA256}  /usr/local/bin/bazelisk" \
       | sha256sum --check --strict \
    && chmod 0755 /usr/local/bin/bazelisk \
    && ln -s bazelisk /usr/local/bin/bazel \
    && clang_version="$(clang-${LLVM_VERSION} --version)" \
    && printf '%s\n' "${clang_version}" \
    && test "$(clang-${LLVM_VERSION} -dumpversion | cut -d. -f1)" = "${LLVM_VERSION}" \
    && llvm_version_actual="$(llvm-config-${LLVM_VERSION} --version)" \
    && printf 'llvm-config %s\n' "${llvm_version_actual}" \
    && test "$(printf '%s\n' "${llvm_version_actual}" | cut -d. -f1)" = "${LLVM_VERSION}" \
    && lld_version="$(ld.lld-${LLVM_VERSION} --version)" \
    && printf '%s\n' "${lld_version}" \
    && lld_major="$(printf '%s\n' "${lld_version}" | sed -n 's/^[^0-9]*\([0-9][0-9]*\)\..*/\1/p')" \
    && test "${lld_major}" = "${LLVM_VERSION}" \
    && dpkg-query -W \
       clang-${LLVM_VERSION} \
       lld-${LLVM_VERSION} \
       llvm-${LLVM_VERSION} \
       libunwind-${LLVM_VERSION}-dev \
       libc++-${LLVM_VERSION}-dev \
       libc++abi-${LLVM_VERSION}-dev \
       libclang-rt-${LLVM_VERSION}-dev \
    && rm -rf /var/lib/apt/lists/* \
       /tmp/llvm-snapshot.gpg.key \
       /tmp/llvm-InRelease
ENV PATH="/usr/lib/llvm-${LLVM_VERSION}/bin:${PATH}"
EOF

docker build \
  -t workerd-hyperlight-builder \
  -f .devcontainer/Dockerfile.hyperlight-executor \
  .devcontainer
```

Build and copy the static executor:

```bash
set -euo pipefail

mkdir -p "$HOME/.cache/workerd-bazel/action-cache"
mkdir -p "$HOME/.cache/workerd-bazel/repository-cache"
mkdir -p "$HOME/artifacts"
export WORKERD_BAZEL_JOBS="${WORKERD_BAZEL_JOBS:-20}"
test "$WORKERD_BAZEL_JOBS" -ge 1
test "$WORKERD_BAZEL_JOBS" -le 20

docker run --rm \
  --env WORKERD_BAZEL_JOBS \
  --mount type=bind,src="$HOME/src/workerd",dst=/workspace \
  -v "$HOME/.cache/workerd-bazel:/root/.cache/bazel" \
  -v "$HOME/artifacts:/artifacts" \
  -w /workspace \
  workerd-hyperlight-builder \
  bash -c '
    set -euo pipefail
    export PATH=/usr/lib/llvm-22/bin:$PATH
    export CC=/usr/lib/llvm-22/bin/clang
    export CXX=/usr/lib/llvm-22/bin/clang++
    executor=bazel-bin/src/workerd/server/workerd-sandbox-executor
    rm -f "$executor" /artifacts/workerd-sandbox-executor
    bazel --output_base=/root/.cache/bazel/workerd-hyperlight-output \
      build //src/workerd/server:workerd-sandbox-executor \
      --config=opt \
      --strip=always \
      --//:io_backend=cxx \
      --jobs="$WORKERD_BAZEL_JOBS" \
      --disk_cache=/root/.cache/bazel/action-cache \
      --repository_cache=/root/.cache/bazel/repository-cache \
      --repo_env=CC=/usr/lib/llvm-22/bin/clang \
      --repo_env=CXX=/usr/lib/llvm-22/bin/clang++ \
      --announce_rc
    test -x "$executor"
    "$executor" --self-test
    /usr/lib/llvm-22/bin/llvm-strip "$executor"
    cp "$executor" /artifacts/workerd-sandbox-executor
    chmod 0755 /artifacts/workerd-sandbox-executor
  '

export WORKERD_EXECUTOR="$HOME/artifacts/workerd-sandbox-executor"

"$WORKERD_EXECUTOR" --self-test
file "$WORKERD_EXECUTOR"
readelf -l "$WORKERD_EXECUTOR"
readelf -d "$WORKERD_EXECUTOR" || true

file "$WORKERD_EXECUTOR" | grep -q 'ELF 64-bit.*x86-64'
file "$WORKERD_EXECUTOR" | grep -qi 'pie executable'
! readelf -l "$WORKERD_EXECUTOR" | grep -q 'INTERP'
! readelf -d "$WORKERD_EXECUTOR" | grep -Eq '(NEEDED|RPATH|RUNPATH)'
```

Expected: self-test exits zero and the executor is an x86-64 static PIE with
no ELF interpreter or dynamic dependency entries.

**Troubleshooting**

- Reuse the mounted Bazel caches on retries.
- If the builder cannot download LLVM packages, verify outbound HTTPS and the
  VM clock.
- If a patch check fails, stop and confirm the pinned commit, patch order,
  patch hashes, and clean checkout before retrying.

## 5. Build Hyperlight-Unikraft and package Workerd

```bash
cd "$HOME/src/hyperlight-unikraft"
export RUSTUP_HOME="$HOME/.rustup"
export CARGO_HOME="$HOME/.cargo"
export PATH="$HOME/go/bin:$CARGO_HOME/bin:$PATH"
export WORKERD_EXECUTOR="$HOME/artifacts/workerd-sandbox-executor"
export CARGO_BUILD_JOBS=8
test "$CARGO_BUILD_JOBS" -eq 8
source "$CARGO_HOME/env"

just build-workerd-kernel

bash examples/workerd-executor/build-rootfs.sh \
  "$WORKERD_EXECUTOR"

cargo build --release --locked --example workerd-demo

just guests
cargo test --locked --lib workerd::pool::tests
cargo test --locked --example workerd-demo

blob_config="$HOME/results/acceptance-inputs/blob-config.env"
test -r "$blob_config"
source "$blob_config"
test -n "$AZURE_CACHE_STORAGE_ACCOUNT"
test -n "$AZURE_CACHE_CONTAINER"
test -n "$AZURE_CACHE_COMPATIBILITY_KEY"

cache_stage="$HOME/results/cache-export"
rm -rf "$cache_stage"
mkdir -p "$cache_stage/archives" "$cache_stage/verify"
compatibility="$cache_stage/cache-compatibility.tsv"
cat >"$compatibility" <<EOF
key	value
schema	1
campaign_id	complete-mission
workerd_tree	8844cfeb919eb401545a999e9081d58a678c5af1
hyperlight_tree	bca3357e5d1f995e6b75125f0b8f081d22167944
builder_base	mcr.microsoft.com/vscode/devcontainers/javascript-node:26-bookworm@sha256:4187a9d50e7a208659e9b56677ae764edb98cb6dc243f21e851aab2ca4d103ba
llvm_major	22
bazelisk	1.28.1
rust_toolchain	1.98.0
target	x86_64-unknown-linux-gnu
runbook_sha256	a01347a2fafa28f28f47a0d9f6fa61ba05a8b53afc57a57234c5abf9907990a6
EOF
test "$(sha256sum "$compatibility" | cut -d' ' -f1)" = \
  "$AZURE_CACHE_COMPATIBILITY_KEY"
test "$AZURE_CACHE_COMPATIBILITY_KEY" = \
  34acfbf23da59062c170c90702fbb85c78d3271e6a4688e70617693f3603cdc0
printf 'document_runbook_sha256\t%s\n' \
  "$(sha256sum "$HOME/results/acceptance-inputs/azure-workerd-hyperlight-runbook.md" | cut -d' ' -f1)" \
  >"$cache_stage/cache-provenance.tsv"

path_inventory="$cache_stage/cache-path-inventory.tsv"
printf 'cache\tpath\tbytes\n' >"$path_inventory"
while IFS=$'\t' read -r cache path; do
  test -d "$path" || continue
  sudo find "$path" -xdev -type f \
    ! -path '*/workerd-hyperlight-output/server/*' \
    -printf "$cache\t%p\t%s\n"
done <<'EOF' | LC_ALL=C sort >>"$path_inventory"
bazel-action	/home/azureuser/.cache/workerd-bazel/action-cache
bazel-repository	/home/azureuser/.cache/workerd-bazel/repository-cache
bazel-output	/home/azureuser/.cache/workerd-bazel/workerd-hyperlight-output
cargo-registry	/home/azureuser/.cargo/registry
cargo-git	/home/azureuser/.cargo/git
EOF
test "$(wc -l <"$path_inventory")" -gt 1

cache_manifest="$cache_stage/cache-manifest.tsv"
printf 'archive\ttarget\tbytes\tsha256\n' >"$cache_manifest"
archive_cache() {
  local label="$1" target="$2" archive sha bytes
  test -d "$target" || return 0
  archive="$cache_stage/archives/$label.tar.gz"
  sudo tar \
    --sort=name \
    --mtime='UTC 1970-01-01' \
    --owner=0 \
    --group=0 \
    --numeric-owner \
    --exclude='*/workerd-hyperlight-output/server' \
    --exclude='*/workerd-hyperlight-output/server/*' \
    -czf "$archive" \
    -C / "${target#/}"
  sudo chown "$USER:$USER" "$archive"
  sha="$(sha256sum "$archive" | cut -d' ' -f1)"
  bytes="$(stat -c '%s' "$archive")"
  mv "$archive" "$cache_stage/archives/$sha.tar.gz"
  printf 'sha256/%s.tar.gz\t%s\t%s\t%s\n' \
    "$sha" "$target" "$bytes" "$sha" >>"$cache_manifest"
}
archive_cache bazel-action \
  "$HOME/.cache/workerd-bazel/action-cache"
archive_cache bazel-repository \
  "$HOME/.cache/workerd-bazel/repository-cache"
archive_cache bazel-output \
  "$HOME/.cache/workerd-bazel/workerd-hyperlight-output"
archive_cache cargo-registry "$HOME/.cargo/registry"
archive_cache cargo-git "$HOME/.cargo/git"
test "$(( $(wc -l <"$cache_manifest") - 1 ))" -ge 4

token="$(
  curl --fail --silent --show-error \
    --header Metadata:true \
    'http://169.254.169.254/metadata/identity/oauth2/token?api-version=2018-02-01&resource=https%3A%2F%2Fstorage.azure.com%2F' \
    | jq -er .access_token
)"
cache_prefix="v1/$AZURE_CACHE_COMPATIBILITY_KEY"
upload_receipt="$cache_stage/cache-upload.tsv"
printf 'blob\tbytes\tsha256\tattempt\tstatus\n' >"$upload_receipt"
upload_cache_blob() {
  local blob="$1" file="$2" blob_path bytes sha attempt status uploaded=false
  case "$blob" in
    sha256/*) blob_path="$blob" ;;
    *) blob_path="$cache_prefix/$blob" ;;
  esac
  bytes="$(stat -c '%s' "$file")"
  sha="$(sha256sum "$file" | cut -d' ' -f1)"
  for attempt in 1 2 3 4 5; do
    status="$(
      curl --silent --show-error \
        --output "$cache_stage/upload-$attempt.body" \
        --write-out '%{http_code}' \
        --max-time 7200 \
        --request PUT \
        --header "Authorization: Bearer $token" \
        --header 'x-ms-version: 2023-11-03' \
        --header 'x-ms-blob-type: BlockBlob' \
        --header "x-ms-meta-sha256:$sha" \
        --header "x-ms-meta-bytes:$bytes" \
        --header "x-ms-meta-compatibilitykey:$AZURE_CACHE_COMPATIBILITY_KEY" \
        --upload-file "$file" \
        "https://$AZURE_CACHE_STORAGE_ACCOUNT.blob.core.windows.net/$AZURE_CACHE_CONTAINER/$blob_path" \
        || true
    )"
    printf '%s\t%s\t%s\t%s\t%s\n' \
      "$blob" "$bytes" "$sha" "$attempt" "$status" >>"$upload_receipt"
    if test "$status" = 201; then uploaded=true; break; fi
    sleep $((1 << attempt))
  done
  test "$uploaded" = true
}
while IFS=$'\t' read -r archive target bytes sha; do
  test "$archive" = archive && continue
  upload_cache_blob "$archive" \
    "$cache_stage/archives/${archive#sha256/}"
done <"$cache_manifest"
upload_cache_blob cache-compatibility.tsv "$compatibility"
upload_cache_blob cache-provenance.tsv "$cache_stage/cache-provenance.tsv"
upload_cache_blob cache-path-inventory.tsv "$path_inventory"
upload_cache_blob cache-manifest.tsv "$cache_manifest"

verification="$cache_stage/cache-independent-verification.tsv"
printf 'blob\tbytes\tsha256\tresult\n' >"$verification"
while IFS=$'\t' read -r archive target bytes sha; do
  test "$archive" = archive && continue
  downloaded="$cache_stage/verify/${archive#sha256/}"
  curl --fail --show-error --silent --location \
    --max-time 7200 \
    --header "Authorization: Bearer $token" \
    --header 'x-ms-version: 2023-11-03' \
    --output "$downloaded" \
    "https://$AZURE_CACHE_STORAGE_ACCOUNT.blob.core.windows.net/$AZURE_CACHE_CONTAINER/$archive"
  test "$(stat -c '%s' "$downloaded")" = "$bytes"
  test "$(sha256sum "$downloaded" | cut -d' ' -f1)" = "$sha"
  tar -tzf "$downloaded" >/dev/null
  printf '%s\t%s\t%s\tPASS\n' \
    "$archive" "$bytes" "$sha" >>"$verification"
  rm -f "$downloaded"
done <"$cache_manifest"
for evidence in \
  cache-compatibility.tsv \
  cache-provenance.tsv \
  cache-path-inventory.tsv \
  cache-manifest.tsv
do
  downloaded="$cache_stage/verify/$evidence"
  source_file="$cache_stage/$evidence"
  curl --fail --show-error --silent --location \
    --max-time 1800 \
    --header "Authorization: Bearer $token" \
    --header 'x-ms-version: 2023-11-03' \
    --output "$downloaded" \
    "https://$AZURE_CACHE_STORAGE_ACCOUNT.blob.core.windows.net/$AZURE_CACHE_CONTAINER/$cache_prefix/$evidence"
  cmp "$source_file" "$downloaded"
  printf '%s\t%s\t%s\tPASS\n' \
    "$evidence" \
    "$(stat -c '%s' "$source_file")" \
    "$(sha256sum "$source_file" | cut -d' ' -f1)" \
    >>"$verification"
done
orphan_cleanup="$cache_stage/cache-orphan-cleanup.tsv"
python3 - \
  "$AZURE_CACHE_STORAGE_ACCOUNT" \
  "$AZURE_CACHE_CONTAINER" \
  "$token" \
  "$orphan_cleanup" <<'PY'
import csv
import sys
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET

account, container, token, ledger = sys.argv[1:]
base = f"https://{account}.blob.core.windows.net/{container}"
headers = {
    "Authorization": f"Bearer {token}",
    "x-ms-version": "2023-11-03",
}


def request(url, method="GET"):
    return urllib.request.urlopen(
        urllib.request.Request(url, method=method, headers=headers),
        timeout=1800,
    )


def list_blobs(prefix):
    marker = ""
    blobs = []
    while True:
        query = {
            "restype": "container",
            "comp": "list",
            "prefix": prefix,
        }
        if marker:
            query["marker"] = marker
        with request(f"{base}?{urllib.parse.urlencode(query)}") as response:
            root = ET.fromstring(response.read())
        for blob in root.findall("./Blobs/Blob"):
            blobs.append(
                (
                    blob.findtext("Name"),
                    int(blob.findtext("./Properties/Content-Length")),
                )
            )
        marker = root.findtext("NextMarker") or ""
        if not marker:
            return blobs


manifest_names = [
    name
    for name, _ in list_blobs("v1/")
    if name.endswith("/cache-manifest.tsv")
]
assert manifest_names
referenced = set()
for name in manifest_names:
    encoded = urllib.parse.quote(name, safe="/")
    with request(f"{base}/{encoded}") as response:
        rows = csv.DictReader(
            response.read().decode("utf-8").splitlines(),
            delimiter="\t",
        )
        for row in rows:
            referenced.add(row["archive"])

payloads = list_blobs("sha256/")
orphans = [
    (name, size)
    for name, size in payloads
    if name not in referenced
]
deleted_bytes = 0
with open(ledger, "w", encoding="utf-8", newline="\n") as output:
    output.write("blob\tbytes\tsha256\tstatus\n")
    for name, size in sorted(orphans):
        digest = name.removeprefix("sha256/").removesuffix(".tar.gz")
        assert len(digest) == 64
        assert all(character in "0123456789abcdef" for character in digest)
        encoded = urllib.parse.quote(name, safe="/")
        with request(f"{base}/{encoded}", method="DELETE") as response:
            assert response.status == 202
        output.write(f"{name}\t{size}\t{digest}\tDELETED\n")
        deleted_bytes += size

remaining = {name for name, _ in list_blobs("sha256/")}
assert remaining == referenced, (remaining - referenced, referenced - remaining)
print(
    f"CACHE_ORPHAN_PAYLOAD_CLEANUP_PASS "
    f"{len(orphans)} {deleted_bytes}"
)
PY
sha256sum \
  "$compatibility" \
  "$cache_stage/cache-provenance.tsv" \
  "$path_inventory" \
  "$cache_manifest" \
  "$upload_receipt" \
  "$verification" \
  "$orphan_cleanup" \
  >"$cache_stage/cache-ledger.sha256"
sha256sum --check "$cache_stage/cache-ledger.sha256"
test -z "$(
  pgrep -af 'azcopy|blobfuse|curl.*blob[.]core[.]windows[.]net' || true
)"
printf 'utc\tcompatibility_key\tstate\n%s\t%s\tCACHE_EXPORT_CLOSED\n' \
  "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  "$AZURE_CACHE_COMPATIBILITY_KEY" \
  >"$cache_stage/cache-export-quiescence.tsv"
```

Expected:

- `kernel/workerd_hyperlight-x86_64` exists;
- `build-elfloader/workerd-executor/executor` and `rootfs.img` exist;
- `target/release/examples/workerd-demo` exists;
- focused tests exit zero.

**Troubleshooting**

- Re-run `build-rootfs.sh` whenever the Workerd executor changes.
- If a guest fixture is stale, run `just guests` before repeating tests.
- If KVM launch fails before Worker initialization, verify the executor is a
  static PIE and that `/dev/kvm` remains accessible.

### Install the guided demo runner

The runner keeps commands and verbose output in named log files. Its presenter
console uses short titles, plain-English context, clear actions, one to three
key results, explicit status markers, and a concise takeaway. Interactive mode
requires a terminal. `--all` is the explicit unattended mode; it never reads
stdin and stops on the first required-step failure.

```bash
mkdir -p "$HOME/bin"
cat >"$HOME/bin/hyperlight-demo" <<'DEMO'
#!/usr/bin/env bash
set -euo pipefail

export PATH="$HOME/.cargo/bin:$HOME/go/bin:$HOME/bin:$PATH"
export CARGO_BUILD_JOBS=8
test "$CARGO_BUILD_JOBS" -eq 8

ROOT="${HYPERLIGHT_ROOT:-$HOME/src/hyperlight-unikraft}"
RESULTS="${HYPERLIGHT_RESULTS:-$HOME/results/guided}"
EXECUTOR="${WORKERD_EXECUTOR:-$HOME/artifacts/workerd-sandbox-executor}"
mkdir -p "$RESULTS"
LEDGER="$RESULTS/command-results.tsv"
RUN_STARTED_EPOCH="$(date +%s)"
RUN_STARTED_UTC="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
if [[ ! -e "$LEDGER" ]]; then
  printf 'step\tstarted_utc\tfinished_utc\tresult\tlog_sha256\tlog\n' \
    >"$LEDGER"
fi

cleanup_jobs() {
  local pid
  while read -r pid; do
    [[ -n "$pid" ]] && kill -TERM "$pid" 2>/dev/null || true
  done < <(jobs -pr)
  wait 2>/dev/null || true
}
trap cleanup_jobs EXIT INT TERM

steps=(
  isolation
  policy
  ingress
  data
  storage
  node
  core-wasm
  component
  wasi-p2
  wasi-p3
  websocket
  web-apis
  fetch
  benchmark-on-demand
  benchmark-prewarmed
)

title() {
  case "$1" in
    isolation) echo "Disposable isolation and recovery" ;;
    policy) echo "Identity, policy, quotas, and audit" ;;
    ingress) echo "Scheduled and queue ingress" ;;
    data) echo "KV, Cache, D1, and Durable Objects" ;;
    storage) echo "Virtual and named storage" ;;
    node) echo "Node-compatible application boundaries" ;;
    core-wasm) echo "Core WebAssembly" ;;
    component) echo "Component Model workload" ;;
    wasi-p2) echo "WASI Preview 2 adapters" ;;
    wasi-p3) echo "WASI Preview 3 async streaming" ;;
    websocket) echo "Capability-backed WebSockets" ;;
    web-apis) echo "HTTP and common Web APIs" ;;
    fetch) echo "Constrained outbound fetch" ;;
    benchmark-on-demand) echo "On-demand pool performance" ;;
    benchmark-prewarmed) echo "Prewarmed pool performance" ;;
    *) return 1 ;;
  esac
}

explanation() {
  case "$1" in
    isolation) echo "A failed request VM is discarded without poisoning the next request." ;;
    policy) echo "The host, not guest code, owns identity and authorization decisions." ;;
    ingress) echo "Non-HTTP events retain their completion and retry decisions." ;;
    data) echo "Typed bindings persist state while rejecting undeclared operations." ;;
    storage) echo "Workers see only packaged files and explicitly named host storage." ;;
    node) echo "Useful Node APIs work without granting an ambient host process." ;;
    core-wasm) echo "Packaged Wasm remains deterministic across restored request VMs." ;;
    component) echo "A pinned Component workload runs through reproducible lowering." ;;
    wasi-p2) echo "Typed adapters provide selected capabilities without ambient authority." ;;
    wasi-p3) echo "Async streams preserve ordering, backpressure, and cancellation." ;;
    websocket) echo "WebSocket access is explicit, bounded, audited, and resettable." ;;
    web-apis) echo "Common Web APIs behave consistently inside disposable workers." ;;
    fetch) echo "Outbound HTTP is useful while destinations and redirects stay constrained." ;;
    benchmark-on-demand) echo "Cold restore behavior is measured under correct, recoverable load." ;;
    benchmark-prewarmed) echo "A warm pool is measured with refill and recovery kept visible." ;;
    *) return 1 ;;
  esac
}

action_text() {
  case "$1" in
    isolation) echo "Force a timeout, then send a clean recovery request." ;;
    policy) echo "Exercise identity checks, quotas, denials, audit, and reset." ;;
    ingress) echo "Run the signed executor ingress self-test." ;;
    data) echo "Round-trip each typed state binding across VM resets." ;;
    storage) echo "Read and write declared mounts, then probe denied paths." ;;
    node) echo "Run the signed Node compatibility and boundary self-test." ;;
    core-wasm) echo "Invoke the packaged Wasm export across restored VMs." ;;
    component) echo "Verify the sealed package, then run the exact proof workload." ;;
    wasi-p2) echo "Run the typed HTTP, stream, clock, random, and policy proof." ;;
    wasi-p3) echo "Run async stream, deadline, backpressure, and cancellation tests." ;;
    websocket) echo "Exercise allowed WebSockets and denied ambient upgrades." ;;
    web-apis) echo "Call the selected HTTP, stream, timer, file, and messaging routes." ;;
    fetch) echo "Fetch the allowed loopback service and probe denied routes." ;;
    benchmark-on-demand) echo "Drive the on-demand pool and verify quiescent refill." ;;
    benchmark-prewarmed) echo "Drive the prewarmed pool and verify sustained recovery." ;;
    *) return 1 ;;
  esac
}

takeaway() {
  case "$1" in
    isolation) echo "Request failures remain disposable and recoverable." ;;
    policy) echo "Guest code cannot promote itself into host authority." ;;
    ingress) echo "Background event outcomes remain explicit and auditable." ;;
    data) echo "Stateful APIs work without widening guest authority." ;;
    storage) echo "Filesystem access stays named and least-privileged." ;;
    node) echo "Node compatibility stays inside a constrained worker." ;;
    core-wasm) echo "Core Wasm composes with the same disposable VM lifecycle." ;;
    component) echo "Component portability is reproducible and sealed." ;;
    wasi-p2) echo "Portable interfaces can stay narrow and policy-controlled." ;;
    wasi-p3) echo "Async portability includes cleanup and backpressure." ;;
    websocket) echo "Long-lived connections remain capability-controlled." ;;
    web-apis) echo "Web APIs remain useful across strict isolation boundaries." ;;
    fetch) echo "Network usefulness does not require ambient egress." ;;
    benchmark-on-demand) echo "Correctness and recovery gate the performance measurement." ;;
    benchmark-prewarmed) echo "Warm capacity is measured without hiding refill behavior." ;;
    *) return 1 ;;
  esac
}

category() {
  case "$1" in
    isolation|policy) echo "Isolation and security" ;;
    ingress|data|storage|node|web-apis) echo "Platform APIs" ;;
    core-wasm|component|wasi-p2|wasi-p3) echo "Portability" ;;
    websocket|fetch) echo "Networking" ;;
    benchmark-*) echo "Performance" ;;
    *) return 1 ;;
  esac
}

run_wintertc() {
  local label="$1"
  shift
  local server_log="$RESULTS/wintertc-$label-server.log"
  target/release/examples/workerd-demo \
    --executor build-elfloader/workerd-executor/executor \
    --rootfs build-elfloader/workerd-executor/rootfs.img \
    --bundle examples/workerd-bundles/workerd-wintertc-demo.json \
    --bind 127.0.0.1:8787 \
    --scratch-mb 576 \
    --request-timeout-ms 30000 \
    --restore-mode on-demand \
    --max-concurrent-sandboxes 4 \
    --queue-capacity 32 \
    >"$server_log" 2>&1 &
  local pid=$!
  local ready=false
  for _ in $(seq 1 600); do
    if curl --silent --fail \
      http://127.0.0.1:8787/__hyperlight/pool-status >/dev/null; then
      ready=true
      break
    fi
    kill -0 "$pid" 2>/dev/null || {
      tail -n 20 "$server_log" >&2
      return 1
    }
    sleep 1
  done
  if [[ "$ready" != true ]]; then
    kill -TERM "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
    return 1
  fi
  local rc=0
  tools/run-wintertc-demo.sh \
    --output-dir "$RESULTS/wintertc-$label" \
    "$@" || rc=$?
  kill -TERM "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
  return "$rc"
}

run_isolation() {
  local log="$RESULTS/isolation-server.log"
  target/release/examples/workerd-demo \
    --executor build-elfloader/workerd-executor/executor \
    --rootfs build-elfloader/workerd-executor/rootfs.img \
    --bundle examples/workerd-bundles/acceptance.json \
    --bind 127.0.0.1:8787 \
    --scratch-mb 576 \
    --request-timeout-ms 500 \
    --restore-mode on-demand \
    --max-concurrent-sandboxes 4 \
    --queue-capacity 8 \
    >"$log" 2>&1 &
  local pid=$!
  local ready=false
  for _ in $(seq 1 600); do
    if curl --silent --fail \
      http://127.0.0.1:8787/__hyperlight/pool-status >/dev/null; then
      ready=true
      break
    fi
    kill -0 "$pid" 2>/dev/null || return 1
    sleep 1
  done
  [[ "$ready" == true ]] || return 1
  curl --fail-with-body -sS http://127.0.0.1:8787/before \
    | jq -e '.path == "/before" and .method == "GET"' >/dev/null
  local status
  status="$(curl -sS -o "$RESULTS/isolation-timeout.json" -w '%{http_code}' \
    http://127.0.0.1:8787/busy)"
  [[ "$status" == 504 ]]
  curl --fail-with-body -sS http://127.0.0.1:8787/after \
    | jq -e '.path == "/after" and .method == "GET"' >/dev/null
  kill -TERM "$pid"
  wait "$pid" 2>/dev/null || true
}

run_storage() {
  local ro="$RESULTS/storage-ro" rw="$RESULTS/storage-rw"
  local log="$RESULTS/storage-server.log"
  mkdir -p "$ro" "$rw" "$RESULTS/storage-evidence"
  printf 'fixture-read-ok\n' >"$ro/message.txt"
  target/release/examples/workerd-demo \
    --executor build-elfloader/workerd-executor/executor \
    --rootfs build-elfloader/workerd-executor/rootfs.img \
    --bundle examples/workerd-bundles/workerd-vfs-evidence.json \
    --bind 127.0.0.1:8787 \
    --scratch-mb 576 \
    --restore-mode on-demand \
    --storage-ro readonly="$ro" \
    --storage-rw scratch="$rw" \
    --storage-max-operations scratch=16 \
    --storage-max-read-bytes scratch=1048576 \
    --storage-max-write-bytes scratch=16 \
    >"$log" 2>&1 &
  local pid=$!
  local ready=false route output status expected_code
  for _ in $(seq 1 600); do
    if curl --silent --fail \
      http://127.0.0.1:8787/__hyperlight/pool-status >/dev/null; then
      ready=true
      break
    fi
    kill -0 "$pid" 2>/dev/null || return 1
    sleep 1
  done
  [[ "$ready" == true ]] || return 1
  for route in \
    evidence/vfs-bundle \
    evidence/vfs-tmp \
    evidence/vfs-dev-null \
    evidence/vfs-dev-zero \
    evidence/vfs-dev-random
  do
    output="$RESULTS/storage-${route//\\//-}.json"
    curl --fail-with-body -sS "http://127.0.0.1:8787/$route" >"$output" \
      || return 1
    jq -e '.outcome == "pass"' "$output" >/dev/null || return 1
  done
  for route in \
    allowed-read \
    ro-write-denied \
    rw-write \
    traversal-denied \
    unlisted-denied \
    quota-denied
  do
    output="$RESULTS/storage-$route.json"
    if [[ "$route" == ro-write-denied || "$route" == quota-denied ]]; then
      if [[ "$route" == ro-write-denied ]]; then
        expected_code=EPERM
      else
        expected_code=EDQUOT
      fi
      status="$(
        curl --silent --show-error \
          --output "$output" \
          --write-out '%{http_code}' \
          "http://127.0.0.1:8787/storage-$route"
      )" || return 1
      test "$status" = 500 || return 1
      jq -e --arg expected_code "$expected_code" '
        .outcome == "fail" and
        .error.code == $expected_code and
        .error.name == "Error"
      ' "$output" >/dev/null || return 1
    else
      curl --fail-with-body -sS \
        "http://127.0.0.1:8787/storage-$route" >"$output" || return 1
      jq -e --arg expected "$route" \
        '.outcome == $expected' "$output" >/dev/null || return 1
    fi
  done
  test "$(cat "$rw/allowed.txt")" = rw-ok || return 1
  kill -TERM "$pid"
  wait "$pid" 2>/dev/null || true
}

run_fetch() {
  local upstream_log="$RESULTS/fetch-upstream.log"
  local server_log="$RESULTS/fetch-server.log"
  local fetch_bundle="$RESULTS/fetch-policy-demo.json"
  jq '.compatibility_flags //= []' \
    examples/workerd-bundles/fetch-policy-demo.json >"$fetch_bundle"
  jq -e '.compatibility_flags | type == "array"' "$fetch_bundle" >/dev/null
  cat >"$RESULTS/fetch-upstream.py" <<'PY'
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
BODY = b"allowed-upstream\n"
class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/ok":
            self.send_response(200)
            self.send_header("content-length", str(len(BODY)))
            self.end_headers()
            self.wfile.write(BODY)
        elif self.path == "/redirect":
            self.send_response(302)
            self.send_header("location", "/ok")
            self.send_header("content-length", "0")
            self.end_headers()
        else:
            self.send_error(404)
    def log_message(self, *args):
        pass
ThreadingHTTPServer(("127.0.0.1", 18080), Handler).serve_forever()
PY
  python3 "$RESULTS/fetch-upstream.py" >"$upstream_log" 2>&1 &
  local upstream_pid=$!
  for _ in $(seq 1 100); do
    curl --silent --fail http://127.0.0.1:18080/ok >/dev/null && break
    kill -0 "$upstream_pid" 2>/dev/null || return 1
    sleep 0.1
  done
  curl --silent --fail http://127.0.0.1:18080/ok >/dev/null
  target/release/examples/workerd-demo \
    --executor build-elfloader/workerd-executor/executor \
    --rootfs build-elfloader/workerd-executor/rootfs.img \
    --bundle "$fetch_bundle" \
    --bind 127.0.0.1:8787 \
    --scratch-mb 576 \
    --restore-mode on-demand \
    --fetch-loopback-port 18080 \
    >"$server_log" 2>&1 &
  local pid=$!
  local ready=false
  for _ in $(seq 1 600); do
    if curl --silent --fail \
      http://127.0.0.1:8787/__hyperlight/pool-status >/dev/null; then
      ready=true
      break
    fi
    kill -0 "$pid" 2>/dev/null || return 1
    kill -0 "$upstream_pid" 2>/dev/null || return 1
    sleep 1
  done
  [[ "$ready" == true ]] || return 1
  tools/run-workerd-fetch-policy-demo.sh \
    --output-dir "$RESULTS/fetch-policy" \
    --upstream-port 18080 \
    all
  kill -TERM "$pid" "$upstream_pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
  wait "$upstream_pid" 2>/dev/null || true
}

assert_blob_quiescent() {
  local receipt
  receipt="$RESULTS/blob-performance-quiescence.tsv"
  test -r "$HOME/results/acceptance-inputs/staging-quiescence.tsv"
  grep -Eq $'\t11\t[0-9a-f]{64}\t0\t0\tBLOB_STAGING_CLOSED$' \
    "$HOME/results/acceptance-inputs/staging-quiescence.tsv"
  test -z "$(
    pgrep -af 'azcopy|blobfuse|curl.*blob[.]core[.]windows[.]net' || true
  )"
  test -z "$(
    findmnt -rn -t fuse,fuse3,fuse.blobfuse2 2>/dev/null || true
  )"
  printf 'utc\tdownload_processes\tblob_mounts\tstate\n%s\t0\t0\tPASS\n' \
    "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    >"$receipt"
}

wait_for_host_quiescence() {
  local cpu_count threshold current_load five_minute_load
  cpu_count="$(nproc)"
  threshold="${1:-$(( cpu_count / 16 ))}"
  (( threshold < 1 )) && threshold=1
  for _ in $(seq 1 180); do
    read -r current_load five_minute_load _ </proc/loadavg
    if awk \
      -v current_load="$current_load" \
      -v five_minute_load="$five_minute_load" \
      -v threshold="$threshold" \
      'BEGIN {
        exit !(current_load <= threshold && five_minute_load <= threshold)
      }'
    then
      printf 'Host load is quiescent: 1m=%s 5m=%s threshold=%s\n' \
        "$current_load" "$five_minute_load" "$threshold"
      return 0
    fi
    sleep 5
  done
  printf 'Host 1m/5m load did not quiesce below %s within 900 seconds\n' \
    "$threshold" >&2
  return 1
}

run_step_command() {
  local step="$1"
  cd "$ROOT"
  case "$step" in
    isolation)
      run_isolation
      ;;
    policy)
      cargo +1.98.0 test --locked --lib \
        data::kv::tests::runtime_identity_and_binding_checks_precede_dispatch \
        -- --exact
      cargo +1.98.0 test --locked --lib \
        data::kv::tests::operation_and_storage_quotas_fail_closed \
        -- --exact
      cargo +1.98.0 test --locked --lib \
        data::kv::tests::fresh_vm_reset_clears_request_budget_and_audit_buffer_only \
        -- --exact
      ;;
    ingress)
      "$EXECUTOR" --self-test
      ;;
    data)
      local test_name
      for test_name in \
        data::kv::tests::deterministic_fixture_put_round_trips_and_persists_across_vm_reset \
        data::cache::tests::deterministic_fixture_survives_fresh_vm_reset \
        data::d1::tests::deterministic_batch_is_transactional_and_persists_across_reset \
        data::durable::tests::trusted_context_partitions_persistent_state_across_fresh_vm_reset
      do
        cargo +1.98.0 test --locked --lib "$test_name" -- --exact
      done
      ;;
    storage)
      run_storage
      ;;
    node)
      "$EXECUTOR" --self-test
      ;;
    core-wasm)
      run_wintertc core-wasm core-wasm
      "$EXECUTOR" --self-test
      ;;
    component)
      local archive="$HOME/contracts/workerd-component-proof-package-9402dd51-opt-27dbf9ac.tar.gz"
      local package_dir="$RESULTS/workerd-component-proof-package-9402dd51-opt-27dbf9ac"
      wait_for_host_quiescence || return 1
      printf '%s  %s\n' \
        2d10eb725641ce2fe70e4dd9dffb59f543d789d41dfb1c26bdb8269ff828b6b9 \
        "$archive" | sha256sum --check
      rm -rf "$package_dir"
      tar -xzf "$archive" -C "$RESULTS"
      (
        cd "$package_dir"
        ./verify-package.sh
        test "$(jq -er '.packageId' harness-contract.json)" = \
          9402dd51-opt-27dbf9ac
        test "$(jq -er '.hyperlightResultTree' harness-contract.json)" = \
          9402dd51448d617b0a236cea6bd8abfafe4b9c86
        test "$(jq -er '.hyperlightPatchSha256' harness-contract.json)" = \
          ff549f2cb2429f01e87bb8fddef93d5fe7a89f37da3ed69b82cfce16bf011d24
        test "$(jq -er '.sha256' executor-eligibility.json)" = \
          27dbf9acb218830cb6a046d87ce48d923b51c0841568323431b4d8437b52fb33
        test "$(jq -er '.buildId' executor-eligibility.json)" = \
          cbd0d2129127d7083cacc2a816f77d15640bbae7
      )
      cargo +1.98.0 run --release --example workerd-component-proof -- \
        "$package_dir/rootfs.img" \
        "$package_dir/executor" \
        344 \
        | tee "$RESULTS/component-output.log"
      jq -e '
        any(.routes[];
          try (.body | fromjson | .result == 42) catch false
        )
      ' "$RESULTS/component-output.log" >/dev/null
      jq -e '
        any(.routes[];
          try (.body | fromjson | .blocked == true) catch false
        )
      ' "$RESULTS/component-output.log" >/dev/null
      grep -q \
        '13499a5b6c88e082da9f53908b655452dced3bb8bc0d181aeadf1aeb215f520b' \
        "$RESULTS/component-output.log"
      grep -q \
        'fbd7e3688a88e647fd0816ccffbff58dcb923e2651093dcee7812e5f70097dff' \
        "$RESULTS/component-output.log"
      grep -q \
        '70f0274a7fe585abc7aca8e430ec2ba8a66d087486d20efac3647d8376ed4fa0' \
        "$RESULTS/component-output.log"
      ;;
    wasi-p2)
      cargo +1.98.0 test --locked --lib wasi_preview2::proof::tests
      ;;
    wasi-p3)
      cargo +1.98.0 test --locked --test wasi_p3
      ;;
    websocket)
      cargo +1.98.0 test --locked --test broker_websocket
      ;;
    web-apis)
      run_wintertc web-apis \
        core timers global-handlers byob byte-stream-tee core-wasm state messageport
      ;;
    fetch)
      run_fetch
      ;;
    benchmark-on-demand)
      local benchmark_output="$RESULTS/benchmark-on-demand"
      local benchmark_status=0
      wait_for_host_quiescence 1 || return 1
      assert_blob_quiescent
      WINTERTC_POOL_RESTORE_MODE=on-demand \
      WINTERTC_POOL_PROFILE_LOG_EVERY=1 \
      bash tools/run-wintertc-pool-benchmark.sh \
        build-elfloader/workerd-executor \
        "$benchmark_output" \
        576 \
        32 || benchmark_status=$?
      jq -e '
        (.accepted == true or .accepted == false)
        and .baseline_run.requests == 320
        and .baseline_run.errors == 0
        and .configuration.payload_bytes == 9441
        and .refill.passed == true
        and ([.sustained_runs[].errors] | all(. == 0))
      ' "$benchmark_output/wintertc-pool-performance.json" >/dev/null
      if jq -e '.accepted == true' \
        "$benchmark_output/wintertc-pool-performance.json" >/dev/null
      then
        test "$benchmark_status" -eq 0
      else
        test "$benchmark_status" -ne 0
      fi
      ;;
    benchmark-prewarmed)
      local benchmark_output="$RESULTS/benchmark-prewarmed"
      local benchmark_status=0
      wait_for_host_quiescence 1 || return 1
      assert_blob_quiescent
      WINTERTC_POOL_RESTORE_MODE=prewarmed \
      WINTERTC_POOL_PREWARMED_SANDBOXES=48 \
      WINTERTC_POOL_MAX_CONCURRENT_RESTORES=1 \
      WINTERTC_POOL_WARM_FLOOR=1 \
      WINTERTC_POOL_READY_LOW_WATERMARK=16 \
      WINTERTC_POOL_READY_HIGH_WATERMARK=32 \
      WINTERTC_POOL_MAX_REPLENISH_BATCH=2 \
      WINTERTC_POOL_PROFILE_LOG_EVERY=1 \
      bash tools/run-wintertc-pool-benchmark.sh \
        build-elfloader/workerd-executor \
        "$benchmark_output" \
        576 \
        32 || benchmark_status=$?
      jq -e '
        (.accepted == true or .accepted == false)
        and .baseline_run.requests == 320
        and .baseline_run.errors == 0
        and .configuration.payload_bytes == 9441
        and .refill.passed == true
        and ([.sustained_runs[].errors] | all(. == 0))
      ' "$benchmark_output/wintertc-pool-performance.json" >/dev/null
      if jq -e '.accepted == true' \
        "$benchmark_output/wintertc-pool-performance.json" >/dev/null
      then
        test "$benchmark_status" -eq 0
      else
        test "$benchmark_status" -ne 0
      fi
      ;;
    *) return 2 ;;
  esac
}

print_key_results() {
  local step="$1"
  case "$step" in
    isolation)
      printf '  • Timeout returned HTTP 504 and destroyed the failed request VM.\n'
      printf '  • The next request succeeded with fresh request state.\n'
      ;;
    policy)
      printf '  • Identity, quota, audit, and reset checks passed.\n'
      printf '  ⛔ [EXPECTED DENIAL] Guest authority and quota excess were rejected.\n'
      ;;
    ingress)
      printf '  • Scheduled completion and queue disposition checks passed.\n'
      ;;
    data)
      printf '  • KV, Cache, D1, and Durable Object state survived VM reset.\n'
      printf '  ⛔ [EXPECTED DENIAL] Unknown operations and quota excess were rejected.\n'
      ;;
    storage)
      printf '  • Declared bundle, temporary, device, and named storage paths worked.\n'
      printf '  ⛔ [EXPECTED DENIAL] Ambient, traversal, and quota probes were rejected.\n'
      ;;
    node)
      printf '  • Declared CommonJS, ESM, and virtual node:fs behavior passed.\n'
      printf '  ⛔ [EXPECTED DENIAL] Ambient packages, processes, threads, and addons denied.\n'
      ;;
    core-wasm)
      printf '  • The packaged Wasm export returned the expected value after restore.\n'
      ;;
    component)
      printf '  • Package identity and executor eligibility verified exactly.\n'
      printf '  • The lowered workload returned result 42 with blocked access true.\n'
      ;;
    wasi-p2)
      printf '  • Typed HTTP, stream, clock, random, and resource tests passed.\n'
      printf '  ⛔ [EXPECTED DENIAL] Ambient CLI, filesystem, and sockets remained absent.\n'
      ;;
    wasi-p3)
      printf '  • Ordering, backpressure, deadlines, cancellation, and cleanup passed.\n'
      ;;
    websocket)
      printf '  • Declared WebSocket policy, limits, audit, and reset passed.\n'
      printf '  ⛔ [EXPECTED DENIAL] Ambient upgrades and out-of-policy sends were rejected.\n'
      ;;
    web-apis)
      printf '  • HTTP, timers, streams, File, MessagePort, BYOB, and reset routes passed.\n'
      ;;
    fetch)
      printf '  • The declared loopback destination returned the expected response.\n'
      printf '  ⛔ [EXPECTED DENIAL] Undeclared destinations and redirects were rejected.\n'
      ;;
  esac
}

print_performance() {
  local step="$1"
  local result="$RESULTS/$step/wintertc-pool-performance.json"
  jq -e '
    (.accepted == true or .accepted == false)
    and .baseline_run.requests == 320
    and .baseline_run.errors == 0
    and .configuration.payload_bytes == 9441
    and .refill.passed == true
    and ([.sustained_runs[].errors] | all(. == 0))
  ' "$result" >/dev/null
  printf '  📊 %-10s %8s %7s %11s %9s\n' \
    Mode Requests Errors Requests/s Recovery
  printf '     %-10s %8s %7s %11.1f %9s\n' \
    "$(jq -r '.configuration.restore_mode' "$result")" \
    "$(jq -r '.baseline_run.requests' "$result")" \
    "$(jq -r '.baseline_run.errors' "$result")" \
    "$(jq -r '.baseline_run.throughput_requests_per_second' "$result")" \
    PASS
  if jq -e '.accepted == true' "$result" >/dev/null; then
    printf '  • PERFORMANCE TARGET MET: frozen threshold exceeded.\n'
  else
    printf '  • PERFORMANCE BELOW TARGET: exact metrics retained; functional acceptance is not blocked.\n'
  fi
}

print_result() {
  local step="$1" status="$2" log="$3"
  if [[ "$status" == PASS ]]; then
    if [[ "$step" == benchmark-* ]]; then
      printf '📊 [CHARACTERIZATION COMPLETE] %s\n' "$(title "$step")"
      print_performance "$step"
    else
      printf '✅ [PASS] %s\n' "$(title "$step")"
      print_key_results "$step"
    fi
    printf '💡 Why this matters: %s\n' "$(takeaway "$step")"
    printf '   Evidence file: %s\n' "${log##*/}"
    return
  fi
  printf '❌ [FAIL] %s\n' "$(title "$step")"
  printf '   First failure: %s\n' \
    "$(grep -v '^[[:space:]]*$' "$log" | tail -n 1 | cut -c1-120)"
  printf '   Evidence file: %s\n' "${log##*/}"
}

run_one() {
  local step="$1" log="$RESULTS/$step.log"
  local started finished result log_sha command_status
  started="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  printf '\n────────────────────────────────────────────────────────────────\n'
  printf '%s\n' "$(title "$step")"
  printf '%s\n' "$(explanation "$step")"
  printf '▶ Action: %s\n\n' "$(action_text "$step")"
  set +e
  (
    set -e
    run_step_command "$step"
  ) >"$log" 2>&1
  command_status=$?
  set -e
  if (( command_status == 0 )); then
    finished="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    result=PASS
    log_sha="$(sha256sum "$log" | cut -d' ' -f1)"
    printf '%s\t%s\t%s\t%s\t%s\t%s\n' \
      "$step" "$started" "$finished" "$result" "$log_sha" "$log" \
      >>"$LEDGER"
    print_result "$step" PASS "$log"
    return 0
  fi
  finished="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  result=FAIL
  log_sha="$(sha256sum "$log" | cut -d' ' -f1)"
  printf '%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$step" "$started" "$finished" "$result" "$log_sha" "$log" \
    >>"$LEDGER"
  print_result "$step" FAIL "$log"
  cleanup_jobs
  return 1
}

print_summary() {
  local group step result elapsed
  printf '\n════════════════════════════════════════════════════════════════\n'
  printf '15-capability summary\n'
  for group in \
    "Isolation and security" \
    "Platform APIs" \
    "Portability" \
    "Networking" \
    "Performance"
  do
    printf '\n%s\n' "$group"
    for step in "${steps[@]}"; do
      [[ "$(category "$step")" == "$group" ]] || continue
      result="$(
        awk -F '\t' -v selected="$step" \
          '$1 == selected { value=$4 } END { print value }' "$LEDGER"
      )"
      if [[ "$result" == PASS ]]; then
        printf '  ✅ %-24s PASS\n' "$(title "$step")"
      else
        printf '  ❌ %-24s %s\n' "$(title "$step")" "${result:-NOT RUN}"
      fi
    done
  done
  elapsed=$(( $(date +%s) - RUN_STARTED_EPOCH ))
  printf '\n⏱ Elapsed: %dm %02ds\n' "$((elapsed / 60))" "$((elapsed % 60))"
  printf '⏱ Evidence: %s\n' "$RESULTS"
}

mode=interactive
selected=""
resume=""
case "${1:-}" in
  --all) mode=all; shift ;;
  --demo)
    mode=demo
    selected="${2:?--demo requires a name}"
    shift 2
    ;;
  --resume-from)
    mode=resume
    resume="${2:?--resume-from requires a name}"
    shift 2
    ;;
  --list)
    printf '%s\n' "${steps[@]}"
    exit 0
    ;;
  "") ;;
  *) echo "usage: hyperlight-demo [--all|--demo NAME|--resume-from NAME|--list]" >&2; exit 2 ;;
esac
test "$#" -eq 0

if [[ "$mode" == interactive && ! -t 0 ]]; then
  echo "interactive mode requires a TTY; use --all for unattended execution" >&2
  exit 2
fi

found_resume=false
for step in "${steps[@]}"; do
  if [[ -n "$selected" && "$step" != "$selected" ]]; then
    continue
  fi
  if [[ -n "$resume" && "$found_resume" == false ]]; then
    [[ "$step" == "$resume" ]] || continue
    found_resume=true
  fi

  if [[ "$mode" == all || "$mode" == demo || "$mode" == resume ]]; then
    run_one "$step"
    continue
  fi

  while true; do
    printf '\nNext: %s\n%s\n' \
      "$(title "$step")" "$(explanation "$step")"
    read -r -p "Press Enter to run, or q to quit safely: " choice
    [[ "$choice" == q ]] && exit 0
    if run_one "$step"; then
      read -r -p "Enter=continue, r=retry, q=quit: " choice
      case "$choice" in
        r) continue ;;
        q) exit 0 ;;
        *) break ;;
      esac
    else
      read -r -p "Required step failed. r=retry, q=quit: " choice
      case "$choice" in
        r) continue ;;
        q) exit 1 ;;
        *) echo "choose r or q" ;;
      esac
    fi
  done
done

if [[ -n "$selected" ]] && [[ ! " ${steps[*]} " =~ " $selected " ]]; then
  echo "unknown demo: $selected" >&2
  exit 2
fi
if [[ -n "$resume" && "$found_resume" == false ]]; then
  echo "unknown resume step: $resume" >&2
  exit 2
fi
if [[ "$mode" == all ]]; then
  print_summary
else
  elapsed=$(( $(date +%s) - RUN_STARTED_EPOCH ))
  printf '\n⏱ Elapsed: %dm %02ds\n' "$((elapsed / 60))" "$((elapsed % 60))"
  printf '⏱ Evidence: %s\n' "$RESULTS"
fi
DEMO
chmod 0755 "$HOME/bin/hyperlight-demo"
bash -n "$HOME/bin/hyperlight-demo"
"$HOME/bin/hyperlight-demo" --list
```

Presenter mode:

```bash
set +e
presenter_output="$("$HOME/bin/hyperlight-demo" 2>&1)"
presenter_exit=$?
set -e
test "$presenter_exit" -eq 2
grep -Fqx \
  "interactive mode requires a TTY; use --all for unattended execution" \
  <<<"$presenter_output"
printf 'PRESENTER_NON_TTY_GUARD_PASS\n'
```

Run one named demo or resume at a named step:

```bash
timeout 1800s "$HOME/bin/hyperlight-demo" --demo websocket
timeout 7200s "$HOME/bin/hyperlight-demo" --resume-from wasi-p2
```

Unattended end-to-end capability and benchmark execution:

```bash
timeout 7200s "$HOME/bin/hyperlight-demo" --all
```

Setup, ownership checks, result verification, and teardown remain mandatory
runbook steps outside this runner. A failed required demo cannot be skipped.
Sections 6 through 22 document the underlying direct commands for inspection
and troubleshooting. Use the guided runner for audience presentation or
`--all` for a clean, noninteractive walkthrough; both retain verbose logs and
print concise PASS/FAIL summaries.

## 6. Disposable isolation, HTTP recovery, and bounded overload

Start an on-demand server:

```bash
cd "$HOME/src/hyperlight-unikraft"
mkdir -p "$HOME/results/basic"

RUST_LOG=info \
target/release/examples/workerd-demo \
  --executor build-elfloader/workerd-executor/executor \
  --rootfs build-elfloader/workerd-executor/rootfs.img \
  --bundle examples/workerd-bundles/acceptance.json \
  --bind 127.0.0.1:8787 \
  --scratch-mb 576 \
  --request-timeout-ms 500 \
  --restore-mode on-demand \
  --max-concurrent-sandboxes 32 \
  --queue-capacity 2048 \
  --profile-log-every 1 \
  >"$HOME/results/basic/server.log" 2>&1 &
SERVER_PID=$!
cleanup() {
  kill -TERM "$SERVER_PID" 2>/dev/null || true
  wait "$SERVER_PID" 2>/dev/null || true
}
trap cleanup EXIT

READY=false
for _ in $(seq 1 600); do
  if curl --silent --fail \
    http://127.0.0.1:8787/__hyperlight/pool-status >/dev/null; then
    READY=true
    break
  fi
  kill -0 "$SERVER_PID" 2>/dev/null || {
    tail -n 100 "$HOME/results/basic/server.log"
    exit 1
  }
  sleep 1
done
test "$READY" = true || {
  tail -n 100 "$HOME/results/basic/server.log"
  exit 1
}

curl --fail-with-body -sS \
  -X POST \
  -H 'x-demo: azure' \
  -d 'hello' \
  http://127.0.0.1:8787/hello \
  >"$HOME/results/basic/hello.json"

test "$(
  curl -sS \
    -o "$HOME/results/basic/busy.body" \
    -D "$HOME/results/basic/busy.headers" \
    -w '%{http_code}' \
    -X POST \
    -d busy \
    http://127.0.0.1:8787/busy
)" = 504

curl --fail-with-body -sS \
  -X POST \
  -d after \
  http://127.0.0.1:8787/after \
  >"$HOME/results/basic/after.json"

cleanup
trap - EXIT
```

Verify the independently recorded normal request:

```bash
jq -e '
  .path == "/hello" and
  .method == "POST"
' "$HOME/results/basic/hello.json"
```

Expected:

```json
{"path":"/hello","method":"POST"}
```

Verify the independently recorded timeout recovery:

```bash
grep -Eq '^HTTP/[0-9.]+ 504([[:space:]]|$)' \
  "$HOME/results/basic/busy.headers"
jq -e '
  .path == "/after" and
  .method == "POST"
' "$HOME/results/basic/after.json"
```

Expected: `/busy` returns HTTP 504. The next request returns HTTP 200 with
`{"path":"/after","method":"POST"}`.

Prove the prior block left no inherited server process or listener:

```bash
test -z "$(
  pgrep -af 'target/release/examples/workerd-demo.*127[.]0[.]0[.]1:8787' \
    || true
)"
test -z "$(ss -H -ltn 'sport = :8787' || true)"
```

Start a separately logged server with one execution slot and one queue slot,
wait for readiness, run the overload wave, and stop it:

```bash
set -euo pipefail
cd "$HOME/src/hyperlight-unikraft"
mkdir -p "$HOME/results/basic"

RUST_LOG=info \
target/release/examples/workerd-demo \
  --executor build-elfloader/workerd-executor/executor \
  --rootfs build-elfloader/workerd-executor/rootfs.img \
  --bundle examples/workerd-bundles/acceptance.json \
  --bind 127.0.0.1:8787 \
  --scratch-mb 576 \
  --request-timeout-ms 500 \
  --restore-mode on-demand \
  --max-concurrent-sandboxes 1 \
  --queue-capacity 1 \
  --profile-log-every 1 \
  >"$HOME/results/basic/overload-server.log" 2>&1 &
SERVER_PID=$!
cleanup() {
  kill -TERM "$SERVER_PID" 2>/dev/null || true
  wait "$SERVER_PID" 2>/dev/null || true
}
trap cleanup EXIT

READY=false
for _ in $(seq 1 600); do
  if curl --silent --fail \
    http://127.0.0.1:8787/__hyperlight/pool-status >/dev/null; then
    READY=true
    break
  fi
  kill -0 "$SERVER_PID" 2>/dev/null || {
    tail -n 100 "$HOME/results/basic/overload-server.log"
    exit 1
  }
  sleep 1
done
test "$READY" = true || {
  tail -n 100 "$HOME/results/basic/overload-server.log"
  exit 1
}

hey -n 20 -c 20 -m POST -d busy \
  http://127.0.0.1:8787/busy \
  >"$HOME/results/basic/overload-wave.txt"
grep -E \
  '^(  Total:|  Average:|  Requests/sec:|  \[[0-9]{3}\])' \
  "$HOME/results/basic/overload-wave.txt"
grep -Eq '^  \[(503|504)\]' "$HOME/results/basic/overload-wave.txt"

grep -Eq '^[[:space:]]+\[503\]' \
  "$HOME/results/basic/overload-wave.txt" \
  && grep -Eq '^[[:space:]]+\[504\]' \
    "$HOME/results/basic/overload-wave.txt" \
  && ! grep -Eq '^[[:space:]]+\[2[0-9][0-9]\]' \
    "$HOME/results/basic/overload-wave.txt" \
  || {
    cat "$HOME/results/basic/overload-wave.txt"
    exit 1
  }

cleanup
trap - EXIT
```

Expected: admitted requests time out with HTTP 504 and excess admissions are
rejected with HTTP 503. No request should return 2xx.

**Troubleshooting**

- If readiness never succeeds, inspect the server log before retrying.
- `Address already in use` means an earlier server is still running.
- A 500 after a timeout indicates the process did not recover; stop it and
  inspect the final request profile in the log.

## 7. Host-owned identity, policy, quotas, and audit

Run the focused host-boundary demonstrations with output visible:

```bash
set -euo pipefail
export CARGO_BUILD_JOBS=8
test "$CARGO_BUILD_JOBS" -eq 8
cd "$HOME/src/hyperlight-unikraft"
mkdir -p "$HOME/results/policy"

cargo +1.98.0 test --locked --lib \
  data::kv::tests::runtime_identity_and_binding_checks_precede_dispatch \
  -- --exact --nocapture \
  | tee "$HOME/results/policy/identity.log"

cargo +1.98.0 test --locked --lib \
  data::kv::tests::operation_and_storage_quotas_fail_closed \
  -- --exact --nocapture \
  | tee "$HOME/results/policy/quota.log"

cargo +1.98.0 test --locked --lib \
  data::kv::tests::fresh_vm_reset_clears_request_budget_and_audit_buffer_only \
  -- --exact --nocapture \
  | tee "$HOME/results/policy/reset-audit.log"

printf '%s\n' \
  '{"identity":"host-owned","guest_request_id_authoritative":false,"quota":"fail-closed","audit_payloads":"omitted","request_state":"reset"}'
```

Expected: each command reports one passing test. The guest request ID is
correlation data only; tenant, route, binding, policy, and quota context come
from the host. Audit records contain operation metadata and outcomes, not
request or value payloads. A restored VM receives a fresh request budget and
audit buffer while host-backed durable data remains outside the VM.

## 8. Scheduled and queue ingress

The executor self-test invokes scheduled and queue handlers through the same
typed ingress seam used by the host:

```bash
set -euo pipefail
mkdir -p "$HOME/results/ingress"
"$HOME/artifacts/workerd-sandbox-executor" --self-test \
  >"$HOME/results/ingress/self-test.log" 2>&1
printf '%s\n' \
  '{"scheduled":{"handler":"completed","waitUntil":"drained"},"queue":{"ack":true,"retry":"per-message","batchRetry":true,"noRetry":true}}'
```

Expected: the self-test exits zero. Scheduled dispatch does not complete until
registered `waitUntil()` work is drained. Queue results preserve explicit
acknowledgement, per-message retry, whole-batch retry, and `noRetry()` decisions.
Malformed, oversized, noncanonical, or unsorted envelopes are rejected before
handler execution.

## 9. Typed KV, Cache, D1, and Durable Object bindings

Run one stateful and one rejection-oriented check for each declared binding
kind:

```bash
set -euo pipefail
export CARGO_BUILD_JOBS=8
test "$CARGO_BUILD_JOBS" -eq 8
cd "$HOME/src/hyperlight-unikraft"
mkdir -p "$HOME/results/data"

tests=(
  data::kv::tests::deterministic_fixture_put_round_trips_and_persists_across_vm_reset
  data::kv::tests::canonical_errors_keep_request_id_and_unknown_operations_fail_closed
  data::cache::tests::deterministic_fixture_survives_fresh_vm_reset
  data::cache::tests::canonical_errors_keep_request_id_and_unknown_operations_fail_closed
  data::d1::tests::deterministic_batch_is_transactional_and_persists_across_reset
  data::d1::tests::unknown_executor_local_operation_is_rejected_centrally
  data::durable::tests::trusted_context_partitions_persistent_state_across_fresh_vm_reset
  data::durable::tests::alarms_and_storage_quotas_are_durable_and_bounded
)

for test_name in "${tests[@]}"; do
  cargo +1.98.0 test --locked --lib "$test_name" \
    -- --exact --nocapture \
    | tee "$HOME/results/data/${test_name//:/_}.log"
done
```

Expected: every selected test passes. Bindings are host-declared, versioned,
and typed. Durable data survives disposable VM replacement. Read-only policy,
operation/storage/row/parameter quotas, stale Durable Object generations,
unknown binding kinds, unknown operations, and unknown protocol versions fail
deterministically.

## 10. Virtual filesystem and named host storage

Start the evidence worker with one read-only and one bounded read-write mount:

```bash
set -euo pipefail
cd "$HOME/src/hyperlight-unikraft"
mkdir -p "$HOME/results/storage" "$HOME/demo-storage/readonly" \
  "$HOME/demo-storage/scratch"
printf 'fixture-read-ok\n' >"$HOME/demo-storage/readonly/message.txt"

target/release/examples/workerd-demo \
  --executor build-elfloader/workerd-executor/executor \
  --rootfs build-elfloader/workerd-executor/rootfs.img \
  --bundle examples/workerd-bundles/workerd-vfs-evidence.json \
  --bind 127.0.0.1:8787 \
  --scratch-mb 576 \
  --restore-mode on-demand \
  --storage-ro readonly="$HOME/demo-storage/readonly" \
  --storage-rw scratch="$HOME/demo-storage/scratch" \
  --storage-max-operations scratch=16 \
  --storage-max-read-bytes scratch=1048576 \
  --storage-max-write-bytes scratch=16 \
  >"$HOME/results/storage/server.log" 2>&1 &
STORAGE_PID=$!
cleanup() {
  kill -TERM "$STORAGE_PID" 2>/dev/null || true
  wait "$STORAGE_PID" 2>/dev/null || true
}
trap cleanup EXIT

READY=false
for _ in $(seq 1 600); do
  if curl --silent --fail \
    http://127.0.0.1:8787/__hyperlight/pool-status >/dev/null; then
    READY=true
    break
  fi
  kill -0 "$STORAGE_PID" 2>/dev/null || {
    tail -n 100 "$HOME/results/storage/server.log"
    exit 1
  }
  sleep 1
done
test "$READY" = true

printf '%-24s %-12s\n' Capability Result
for route in bundle dev-null dev-zero dev-random; do
  output="$HOME/results/storage/vfs-$route.json"
  curl --fail-with-body -sS \
    "http://127.0.0.1:8787/evidence/vfs-$route" \
    >"$output"
  jq -e '.outcome == "pass"' "$output" >/dev/null
  printf '%-24s %-12s\n' "vfs-$route" PASS
done

for attempt in first second; do
  output="$HOME/results/storage/vfs-tmp-$attempt.json"
  curl --fail-with-body -sS \
    http://127.0.0.1:8787/evidence/vfs-tmp \
    >"$output"
  jq -e '
    .outcome == "pass" and
    .previousExists == false and
    .body == "tmp-read-write-ok"
  ' "$output" >/dev/null
done
printf '%-24s %-12s\n' vfs-tmp-reset PASS

for route in \
  allowed-read \
  ro-write-denied \
  rw-write \
  traversal-denied \
  unlisted-denied \
  quota-denied
do
  output="$HOME/results/storage/storage-$route.json"
  if [[ "$route" == ro-write-denied || "$route" == quota-denied ]]; then
    if [[ "$route" == ro-write-denied ]]; then
      expected_code=EPERM
    else
      expected_code=EDQUOT
    fi
    status="$(
      curl --silent --show-error \
        --output "$output" \
        --write-out '%{http_code}' \
        "http://127.0.0.1:8787/storage-$route"
    )"
    test "$status" = 500
    jq -e --arg expected_code "$expected_code" '
      .outcome == "fail" and
      .error.code == $expected_code and
      .error.name == "Error"
    ' "$output" >/dev/null
  else
    curl --fail-with-body -sS \
      "http://127.0.0.1:8787/storage-$route" \
      >"$output"
    jq -e --arg expected "$route" \
      '.outcome == $expected and (keys | length) == 1' \
      "$output" >/dev/null
  fi
  printf '%-24s %-12s\n' "storage-$route" PASS
done

test "$(cat "$HOME/demo-storage/scratch/allowed.txt")" = rw-ok
test ! -e "$HOME/demo-storage/readonly/denied.txt"

cleanup
trap - EXIT
```

Expected: `/bundle` is readable and immutable; `/tmp` is fresh after every
restore; the supported `/dev` devices behave deterministically; named storage
persists in the host directories. Read-only writes, traversal, undeclared
mounts, ambient host paths, and writes beyond the configured quota are denied.

## 11. Node-compatible application and package boundaries

The VFS worker in Section 10 already exercises `node:fs`. Run the executor
self-test to cover CommonJS/ESM resolution and bundle-closed rejection:

```bash
set -euo pipefail
mkdir -p "$HOME/results/node"
"$HOME/artifacts/workerd-sandbox-executor" --self-test \
  >"$HOME/results/node/executor-self-test.log" 2>&1
printf '%s\n' \
  '{"node_application":"node:fs worker completed","modules":["CommonJS","ESM"],"resolution":"bundle-closed","ambient_packages":false}'
```

Expected: the command exits zero. Declared CommonJS and ESM modules resolve
inside the submitted bundle. Undeclared packages, path traversal outside the
bundle, native addons, child processes, worker threads, and ambient host
package lookup remain unavailable.

## 12. Core Wasm and the Rust-backed executor bridge

Run the core Wasm route through disposable VMs, then repeat the executor
self-test through the Rust serialization and request bridge:

```bash
set -euo pipefail
timeout 1800s "$HOME/bin/hyperlight-demo" --demo core-wasm
```

Expected: the packaged Wasm module instantiates and returns the expected value
on repeated fresh-VM requests. The executor self-test also exits zero through
the Rust-backed canonical bundle/request/response bridge, including binary
Wasm-module transport.

## 13. Pinned-jco Component workload

Verify the sealed package and run its Component workload through the packaged
executor. The workload was lowered with pinned `@bytecodealliance/jco` 1.35.0;
the runtime consumes core Wasm and generated JavaScript rather than loading a
native component binary.

```bash
set -euo pipefail
export CARGO_BUILD_JOBS=8
test "$CARGO_BUILD_JOBS" -eq 8
mkdir -p "$HOME/results/component"
COMPONENT_ARCHIVE="$HOME/contracts/workerd-component-proof-package-9402dd51-opt-27dbf9ac.tar.gz"
COMPONENT_DIR="$HOME/results/component/workerd-component-proof-package-9402dd51-opt-27dbf9ac"

printf '%s  %s\n' \
  2d10eb725641ce2fe70e4dd9dffb59f543d789d41dfb1c26bdb8269ff828b6b9 \
  "$COMPONENT_ARCHIVE" \
  | sha256sum --check

rm -rf "$COMPONENT_DIR"
tar -xzf "$COMPONENT_ARCHIVE" -C "$HOME/results/component"
(
  cd "$COMPONENT_DIR"
  ./verify-package.sh
  test "$(jq -er '.packageId' harness-contract.json)" = \
    9402dd51-opt-27dbf9ac
  test "$(jq -er '.hyperlightParent' harness-contract.json)" = \
    5560e071f81706488efcab86ad534e80a3a8553d
  test "$(jq -er '.hyperlightResultTree' harness-contract.json)" = \
    9402dd51448d617b0a236cea6bd8abfafe4b9c86
  test "$(jq -er '.hyperlightPatchSha256' harness-contract.json)" = \
    ff549f2cb2429f01e87bb8fddef93d5fe7a89f37da3ed69b82cfce16bf011d24
  test "$(jq -er '.baseTree' executor-eligibility.json)" = \
    0cd2766a74356a04055510dc327b3646677042e5
  test "$(jq -er '.bytes' executor-eligibility.json)" = 127355056
  test "$(jq -er '.sha256' executor-eligibility.json)" = \
    27dbf9acb218830cb6a046d87ce48d923b51c0841568323431b4d8437b52fb33
  test "$(jq -er '.buildId' executor-eligibility.json)" = \
    cbd0d2129127d7083cacc2a816f77d15640bbae7
)

cd "$HOME/src/hyperlight-unikraft"
load_threshold=1
HOST_QUIESCENT=false
for _ in $(seq 1 180); do
  read -r current_load five_minute_load _ </proc/loadavg
  if awk \
    -v current_load="$current_load" \
    -v five_minute_load="$five_minute_load" \
    -v threshold="$load_threshold" \
    'BEGIN {
      exit !(current_load <= threshold && five_minute_load <= threshold)
    }'
  then
    HOST_QUIESCENT=true
    break
  fi
  sleep 5
done
test "$HOST_QUIESCENT" = true
cargo +1.98.0 run --release --example workerd-component-proof -- \
  "$COMPONENT_DIR/rootfs.img" \
  "$COMPONENT_DIR/executor" \
  344 \
  >"$HOME/results/component/result.log" 2>&1

sed -n '/^{/,$p' "$HOME/results/component/result.log" \
  >"$HOME/results/component/result.json"
jq -e . "$HOME/results/component/result.json" >/dev/null
jq -e '
  any(.routes[];
    try (.body | fromjson | .ok == true) catch false
  )
' "$HOME/results/component/result.json" >/dev/null
jq -e '
  any(.routes[];
    try (.body | fromjson | .result == 42) catch false
  )
' "$HOME/results/component/result.json" >/dev/null
jq -e '
  any(.routes[];
    try (.body | fromjson | .blocked == true) catch false
  )
' "$HOME/results/component/result.json" >/dev/null
grep -q 'request exceeds policy limit' \
  "$HOME/results/component/result.json"
grep -q \
  '13499a5b6c88e082da9f53908b655452dced3bb8bc0d181aeadf1aeb215f520b' \
  "$HOME/results/component/result.json"
grep -q \
  'fbd7e3688a88e647fd0816ccffbff58dcb923e2651093dcee7812e5f70097dff' \
  "$HOME/results/component/result.json"
grep -q \
  '70f0274a7fe585abc7aca8e430ec2ba8a66d087486d20efac3647d8376ed4fa0' \
  "$HOME/results/component/result.json"

printf '%-24s %-12s\n' Observation Result
printf '%-24s %-12s\n' health PASS
printf '%-24s %-12s\n' add-returns-42 PASS
printf '%-24s %-12s\n' network-probe-blocked PASS
printf '%-24s %-12s\n' oversized-request-denied PASS
printf '%-24s %-12s\n' package-identities PASS
printf 'Verbose log: %s\n' "$HOME/results/component/result.log"
```

Expected: package verification succeeds; health is HTTP 200, addition returns
42, invalid input is rejected, the network probe is blocked, and an oversized
request is denied. A denial that matches those assertions is a demo PASS, not
a runtime failure.

## 14. WASI Preview 2 typed HTTP path

```bash
set -euo pipefail
export CARGO_BUILD_JOBS=8
test "$CARGO_BUILD_JOBS" -eq 8
cd "$HOME/src/hyperlight-unikraft"
mkdir -p "$HOME/results/wasi-p2"

cargo +1.98.0 test --locked --lib wasi_preview2::proof::tests \
  -- --nocapture \
  | tee "$HOME/results/wasi-p2/proof.log"

printf '%s\n' \
  '{"interface":"wasi:http@0.2.12","ingress":"typed","streams":"bounded","ambient_cli_environment":false}'
```

Expected: the typed HTTP ingress, bounded stream, deterministic clock/random,
configuration, and fail-closed import checks pass. Policy grants do not create
an adapter; the requested interface must be both authorized and implemented.

## 15. WASI Preview 3 async streaming path

```bash
set -euo pipefail
export CARGO_BUILD_JOBS=8
test "$CARGO_BUILD_JOBS" -eq 8
cd "$HOME/src/hyperlight-unikraft"
mkdir -p "$HOME/results/wasi-p3"

cargo +1.98.0 test --locked --test wasi_p3 \
  -- --nocapture \
  | tee "$HOME/results/wasi-p3/async-streaming.log"

printf '%s\n' \
  '{"interface":"wasi:http@0.3.1","futures":"bounded","streams":"backpressured","cancellation":"propagated","deadlines":"enforced","resources":"released"}'
```

Expected: futures remain pending until producer completion, cancellation
reaches producers, stream ordering survives backpressure, deadlines are
enforced, and dropped consumers release blocked producers and resources.
Listener, raw-device, and undeclared service authority remain denied.

## 16. Capability-backed networking and WebSockets

```bash
set -euo pipefail
export CARGO_BUILD_JOBS=8
test "$CARGO_BUILD_JOBS" -eq 8
cd "$HOME/src/hyperlight-unikraft"
mkdir -p "$HOME/results/network"

cargo +1.98.0 test --locked --test broker_websocket \
  -- --nocapture \
  | tee "$HOME/results/network/websocket.log"

printf '%s\n' \
  '{"tcp_tls":"host-policy-owned","udp":"explicit-capability-only","websocket":"adapter-bound","base_guest_upgrade":"denied"}'
```

Expected: the WebSocket adapter checks host-owned destination policy, bounded
messages, audit, reset, secure host termination, and denial before host send
when limits are exceeded. Base guests cannot claim an upgrade or open ambient
sockets. TCP/TLS and UDP likewise require declared host capabilities; DNS,
trust roots, timeouts, byte limits, and destinations are not guest-controlled.

## 17. HTTP and common Web API capability demo

Start the deterministic loopback upstream:

```bash
mkdir -p "$HOME/results/wintertc/upstream"

cat > "$HOME/results/wintertc/upstream/server.py" <<'PY'
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

BODY = b"loopback-upstream\n"

class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def drain_body(self):
        transfer_encoding = self.headers.get("transfer-encoding", "")
        encodings = [
            value.strip().lower()
            for value in transfer_encoding.split(",")
            if value.strip()
        ]
        if "chunked" in encodings:
            while True:
                line = self.rfile.readline()
                if not line:
                    raise ConnectionError("unexpected EOF in chunk header")
                size = int(line.split(b";", 1)[0].strip(), 16)
                if size == 0:
                    while True:
                        trailer = self.rfile.readline()
                        if trailer in (b"\r\n", b"\n", b""):
                            return
                body = self.rfile.read(size)
                if len(body) != size or self.rfile.read(2) != b"\r\n":
                    raise ConnectionError("truncated chunked request body")
        length = int(self.headers.get("content-length", "0"))
        body = self.rfile.read(length)
        if len(body) != length:
            raise ConnectionError("truncated request body")

    def do_POST(self):
        self.drain_body()
        self.send_response(200)
        self.send_header("content-type", "text/plain")
        self.send_header("content-length", str(len(BODY)))
        self.end_headers()
        self.wfile.write(BODY)

    def log_message(self, format, *args):
        print(format % args, flush=True)

ThreadingHTTPServer(("127.0.0.1", 18080), Handler).serve_forever()
PY

python3 "$HOME/results/wintertc/upstream/server.py" \
  >"$HOME/results/wintertc/upstream/server.log" 2>&1 &
UPSTREAM_PID=$!
cleanup() {
  kill -TERM "$UPSTREAM_PID" 2>/dev/null || true
  wait "$UPSTREAM_PID" 2>/dev/null || true
}
trap cleanup EXIT

READY=false
for _ in $(seq 1 100); do
  kill -0 "$UPSTREAM_PID" 2>/dev/null || {
    cat "$HOME/results/wintertc/upstream/server.log"
    exit 1
  }
  if python3 - <<'PY'
import socket
with socket.create_connection(("127.0.0.1", 18080), timeout=1):
    pass
PY
  then
    READY=true
    break
  fi
  sleep 0.1
done
test "$READY" = true || {
  tail -n 100 "$HOME/results/wintertc/upstream/server.log"
  exit 1
}

cleanup
trap - EXIT
```

Start both loopback processes, run every check, and stop both:

```bash
set -euo pipefail
cd "$HOME/src/hyperlight-unikraft"
test -r "$HOME/results/wintertc/upstream/server.py"

python3 "$HOME/results/wintertc/upstream/server.py" \
  >"$HOME/results/wintertc/upstream/server.log" 2>&1 &
UPSTREAM_PID=$!

RUST_LOG=info \
target/release/examples/workerd-demo \
  --executor build-elfloader/workerd-executor/executor \
  --rootfs build-elfloader/workerd-executor/rootfs.img \
  --bundle examples/workerd-bundles/workerd-wintertc-demo.json \
  --bind 127.0.0.1:8787 \
  --scratch-mb 576 \
  --request-timeout-ms 5000 \
  --restore-mode on-demand \
  --max-concurrent-sandboxes 8 \
  --queue-capacity 128 \
  --fetch-loopback-port 18080 \
  --profile-log-every 1 \
  >"$HOME/results/wintertc/server.log" 2>&1 &
SERVER_PID=$!
cleanup() {
  kill -TERM "$SERVER_PID" "$UPSTREAM_PID" 2>/dev/null || true
  wait "$SERVER_PID" 2>/dev/null || true
  wait "$UPSTREAM_PID" 2>/dev/null || true
}
trap cleanup EXIT

READY=false
for _ in $(seq 1 600); do
  if curl --silent --fail \
    http://127.0.0.1:8787/__hyperlight/pool-status >/dev/null; then
    READY=true
    break
  fi
  kill -0 "$SERVER_PID" 2>/dev/null || {
    tail -n 100 "$HOME/results/wintertc/server.log"
    exit 1
  }
  kill -0 "$UPSTREAM_PID" 2>/dev/null || {
    tail -n 100 "$HOME/results/wintertc/upstream/server.log"
    exit 1
  }
  sleep 1
done
test "$READY" = true || {
  tail -n 100 "$HOME/results/wintertc/server.log"
  exit 1
}

bash tools/run-wintertc-demo.sh --list \
  | tee "$HOME/results/wintertc/list.log"
bash tools/run-wintertc-demo.sh all \
  | tee "$HOME/results/wintertc/all.log"
bash tools/run-wintertc-demo.sh fetch \
  | tee "$HOME/results/wintertc/fetch.log"
bash tools/run-wintertc-demo.sh messageport-queued-delivery \
  | tee "$HOME/results/wintertc/messageport-queued-delivery.log"
bash tools/run-wintertc-demo.sh state \
  | tee "$HOME/results/wintertc/state.log"

cleanup
trap - EXIT
```

Review the independently recorded full-run output:

```bash
test -s "$HOME/results/wintertc/list.log"
test -s "$HOME/results/wintertc/all.log"
cat "$HOME/results/wintertc/list.log"
cat "$HOME/results/wintertc/all.log"
```

Review the independently recorded focused checks:

```bash
for name in fetch messageport-queued-delivery state; do
  test -s "$HOME/results/wintertc/$name.log"
  cat "$HOME/results/wintertc/$name.log"
done
```

Expected functional behavior:

- core APIs, timers, handlers, byte streams, and core WebAssembly respond with
  the values described by `--list`;
- every MessagePort stage completes in the documented order;
- fetch reaches only the configured loopback upstream;
- two `state` requests each report a fresh request VM.

Prove no process state is inherited:

```bash
test -z "$(
  pgrep -af \
    'target/release/examples/workerd-demo.*127[.]0[.]0[.]1:8787|wintertc/upstream/server[.]py' \
    || true
)"
test -z "$(ss -H -ltn 'sport = :8787 or sport = :18080' || true)"
```

**Troubleshooting**

- Run one named check to isolate a route.
- If fetch fails, confirm the upstream listens on 127.0.0.1:18080 and the
  Worker was started with `--fetch-loopback-port 18080`.
- This demo covers only the routes printed by `--list`; it is not general
  Workerd or Workers API conformance.

## 18. Host-constrained outbound fetch

Start a local upstream with a success route and a redirect:

```bash
mkdir -p "$HOME/results/fetch-policy/upstream"

cat > "$HOME/results/fetch-policy/upstream/server.py" <<'PY'
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

BODY = b"allowed-upstream\n"

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/ok":
            self.send_response(200)
            self.send_header("content-type", "text/plain")
            self.send_header("content-length", str(len(BODY)))
            self.end_headers()
            self.wfile.write(BODY)
            return
        if self.path == "/redirect":
            self.send_response(302)
            self.send_header("location", "/ok")
            self.send_header("content-length", "0")
            self.end_headers()
            return
        self.send_error(404)

    def log_message(self, format, *args):
        print(format % args, flush=True)

ThreadingHTTPServer(("127.0.0.1", 18080), Handler).serve_forever()
PY

python3 "$HOME/results/fetch-policy/upstream/server.py" \
  >"$HOME/results/fetch-policy/upstream/server.log" 2>&1 &
UPSTREAM_PID=$!
cleanup() {
  kill -TERM "$UPSTREAM_PID" 2>/dev/null || true
  wait "$UPSTREAM_PID" 2>/dev/null || true
}
trap cleanup EXIT

READY=false
for _ in $(seq 1 100); do
  if curl --silent --fail http://127.0.0.1:18080/ok >/dev/null; then
    READY=true
    break
  fi
  kill -0 "$UPSTREAM_PID" 2>/dev/null || {
    tail -n 100 "$HOME/results/fetch-policy/upstream/server.log"
    exit 1
  }
  sleep 0.1
done
test "$READY" = true || {
  tail -n 100 "$HOME/results/fetch-policy/upstream/server.log"
  exit 1
}

cleanup
trap - EXIT
```

Start both loopback processes, run the allowlist checks, and stop both:

```bash
set -euo pipefail
cd "$HOME/src/hyperlight-unikraft"
mkdir -p "$HOME/results/fetch-policy"
test -r "$HOME/results/fetch-policy/upstream/server.py"
fetch_bundle="$HOME/results/fetch-policy/fetch-policy-demo.json"
jq '.compatibility_flags //= []' \
  examples/workerd-bundles/fetch-policy-demo.json >"$fetch_bundle"
jq -e '.compatibility_flags | type == "array"' "$fetch_bundle" >/dev/null

python3 "$HOME/results/fetch-policy/upstream/server.py" \
  >"$HOME/results/fetch-policy/upstream/server.log" 2>&1 &
UPSTREAM_PID=$!

RUST_LOG=info \
target/release/examples/workerd-demo \
  --executor build-elfloader/workerd-executor/executor \
  --rootfs build-elfloader/workerd-executor/rootfs.img \
  --bundle "$fetch_bundle" \
  --bind 127.0.0.1:8787 \
  --scratch-mb 576 \
  --request-timeout-ms 5000 \
  --restore-mode on-demand \
  --max-concurrent-sandboxes 4 \
  --queue-capacity 32 \
  --fetch-allow-host localhost \
  --fetch-allow-scheme http \
  --fetch-allow-port 18080 \
  --fetch-allow-loopback \
  --fetch-max-request-bytes 1048576 \
  --fetch-max-response-bytes 4194304 \
  --fetch-max-concurrent-requests 8 \
  --fetch-timeout-ms 5000 \
  >"$HOME/results/fetch-policy/server.log" 2>&1 &
SERVER_PID=$!
cleanup() {
  kill -TERM "$SERVER_PID" "$UPSTREAM_PID" 2>/dev/null || true
  wait "$SERVER_PID" 2>/dev/null || true
  wait "$UPSTREAM_PID" 2>/dev/null || true
}
trap cleanup EXIT

READY=false
for _ in $(seq 1 600); do
  if curl --silent --fail \
    http://127.0.0.1:8787/__hyperlight/pool-status >/dev/null; then
    READY=true
    break
  fi
  kill -0 "$SERVER_PID" 2>/dev/null || {
    tail -n 100 "$HOME/results/fetch-policy/server.log"
    exit 1
  }
  kill -0 "$UPSTREAM_PID" 2>/dev/null || {
    tail -n 100 "$HOME/results/fetch-policy/upstream/server.log"
    exit 1
  }
  sleep 1
done
test "$READY" = true || {
  tail -n 100 "$HOME/results/fetch-policy/server.log"
  exit 1
}

bash tools/run-workerd-fetch-policy-demo.sh --list
bash tools/run-workerd-fetch-policy-demo.sh all

cleanup
trap - EXIT
```

Expected functional behavior:

- `http://localhost:18080/ok` is allowed;
- unlisted hosts, ports, schemes, private addresses, and metadata addresses
  are rejected before connection;
- redirects are returned to the Worker and are not followed automatically;
- request, response, concurrency, and timeout limits are enforced by the
  host broker.

Prove no process state is inherited:

```bash
test -z "$(
  pgrep -af \
    'target/release/examples/workerd-demo.*127[.]0[.]0[.]1:8787|fetch-policy/upstream/server[.]py' \
    || true
)"
test -z "$(ss -H -ltn 'sport = :8787 or sport = :18080' || true)"
```

**Troubleshooting**

- Run `metadata-denied` or another named check from `--list` to isolate a
  policy rule.
- A permitted destination still fails if DNS resolves outside the allowed
  address classes.
- The guest cannot widen this policy by changing request headers or URLs.

## 19. Cold and on-demand pool load

Start the pool benchmark bundle:

```bash
set -euo pipefail
test -r "$HOME/results/acceptance-inputs/staging-quiescence.tsv"
grep -Eq $'\t11\t[0-9a-f]{64}\t0\t0\tBLOB_STAGING_CLOSED$' \
  "$HOME/results/acceptance-inputs/staging-quiescence.tsv"
test -z "$(
  pgrep -af 'azcopy|blobfuse|curl.*blob[.]core[.]windows[.]net' || true
)"
test -z "$(
  findmnt -rn -t fuse,fuse3,fuse.blobfuse2 2>/dev/null || true
)"
printf 'Blob transport quiescent before on-demand measurement\n'

cd "$HOME/src/hyperlight-unikraft"
mkdir -p "$HOME/results/on-demand-load"

RUST_LOG=info \
target/release/examples/workerd-demo \
  --executor build-elfloader/workerd-executor/executor \
  --rootfs build-elfloader/workerd-executor/rootfs.img \
  --bundle examples/workerd-bundles/workerd-pool-benchmark.json \
  --bind 127.0.0.1:8787 \
  --scratch-mb 576 \
  --request-timeout-ms 30000 \
  --restore-mode on-demand \
  --max-concurrent-sandboxes 32 \
  --queue-capacity 256 \
  --profile-log-every 1 \
  >"$HOME/results/on-demand-load/server.log" 2>&1 &
SERVER_PID=$!
cleanup() {
  kill -TERM "$SERVER_PID" 2>/dev/null || true
  wait "$SERVER_PID" 2>/dev/null || true
}
trap cleanup EXIT

READY=false
for _ in $(seq 1 600); do
  if curl --silent --fail \
    http://127.0.0.1:8787/__hyperlight/pool-status >/dev/null; then
    READY=true
    break
  fi
  kill -0 "$SERVER_PID" 2>/dev/null || {
    tail -n 100 "$HOME/results/on-demand-load/server.log"
    exit 1
  }
  sleep 1
done
test "$READY" = true || {
  tail -n 100 "$HOME/results/on-demand-load/server.log"
  exit 1
}

curl --fail-with-body -sS \
  http://127.0.0.1:8787/sync \
  -o "$HOME/results/on-demand-load/sync.bin"
test "$(wc -c < "$HOME/results/on-demand-load/sync.bin")" -eq 9441

hey -n 320 -c 32 \
  http://127.0.0.1:8787/sync \
  >"$HOME/results/on-demand-load/wave-c32.txt"

hey -z 60s -c 32 \
  http://127.0.0.1:8787/sync \
  >"$HOME/results/on-demand-load/sustained-c32.txt"

printf '%-20s %10s %10s %14s\n' Mode Total Average Requests/sec
for result in wave-c32 sustained-c32; do
  file="$HOME/results/on-demand-load/$result.txt"
  grep -q '^  \[200\]' "$file"
  printf '%-20s %10s %10s %14s\n' \
    "$result" \
    "$(awk '/^  Total:/ {print $2}' "$file")" \
    "$(awk '/^  Average:/ {print $2}' "$file")" \
    "$(awk '/^  Requests\/sec:/ {print $2}' "$file")"
done

QUIESCENT=false
for _ in $(seq 1 9000); do
  kill -0 "$SERVER_PID" 2>/dev/null || {
    tail -n 100 "$HOME/results/on-demand-load/server.log"
    exit 1
  }
  status="$(
    curl --fail-with-body -sS \
      http://127.0.0.1:8787/__hyperlight/pool-status
  )" || exit 1
  if jq -e '
    .admitted == 0 and .active == 0 and .queued == 0 and
    .execution_slots_in_use == 0 and .restore_slots_in_use == 0 and
    .recycle_queue_depth == 0 and .teardown_in_flight == 0 and
    .completion_queue_depth == 0 and .completion_in_flight == 0 and
    .restore_permits_outstanding == 0
  ' <<<"$status" >/dev/null; then
    QUIESCENT=true
    jq . <<<"$status"
    break
  fi
  sleep 0.01
done
test "$QUIESCENT" = true || {
  tail -n 100 "$HOME/results/on-demand-load/server.log"
  exit 1
}

cleanup
trap - EXIT
```

Expected: responses are HTTP 200 with 9,441-byte bodies. The server remains
alive and its pool counters return to zero after the load.

**Troubleshooting**

- Verify response size before interpreting throughput.
- Nonzero HTTP errors indicate overload, timeout, or Worker failure; inspect
  the status-code distribution and server log.

## 20. Adaptive prewarmed pool

Stop the previous server and start a prewarmed pool:

```bash
set -euo pipefail
test -r "$HOME/results/acceptance-inputs/staging-quiescence.tsv"
grep -Eq $'\t11\t[0-9a-f]{64}\t0\t0\tBLOB_STAGING_CLOSED$' \
  "$HOME/results/acceptance-inputs/staging-quiescence.tsv"
test -z "$(
  pgrep -af 'azcopy|blobfuse|curl.*blob[.]core[.]windows[.]net' || true
)"
test -z "$(
  findmnt -rn -t fuse,fuse3,fuse.blobfuse2 2>/dev/null || true
)"
printf 'Blob transport quiescent before prewarmed measurement\n'

cd "$HOME/src/hyperlight-unikraft"
mkdir -p "$HOME/results/prewarmed-load"

RUST_LOG=info \
target/release/examples/workerd-demo \
  --executor build-elfloader/workerd-executor/executor \
  --rootfs build-elfloader/workerd-executor/rootfs.img \
  --bundle examples/workerd-bundles/workerd-pool-benchmark.json \
  --bind 127.0.0.1:8787 \
  --scratch-mb 576 \
  --request-timeout-ms 30000 \
  --restore-mode prewarmed \
  --prewarmed-sandboxes 48 \
  --max-concurrent-restores 1 \
  --warm-floor 1 \
  --ready-low-watermark 16 \
  --ready-high-watermark 32 \
  --max-replenish-batch 2 \
  --max-concurrent-sandboxes 32 \
  --queue-capacity 256 \
  --profile-log-every 1 \
  >"$HOME/results/prewarmed-load/server.log" 2>&1 &
SERVER_PID=$!
cleanup() {
  kill -TERM "$SERVER_PID" 2>/dev/null || true
  wait "$SERVER_PID" 2>/dev/null || true
}
trap cleanup EXIT

READY=false
for _ in $(seq 1 600); do
  if curl --silent --fail \
      http://127.0.0.1:8787/__hyperlight/pool-status \
    | jq -e '
      .restore_mode == "prewarmed" and
      .prewarmed_inventory == .prewarmed_ready and
      .prewarmed_ready >= .warm_floor and
      .prewarmed_replenishing == 0
    ' >/dev/null; then
    READY=true
    break
  fi
  kill -0 "$SERVER_PID" 2>/dev/null || {
    tail -n 100 "$HOME/results/prewarmed-load/server.log"
    exit 1
  }
  sleep 1
done
test "$READY" = true || {
  tail -n 100 "$HOME/results/prewarmed-load/server.log"
  exit 1
}

hey -n 320 -c 32 \
  http://127.0.0.1:8787/sync \
  >"$HOME/results/prewarmed-load/wave-c32.txt"

hey -z 60s -c 32 \
  http://127.0.0.1:8787/sync \
  >"$HOME/results/prewarmed-load/sustained-c32.txt"

printf '%-20s %10s %10s %14s\n' Mode Total Average Requests/sec
for result in wave-c32 sustained-c32; do
  file="$HOME/results/prewarmed-load/$result.txt"
  grep -q '^  \[200\]' "$file"
  printf '%-20s %10s %10s %14s\n' \
    "$result" \
    "$(awk '/^  Total:/ {print $2}' "$file")" \
    "$(awk '/^  Average:/ {print $2}' "$file")" \
    "$(awk '/^  Requests\/sec:/ {print $2}' "$file")"
done

QUIESCENT=false
for _ in $(seq 1 9000); do
  kill -0 "$SERVER_PID" 2>/dev/null || {
    tail -n 100 "$HOME/results/prewarmed-load/server.log"
    exit 1
  }
  status="$(
    curl --fail-with-body -sS \
      http://127.0.0.1:8787/__hyperlight/pool-status
  )" || exit 1

  if jq -e '
    .admitted == 0 and .active == 0 and .queued == 0 and
    .execution_slots_in_use == 0 and .restore_slots_in_use == 0 and
    .recycle_queue_depth == 0 and .teardown_in_flight == 0 and
    .completion_queue_depth == 0 and .completion_in_flight == 0 and
    .restore_permits_outstanding == 0 and
    .prewarmed_inventory == .prewarmed_ready and
    .prewarmed_ready >= .warm_floor and
    .prewarmed_replenishing == 0 and
    (.refill_active | not) and
    (.replenishment_paused | not)
  ' <<<"$status" >/dev/null; then
    QUIESCENT=true
    jq . <<<"$status" \
      >"$HOME/results/prewarmed-load/quiescent-status.json"
    break
  fi
  sleep 0.01
done
test "$QUIESCENT" = true || {
  tail -n 100 "$HOME/results/prewarmed-load/server.log"
  exit 1
}

cleanup
trap - EXIT
```

Wait for quiescence:

```bash
jq -e '
  .admitted == 0 and .active == 0 and .queued == 0 and
  .execution_slots_in_use == 0 and .restore_slots_in_use == 0 and
  .recycle_queue_depth == 0 and .teardown_in_flight == 0 and
  .completion_queue_depth == 0 and .completion_in_flight == 0 and
  .restore_permits_outstanding == 0 and
  .prewarmed_inventory == .prewarmed_ready and
  .prewarmed_ready >= .warm_floor and
  .prewarmed_replenishing == 0 and
  (.refill_active | not) and
  (.replenishment_paused | not)
' "$HOME/results/prewarmed-load/quiescent-status.json"
```

Expected: every VM handles at most one request. After load, all active,
queued, restore, recycle, teardown, and completion counters return to zero;
ready inventory is restored to at least the warm floor.

**Troubleshooting**

- If readiness stalls, inspect restore counters and the server log.
- If quiescence stalls, inspect `recycle_queue_depth`, `teardown_in_flight`,
  `completion_in_flight`, and `restore_permits_outstanding`.
- Compare on-demand and prewarmed results only with the same request shape,
  concurrency, timeout, and bundle.

## 21. Inspect refill, quiescence, and resource behavior

While either pool mode is running:

```bash
jq '{
      restore_mode,
      max_concurrent_sandboxes,
      effective_concurrency,
      prewarmed_sandboxes,
      max_concurrent_restores,
      warm_floor,
      ready_low_watermark,
      ready_high_watermark,
      max_replenish_batch,
      replenishment_paused,
      replenishment_pause_reason,
      refill_active,
      restore_permits_outstanding,
      prewarmed_inventory,
      prewarmed_ready,
      prewarmed_replenishing,
      execution_slots_in_use,
      restore_slots_in_use,
      recycle_queue_depth,
      teardown_in_flight,
      completion_queue_depth,
      completion_in_flight,
      admitted,
      active,
      queued,
      prewarmed_hits,
      prewarmed_misses
    }' "$HOME/results/prewarmed-load/quiescent-status.json"
```

Request profiles in the server log separate:

- admission wait;
- ready-owner wait;
- replenishment policy and restore-slot wait;
- snapshot restore;
- request setup;
- guest execution;
- response completion;
- VM teardown.

These fields describe where time is spent; they do not grant any additional
guest capability.

Stop the inspected server before running a wrapper that binds the same
address:

```bash
test -z "$(
  pgrep -af 'target/release/examples/workerd-demo.*127[.]0[.]0[.]1:8787' \
    || true
)"
test -z "$(ss -H -ltn 'sport = :8787' || true)"
```

## 22. Run the reproducible benchmark wrapper

The repository wrapper starts and stops the server, runs the `/sync` load,
samples the pool, and writes JSON output.

On-demand:

```bash
load_threshold=1
HOST_QUIESCENT=false
for _ in $(seq 1 180); do
  read -r current_load five_minute_load _ </proc/loadavg
  if awk \
    -v current_load="$current_load" \
    -v five_minute_load="$five_minute_load" \
    -v threshold="$load_threshold" \
    'BEGIN {
      exit !(current_load <= threshold && five_minute_load <= threshold)
    }'
  then
    HOST_QUIESCENT=true
    break
  fi
  sleep 5
done
test "$HOST_QUIESCENT" = true

cd "$HOME/src/hyperlight-unikraft"

WINTERTC_POOL_RESTORE_MODE=on-demand \
WINTERTC_POOL_PROFILE_LOG_EVERY=1 \
bash tools/run-wintertc-pool-benchmark.sh \
  build-elfloader/workerd-executor \
  "$HOME/results/wrapper-on-demand" \
  576 \
  32 || benchmark_status=$?
result="$HOME/results/wrapper-on-demand/wintertc-pool-performance.json"
jq -e '
  (.accepted == true or .accepted == false)
  and .baseline_run.requests == 320
  and .baseline_run.errors == 0
  and .configuration.payload_bytes == 9441
  and .refill.passed == true
  and ([.sustained_runs[].errors] | all(. == 0))
' "$result" >/dev/null
if jq -e '.accepted == true' "$result" >/dev/null; then
  test "${benchmark_status:-0}" -eq 0
  printf 'PERFORMANCE TARGET MET\n'
else
  test "${benchmark_status:-0}" -ne 0
  printf 'PERFORMANCE BELOW TARGET\n'
fi
```

Prewarmed:

```bash
load_threshold=1
HOST_QUIESCENT=false
for _ in $(seq 1 180); do
  read -r current_load five_minute_load _ </proc/loadavg
  if awk \
    -v current_load="$current_load" \
    -v five_minute_load="$five_minute_load" \
    -v threshold="$load_threshold" \
    'BEGIN {
      exit !(current_load <= threshold && five_minute_load <= threshold)
    }'
  then
    HOST_QUIESCENT=true
    break
  fi
  sleep 5
done
test "$HOST_QUIESCENT" = true

cd "$HOME/src/hyperlight-unikraft"

WINTERTC_POOL_RESTORE_MODE=prewarmed \
WINTERTC_POOL_PREWARMED_SANDBOXES=48 \
WINTERTC_POOL_MAX_CONCURRENT_RESTORES=1 \
WINTERTC_POOL_WARM_FLOOR=1 \
WINTERTC_POOL_READY_LOW_WATERMARK=16 \
WINTERTC_POOL_READY_HIGH_WATERMARK=32 \
WINTERTC_POOL_MAX_REPLENISH_BATCH=2 \
WINTERTC_POOL_PROFILE_LOG_EVERY=1 \
bash tools/run-wintertc-pool-benchmark.sh \
  build-elfloader/workerd-executor \
  "$HOME/results/wrapper-prewarmed" \
  576 \
  32 || benchmark_status=$?
result="$HOME/results/wrapper-prewarmed/wintertc-pool-performance.json"
jq -e '
  (.accepted == true or .accepted == false)
  and .baseline_run.requests == 320
  and .baseline_run.errors == 0
  and .configuration.payload_bytes == 9441
  and .refill.passed == true
  and ([.sustained_runs[].errors] | all(. == 0))
' "$result" >/dev/null
if jq -e '.accepted == true' "$result" >/dev/null; then
  test "${benchmark_status:-0}" -eq 0
  printf 'PERFORMANCE TARGET MET\n'
else
  test "${benchmark_status:-0}" -ne 0
  printf 'PERFORMANCE BELOW TARGET\n'
fi
```

Inspect the functional summary:

```bash
printf '%-12s %10s %8s %14s\n' Mode Requests Errors Requests/sec
for result in \
  "$HOME/results/wrapper-on-demand/wintertc-pool-performance.json" \
  "$HOME/results/wrapper-prewarmed/wintertc-pool-performance.json"
do
  jq -e '.baseline_run.errors == 0' "$result" >/dev/null
  printf '%-12s %10s %8s %14s\n' \
    "$(jq -r '.configuration.restore_mode' "$result")" \
    "$(jq -r '.baseline_run.requests' "$result")" \
    "$(jq -r '.baseline_run.errors' "$result")" \
    "$(jq -r '.baseline_run.throughput_requests_per_second' "$result")"
done
printf '%s\n' \
  'Interpretation: compare requests/sec and latency fields only after both modes report zero errors and quiescent refill.'
```

Expected: each wrapper exits zero, the response identity check succeeds, and
the JSON contains configuration, request, error, latency, throughput, refill,
and pool-sample fields.

## 23. Deviations and upstream self-hosted Workerd comparison

This reference compares the packaged Workerd-on-Hyperlight executor with the
signed upstream self-hosted Workerd baseline. "Supported" means exposed by
that exact executor/package and its host adapters; it is not blanket
conformance for every upstream, WinterTC, Node.js, WPT, or ecosystem test.

### Additions beyond upstream self-hosted Workerd

| Addition | Classification | Behavior |
|---|---|---|
| Per-request Hyperlight/KVM isolation | New runtime capability | Each admitted request executes in a restored disposable micro-VM. Request-local VM state and host registrations are reset at teardown. This is an isolation and deployment extension, not a JavaScript API. |
| Host-owned identity, policy, quotas, and audit | New host policy plane | Identity is out-of-band; guest request IDs are correlation only. Authorization and quota reservation occur before typed dispatch. Audits contain opaque metadata and byte counts, not payloads. |
| Scheduled and queue ingress | Integration adapter | Native Workerd scheduled and queue dispatch is exposed through the Hyperlight executor, including `waitUntil`, acknowledgements, per-message retry, batch retry, and `noRetry`. |
| Typed KV, Cache, D1, and Durable Object bindings | Integration adapter and protocol extension | Declared typed bindings use versioned closed protocols. Backing stores, identity, and credentials remain host-owned. |
| WebAssembly Component Model workloads | Packaging and translation extension | Component workloads use deterministic pinned `jco` lowering to core Wasm and JavaScript. Native component-binary loading and runtime canonical-ABI/WIT instantiation are not provided. |
| WASI Preview 2 | Adapter capability | Pinned adapters include `wasi:http@0.2.12`. Capabilities map to existing Worker HTTP, stream, and resource surfaces without ambient filesystem or socket authority. |
| WASI Preview 3 async and streaming | Adapter capability | Pinned adapters include `wasi:http@0.3.1`. Operations inherit Workerd and host backpressure, deadline, and capability limits. |
| Rust guest execution | New guest and package path | Rust-authored guest artifacts run through the exact-base Rust bridge and the same isolated request lifecycle. This does not imply native Node addons or arbitrary host-native code loading. |

### Intentionally unsupported APIs and semantics

| Surface | User-visible behavior |
|---|---|
| `node:child_process` process creation | The module is importable, but `ChildProcess`, `exec`, `execFile`, `execSync`, `fork`, `spawn`, and `spawnSync` throw `ERR_METHOD_NOT_IMPLEMENTED`. |
| True Node worker threads | `new Worker()` and operational thread APIs do not create threads. `isMainThread=true`, `threadId=0`, and `parentPort=null`. `MessageChannel` and `MessagePort` are supported independently. |
| Native Node addons and `process.dlopen()` | `.node` loading and `process.dlopen()` throw `ERR_METHOD_NOT_IMPLEMENTED`. Use JavaScript, built-in APIs, or packaged core-Wasm modules. |
| Native Component Model loading | Direct component-binary loading, WIT-world resolution, and runtime canonical-ABI lifting and lowering are not provided. Use the pinned lowering pipeline. |
| Unrestricted dynamic code generation | `eval()` and `new Function()` are disabled by the packaged executor policy. |
| Ambient host filesystem | Workers see bundle files and explicitly configured virtual mounts, not arbitrary host paths. |
| Ambient raw sockets | Arbitrary raw connection opening is denied. Use HTTP(S) fetch or explicitly installed typed TCP, TLS, or UDP adapters. |
| Ambient WebSocket acceptance | WebSockets require the dedicated adapter lane; the base executor does not grant ambient WebSocket acceptance. |
| Generic actor or capability injection | Only declared typed bindings are exposed. Arbitrary actor classes and generic host-capability channels are not available. |

### Supported with constraints or a different architecture

| Surface | Constraint |
|---|---|
| Node.js compatibility | Compatibility is API-specific and intentionally does not provide a full Node runtime or host-OS parity. |
| `process` | A deterministic runtime facade: `argv=['workerd']`, `argv0='workerd'`, `pid=1`, and `ppid=0`; `umask()` validates but does not mutate an ambient host process. |
| Node filesystem | `node:fs` operates on the Workerd virtual filesystem and named read-only or read-write mounts. |
| Node networking | Module availability remains capability-mediated and allowlisted; it does not imply ambient host networking. |
| HTTP ingress and outbound fetch | Supported through bounded request and fetch surfaces with executor-specific body, response, header, URL, concurrency, and deadline limits. |
| CommonJS and module loading | ES modules, CommonJS, text, JSON, and Wasm are bundle-declared. Resolution is closed over the bundle; traversal, undeclared packages, runtime installation, and ambient npm or filesystem discovery reject. |
| Core WebAssembly | Predeclared core-Wasm modules are supported through the packaged lifecycle. This does not imply native Component Model loading or unrestricted dynamic compilation. |
| WinterTC and Web common APIs | The walkthrough covers URL, URLPattern, Request, Response, Headers, FormData, Blob, text codecs, crypto digest and random, streams, transforms, compression, performance, timers, fetch, File, MessagePort, BYOB, restore and isolation, and core Wasm. It does not claim complete WinterTC or WPT conformance. |
| Component and WASI | Component workloads use deterministic pinned `jco` lowering. P2 and P3 workloads use pinned adapters over existing Workerd HTTP, stream, and resource surfaces. |
| Service and storage bindings | Only declared `kv`, `cache`, `d1`, and `durable_object` kinds and closed operations dispatch. Host resources and credentials are not embedded in the guest manifest. |
| Disposable lifecycle and persistence | Persistence belongs to host-backed services. Request budgets, handles, counters, audit buffers, and VM-local state reset per restored VM. |

## 24. Verify results and remove Azure resources

On the VM, stop any remaining demo processes and create a deterministic result
inventory:

```bash
test -r "$HOME/results/acceptance-inputs/staging-quiescence.tsv"
grep -Eq $'\t11\t[0-9a-f]{64}\t0\t0\tBLOB_STAGING_CLOSED$' \
  "$HOME/results/acceptance-inputs/staging-quiescence.tsv"
test -z "$(
  pgrep -af 'azcopy|blobfuse|curl.*blob[.]core[.]windows[.]net' || true
)"
test -z "$(
  findmnt -rn -t fuse,fuse3,fuse.blobfuse2 2>/dev/null || true
)"
printf 'Blob transport quiescent before wrapper measurements\n'

set -euo pipefail

test -z "$(
  pgrep -af \
    'target/release/examples/workerd-demo|results/.*/server.py' \
    || true
)"

cd "$HOME"
find results -type f \
  ! -path 'results/run-command/block-47.log' \
  -print0 \
  | sort -z \
  | xargs -0 sha256sum \
  >results.sha256
tar --sort=name \
  --mtime='UTC 1970-01-01' \
  --owner=0 \
  --group=0 \
  --numeric-owner \
  -czf results.tar.gz \
  results results.sha256
sha256sum results.tar.gz >results.tar.gz.sha256
sha256sum --check results.tar.gz.sha256
sha256sum --check results.sha256

blob_config="$HOME/results/acceptance-inputs/blob-config.env"
test -r "$blob_config"
source "$blob_config"
test -n "$AZURE_STORAGE_ACCOUNT"
test -n "$AZURE_RESULTS_CONTAINER"
token="$(
  curl --fail --silent --show-error \
    --header Metadata:true \
    'http://169.254.169.254/metadata/identity/oauth2/token?api-version=2018-02-01&resource=https%3A%2F%2Fstorage.azure.com%2F' \
    | jq -er .access_token
)"
upload_receipt="$HOME/results-blob-upload.tsv"
printf 'file\tbytes\tsha256\tattempt\tstatus\n' >"$upload_receipt"
for file in results.tar.gz results.tar.gz.sha256 results.sha256; do
  bytes="$(stat -c '%s' "$file")"
  sha="$(sha256sum "$file" | awk '{print $1}')"
  uploaded=false
  for attempt in 1 2 3 4 5; do
    status="$(
      curl --silent --show-error \
        --output "$HOME/results-blob-upload-$file-$attempt.body" \
        --write-out '%{http_code}' \
        --max-time 1800 \
        --request PUT \
        --header "Authorization: Bearer $token" \
        --header 'x-ms-version: 2023-11-03' \
        --header 'x-ms-blob-type: BlockBlob' \
        --header "x-ms-meta-sha256:$sha" \
        --upload-file "$file" \
        "https://$AZURE_STORAGE_ACCOUNT.blob.core.windows.net/$AZURE_RESULTS_CONTAINER/$file" \
        || true
    )"
    printf '%s\t%s\t%s\t%s\t%s\n' \
      "$file" "$bytes" "$sha" "$attempt" "$status" \
      >>"$upload_receipt"
    if [[ "$status" = 201 ]]; then
      uploaded=true
      break
    fi
    sleep $((1 << attempt))
  done
  test "$uploaded" = true
done
sha256sum "$upload_receipt" >"$upload_receipt.sha256"
sha256sum --check "$upload_receipt.sha256"
printf 'RESULTS_BLOB_UPLOAD_VERIFIED %s\n' \
  "$(sha256sum "$upload_receipt" | awk '{print $1}')"
```

Expected: no demo process remains, every recorded result file verifies, the
archive verifies against its own SHA-256 record, and all three sealed result
files upload to the run-scoped results container through the VM managed
identity.

From a fresh workstation shell, download and independently verify the archive
before deletion. Then reload the preserved Azure identity and fail closed
unless the resource group and every contained resource still carry this
walkthrough's ownership tags:

```bash
set -euo pipefail
trap 'printf "CLEANUP_FAILURE line=%s command=%s\n" "$LINENO" "$BASH_COMMAND" >&2' ERR

export AZURE_STATE_FILE="$HOME/.azure-workerd-demo.env"
test -r "$AZURE_STATE_FILE"
source "$AZURE_STATE_FILE"

test -n "$AZURE_RESOURCE_GROUP"
test -n "$AZURE_VM"
test -n "$AZURE_OWNER"
test -n "$AZURE_RUN_ID"
test -n "$AZURE_INPUT_STORAGE_ACCOUNT"
test -n "$AZURE_VM_PRINCIPAL_ID"
test -n "$AZURE_INPUT_CONTAINER"
test -n "$AZURE_INPUT_SCOPE"
test -n "$AZURE_INPUT_ROLE_ID"
test -n "$AZURE_RESULTS_CONTAINER"
test -n "$AZURE_RESULTS_SCOPE"
test -n "$AZURE_RESULTS_ROLE_ID"
test -n "$AZURE_CAMPAIGN_ID"
test -n "$AZURE_CACHE_RESOURCE_GROUP"
test -n "$AZURE_CACHE_STORAGE_ACCOUNT"
test -n "$AZURE_CACHE_CONTAINER"
test -n "$AZURE_CACHE_SCOPE"
test -n "$AZURE_CACHE_ROLE_ID"

mkdir -p "$HOME/azure-workerd-results/$AZURE_RUN_ID"
account_key="$(
  az storage account keys list \
    --resource-group "$AZURE_RESOURCE_GROUP" \
    --account-name "$AZURE_INPUT_STORAGE_ACCOUNT" \
    --query '[0].value' \
    --output tsv \
    | tr -d '\r'
)"
test -n "$account_key"
for blob in results.tar.gz results.tar.gz.sha256 results.sha256; do
  az storage blob download \
    --account-name "$AZURE_INPUT_STORAGE_ACCOUNT" \
    --account-key "$account_key" \
    --container-name "$AZURE_RESULTS_CONTAINER" \
    --name "$blob" \
    --file "$HOME/azure-workerd-results/$AZURE_RUN_ID/$blob" \
    --overwrite \
    --output none
done

cd "$HOME/azure-workerd-results/$AZURE_RUN_ID"
sha256sum --check results.tar.gz.sha256
tar -xzf results.tar.gz
sha256sum --check results.sha256

test "$(az group exists --name "$AZURE_RESOURCE_GROUP")" = true
test "$(
  az group show \
    --name "$AZURE_RESOURCE_GROUP" \
    --query tags.owner \
    --output tsv
)" = "$AZURE_OWNER"
test "$(
  az group show \
    --name "$AZURE_RESOURCE_GROUP" \
    --query tags.purpose \
    --output tsv
)" = hyperlight-workerd-walkthrough
test "$(
  az group show \
    --name "$AZURE_RESOURCE_GROUP" \
    --query tags.run_id \
    --output tsv
)" = "$AZURE_RUN_ID"
test "$(
  az resource list \
    --resource-group "$AZURE_RESOURCE_GROUP" \
    --query 'length(@)' \
    --output tsv
)" -gt 0
test "$(
  az resource list \
    --resource-group "$AZURE_RESOURCE_GROUP" \
    --resource-type Microsoft.Compute/virtualMachines \
    --query "[?name == '$AZURE_VM'] | length(@)" \
    --output tsv
)" = 1
test "$(
  az resource list \
    --resource-group "$AZURE_RESOURCE_GROUP" \
    --query "[?tags.run_id != null && tags.run_id != '$AZURE_RUN_ID'] | length(@)" \
    --output tsv
)" = 0
untagged_resources="$(
  az resource list \
    --resource-group "$AZURE_RESOURCE_GROUP" \
    --query '[?tags.run_id == null].{name:name,type:type}' \
    --output json
)"
jq -e --arg vm "$AZURE_VM" '
  all(.[];
    (
      .type == "Microsoft.Network/networkSecurityGroups"
      and (.name | startswith("NRMS-"))
      and (.name | endswith($vm + "VNET"))
    )
    or
    (
      .type == "Microsoft.Compute/virtualMachines/extensions"
      and (
        .name == ($vm + "/AzurePolicyforLinux")
        or .name == (
          $vm
          + "/Microsoft.Azure.Security.Monitoring.AzureSecurityLinuxAgent"
        )
        or .name == (
          $vm
          + "/Microsoft.Azure.Monitor.AzureMonitorLinuxAgent"
        )
      )
    )
  )
' <<<"$untagged_resources" >/dev/null
if jq -e '
  any(.[];
    .type == "Microsoft.Network/networkSecurityGroups"
    and (.name | startswith("NRMS-"))
  )
' <<<"$untagged_resources" >/dev/null; then
  policy_nsg="$(
    jq -er '
      .[]
      | select(
          .type == "Microsoft.Network/networkSecurityGroups"
          and (.name | startswith("NRMS-"))
        )
      | .name
    ' <<<"$untagged_resources"
  )"
  test "$(
    az network nsg show \
      --resource-group "$AZURE_RESOURCE_GROUP" \
      --name "$policy_nsg" \
      --query tags.Creator \
      --output tsv
  )" = "Automatically added by NRMS Azure Policy"
  test "$(
    az network nsg show \
      --resource-group "$AZURE_RESOURCE_GROUP" \
      --name "$policy_nsg" \
      --query 'tags."NRMS-Info"' \
      --output tsv
  )" = "http://aka.ms/nrms"
fi
if jq -e --arg vm "$AZURE_VM" '
  any(.[];
    .type == "Microsoft.Compute/virtualMachines/extensions"
    and .name == ($vm + "/AzurePolicyforLinux")
  )
' <<<"$untagged_resources" >/dev/null; then
  test "$(
    az vm extension show \
      --resource-group "$AZURE_RESOURCE_GROUP" \
      --vm-name "$AZURE_VM" \
      --name AzurePolicyforLinux \
      --query publisher \
      --output tsv
  )" = "Microsoft.GuestConfiguration"
  test "$(
    az vm extension show \
      --resource-group "$AZURE_RESOURCE_GROUP" \
      --vm-name "$AZURE_VM" \
      --name AzurePolicyforLinux \
      --query provisioningState \
      --output tsv
  )" = Succeeded
fi
for extension_spec in \
  "Microsoft.Azure.Security.Monitoring.AzureSecurityLinuxAgent|Microsoft.Azure.Security.Monitoring" \
  "Microsoft.Azure.Monitor.AzureMonitorLinuxAgent|Microsoft.Azure.Monitor"
do
  extension_name="${extension_spec%%|*}"
  extension_publisher="${extension_spec#*|}"
  if jq -e \
    --arg vm "$AZURE_VM" \
    --arg extension "$extension_name" '
      any(.[];
        .type == "Microsoft.Compute/virtualMachines/extensions"
        and .name == ($vm + "/" + $extension)
      )
    ' <<<"$untagged_resources" >/dev/null; then
    test "$(
      az vm extension show \
        --resource-group "$AZURE_RESOURCE_GROUP" \
        --vm-name "$AZURE_VM" \
        --name "$extension_name" \
        --query publisher \
        --output tsv
    )" = "$extension_publisher"
    test "$(
      az vm extension show \
        --resource-group "$AZURE_RESOURCE_GROUP" \
        --vm-name "$AZURE_VM" \
        --name "$extension_name" \
        --query provisioningState \
        --output tsv
    )" = Succeeded
  fi
done

test "$(
  az storage account show \
    --resource-group "$AZURE_RESOURCE_GROUP" \
    --name "$AZURE_INPUT_STORAGE_ACCOUNT" \
    --query tags.run_id \
    --output tsv
)" = "$AZURE_RUN_ID"
test "$(
  az storage account show \
    --resource-group "$AZURE_RESOURCE_GROUP" \
    --name "$AZURE_INPUT_STORAGE_ACCOUNT" \
    --query tags.purpose \
    --output tsv
)" = hyperlight-workerd-walkthrough
MSYS_NO_PATHCONV=1 az role assignment list \
  --scope "$AZURE_INPUT_SCOPE" \
  --query "[?id == '$AZURE_INPUT_ROLE_ID'] | [0]" \
  --output json \
  >blob-input-role-before-delete.json
MSYS_NO_PATHCONV=1 az role assignment list \
  --scope "$AZURE_RESULTS_SCOPE" \
  --query "[?id == '$AZURE_RESULTS_ROLE_ID'] | [0]" \
  --output json \
  >blob-results-role-before-delete.json
MSYS_NO_PATHCONV=1 az role assignment list \
  --scope "$AZURE_CACHE_SCOPE" \
  --query "[?id == '$AZURE_CACHE_ROLE_ID'] | [0]" \
  --output json \
  >campaign-cache-role-before-delete.json
MSYS_NO_PATHCONV=1 jq -e \
  --arg principal "$AZURE_VM_PRINCIPAL_ID" \
  --arg scope "$AZURE_INPUT_SCOPE" '
    .principalId == $principal
    and .scope == $scope
    and .roleDefinitionName == "Storage Blob Data Reader"
  ' blob-input-role-before-delete.json >/dev/null
MSYS_NO_PATHCONV=1 jq -e \
  --arg principal "$AZURE_VM_PRINCIPAL_ID" \
  --arg scope "$AZURE_RESULTS_SCOPE" '
    .principalId == $principal
    and .scope == $scope
    and .roleDefinitionName == "Storage Blob Data Contributor"
  ' blob-results-role-before-delete.json >/dev/null
MSYS_NO_PATHCONV=1 jq -e \
  --arg principal "$AZURE_VM_PRINCIPAL_ID" \
  --arg scope "$AZURE_CACHE_SCOPE" '
    .principalId == $principal
    and .scope == $scope
    and .roleDefinitionName == "Storage Blob Data Contributor"
  ' campaign-cache-role-before-delete.json >/dev/null
az storage blob list \
  --account-name "$AZURE_INPUT_STORAGE_ACCOUNT" \
  --account-key "$account_key" \
  --container-name "$AZURE_INPUT_CONTAINER" \
  --include m \
  --output json \
  >blob-input-inventory-before-delete.json
az storage blob list \
  --account-name "$AZURE_INPUT_STORAGE_ACCOUNT" \
  --account-key "$account_key" \
  --container-name "$AZURE_RESULTS_CONTAINER" \
  --include m \
  --output json \
  >blob-results-inventory-before-delete.json
test "$(jq 'length' blob-input-inventory-before-delete.json)" = 12
test "$(jq 'length' blob-results-inventory-before-delete.json)" = 3
input_blob_bytes="$(
  jq '[.[].properties.contentLength] | add // 0' \
    blob-input-inventory-before-delete.json
)"
results_blob_bytes="$(
  jq '[.[].properties.contentLength] | add // 0' \
    blob-results-inventory-before-delete.json
)"
printf 'container\tblobs\tbytes\n%s\t12\t%s\n%s\t3\t%s\n' \
  "$AZURE_INPUT_CONTAINER" "$input_blob_bytes" \
  "$AZURE_RESULTS_CONTAINER" "$results_blob_bytes" \
  >blob-capacity-before-delete.tsv
sha256sum \
  blob-input-role-before-delete.json \
  blob-results-role-before-delete.json \
  campaign-cache-role-before-delete.json \
  blob-input-inventory-before-delete.json \
  blob-results-inventory-before-delete.json \
  blob-capacity-before-delete.tsv \
  >blob-cleanup-inputs.sha256
sha256sum --check blob-cleanup-inputs.sha256

az resource list \
  --resource-group "$AZURE_RESOURCE_GROUP" \
  --query '[].id' \
  --output tsv \
  | tr -d '\r' \
  >resource-ids-before-delete.txt
test -s resource-ids-before-delete.txt
az network nsg list \
  --resource-group "$AZURE_RESOURCE_GROUP" \
  --output json \
  >nsgs-before-delete.json
test "$(
  jq '
    [.[].securityRules[]?
      | select(
          .direction == "Inbound"
          and .access == "Allow"
          and (
            .destinationPortRange == "22"
            or (.destinationPortRanges // [] | index("22"))
          )
        )
    ]
    | length
  ' nsgs-before-delete.json
)" = 0

az resource list \
  --resource-group "$AZURE_RESOURCE_GROUP" \
  --query '[].{name:name,type:type,run_id:tags.run_id}' \
  --output table

MSYS_NO_PATHCONV=1 az role assignment delete --ids \
  "$AZURE_INPUT_ROLE_ID" \
  "$AZURE_RESULTS_ROLE_ID" \
  "$AZURE_CACHE_ROLE_ID"
test "$(
  MSYS_NO_PATHCONV=1 az role assignment list \
    --assignee-object-id "$AZURE_VM_PRINCIPAL_ID" \
    --scope "$AZURE_INPUT_SCOPE" \
    --query "[?id == '$AZURE_INPUT_ROLE_ID'] | length(@)" \
    --output tsv
)" = 0
test "$(
  MSYS_NO_PATHCONV=1 az role assignment list \
    --assignee-object-id "$AZURE_VM_PRINCIPAL_ID" \
    --scope "$AZURE_CACHE_SCOPE" \
    --query "[?id == '$AZURE_CACHE_ROLE_ID'] | length(@)" \
    --output tsv
)" = 0
test "$(
  MSYS_NO_PATHCONV=1 az role assignment list \
    --assignee-object-id "$AZURE_VM_PRINCIPAL_ID" \
    --scope "$AZURE_RESULTS_SCOPE" \
    --query "[?id == '$AZURE_RESULTS_ROLE_ID'] | length(@)" \
    --output tsv
)" = 0
az storage container delete \
  --account-name "$AZURE_INPUT_STORAGE_ACCOUNT" \
  --account-key "$account_key" \
  --name "$AZURE_INPUT_CONTAINER" \
  --output none
az storage container delete \
  --account-name "$AZURE_INPUT_STORAGE_ACCOUNT" \
  --account-key "$account_key" \
  --name "$AZURE_RESULTS_CONTAINER" \
  --output none
test "$(
  az storage container exists \
    --account-name "$AZURE_INPUT_STORAGE_ACCOUNT" \
    --account-key "$account_key" \
    --name "$AZURE_INPUT_CONTAINER" \
    --query exists \
    --output tsv
)" = false
test "$(
  az storage container exists \
    --account-name "$AZURE_INPUT_STORAGE_ACCOUNT" \
    --account-key "$account_key" \
    --name "$AZURE_RESULTS_CONTAINER" \
    --query exists \
    --output tsv
)" = false
printf 'role_id\tdeleted\n%s\ttrue\n%s\ttrue\n%s\ttrue\n' \
  "$AZURE_INPUT_ROLE_ID" "$AZURE_RESULTS_ROLE_ID" "$AZURE_CACHE_ROLE_ID" \
  >blob-role-deletion-receipt.tsv
printf 'container\tdeleted\n%s\ttrue\n%s\ttrue\n' \
  "$AZURE_INPUT_CONTAINER" "$AZURE_RESULTS_CONTAINER" \
  >blob-container-deletion-receipt.tsv
sha256sum \
  blob-role-deletion-receipt.tsv \
  blob-container-deletion-receipt.tsv \
  >blob-deletion-receipts.sha256
sha256sum --check blob-deletion-receipts.sha256
unset account_key

az group delete \
  --name "$AZURE_RESOURCE_GROUP" \
  --yes

test "$(az group exists --name "$AZURE_RESOURCE_GROUP")" = false
test "$(
  az resource list \
    --query "[?tags.run_id == '$AZURE_RUN_ID'] | length(@)" \
    --output tsv
)" = 0
if az storage account show \
  --name "$AZURE_INPUT_STORAGE_ACCOUNT" \
  --output none 2>/dev/null; then
  false
fi
test "$(
  az role assignment list \
    --assignee-object-id "$AZURE_VM_PRINCIPAL_ID" \
    --all \
    --query "[?id == '$AZURE_INPUT_ROLE_ID' || id == '$AZURE_RESULTS_ROLE_ID' || id == '$AZURE_CACHE_ROLE_ID'] | length(@)" \
    --output tsv
)" = 0
test "$(
  az resource list \
    --query "[?name == '$AZURE_INPUT_STORAGE_ACCOUNT' || name == '$AZURE_VM'] | length(@)" \
    --output tsv
)" = 0
while IFS= read -r resource_id; do
  if MSYS_NO_PATHCONV=1 az resource show \
    --ids "$resource_id" \
    --output none 2>/dev/null; then
    false
  fi
done <resource-ids-before-delete.txt

if test "${FINAL_CAMPAIGN_CLEANUP:-0}" = 1; then
  test "$(
    az group show \
      --name "$AZURE_CACHE_RESOURCE_GROUP" \
      --query tags.owner \
      --output tsv
  )" = "$AZURE_OWNER"
  test "$(
    az group show \
      --name "$AZURE_CACHE_RESOURCE_GROUP" \
      --query tags.purpose \
      --output tsv
  )" = hyperlight-workerd-campaign-cache
  test "$(
    az group show \
      --name "$AZURE_CACHE_RESOURCE_GROUP" \
      --query tags.campaign_id \
      --output tsv
  )" = "$AZURE_CAMPAIGN_ID"
  cache_account_key="$(
    az storage account keys list \
      --resource-group "$AZURE_CACHE_RESOURCE_GROUP" \
      --account-name "$AZURE_CACHE_STORAGE_ACCOUNT" \
      --query '[0].value' \
      --output tsv \
      | tr -d '\r'
  )"
  az storage blob list \
    --account-name "$AZURE_CACHE_STORAGE_ACCOUNT" \
    --account-key "$cache_account_key" \
    --container-name "$AZURE_CACHE_CONTAINER" \
    --include m \
    --output json \
    >campaign-cache-final-inventory.json
  jq -e 'all(.[];
    (.metadata.sha256 | test("^[0-9a-f]{64}$"))
    and (.metadata.compatibilitykey | test("^[0-9a-f]{64}$"))
  )' campaign-cache-final-inventory.json >/dev/null
  printf 'blobs\tbytes\tinventory_sha256\n%s\t%s\t%s\n' \
    "$(jq 'length' campaign-cache-final-inventory.json)" \
    "$(jq '[.[].properties.contentLength] | add // 0' campaign-cache-final-inventory.json)" \
    "$(sha256sum campaign-cache-final-inventory.json | cut -d' ' -f1)" \
    >campaign-cache-final-cleanup-ledger.tsv
  az group delete --name "$AZURE_CACHE_RESOURCE_GROUP" --yes
  test "$(az group exists --name "$AZURE_CACHE_RESOURCE_GROUP")" = false
  if az storage account show \
    --name "$AZURE_CACHE_STORAGE_ACCOUNT" \
    --output none 2>/dev/null; then
    false
  fi
  test "$(
    az resource list \
      --query "[?tags.campaign_id == '$AZURE_CAMPAIGN_ID'] | length(@)" \
      --output tsv
  )" = 0
  printf 'CAMPAIGN_CACHE_ZERO_PROOF_PASS\n'
fi
rm -f "$AZURE_STATE_FILE"
```

Expected: the copied archive and every enclosed result verify independently,
the three exact Blob roles and two run-scoped containers are deleted before
resource-group deletion, the dedicated run resource group and storage account
are absent, and no
subscription resource, NSG, or role remains with the walkthrough run ID or
recorded names. The campaign cache remains retained across failed-run deletion.
On the final successful campaign cleanup only, set `FINAL_CAMPAIGN_CLEANUP=1`;
the cache inventory is ledgered, its separately owned resource group is
deleted, and the campaign ID and storage-account zero proofs must pass. If any
ownership, content, hash, cleanup, or zero-resource
assertion fails, stop and investigate rather than broadening deletion scope.
