# Run Workerd in Hyperlight-Unikraft on Azure KVM

This walkthrough provisions a clean Azure Linux VM, validates native KVM,
builds and packages a trusted Workerd executor, runs the HTTP demo, exercises
the runnable WinterTC routes, and compares on-demand and adaptive prewarmed
execution.

> [!WARNING]
> Do not use QEMU for this walkthrough. The runtime path is native Linux KVM
> through `/dev/kvm`.

> [!WARNING]
> Workerd by itself is not a hardened sandbox. The boundary described here is
> the Hyperlight micro-VM plus the explicitly registered host capabilities.
> Treat the Workerd executor and Worker bundle as trusted build inputs.

The commands target signed Hyperlight-Unikraft commit
`d240b022fe677db482a5b10b6cea16d5ab39cf07`. Use 384 MiB of guest scratch
memory. A previous qualification found 343 MiB to be the minimum complete-pass
boundary and 342 MiB to fail during boot; do not operate at that cliff.

## 1. Provision a dedicated Azure VM

Use a unique resource group containing no shared resources. The example below
uses a 32-vCPU `Standard_D32s_v5` VM in UK South, Ubuntu 24.04, Standard
security, a Premium OS disk, and SSH restricted to one operator address.

Run from a machine with Azure CLI authenticated:

```powershell
$ErrorActionPreference = 'Stop'

$Subscription = '<subscription-name-or-id>'
$Location = 'uksouth'
$Stamp = (Get-Date).ToUniversalTime().ToString('yyyyMMddHHmmss')
$ResourceGroup = "hluk-workerd-kvm-$Stamp-rg"
$VmName = 'workerd-kvm-01'
$NsgName = "$VmName-nsg"
$PublicIpName = "$VmName-ip"
$Size = 'Standard_D32s_v5'
$Image = 'Canonical:ubuntu-24_04-lts:server:latest'
$AdminUser = 'azureuser'
$SshPublicKeyPath = '<path-to-public-key>'
$AdminSourceCidr = '<operator-public-ip>/32'

az account set --subscription $Subscription
$SubscriptionId = az account show --query id -o tsv

if ((az group exists --name $ResourceGroup).Trim() -ne 'false') {
  throw "Resource group already exists: $ResourceGroup"
}

az group create `
  --name $ResourceGroup `
  --location $Location `
  --tags purpose=workerd-hyperlight-kvm expires=10h

az network nsg create `
  --resource-group $ResourceGroup `
  --name $NsgName `
  --location $Location

az network nsg rule create `
  --resource-group $ResourceGroup `
  --nsg-name $NsgName `
  --name AllowSshFromOperator `
  --priority 100 `
  --access Allow `
  --protocol Tcp `
  --direction Inbound `
  --source-address-prefixes $AdminSourceCidr `
  --destination-port-ranges 22

az vm create `
  --resource-group $ResourceGroup `
  --name $VmName `
  --location $Location `
  --size $Size `
  --image $Image `
  --admin-username $AdminUser `
  --ssh-key-values $SshPublicKeyPath `
  --security-type Standard `
  --storage-sku Premium_LRS `
  --os-disk-size-gb 512 `
  --public-ip-address $PublicIpName `
  --public-ip-sku Standard `
  --nsg $NsgName

$PublicIp = az vm show `
  --resource-group $ResourceGroup `
  --name $VmName `
  --show-details `
  --query publicIps `
  -o tsv

"Subscription: $SubscriptionId"
"Resource group: $ResourceGroup"
"VM: $VmName"
"Public IP: $PublicIp"
```

Expected success indicators:

- `az vm create` returns `powerState: VM running`;
- the VM size is `Standard_D32s_v5`;
- only the operator `/32` has inbound SSH access.

Connect:

```powershell
ssh "$AdminUser@$PublicIp"
```

Never use `0.0.0.0/0` for SSH. Keep the resource-group variables in the
control-host shell for the teardown in Section 14.

## 2. Validate the host and `/dev/kvm`

Run on the Azure VM:

```bash
set -euo pipefail

uname -a
lscpu
findmnt -no FSTYPE,TARGET /
free -h
df -h /

grep -qE '(^| )vmx( |$)' /proc/cpuinfo
test -c /dev/kvm

if [[ ! -r /dev/kvm || ! -w /dev/kvm ]]; then
  sudo usermod -aG kvm "$(whoami)"
  echo "Reconnect SSH, then rerun this section."
  exit 1
fi

python3 - <<'PY'
import fcntl
import os

fd = os.open("/dev/kvm", os.O_RDWR | os.O_CLOEXEC)
try:
    version = fcntl.ioctl(fd, 0xAE00, 0)  # KVM_GET_API_VERSION
    assert version == 12, version
    print({"kvm_api_version": version})
finally:
    os.close(fd)
PY
```

Expected: an ext4 root filesystem, 32 CPUs for `Standard_D32s_v5`, readable
and writable `/dev/kvm`, and `{"kvm_api_version": 12}`.

## 3. Install prerequisites and clone the signed tree

Install host dependencies:

```bash
set -euo pipefail

sudo apt-get update
sudo apt-get install -y \
  build-essential \
  cpio \
  curl \
  binutils \
  file \
  git \
  jq \
  pkg-config \
  python3 \
  python3-venv \
  rsync \
  unzip

if ! command -v docker >/dev/null; then
  sudo apt-get install -y docker.io
fi
sudo usermod -aG docker "$(whoami)"
```

Reconnect after adding the Docker group, then verify:

```bash
docker version
docker run --rm hello-world
```

Install Rust and `just`:

```bash
export HOME="${HOME:-/home/$(whoami)}"
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source "$HOME/.cargo/env"
rustup toolchain install 1.98.0 --profile minimal
cargo install just
```

Install the load generator:

```bash
sudo apt-get install -y golang-go
go install github.com/rakyll/hey@v0.1.4
export PATH="$HOME/go/bin:$HOME/.cargo/bin:$PATH"
hey -version
```

Clone and verify Hyperlight-Unikraft:

```bash
set -euo pipefail

HYPERLIGHT_UNIKRAFT_REF=d240b022fe677db482a5b10b6cea16d5ab39cf07

mkdir -p "$HOME/src" "$HOME/results"
cd "$HOME/src"
git clone https://github.com/simongdavies/hyperlight-unikraft.git
cd hyperlight-unikraft
git checkout --detach "$HYPERLIGHT_UNIKRAFT_REF"
git submodule update --init --recursive
test "$(git rev-parse HEAD)" = "$HYPERLIGHT_UNIKRAFT_REF"
git verify-commit HEAD
git status --short
```

Expected: `git verify-commit` reports a good signature and `git status
--short` prints nothing.

## 4. Build or obtain the Workerd executor

Hyperlight-Unikraft packages a trusted external Workerd-fork executor; it does
not build stock Workerd. The real executor must be an executable x86-64 Linux
PIE implementing this repository's Workerd sandbox ABI.

### Option A: use a trusted executor produced by your Workerd build

Copy the executor to the VM and set:

```bash
WORKERD_EXECUTOR="$HOME/artifacts/workerd-sandbox-executor"
test -x "$WORKERD_EXECUTOR"
```

### Option B: build the executor from a public Workerd fork

Pin the exact Workerd revision. Do not build a moving branch:

```bash
: "${WORKERD_REF:?set WORKERD_REF to the signed Workerd revision}"

cd "$HOME/src"
git clone https://github.com/simongdavies/workerd.git
cd workerd
git checkout --detach "$WORKERD_REF"
git submodule update --init --recursive
test "$(git rev-parse HEAD)" = "$WORKERD_REF"
```

The validated build uses a container with LLVM 22 and Bazelisk:

```bash
cat > .devcontainer/Dockerfile.hyperlight-executor <<'EOF'
FROM mcr.microsoft.com/vscode/devcontainers/javascript-node:26
ARG LLVM_VERSION=22
RUN export DEBIAN_FRONTEND=noninteractive \
    && apt-get update \
    && apt-get install -y --no-install-recommends curl tcl \
    && curl -fSsL -o /tmp/llvm.sh https://apt.llvm.org/llvm.sh \
    && bash /tmp/llvm.sh ${LLVM_VERSION} \
    && apt-get install -y --no-install-recommends \
       libunwind-${LLVM_VERSION}-dev \
       libc++-${LLVM_VERSION}-dev \
       libc++abi-${LLVM_VERSION}-dev \
       libclang-rt-${LLVM_VERSION}-dev \
       -o DPkg::options::="--force-overwrite" \
    && npm install -g @bazel/bazelisk \
    && rm -rf /var/lib/apt/lists/* /tmp/llvm.sh
ENV PATH="/usr/lib/llvm-${LLVM_VERSION}/bin:${PATH}"
EOF

docker build \
  -t workerd-hyperlight-builder \
  -f .devcontainer/Dockerfile.hyperlight-executor \
  .devcontainer

mkdir -p "$HOME/.cache/workerd-bazel/action-cache"
mkdir -p "$HOME/.cache/workerd-bazel/repository-cache"
mkdir -p "$HOME/.cache/workerd-libcxx22"
mkdir -p "$HOME/artifacts"

docker run --rm \
  -v "$HOME/.cache/workerd-libcxx22:/out" \
  workerd-hyperlight-builder \
  bash -lc '
    cp -a /usr/lib/llvm-22/lib/libc++.so* /out/
    cp -a /usr/lib/llvm-22/lib/libc++abi.so* /out/
  '

docker run --rm \
  -v "$PWD:/workspace" \
  -v "$HOME/.cache/workerd-bazel:/root/.cache/bazel" \
  -v "$HOME/.cache/workerd-libcxx22:/opt/libcxx22:ro" \
  -v "$HOME/artifacts:/artifacts" \
  -w /workspace \
  workerd-hyperlight-builder \
  bash -lc '
    set -euxo pipefail
    bazel --output_base=/root/.cache/bazel/workerd-hyperlight-output \
      build //src/workerd/server:workerd-sandbox-executor \
      --config=opt \
      --strip=always \
      --//:io_backend=cxx \
      --workspace_status_command=/bin/true \
      --jobs="${WORKERD_BAZEL_JOBS:-16}" \
      --disk_cache=/root/.cache/bazel/action-cache \
      --repository_cache=/root/.cache/bazel/repository-cache \
      --repo_env=CC=/usr/lib/llvm-22/bin/clang \
      --repo_env=AR=/usr/lib/llvm-22/bin/llvm-ar \
      --linkopt=--ld-path=/usr/lib/llvm-22/bin/ld.lld \
      --host_linkopt=--ld-path=/usr/lib/llvm-22/bin/ld.lld \
      --host_linkopt=-L/opt/libcxx22 \
      --host_linkopt=-Wl,-rpath,/opt/libcxx22 \
      --action_env=LD_LIBRARY_PATH=/opt/libcxx22 \
      --host_action_env=LD_LIBRARY_PATH=/opt/libcxx22

    executor=bazel-bin/src/workerd/server/workerd-sandbox-executor
    "$executor" --self-test
    /usr/lib/llvm-22/bin/llvm-strip "$executor"
    cp "$executor" /artifacts/workerd-sandbox-executor
    chmod 0755 /artifacts/workerd-sandbox-executor
  '

WORKERD_EXECUTOR="$HOME/artifacts/workerd-sandbox-executor"
```

Validate whichever executor you selected:

```bash
set -euo pipefail

"$WORKERD_EXECUTOR" --self-test
file "$WORKERD_EXECUTOR"
readelf -n "$WORKERD_EXECUTOR" | sed -n '/Build ID/p'
readelf -l "$WORKERD_EXECUTOR"
readelf -d "$WORKERD_EXECUTOR" || true
sha256sum "$WORKERD_EXECUTOR"

file "$WORKERD_EXECUTOR" | grep -q 'ELF 64-bit.*x86-64'
file "$WORKERD_EXECUTOR" | grep -qi 'pie executable'
! readelf -l "$WORKERD_EXECUTOR" | grep -q 'INTERP'
! readelf -d "$WORKERD_EXECUTOR" | grep -Eq '(NEEDED|RPATH|RUNPATH)'
```

Expected: executor self-test passes; the artifact is a stripped x86-64 static
PIE with a GNU Build ID and no interpreter or dynamic dependencies. Record the
Workerd revision, executor Build ID, byte size, and SHA-256.

## 5. Build Hyperlight-Unikraft and package the rootfs

```bash
set -euo pipefail
cd "$HOME/src/hyperlight-unikraft"
source "$HOME/.cargo/env"

export CARGO_TARGET_DIR="$HOME/.cache/hyperlight-unikraft-target"
mkdir -p "$CARGO_TARGET_DIR"

just build-workerd-kernel

bash examples/workerd-executor/build-rootfs.sh \
  "$WORKERD_EXECUTOR"

cargo build --release --locked --example workerd-demo

sha256sum \
  kernel/workerd_hyperlight-x86_64 \
  build-elfloader/workerd-executor/executor \
  build-elfloader/workerd-executor/rootfs.img \
  target/release/examples/workerd-demo
```

For a static PIE, `executor` and `rootfs.img` are byte-identical. The packaging
script rejects a non-executable, non-x86-64, or non-PIE executor and records
the dependency closure.

Run focused scheduler and mock-fixture validation:

```bash
just guests
cargo test --locked --lib workerd::pool::tests
cargo test --locked --example workerd-demo
```

Expected: all tests pass. The mock fixture validates the host harness; it is
not a substitute for the real executor probes below.

## 6. Launch the demo and verify basic behavior

Start the on-demand server:

```bash
cd "$HOME/src/hyperlight-unikraft"
mkdir -p "$HOME/results/on-demand"

RUST_LOG=info \
target/release/examples/workerd-demo \
  --executor build-elfloader/workerd-executor/executor \
  --rootfs build-elfloader/workerd-executor/rootfs.img \
  --bundle examples/workerd-bundles/acceptance.json \
  --bind 127.0.0.1:8787 \
  --scratch-mb 384 \
  --request-timeout-ms 500 \
  --restore-mode on-demand \
  --max-concurrent-sandboxes 32 \
  --queue-capacity 2048 \
  --profile-log-every 1 \
  >"$HOME/results/on-demand/server.log" 2>&1 &

SERVER_PID=$!
echo "$SERVER_PID" > "$HOME/results/on-demand/server.pid"

for _ in $(seq 1 600); do
  curl --silent --fail \
    http://127.0.0.1:8787/__hyperlight/pool-status \
    >"$HOME/results/on-demand/startup-status.json" && break
  kill -0 "$SERVER_PID"
  sleep 1
done

jq . "$HOME/results/on-demand/startup-status.json"
```

Expected: `restore_mode` is `on-demand`, the server process remains alive, and
the status endpoint returns JSON.

Send a normal request:

```bash
curl --fail-with-body -sS \
  -X POST \
  -H 'x-demo: azure' \
  -d 'hello' \
  http://127.0.0.1:8787/hello | jq .
```

Expected:

```json
{"path":"/hello","method":"POST"}
```

Prove timeout recovery:

```bash
curl -i -X POST -d busy http://127.0.0.1:8787/busy
curl --fail-with-body -sS \
  -X POST \
  -d after \
  http://127.0.0.1:8787/after | jq .
```

Expected: `/busy` returns HTTP 504. The following `/after` request returns
HTTP 200 and `{"path":"/after","method":"POST"}`. A timed-out VM does not
poison the immutable snapshot or a later request.

Test bounded overload by restarting with one active slot and one queued slot:

```bash
kill -TERM "$SERVER_PID"
wait "$SERVER_PID"

RUST_LOG=info \
target/release/examples/workerd-demo \
  --executor build-elfloader/workerd-executor/executor \
  --rootfs build-elfloader/workerd-executor/rootfs.img \
  --bundle examples/workerd-bundles/acceptance.json \
  --bind 127.0.0.1:8787 \
  --scratch-mb 384 \
  --request-timeout-ms 500 \
  --restore-mode on-demand \
  --max-concurrent-sandboxes 1 \
  --queue-capacity 1 \
  >"$HOME/results/on-demand/overload-server.log" 2>&1 &
SERVER_PID=$!

until curl --silent --fail \
  http://127.0.0.1:8787/__hyperlight/pool-status >/dev/null; do
  kill -0 "$SERVER_PID"
  sleep 1
done

hey -n 20 -c 20 -m POST -d busy http://127.0.0.1:8787/busy
```

Expected: one request executes, one waits, and excess admissions receive HTTP
503. HTTP 504s are expected for the admitted `/busy` requests.

Stop the server cleanly:

```bash
kill -TERM "$SERVER_PID"
wait "$SERVER_PID"
```

To demonstrate host reboot recovery, record the hashes, reboot, reconnect,
rerun Section 2, verify the hashes again, and relaunch the server:

```bash
sha256sum \
  build-elfloader/workerd-executor/executor \
  build-elfloader/workerd-executor/rootfs.img \
  target/release/examples/workerd-demo \
  | tee "$HOME/results/pre-reboot-sha256.txt"

sudo reboot
```

After reconnecting:

```bash
cd "$HOME/src/hyperlight-unikraft"
sha256sum -c "$HOME/results/pre-reboot-sha256.txt"
```

## 7. Run every WinterTC capability demo

Start a deterministic loopback upstream:

```bash
mkdir -p "$HOME/results/upstream"
printf 'loopback-upstream\n' > "$HOME/results/upstream/index.html"
python3 -m http.server 18080 \
  --bind 127.0.0.1 \
  --directory "$HOME/results/upstream" \
  >"$HOME/results/upstream/server.log" 2>&1 &
UPSTREAM_PID=$!
```

Start the human-runnable WinterTC demo bundle with only that loopback port
allowed for outbound fetch:

```bash
cd "$HOME/src/hyperlight-unikraft"
mkdir -p "$HOME/results/wintertc-demo"

RUST_LOG=info \
target/release/examples/workerd-demo \
  --executor build-elfloader/workerd-executor/executor \
  --rootfs build-elfloader/workerd-executor/rootfs.img \
  --bundle examples/workerd-bundles/workerd-wintertc-demo.json \
  --bind 127.0.0.1:8787 \
  --scratch-mb 384 \
  --request-timeout-ms 5000 \
  --restore-mode on-demand \
  --max-concurrent-sandboxes 8 \
  --queue-capacity 128 \
  --fetch-loopback-port 18080 \
  --profile-log-every 1 \
  >"$HOME/results/wintertc-demo/server.log" 2>&1 &
SERVER_PID=$!

until curl --silent --fail \
  http://127.0.0.1:8787/__hyperlight/pool-status >/dev/null; do
  kill -0 "$SERVER_PID"
  sleep 1
done
```

Run each independent route:

```bash
set -euo pipefail

for route in \
  core \
  timers \
  global-handlers \
  byob \
  byte-stream-tee \
  core-wasm
do
  curl --fail-with-body -sS \
    "http://127.0.0.1:8787/evidence/$route" \
    | tee "$HOME/results/wintertc-demo/$route.json"
  printf '\n'
done

curl --fail-with-body -sS \
  'http://127.0.0.1:8787/evidence/state?token=azure-kvm-demo' \
  | tee "$HOME/results/wintertc-demo/state-first.json"
printf '\n'

curl --fail-with-body -sS \
  'http://127.0.0.1:8787/evidence/state?token=azure-kvm-demo' \
  | tee "$HOME/results/wintertc-demo/state-second.json"
printf '\n'

curl --fail-with-body -sS \
  'http://127.0.0.1:8787/evidence/fetch?upstream=http://127.0.0.1:18080/' \
  | tee "$HOME/results/wintertc-demo/fetch.json"
printf '\n'
```

Exercise every MessagePort stage:

```bash
for stage in \
  construct \
  listener-registration \
  start \
  post-message \
  queued-delivery \
  close \
  transfer-reentanglement \
  clone-failure
do
  curl --fail-with-body -sS \
    "http://127.0.0.1:8787/evidence/messageport?stage=$stage" \
    | tee "$HOME/results/wintertc-demo/messageport-$stage.json"
  printf '\n'
done
```

Success means every command returns HTTP 200 and the route JSON reports its
behavior as passing. The two state requests must both show fresh request state;
mutable module state must not carry from one request VM to the next. A passing
core Wasm route does not imply WebAssembly Component Model support.

Stop both processes:

```bash
kill -TERM "$SERVER_PID" "$UPSTREAM_PID"
wait "$SERVER_PID"
wait "$UPSTREAM_PID"
```

## 8. Run a simple parallel `hey` benchmark

Use the 9,441-byte `/sync` response from
`examples/workerd-bundles/workerd-pool-benchmark.json`.

Start a fresh on-demand server:

```bash
cd "$HOME/src/hyperlight-unikraft"
mkdir -p "$HOME/results/hey-on-demand"

RUST_LOG=info \
target/release/examples/workerd-demo \
  --executor build-elfloader/workerd-executor/executor \
  --rootfs build-elfloader/workerd-executor/rootfs.img \
  --bundle examples/workerd-bundles/workerd-pool-benchmark.json \
  --bind 127.0.0.1:8787 \
  --scratch-mb 384 \
  --request-timeout-ms 30000 \
  --restore-mode on-demand \
  --max-concurrent-sandboxes 32 \
  --queue-capacity 256 \
  --profile-log-every 1 \
  >"$HOME/results/hey-on-demand/server.log" 2>&1 &
SERVER_PID=$!

until curl --silent --fail \
  http://127.0.0.1:8787/__hyperlight/pool-status \
  >"$HOME/results/hey-on-demand/startup-status.json"; do
  kill -0 "$SERVER_PID"
  sleep 1
done
```

Verify response identity before load:

```bash
curl --fail-with-body -sS \
  http://127.0.0.1:8787/sync \
  -o "$HOME/results/hey-on-demand/sync.bin"

test "$(wc -c < "$HOME/results/hey-on-demand/sync.bin")" -eq 9441
sha256sum "$HOME/results/hey-on-demand/sync.bin"
```

Run a single request, one exact 32-client wave, and a 60-second sustained run:

```bash
hey -n 1 -c 1 \
  http://127.0.0.1:8787/sync \
  | tee "$HOME/results/hey-on-demand/single.txt"

hey -n 320 -c 32 \
  http://127.0.0.1:8787/sync \
  | tee "$HOME/results/hey-on-demand/wave-c32.txt"

hey -z 60s -c 32 \
  http://127.0.0.1:8787/sync \
  | tee "$HOME/results/hey-on-demand/sustained-c32.txt"

hey -z 60s -c 64 \
  http://127.0.0.1:8787/sync \
  | tee "$HOME/results/hey-on-demand/sustained-c64.txt"
```

Expected for an ordinary correctness/performance row:

- the `Status code distribution` contains only `[200]`;
- `Error distribution` is absent or zero;
- the server remains alive;
- the status endpoint returns to quiescence after load.

`hey` latency is client-observed end-to-end latency. Compare requests/second
and p50/p95/p99 only across matched server settings, payloads, request counts,
client concurrency, host size, and instrumentation.

Inspect status and stop cleanly:

```bash
curl --fail-with-body -sS \
  http://127.0.0.1:8787/__hyperlight/pool-status \
  | tee "$HOME/results/hey-on-demand/final-status.json" \
  | jq .

kill -TERM "$SERVER_PID"
wait "$SERVER_PID"
```

## 9. Compare on-demand and adaptive prewarmed execution

The prewarmed pool preserves one-request-per-VM isolation. Every fixed owner
restores a VM on its own thread, advertises a mailbox through the central ready
queue, executes at most one request, destroys that VM, and later restores a
replacement when the scheduler grants a permit.

Start the adaptive configuration:

```bash
cd "$HOME/src/hyperlight-unikraft"
mkdir -p "$HOME/results/hey-adaptive-o48-r1"

RUST_LOG=info \
target/release/examples/workerd-demo \
  --executor build-elfloader/workerd-executor/executor \
  --rootfs build-elfloader/workerd-executor/rootfs.img \
  --bundle examples/workerd-bundles/workerd-pool-benchmark.json \
  --bind 127.0.0.1:8787 \
  --scratch-mb 384 \
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
  >"$HOME/results/hey-adaptive-o48-r1/server.log" 2>&1 &
SERVER_PID=$!

until curl --silent --fail \
  http://127.0.0.1:8787/__hyperlight/pool-status \
  | tee "$HOME/results/hey-adaptive-o48-r1/startup-status.json" \
  | jq -e '
      .restore_mode == "prewarmed" and
      .prewarmed_inventory == .prewarmed_ready and
      .prewarmed_ready >= .warm_floor and
      .prewarmed_replenishing == 0
    ' >/dev/null; do
  kill -0 "$SERVER_PID"
  sleep 1
done
```

Run the same matched load:

```bash
hey -n 320 -c 32 \
  http://127.0.0.1:8787/sync \
  | tee "$HOME/results/hey-adaptive-o48-r1/wave-c32.txt"

hey -z 60s -c 32 \
  http://127.0.0.1:8787/sync \
  | tee "$HOME/results/hey-adaptive-o48-r1/sustained-c32.txt"

curl --fail-with-body -sS \
  http://127.0.0.1:8787/__hyperlight/pool-status \
  | tee "$HOME/results/hey-adaptive-o48-r1/final-status.json" \
  | jq .

kill -TERM "$SERVER_PID"
wait "$SERVER_PID"
```

The warm floor is fully restored but non-dispatchable, and shutdown is the only
normal path that destroys the final warm VM. Replenishment begins below the
low watermark and refills toward the high watermark in bounded batches.

Do not assume prewarming improves sustained throughput. On the validated KVM
host, adaptive watermarking preserved correctness but concurrent replacement
restore increased guest execution time enough to trail the matched on-demand
control. Near-zero ready misses prove inventory availability, not a throughput
win. Always retain the on-demand control.

### Exact no-refill diagnostic wave

Start 64 owners, prefill all of them, and suppress normal replacement restore
until one exact 32-request wave completes:

```bash
mkdir -p "$HOME/results/hey-no-refill-o64"

RUST_LOG=info \
target/release/examples/workerd-demo \
  --executor build-elfloader/workerd-executor/executor \
  --rootfs build-elfloader/workerd-executor/rootfs.img \
  --bundle examples/workerd-bundles/workerd-pool-benchmark.json \
  --bind 127.0.0.1:8787 \
  --scratch-mb 384 \
  --request-timeout-ms 30000 \
  --restore-mode prewarmed \
  --prewarmed-sandboxes 64 \
  --max-concurrent-restores 1 \
  --warm-floor 1 \
  --ready-low-watermark 16 \
  --ready-high-watermark 32 \
  --max-replenish-batch 2 \
  --diagnostic-no-refill-wave 32 \
  --max-concurrent-sandboxes 32 \
  --queue-capacity 256 \
  --profile-log-every 1 \
  >"$HOME/results/hey-no-refill-o64/server.log" 2>&1 &
SERVER_PID=$!

until curl --silent --fail \
  http://127.0.0.1:8787/__hyperlight/pool-status \
  | tee "$HOME/results/hey-no-refill-o64/startup-status.json" \
  | jq -e '.prewarmed_ready == 64' >/dev/null; do
  kill -0 "$SERVER_PID"
  sleep 1
done

hey -n 32 -c 32 \
  http://127.0.0.1:8787/sync \
  | tee "$HOME/results/hey-no-refill-o64/wave-c32.txt"

curl --fail-with-body -sS \
  http://127.0.0.1:8787/__hyperlight/pool-status \
  | tee "$HOME/results/hey-no-refill-o64/post-wave-status.json" \
  | jq .

kill -TERM "$SERVER_PID"
wait "$SERVER_PID"
```

Expected: all 32 responses are HTTP 200, the diagnostic completion count
reaches 32, replacement restore is paused until the wave ends, and the warm
floor is never violated.

## 10. Inspect pool status and request timing

Query live policy and pressure:

```bash
curl --fail-with-body -sS \
  http://127.0.0.1:8787/__hyperlight/pool-status \
  | jq '{
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
    }'
```

Per-request JSON profiles are written to the server log:

- `admission_wait_ms`: bounded admission/active-capacity delay;
- `ready_owner_wait_ms`: wait after active capacity exists but no dispatchable
  ready owner exists;
- `replenishment_policy_wait_ms`: owner wait for an adaptive restore permit;
- `replenishment_wait_ms`: wait for a restore slot;
- `replenishment_restore_ms`: actual replacement restore, excluding startup
  prefill;
- `snapshot_restore_ms`, `request_setup_ms`, `guest_execution_ms`,
  `response_finish_ms`, `vm_teardown_ms`, and `total_ms`: request phases.

The pool is quiescent when admitted, active, queued, execution/restore slots,
recycle, teardown, completion, and outstanding restore permits are all zero.
In prewarmed mode, inventory must equal ready depth, ready depth must remain at
least the warm floor, and refill/pause state must be inactive.

## 11. Use the repository benchmark wrapper

`tools/run-wintertc-pool-benchmark.sh` automates the same `/sync` workload,
server lifecycle, status sampling, identity checks, and JSON reporting. It
requires `cargo`, `curl`, `hey`, and Python 3.

On-demand:

```bash
WINTERTC_POOL_RESTORE_MODE=on-demand \
WINTERTC_POOL_PROFILE_LOG_EVERY=1 \
bash tools/run-wintertc-pool-benchmark.sh \
  build-elfloader/workerd-executor \
  "$HOME/results/wrapper-on-demand" \
  384 \
  32
```

Adaptive:

```bash
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
  "$HOME/results/wrapper-adaptive-o48-r1" \
  384 \
  32
```

The wrapper retains its JSON and server log when a performance acceptance gate
fails. Treat that as a measured result, not a reason to discard the row.

## 12. Capture 10 ms telemetry and optional `perf` profiles

Build the release binary and calculate clean-tree provenance:

```bash
cd "$HOME/src/hyperlight-unikraft"
cargo build --release --locked --example workerd-demo

SOURCE_COMMIT="$(git rev-parse HEAD)"
TREE_DIFF_SHA256="$(git diff --binary HEAD | sha256sum | awk '{print $1}')"
BINARY=target/release/examples/workerd-demo
ARTIFACT_DIR=build-elfloader/workerd-executor
BUNDLE=examples/workerd-bundles/workerd-pool-benchmark.json
```

Matched on-demand control:

```bash
python3 tools/run-workerd-prewarm-profile.py \
  --binary "$BINARY" \
  --artifact-dir "$ARTIFACT_DIR" \
  --bundle "$BUNDLE" \
  --output-dir "$HOME/results/profile-on-demand" \
  --source-commit "$SOURCE_COMMIT" \
  --patch-sha256 "$TREE_DIFF_SHA256" \
  --scratch-mib 384 \
  --restore-mode on-demand \
  --active 32 \
  --requests 320 \
  --concurrency 32 \
  --profile-log-every 1 \
  --perf-stat
```

Exact no-refill wave:

```bash
python3 tools/run-workerd-prewarm-profile.py \
  --binary "$BINARY" \
  --artifact-dir "$ARTIFACT_DIR" \
  --bundle "$BUNDLE" \
  --output-dir "$HOME/results/profile-no-refill-o64" \
  --source-commit "$SOURCE_COMMIT" \
  --patch-sha256 "$TREE_DIFF_SHA256" \
  --scratch-mib 384 \
  --restore-mode prewarmed \
  --active 32 \
  --owners 64 \
  --restores 1 \
  --warm-floor 1 \
  --ready-low 16 \
  --ready-high 32 \
  --replenish-batch 2 \
  --diagnostic-wave 32 \
  --requests 32 \
  --concurrency 32 \
  --profile-log-every 1 \
  --perf-stat
```

Sustained adaptive row:

```bash
python3 tools/run-workerd-prewarm-profile.py \
  --binary "$BINARY" \
  --artifact-dir "$ARTIFACT_DIR" \
  --bundle "$BUNDLE" \
  --output-dir "$HOME/results/profile-adaptive-o48-r1" \
  --source-commit "$SOURCE_COMMIT" \
  --patch-sha256 "$TREE_DIFF_SHA256" \
  --scratch-mib 384 \
  --restore-mode prewarmed \
  --active 32 \
  --owners 48 \
  --restores 1 \
  --warm-floor 1 \
  --ready-low 16 \
  --ready-high 32 \
  --replenish-batch 2 \
  --duration 60s \
  --concurrency 32 \
  --profile-log-every 1 \
  --perf-stat \
  --deep-profile
```

The runner writes:

- `result.json` with configuration, artifact hashes, correctness, throughput,
  latency, phase distributions, process/host deltas, and shutdown status;
- `timeseries.jsonl` with 10 ms process, host load/PSI, and pool samples;
- raw `hey` CSV and the server log;
- `perf stat`, CPU, off-CPU, and syscall-trace outputs when supported.

Some Azure kernels or security policies deny hardware counters or call stacks.
Keep the supported `perf stat`, `/proc`, load, and PSI data and retain
`profiler_limitations` from `result.json`; do not substitute QEMU.

## 13. Preserve a small reproducible result set

Record the environment and hashes:

```bash
mkdir -p "$HOME/results/provenance"
date --iso-8601=seconds \
  | tee "$HOME/results/provenance/timestamp.txt"
uname -a \
  | tee "$HOME/results/provenance/uname.txt"
lscpu -J \
  | tee "$HOME/results/provenance/lscpu.json"
free -b \
  | tee "$HOME/results/provenance/memory.txt"
git rev-parse HEAD \
  | tee "$HOME/results/provenance/hyperlight-unikraft-commit.txt"
git status --short \
  | tee "$HOME/results/provenance/git-status.txt"
sha256sum \
  build-elfloader/workerd-executor/executor \
  build-elfloader/workerd-executor/rootfs.img \
  kernel/workerd_hyperlight-x86_64 \
  target/release/examples/workerd-demo \
  examples/workerd-bundles/workerd-pool-benchmark.json \
  | tee "$HOME/results/provenance/artifact-sha256.txt"
```

Archive only an explicit allowlist of result directories. Do not archive
repository checkouts, Cargo/Bazel targets, credentials, SSH keys, or caches:

```bash
cd "$HOME/results"
printf '%s\n' \
  provenance \
  on-demand \
  wintertc-demo \
  hey-on-demand \
  hey-adaptive-o48-r1 \
  hey-no-refill-o64 \
  profile-on-demand \
  profile-no-refill-o64 \
  profile-adaptive-o48-r1 \
  > archive-allowlist.txt

tar -czf workerd-kvm-results.tar.gz \
  $(cat archive-allowlist.txt) \
  archive-allowlist.txt

tar -tzf workerd-kvm-results.tar.gz
sha256sum workerd-kvm-results.tar.gz
```

Inspect the archive member list before copying it off the VM.

## 14. Delete the Azure resource group and prove zero remains

Copy required results off the VM before deletion. Then run from the Azure CLI
control host:

```powershell
$ErrorActionPreference = 'Stop'

az group delete `
  --subscription $SubscriptionId `
  --name $ResourceGroup `
  --yes `
  --no-wait

$Deadline = (Get-Date).AddMinutes(30)
do {
  Start-Sleep -Seconds 15
  $Exists = az group exists `
    --subscription $SubscriptionId `
    --name $ResourceGroup `
    -o tsv
  "group_exists=$Exists"
} while ($Exists -eq 'true' -and (Get-Date) -lt $Deadline)

if ($Exists -eq 'true') {
  throw "Timed out deleting $ResourceGroup"
}

$MatchingGroups = az group list `
  --subscription $SubscriptionId `
  --query "[?name=='$ResourceGroup'].name" `
  -o tsv

$MatchingResources = az resource list `
  --subscription $SubscriptionId `
  --query "[?resourceGroup=='$ResourceGroup'].[resourceGroup,name,type]" `
  -o tsv

if ($MatchingGroups -or $MatchingResources) {
  throw "Matching Azure resources remain"
}

"Azure cleanup verified: zero matching groups and resources"
```

The walkthrough is incomplete until `az group exists` returns `false` and the
subscription-wide group/resource queries return no matches.

## 15. Troubleshooting

### `/dev/kvm` exists but is not writable

Add the user to `kvm`, disconnect, reconnect, and rerun Section 2. Do not run
the benchmark through `sudo` as a workaround.

### The executor is rejected by `build-rootfs.sh`

Run `file`, `readelf -l`, `readelf -d`, and `ldd`. The executor must be an
executable x86-64 Linux PIE. Static PIE is preferred. Dynamic PIE is supported
only when its interpreter and dependency closure resolve unambiguously.

### Server startup takes several minutes

Prewarmed startup restores every configured owner before the pool becomes
fully ready. Watch the server log and
`/__hyperlight/pool-status`; do not begin a matched row until the expected
ready depth is reached.

### Requests queue even though ready VMs exist

Inspect `max_concurrent_sandboxes`, `effective_concurrency`, `warm_floor`,
`admitted`, `active`, and `execution_slots_in_use`. The warm floor is reserved,
and the active execution cap remains independent of owner count.

### Adaptive throughput is below on-demand

Inspect `guest_execution_ms`, restore-slot use, replenishment restore time,
context switches, migrations, page faults, CPU PSI, and run queue. Background
restore consumes the same finite host resources as executing KVM guests. Lower
restore concurrency or active execution may reduce contention, but accept a
change only if it beats a matched on-demand control.

### `perf` reports unsupported or permission errors

Keep `perf stat` events that work and retain `/proc`, load, PSI, process CPU,
fault, context-switch, RSS, thread, and file-descriptor telemetry. Record the
limitation instead of changing the backend.

### The full test suite depends on external network access

Run focused scheduler and real-guest tests first. If an unrelated external
endpoint times out, retain the exact failure and rerun only that test. Do not
misreport an environmental failure as a scheduler failure.

## 16. Optional MSHV note

This runbook is the generally reproducible Linux KVM path. MSHV is a separate
backend:

```bash
cargo build --release --locked --features mshv --example workerd-demo
```

MSHV requires a compatible Azure Linux host, kernel, device access, and image
provenance. Internal image names or subscriptions are intentionally not part of
this public walkthrough. If those prerequisites are unavailable, report the
backend blocker; do not substitute QEMU and do not compare unmatched KVM and
MSHV measurements.
