#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 The Hyperlight Authors.
set -euo pipefail

HYPERLIGHT_COMMIT=c0564669d7cc7cfd42f33d28e4a0f69261f3dca6
WORKERD_COMMIT=9c698099ccbad54609901a2107aa12a55fe415db
RUST_VERSION=1.98.0
JUST_VERSION=1.58.0
BAZELISK_VERSION=1.28.1
BAZELISK_SHA256=22e7d3a188699982f661cf4687137ee52d1f24fec1ec893d91a6c4d791a75de8
LLVM_VERSION=22

root="$(cd "$(dirname "$0")/.." && pwd)"
cache_root="${XDG_CACHE_HOME:-$HOME/.cache}/hyperlight-workerd"
workerd_dir="${WORKERD_DIR:-$cache_root/workerd}"
install_deps=false

usage() {
    cat <<EOF
Build the Workerd fork used by this demo and package it for Hyperlight.

Usage: tools/setup-workerd-demo.sh [OPTIONS]

Options:
  --workerd-dir PATH  Workerd checkout (default: ~/.cache/hyperlight-workerd/workerd)
  --install-deps      Install Ubuntu/Debian host packages with apt
  -h, --help          Show this help

Environment:
  CARGO_HOME             Cargo home (must be user-writable; default: ~/.cargo)
  RUSTUP_HOME            Rustup home (must be user-writable; default: ~/.rustup)
  CARGO_BUILD_JOBS       Cargo parallelism (default: 8)
  WORKERD_BAZEL_JOBS    Bazel parallelism (default: nproc)

The script must run on x86-64 Linux with Docker and KVM available. It checks
out the exact Hyperlight and Workerd fork revisions used by this demo, builds
Workerd from source, packages the executor, and builds workerd-demo.
EOF
}

while (($#)); do
    case "$1" in
        --workerd-dir)
            workerd_dir="${2:?missing value for --workerd-dir}"
            shift 2
            ;;
        --install-deps)
            install_deps=true
            shift
            ;;
        -h | --help)
            usage
            exit 0
            ;;
        *)
            printf 'error: unknown argument: %s\n\n' "$1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

step() {
    printf '\n==> %s\n' "$*"
}

fail() {
    printf 'error: %s\n' "$*" >&2
    exit 1
}

configure_user_home() {
    local variable="$1"
    local fallback="$2"
    local value="${!variable:-$fallback}"
    local probe

    if ! mkdir -p "$value" 2>/dev/null ||
        ! probe="$(mktemp "$value/.hyperlight-write-test.XXXXXX" 2>/dev/null)"; then
        if [[ "$value" == "$fallback" ]]; then
            fail "$variable directory '$value' is not writable by the current user"
        fi
        printf 'warning: %s=%s is not writable; using %s\n' \
            "$variable" "$value" "$fallback" >&2
        value="$fallback"
        mkdir -p "$value" ||
            fail "could not create user-local $variable directory '$value'"
        probe="$(mktemp "$value/.hyperlight-write-test.XXXXXX")" ||
            fail "$variable directory '$value' is not writable by the current user"
    fi
    rm -f -- "$probe"
    printf -v "$variable" '%s' "$value"
    export "$variable"
}

installed_docker_ce_packages() {
    local package

    for package in containerd.io docker-ce docker-ce-cli \
        docker-buildx-plugin docker-compose-plugin; do
        if dpkg-query -W -f='${Status}' "$package" 2>/dev/null |
            grep -q '^install ok installed$'; then
            printf '%s\n' "$package"
        fi
    done
}

[[ "$(uname -s)" == Linux ]] || fail "this setup script requires Linux"
[[ "$(uname -m)" == x86_64 ]] || fail "this setup script requires x86-64"
((EUID != 0)) ||
    fail "run this script as a normal user, not with sudo (it invokes sudo only for apt)"

configure_user_home CARGO_HOME "$HOME/.cargo"
configure_user_home RUSTUP_HOME "$HOME/.rustup"
export PATH="$HOME/go/bin:$CARGO_HOME/bin:$PATH"

if "$install_deps"; then
    command -v sudo >/dev/null || fail "sudo is required with --install-deps"
    command -v apt-get >/dev/null ||
        fail "apt-get is required with --install-deps"
    command -v apt-cache >/dev/null ||
        fail "apt-cache is required with --install-deps"
    command -v dpkg-query >/dev/null ||
        fail "dpkg-query is required with --install-deps"
    mapfile -t docker_ce_packages < <(installed_docker_ce_packages)
    if ((${#docker_ce_packages[@]})); then
        printf 'error: Ubuntu docker.io/containerd conflicts with installed Docker CE packages: %s\n' \
            "${docker_ce_packages[*]}" >&2
        printf 'Remove them with the appropriate apt command, then rerun this script.\n' >&2
        printf 'For example: sudo apt-get remove %s\n' \
            "${docker_ce_packages[*]}" >&2
        exit 1
    fi
    step "Installing host packages"
    sudo apt-get update
    host_packages=(
        binutils build-essential ca-certificates containerd cpio curl \
        docker.io file git golang-go jq patch pkg-config python3 \
        rsync unzip
    )
    if apt-cache show docker-buildx >/dev/null 2>&1; then
        host_packages+=(docker-buildx)
    else
        printf 'warning: Ubuntu package docker-buildx is unavailable; continuing without it\n' >&2
    fi
    sudo apt-get install -y "${host_packages[@]}"
fi

for command in cargo curl docker file git go patch python3 \
    readelf rustup sha256sum; do
    command -v "$command" >/dev/null ||
        fail "missing '$command' (rerun with --install-deps where applicable)"
done
[[ -c /dev/kvm && -r /dev/kvm && -w /dev/kvm ]] ||
    fail "/dev/kvm must exist and be readable/writable by the current user"
docker info >/dev/null 2>&1 ||
    fail "Docker is not usable by the current user"

step "Checking the Hyperlight fork revision"
git -C "$root" merge-base --is-ancestor "$HYPERLIGHT_COMMIT" HEAD ||
    fail "the checkout is not based on Hyperlight commit $HYPERLIGHT_COMMIT"
git -C "$root" diff --quiet &&
    git -C "$root" diff --cached --quiet ||
    fail "the Hyperlight checkout has uncommitted tracked changes"
git -C "$root" submodule update --init --recursive

builder_file="$(mktemp)"
cleanup() {
    rm -f -- "$builder_file"
}
trap cleanup EXIT

step "Checking out the Workerd fork revision"
if [[ ! -d "$workerd_dir/.git" ]]; then
    git clone \
        --branch simongdavies-workerd-ingress-bindings \
        --single-branch \
        https://github.com/simongdavies/workerd.git \
        "$workerd_dir"
fi
git -C "$workerd_dir" fetch origin \
    refs/heads/simongdavies-workerd-ingress-bindings
git -C "$workerd_dir" checkout --detach "$WORKERD_COMMIT"
git -C "$workerd_dir" submodule update --init --recursive
[[ "$(git -C "$workerd_dir" rev-parse HEAD)" == "$WORKERD_COMMIT" ]] ||
    fail "Workerd checkout did not resolve to $WORKERD_COMMIT"
[[ -z "$(git -C "$workerd_dir" status --porcelain)" ]] ||
    fail "Workerd checkout is not clean"

step "Installing the required Rust toolchain and just"
rustup toolchain install "$RUST_VERSION" --profile minimal
if ! command -v just >/dev/null; then
    cargo +"$RUST_VERSION" install just --version "$JUST_VERSION" --locked
fi
if ! command -v hey >/dev/null; then
    go install github.com/rakyll/hey@v0.1.4
fi
if ! command -v wasm-tools >/dev/null ||
    [[ "$(wasm-tools --version)" != "wasm-tools 1.252.0" ]]; then
    cargo +"$RUST_VERSION" install wasm-tools --version 1.252.0 --locked
fi

cat >"$builder_file" <<EOF
FROM node:trixie
ARG LLVM_VERSION=$LLVM_VERSION
ARG BAZELISK_VERSION=$BAZELISK_VERSION
ARG BAZELISK_SHA256=$BAZELISK_SHA256
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        ca-certificates curl dpkg-dev git gnupg lsb-release tcl wget; \
    curl -fsSL https://apt.llvm.org/llvm-snapshot.gpg.key \
        | gpg --dearmor -o /usr/share/keyrings/apt.llvm.org.gpg; \
    codename="\$(. /etc/os-release; printf '%s' "\$VERSION_CODENAME")"; \
    printf 'deb [signed-by=/usr/share/keyrings/apt.llvm.org.gpg] https://apt.llvm.org/%s/ llvm-toolchain-%s-%s main\n' \
        "\$codename" "\$codename" "\$LLVM_VERSION" \
        >/etc/apt/sources.list.d/apt.llvm.org.list; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        clang-\$LLVM_VERSION lld-\$LLVM_VERSION llvm-\$LLVM_VERSION \
        libc++-\$LLVM_VERSION-dev \
        libclang-rt-\$LLVM_VERSION-dev libunwind-\$LLVM_VERSION-dev \
        -o DPkg::options::=--force-overwrite; \
    multiarch="\$(dpkg-architecture -qDEB_HOST_MULTIARCH)"; \
    mkdir -p /opt/libcxx22; \
    /usr/lib/llvm-\$LLVM_VERSION/bin/clang++ -shared \
        -o /opt/libcxx22/libc++.so.1 \
        -Wl,-soname,libc++.so.1 \
        -Wl,--whole-archive "/usr/lib/\$multiarch/libc++.a" \
        -Wl,--no-whole-archive \
        -lunwind -lpthread -ldl -lm -lc; \
    ln -s libc++.so.1 /opt/libcxx22/libc++.so; \
    /usr/lib/llvm-\$LLVM_VERSION/bin/llvm-nm -D -C \
        /opt/libcxx22/libc++.so.1 \
        | grep -F 'std::__1::__hash_memory'; \
    curl -fsSL \
        "https://github.com/bazelbuild/bazelisk/releases/download/v\$BAZELISK_VERSION/bazelisk-linux-amd64" \
        -o /usr/local/bin/bazelisk; \
    echo "\$BAZELISK_SHA256  /usr/local/bin/bazelisk" | sha256sum --check; \
    chmod 0755 /usr/local/bin/bazelisk; \
    ln -s bazelisk /usr/local/bin/bazel; \
    rm -rf /var/lib/apt/lists/*
ENV PATH="/usr/lib/llvm-$LLVM_VERSION/bin:\${PATH}"
EOF

step "Preparing the Workerd executor builder"
docker build \
    --tag workerd-hyperlight-builder \
    --file "$builder_file" \
    "$workerd_dir/.devcontainer"

builder_image_id="$(
    docker image inspect --format '{{.Id}}' workerd-hyperlight-builder
)"
builder_cache_key="${builder_image_id#sha256:}"
[[ "$builder_cache_key" =~ ^[0-9a-f]{64}$ ]] ||
    fail "could not determine the Workerd builder image ID"
builder_cache_root="$cache_root/bazel/builders/$builder_cache_key"
executor_dir="$root/build-elfloader/workerd-executor"
executor_path="$executor_dir/workerd-sandbox-executor"
packaged_executor_path="$executor_dir/executor"
executor_stamp="$executor_dir/build.stamp"
expected_executor_stamp="$(
    printf 'workerd=%s\nbuilder=%s\n' "$WORKERD_COMMIT" "$builder_image_id"
)"
stamped_workerd=""
mkdir -p \
    "$builder_cache_root" \
    "$cache_root/bazel/repository-cache" \
    "$executor_dir"
if [[ ! -x "$executor_path" ]] && [[ -x "$packaged_executor_path" ]]; then
    step "Restoring the packaged Workerd executor"
    install -m 0755 "$packaged_executor_path" "$executor_path"
fi
if [[ -x "$executor_path" ]] && [[ ! -e "$executor_stamp" ]]; then
    step "Validating the existing Workerd executor"
    "$executor_path" --self-test
    printf '%s\n' "$expected_executor_stamp" >"$executor_stamp.tmp"
    mv "$executor_stamp.tmp" "$executor_stamp"
fi
if [[ -f "$executor_stamp" ]]; then
    stamped_workerd="$(
        sed -n 's/^workerd=//p' "$executor_stamp" |
            head -n 1
    )"
fi
if [[ -x "$executor_path" ]] &&
    [[ "$stamped_workerd" == "$WORKERD_COMMIT" ]]; then
    step "Reusing the validated Workerd executor"
else
    step "Building the Workerd executor"
    jobs="${WORKERD_BAZEL_JOBS:-$(nproc)}"
    docker run --rm \
        --env "WORKERD_BAZEL_JOBS=$jobs" \
        --mount "type=bind,src=$workerd_dir,dst=/workspace" \
        --mount "type=bind,src=$builder_cache_root,dst=/root/.cache/bazel/builder" \
        --mount "type=bind,src=$cache_root/bazel/repository-cache,dst=/root/.cache/bazel/repository-cache" \
        --mount "type=bind,src=$executor_dir,dst=/output" \
        --workdir /workspace \
        workerd-hyperlight-builder \
        bash -c '
            set -euo pipefail
            export CC=/usr/lib/llvm-22/bin/clang
            export CXX=/usr/lib/llvm-22/bin/clang++
            clang_major="$("$CXX" --version |
                sed -n "s/.*clang version \([0-9][0-9]*\).*/\1/p" |
                head -n 1)"
            [[ "$clang_major" == 22 ]] ||
                { echo "error: expected Clang 22, found ${clang_major:-unknown}" >&2; exit 1; }
            dpkg-query -W -f="\${binary:Package} \${Version}\n" \
                libc++-22-dev libc++abi-22-dev libunwind-22-dev
            printf "%s\n" \
                "#include <string>" \
                "#include <unordered_map>" \
                "int main() {" \
                "  std::unordered_map<std::string, int> values{{\"ok\", 1}};" \
                "  return values[\"ok\"] == 1 ? 0 : 1;" \
                "}" >/tmp/libcxx-check.cc
            "$CXX" -std=c++20 -stdlib=libc++ -fuse-ld=lld \
                -L/opt/libcxx22 -Wl,-rpath,/opt/libcxx22 \
                /tmp/libcxx-check.cc -o /tmp/libcxx-check
            LD_LIBRARY_PATH=/opt/libcxx22 /tmp/libcxx-check
            executor=bazel-bin/src/workerd/server/workerd-sandbox-executor
            bazel --output_base=/root/.cache/bazel/builder/output \
                build //src/workerd/server:workerd-sandbox-executor \
                --config=opt \
                --strip=always \
                --//:io_backend=cxx \
                --jobs="$WORKERD_BAZEL_JOBS" \
                --disk_cache=/root/.cache/bazel/builder/action-cache \
                --repository_cache=/root/.cache/bazel/repository-cache \
                --repo_env=CC="$CC" \
                --repo_env=CXX="$CXX" \
                --host_linkopt=-L/opt/libcxx22 \
                --host_linkopt=-Wl,-rpath,/opt/libcxx22 \
                --action_env=LD_LIBRARY_PATH=/opt/libcxx22 \
                --host_action_env=LD_LIBRARY_PATH=/opt/libcxx22
            "$executor" --self-test
            llvm-strip "$executor"
            install -m 0755 "$executor" /output/workerd-sandbox-executor
        '
    printf '%s\n' "$expected_executor_stamp" >"$executor_stamp.tmp"
    mv "$executor_stamp.tmp" "$executor_stamp"
fi

step "Building the Workerd guest kernel and root filesystem"
cd "$root"
export CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-8}"
just build-workerd-kernel
bash examples/workerd-executor/build-rootfs.sh \
    build-elfloader/workerd-executor/workerd-sandbox-executor

step "Building the Hyperlight demo"
cargo +"$RUST_VERSION" build --release --locked --example workerd-demo

step "Verifying the Component Model fixture"
(
    cd experiments/workerd-component-model
    wasm-tools parse component.wat -o component.wasm
    wasm-tools validate --features component-model component.wasm
)
docker run --rm \
    --user "$(id -u):$(id -g)" \
    --env HOME=/tmp \
    --mount "type=bind,src=$root,dst=/repo" \
    --workdir /repo/experiments/workerd-component-model \
    workerd-hyperlight-builder \
    bash -c '
        set -euo pipefail
        node_major="$(node --version |
            sed -n "s/^v\([0-9][0-9]*\).*/\1/p")"
        ((node_major >= 22)) ||
            { echo "error: expected Node.js 22 or newer, found $(node --version)" >&2; exit 1; }
        node --test test/*.test.mjs
    '
git diff --exit-code -- experiments/workerd-component-model

step "Checking the packaged executor"
build-elfloader/workerd-executor/executor --self-test
file build-elfloader/workerd-executor/executor
if readelf -l build-elfloader/workerd-executor/executor | grep -q INTERP; then
    fail "the packaged executor is not static"
fi

cat <<EOF

Setup complete.

Demo binary:
  $root/target/release/examples/workerd-demo

Packaged executor:
  $root/build-elfloader/workerd-executor/executor

Next:
  docs/azure-workerd-hyperlight-runbook.md
EOF
