#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 The Hyperlight Authors.
set -euo pipefail

HYPERLIGHT_COMMIT=c0564669d7cc7cfd42f33d28e4a0f69261f3dca6
WORKERD_COMMIT=621cb07e7d2cf0cb0f49872129d4408f6319acef
RUST_VERSION=1.98.0
JUST_VERSION=1.58.0
BAZELISK_VERSION=1.28.1
BAZELISK_SHA256=22e7d3a188699982f661cf4687137ee52d1f24fec1ec893d91a6c4d791a75de8
LLVM_VERSION=22
SIGNING_FINGERPRINT=B23C39FC43F625F276EC54713A82F8BAFEA39557

root="$(cd "$(dirname "$0")/.." && pwd)"
cache_root="${XDG_CACHE_HOME:-$HOME/.cache}/hyperlight-workerd"
workerd_dir="${WORKERD_DIR:-$cache_root/workerd}"
install_deps=false
export PATH="$HOME/go/bin:$HOME/.cargo/bin:$PATH"

usage() {
    cat <<EOF
Build the signed Workerd executor and package it for Hyperlight.

Usage: tools/setup-workerd-demo.sh [OPTIONS]

Options:
  --workerd-dir PATH  Workerd checkout (default: ~/.cache/hyperlight-workerd/workerd)
  --install-deps      Install Ubuntu/Debian host packages with apt
  -h, --help          Show this help

Environment:
  CARGO_BUILD_JOBS       Cargo parallelism (default: 8)
  WORKERD_BAZEL_JOBS    Bazel parallelism (default: min(nproc, 20))

The script must run on x86-64 Linux with Docker and KVM available. It verifies
the signed public commits, builds Workerd from source, packages the executor,
and builds the release workerd-demo binary.
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

[[ "$(uname -s)" == Linux ]] || fail "this setup script requires Linux"
[[ "$(uname -m)" == x86_64 ]] || fail "this setup script requires x86-64"

if "$install_deps"; then
    command -v sudo >/dev/null || fail "sudo is required with --install-deps"
    step "Installing host packages"
    sudo apt-get update
    sudo apt-get install -y \
        binutils build-essential ca-certificates cpio curl docker.io file \
        git gnupg golang-go jq nodejs npm patch pkg-config python3 rsync unzip
fi

for command in cargo curl docker file git go gpg node npm patch python3 \
    readelf rustup sha256sum; do
    command -v "$command" >/dev/null ||
        fail "missing '$command' (rerun with --install-deps where applicable)"
done
[[ -c /dev/kvm && -r /dev/kvm && -w /dev/kvm ]] ||
    fail "/dev/kvm must exist and be readable/writable by the current user"
docker info >/dev/null 2>&1 ||
    fail "Docker is not usable by the current user"

step "Verifying the Hyperlight runtime source"
git -C "$root" merge-base --is-ancestor "$HYPERLIGHT_COMMIT" HEAD ||
    fail "the checkout is not based on signed Hyperlight commit $HYPERLIGHT_COMMIT"
git -C "$root" diff --quiet "$HYPERLIGHT_COMMIT" -- \
    . \
    ':(exclude)docs/**' \
    ':(exclude)README.md' \
    ':(exclude)examples/workerd-executor/README.md' \
    ':(exclude)tools/setup-workerd-demo.sh' \
    ':(exclude)tools/hyperlight-demo' ||
    fail "runtime files differ from signed Hyperlight commit $HYPERLIGHT_COMMIT"
git -C "$root" submodule update --init --recursive

verify_home="$(mktemp -d)"
builder_file="$(mktemp)"
cleanup() {
    rm -rf -- "$verify_home"
    rm -f -- "$builder_file"
}
trap cleanup EXIT
chmod 0700 "$verify_home"
curl --fail --show-error --silent --location \
    https://github.com/simongdavies.gpg |
    GNUPGHOME="$verify_home" gpg --batch --import
GNUPGHOME="$verify_home" gpg --batch --with-colons --fingerprint |
    grep -Fq "fpr:::::::::$SIGNING_FINGERPRINT:" ||
    fail "the downloaded signing key has an unexpected fingerprint"
GNUPGHOME="$verify_home" git -C "$root" verify-commit "$HYPERLIGHT_COMMIT"

step "Checking out signed Workerd source"
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
GNUPGHOME="$verify_home" git -C "$workerd_dir" verify-commit "$WORKERD_COMMIT"

step "Installing the pinned Rust toolchain and just"
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
FROM mcr.microsoft.com/vscode/devcontainers/javascript-node:26-bookworm@sha256:4187a9d50e7a208659e9b56677ae764edb98cb6dc243f21e851aab2ca4d103ba
ARG LLVM_VERSION=$LLVM_VERSION
ARG BAZELISK_VERSION=$BAZELISK_VERSION
ARG BAZELISK_SHA256=$BAZELISK_SHA256
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        ca-certificates curl gnupg lsb-release tcl; \
    curl -fsSL https://apt.llvm.org/llvm-snapshot.gpg.key \
        | gpg --dearmor -o /usr/share/keyrings/apt.llvm.org.gpg; \
    codename="\$(. /etc/os-release; printf '%s' "\$VERSION_CODENAME")"; \
    printf 'deb [signed-by=/usr/share/keyrings/apt.llvm.org.gpg] https://apt.llvm.org/%s/ llvm-toolchain-%s-%s main\n' \
        "\$codename" "\$codename" "\$LLVM_VERSION" \
        >/etc/apt/sources.list.d/apt.llvm.org.list; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        clang-\$LLVM_VERSION lld-\$LLVM_VERSION llvm-\$LLVM_VERSION \
        libc++-\$LLVM_VERSION-dev libc++abi-\$LLVM_VERSION-dev \
        libclang-rt-\$LLVM_VERSION-dev libunwind-\$LLVM_VERSION-dev \
        -o DPkg::options::=--force-overwrite; \
    curl -fsSL \
        "https://github.com/bazelbuild/bazelisk/releases/download/v\$BAZELISK_VERSION/bazelisk-linux-amd64" \
        -o /usr/local/bin/bazelisk; \
    echo "\$BAZELISK_SHA256  /usr/local/bin/bazelisk" | sha256sum --check; \
    chmod 0755 /usr/local/bin/bazelisk; \
    ln -s bazelisk /usr/local/bin/bazel; \
    rm -rf /var/lib/apt/lists/*
ENV PATH="/usr/lib/llvm-$LLVM_VERSION/bin:\${PATH}"
EOF

step "Building the Workerd executor"
docker build \
    --tag workerd-hyperlight-builder \
    --file "$builder_file" \
    "$workerd_dir/.devcontainer"

mkdir -p "$cache_root/bazel" "$root/build-elfloader/workerd-executor"
jobs="${WORKERD_BAZEL_JOBS:-$(nproc)}"
((jobs > 20)) && jobs=20
docker run --rm \
    --env "WORKERD_BAZEL_JOBS=$jobs" \
    --mount "type=bind,src=$workerd_dir,dst=/workspace" \
    --mount "type=bind,src=$cache_root/bazel,dst=/root/.cache/bazel" \
    --mount "type=bind,src=$root/build-elfloader/workerd-executor,dst=/output" \
    --workdir /workspace \
    workerd-hyperlight-builder \
    bash -c '
        set -euo pipefail
        export CC=/usr/lib/llvm-22/bin/clang
        export CXX=/usr/lib/llvm-22/bin/clang++
        executor=bazel-bin/src/workerd/server/workerd-sandbox-executor
        bazel --output_base=/root/.cache/bazel/output \
            build //src/workerd/server:workerd-sandbox-executor \
            --config=opt \
            --strip=always \
            --//:io_backend=cxx \
            --jobs="$WORKERD_BAZEL_JOBS" \
            --disk_cache=/root/.cache/bazel/action-cache \
            --repository_cache=/root/.cache/bazel/repository-cache \
            --repo_env=CC="$CC" \
            --repo_env=CXX="$CXX"
        "$executor" --self-test
        llvm-strip "$executor"
        install -m 0755 "$executor" /output/workerd-sandbox-executor
    '

step "Building the Workerd guest kernel and root filesystem"
cd "$root"
export CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-8}"
just build-workerd-kernel
bash examples/workerd-executor/build-rootfs.sh \
    build-elfloader/workerd-executor/workerd-sandbox-executor

step "Building the Hyperlight demo"
cargo +"$RUST_VERSION" build --release --locked --example workerd-demo

step "Rebuilding the pinned Component Model fixture"
(
    cd experiments/workerd-component-model
    npm install \
        --ignore-scripts \
        --no-audit \
        --no-fund \
        --package-lock=false
    npm run build:component
    npm run transpile
    npm run lock
    npm test
    rm -rf node_modules
)
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
