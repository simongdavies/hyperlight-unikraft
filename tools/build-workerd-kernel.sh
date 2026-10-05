#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 The Hyperlight Authors.
set -euo pipefail

root="${HLUK_ROOT:-$(cd "$(dirname "$0")/.." && pwd)}"
output="${1:-$root/kernel/workerd_hyperlight-x86_64}"
build="$(mktemp -d)"
trap 'rm -rf -- "$build"' EXIT

mkdir -p "$build/kernel/unikraft" \
    "$build/kernel/app-elfloader" \
    "$build/kernel/libs/libelf"
git -C "$root/kernel/unikraft" archive HEAD |
    tar -x -C "$build/kernel/unikraft"
patch -d "$build/kernel/unikraft" -p1 \
    < "$root/kernel/patches/hyperlight-hostcall-ioctl.patch"
git -C "$root/kernel/app-elfloader" archive HEAD |
    tar -x -C "$build/kernel/app-elfloader"
git -C "$root/kernel/libs/libelf" archive HEAD |
    tar -x -C "$build/kernel/libs/libelf"
cp "$root/kernel/Dockerfile.build" "$build/kernel/Dockerfile.build"
cp "$root/defconfig-workerd" "$build/defconfig-workerd"

docker build --progress=plain -t hluk-kernel-builder \
    -f "$build/kernel/Dockerfile.build" "$build/kernel"
docker run --rm \
    -v "$build/kernel:/kernel" \
    -v "$build/defconfig-workerd:/defconfig-workerd:ro" \
    -e HOST_UID="$(id -u)" \
    -e HOST_GID="$(id -g)" \
    hluk-kernel-builder bash -c '
        set -euo pipefail
        mkdir -p /kernel/.build /kernel/app-elfloader/workdir/libs
        ln -sfn /kernel/unikraft /kernel/app-elfloader/workdir/unikraft
        ln -sfn /kernel/libs/libelf /kernel/app-elfloader/workdir/libs/libelf
        ln -sfn /kernel/.build /kernel/app-elfloader/workdir/build
        cp /defconfig-workerd /kernel/app-elfloader/.config
        cd /kernel/app-elfloader
        yes "" 2>/dev/null | make WITH_LWIP=n olddefconfig || true
        make WITH_LWIP=n -j$(nproc)
        cp /kernel/.build/workerd_hyperlight-x86_64 \
            /kernel/workerd_hyperlight-x86_64
        chown -R "$HOST_UID:$HOST_GID" /kernel
    '

mkdir -p "$(dirname "$output")"
cp "$build/kernel/workerd_hyperlight-x86_64" "$output"
sha256sum "$output"
