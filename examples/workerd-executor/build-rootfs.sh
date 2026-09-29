#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 The Hyperlight Authors.
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
if [[ "${1:-}" == "--mock" ]]; then
    executor=""
    out="${2:-$root/build-elfloader/workerd-executor-fixture}"
else
    executor="${1:-}"
    out="${2:-$root/build-elfloader/workerd-executor}"
fi
mkdir -p "$out"
stage="$(mktemp -d "$out/stage.XXXXXX")"
trap 'rm -rf -- "$stage"' EXIT
mkdir -p "$stage/rootfs/bin" "$stage/rootfs/dev"

declare -A copied
copy_runtime_path() {
    local source="$1"
    [[ "$source" == /* ]] || {
        echo "runtime dependency is not an absolute path: $source" >&2
        exit 1
    }
    source="$(realpath -ms -- "$source")"
    [[ -n "${copied[$source]:-}" ]] && return
    copied["$source"]=1
    local destination="$stage/rootfs$source"
    mkdir -p -- "$(dirname "$destination")"
    if [[ -L "$source" ]]; then
        local target next
        target="$(readlink -- "$source")"
        [[ -n "$target" && "$target" != *$'\n'* ]] || {
            echo "unsafe dependency symlink: $source" >&2
            exit 1
        }
        ln -s -- "$target" "$destination"
        if [[ "$target" == /* ]]; then
            next="$target"
        else
            next="$(realpath -ms -- "$(dirname "$source")/$target")"
        fi
        copy_runtime_path "$next"
    elif [[ -f "$source" ]]; then
        cp -p -- "$source" "$destination"
    else
        echo "runtime dependency is not a regular file or safe symlink: $source" >&2
        exit 1
    fi
}

interpreter=""
if [[ -n "$executor" ]]; then
    if [[ ! -f "$executor" || ! -x "$executor" ]]; then
        echo "executor must be an executable file: $executor" >&2
        exit 1
    fi
    description="$(file -b "$executor")"
    case "$description" in
        *"ELF 64-bit"*"x86-64"*) ;;
        *)
            echo "executor must be an x86-64 Linux ELF: $description" >&2
            exit 1
            ;;
    esac
    if ! grep -qi 'pie executable' <<<"$description"; then
        echo "executor must be position-independent (PIE): $description" >&2
        exit 1
    fi
    echo "External executor: $description"
    sha256sum "$executor"
    cp -- "$executor" "$stage/rootfs/bin/workerd-executor"
    interpreter="$(
        readelf -l "$executor" |
            sed -n 's/.*Requesting program interpreter: \(.*\)]/\1/p'
    )"
    if [[ -n "$interpreter" ]]; then
        echo "Dynamic executor interpreter: $interpreter"
        copy_runtime_path "$interpreter"
        while IFS= read -r line; do
            if [[ "$line" == *"not found"* ]]; then
                echo "unresolved runtime dependency: $line" >&2
                exit 1
            elif [[ "$line" =~ ^[[:space:]]*linux-vdso ]]; then
                continue
            elif [[ "$line" =~ =\>[[:space:]]+(/[^[:space:]]+) ]]; then
                copy_runtime_path "${BASH_REMATCH[1]}"
            elif [[ "$line" =~ ^[[:space:]]*(/[^[:space:]]+) ]]; then
                copy_runtime_path "${BASH_REMATCH[1]}"
            elif [[ -n "${line//[[:space:]]/}" ]]; then
                echo "ambiguous ldd output: $line" >&2
                exit 1
            fi
        done < <(ldd "$executor")
    fi
else
    gcc -static-pie -fPIE -fno-stack-protector -O2 -Wall -Wextra -Werror \
        -I "$root/drivers" "$root/examples/workerd-executor/mock_executor.c" \
        -o "$stage/rootfs/bin/workerd-executor"
fi
chmod 755 "$stage/rootfs/bin/workerd-executor"
(
    cd "$stage/rootfs"
    find . \( -type f -o -type l \) -print0 |
        LC_ALL=C sort -z |
        while IFS= read -r -d '' path; do
            if [[ -L "$path" ]]; then
                printf 'symlink  %s -> %s\n' "$path" "$(readlink -- "$path")"
            else
                sha256sum "$path"
            fi
        done
) > "$stage/dependency-closure.manifest"
closure_digest="$(sha256sum "$stage/dependency-closure.manifest" | cut -d' ' -f1)"
printf '%s\n' "$closure_digest" > "$stage/rootfs/workerd-dependencies.sha256"
if [[ -z "$interpreter" ]]; then
    cp -- "$stage/rootfs/bin/workerd-executor" "$stage/rootfs.img"
    image_mode="direct-elf"
else
    (
        cd "$stage/rootfs"
        find . -print0 | LC_ALL=C sort -z | cpio --null -o -H newc > "$stage/rootfs.img"
    )
    image_mode="cpio"
fi
cp -- "$stage/rootfs/bin/workerd-executor" "$out/executor"
cp -- "$stage/dependency-closure.manifest" "$out/dependency-closure.manifest"
printf '%s\n' "$closure_digest" > "$out/dependency-closure.sha256"
printf '%s\n' "$image_mode" > "$out/image-mode"
mv -- "$stage/rootfs.img" "$out/rootfs.img"
# Compatibility alias for existing fixture consumers. The contents are a raw
# ELF in direct mode and a CPIO archive in dynamic mode; host-side magic
# detection selects the matching kernel.
cp -- "$out/rootfs.img" "$out/rootfs.cpio"
echo "Dependency closure SHA-256: $closure_digest"
echo "Built $out/rootfs.img ($image_mode) and matching executor"
