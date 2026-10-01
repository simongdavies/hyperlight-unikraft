#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 The Hyperlight Authors.
set -euo pipefail

if [[ $# -ne 2 ]]; then
    echo "usage: $0 EXPORT_DIR ARCHIVE_PATH" >&2
    exit 2
fi

for command in find realpath sha256sum sort tar xargs; do
    command -v "$command" >/dev/null || {
        echo "missing required command: $command" >&2
        exit 2
    }
done

export_dir="$(realpath "$1")"
archive="$(realpath -m "$2")"
manifest="$export_dir/SHA256SUMS"
archive_sha="$archive.sha256"

test -d "$export_dir"
case "$archive" in
    "$export_dir" | "$export_dir"/*)
        echo "archive must be outside the export directory: $archive" >&2
        exit 2
        ;;
esac

mkdir -p "$(dirname "$archive")"
temporary_manifest="$(mktemp)"
trap 'rm -f "$temporary_manifest"' EXIT

(
    cd "$export_dir"
    find . -type f ! -path './SHA256SUMS' -print0 \
        | LC_ALL=C sort -z \
        | xargs -0 -r sha256sum
) >"$temporary_manifest"

test -s "$temporary_manifest"
mv "$temporary_manifest" "$manifest"
trap - EXIT

(
    cd "$export_dir"
    sha256sum --check SHA256SUMS
)

tar -C "$(dirname "$export_dir")" -czf "$archive" "$(basename "$export_dir")"
sha256sum "$archive" | tee "$archive_sha"
