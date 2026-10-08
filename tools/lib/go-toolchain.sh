#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 The Hyperlight Authors.
#
# Pinned Go toolchain installer shared by tools/setup-workerd-demo.sh and
# exercised directly by tools/test-go-toolchain.py. The caller must already
# define `step`, `fail`, and `cache_root` (see tools/setup-workerd-demo.sh)
# and may override GO_TOOLCHAIN_VERSION before sourcing this file.
# shellcheck disable=SC2154 # cache_root: provided by the sourcing caller
#
# Ubuntu jammy's golang-go package is 1.18, too old for vegeta's go.mod
# (requires go 1.22+). ensure_pinned_go installs a newer toolchain, used
# only for the hey/vegeta `go install`s; the system golang-go stays
# untouched.
#
# Preferred path: golang.org/dl (Go's own SDK installer), fetched via the
# module proxy/checksum database (GOSUMDB) rather than a raw binary
# download. This requires the *existing* `go` to be 1.19+: its
# internal/version package gates signal_unix.go on the "unix" build-tag
# meta-constraint, which older `go` toolchains (e.g. jammy's 1.18) do not
# recognize, so the file defining signalsToIgnore is silently excluded
# and the build fails with "undefined: signalsToIgnore". When the
# existing `go` is older than 1.19, fall back to a direct,
# checksum-verified download of the official tarball from go.dev instead.

GO_TOOLCHAIN_VERSION="${GO_TOOLCHAIN_VERSION:-1.27.1}"

# Authoritative source: https://go.dev/dl/?mode=json&include=all
# Recorded 2026-10-08 for the go1.27.1 release (these are immutable
# release artifacts; re-verify against that endpoint before bumping
# GO_TOOLCHAIN_VERSION). tools/test-go-toolchain.py checks these two
# tables stay in sync with this comment's recorded values.
declare -A GO_TOOLCHAIN_SHA256=(
    [linux-amd64]=63d339f0da5ab53635a56f2490a7984dfe12dfcff22ad749f63edaf590168445
)
declare -A GO_TOOLCHAIN_SIZE=(
    [linux-amd64]=70553950
)

pinned_go=""

system_go_supports_dl_installer() {
    local existing_version major minor
    existing_version="$(go env GOVERSION 2>/dev/null || true)"
    [[ "$existing_version" =~ ^go([0-9]+)\.([0-9]+) ]] || return 1
    major="${BASH_REMATCH[1]}"
    minor="${BASH_REMATCH[2]}"
    ((major > 1 || (major == 1 && minor >= 19)))
}

go_toolchain_platform() {
    # The caller already requires linux/x86_64 (see setup-workerd-demo.sh);
    # kept as a function, and GO_TOOLCHAIN_SHA256/_SIZE as tables keyed by
    # platform, so adding another platform later is a one-line change.
    printf 'linux-amd64\n'
}

# Downloads and verifies one Go toolchain tarball attempt into $tarball.
# Prints "$actual_sha256 $size_download $http_code $content_type" on
# stdout for the caller to compare/log; never fails on checksum mismatch
# itself (that's the caller's job, so it can retry with a fresh file).
go_toolchain_download_attempt() {
    local url="$1" tarball="$2"
    local info http_code size_download content_type actual_sha256

    info="$(curl --proto '=https' --tlsv1.2 --location --fail \
        --show-error --silent \
        --retry 2 --retry-all-errors --retry-delay 2 \
        -o "$tarball" \
        -w '%{http_code} %{size_download} %{content_type}' \
        "$url")" || fail "download of $url failed: curl reported an error"
    read -r http_code size_download content_type <<<"$info"
    actual_sha256="$(sha256sum "$tarball" | cut -d' ' -f1)"
    printf '%s %s %s %s\n' "$actual_sha256" "$size_download" "$http_code" "$content_type"
}

# Downloads go$GO_TOOLCHAIN_VERSION.$platform.tar.gz, verifies it against
# the pinned sha256/size, and extracts it into $go_root. Retries a
# bounded number of times with a fresh download on checksum mismatch
# (which a prior missing --location bug showed can be a real, 100%
# reproducible condition, not just transient network noise), logging
# the actual sha256/size/HTTP status/content-type observed on every
# attempt so a persistent mismatch is diagnosable from the output alone.
go_toolchain_download_and_verify() {
    local platform="$1" expected_sha256="$2" expected_size="$3" go_root="$4"
    local url="https://go.dev/dl/go$GO_TOOLCHAIN_VERSION.$platform.tar.gz"
    local max_attempts=3 attempt tarball result
    local actual_sha256 size_download http_code content_type

    for ((attempt = 1; attempt <= max_attempts; attempt++)); do
        tarball="$(mktemp)"
        result="$(go_toolchain_download_attempt "$url" "$tarball")"
        read -r actual_sha256 size_download http_code content_type <<<"$result"
        if [[ "$actual_sha256" == "$expected_sha256" ]]; then
            rm -rf -- "$go_root"
            mkdir -p "$go_root"
            tar -xzf "$tarball" -C "$go_root" --strip-components=1
            rm -f -- "$tarball"
            return 0
        fi
        printf 'warning: checksum mismatch on attempt %d/%d for %s\n' \
            "$attempt" "$max_attempts" "$url" >&2
        printf '  expected sha256 %s (size %s bytes)\n' \
            "$expected_sha256" "$expected_size" >&2
        printf '  actual   sha256 %s (HTTP %s, content-type %s, %s bytes)\n' \
            "$actual_sha256" "$http_code" "$content_type" "$size_download" >&2
        rm -f -- "$tarball"
    done
    fail "downloaded Go $GO_TOOLCHAIN_VERSION $platform tarball never matched" \
        "its pinned checksum after $max_attempts attempts (see the warnings" \
        "above for the actual sha256/size/HTTP status/content-type observed" \
        "on each one); compare those against" \
        "https://go.dev/dl/?mode=json&include=all to diagnose further"
}

ensure_pinned_go() {
    [[ -n "$pinned_go" ]] && return
    if system_go_supports_dl_installer; then
        if ! command -v "go$GO_TOOLCHAIN_VERSION" >/dev/null; then
            step "Installing a pinned Go $GO_TOOLCHAIN_VERSION toolchain (golang.org/dl)"
            go install "golang.org/dl/go$GO_TOOLCHAIN_VERSION@latest"
        fi
        # Idempotent: no-ops if the SDK is already downloaded. Fetched
        # via Go's own module proxy/checksum database (GOSUMDB), not a
        # raw binary download.
        "go$GO_TOOLCHAIN_VERSION" download
        pinned_go="go$GO_TOOLCHAIN_VERSION"
        return
    fi

    # Fall back to a direct, checksum-verified download of the official
    # tarball: the existing `go` is too old to build golang.org/dl.
    local platform expected_sha256 expected_size go_root
    platform="$(go_toolchain_platform)"
    expected_sha256="${GO_TOOLCHAIN_SHA256[$platform]:-}"
    expected_size="${GO_TOOLCHAIN_SIZE[$platform]:-}"
    [[ -n "$expected_sha256" ]] ||
        fail "no pinned checksum for Go $GO_TOOLCHAIN_VERSION $platform"

    go_root="$cache_root/go-$GO_TOOLCHAIN_VERSION"
    if [[ ! -x "$go_root/bin/go" ]]; then
        step "Installing a pinned Go $GO_TOOLCHAIN_VERSION toolchain (direct download)"
        go_toolchain_download_and_verify \
            "$platform" "$expected_sha256" "$expected_size" "$go_root"
    fi
    pinned_go="$go_root/bin/go"
}
