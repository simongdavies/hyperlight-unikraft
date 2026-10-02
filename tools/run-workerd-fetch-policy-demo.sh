#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 The Hyperlight Authors.
set -uo pipefail

base_url="http://127.0.0.1:8787"
output_dir="${HOME}/results/workerd-fetch-policy"
upstream_port=18080
pause=false
list=false
checks=(
    allowed
    wrong-port-denied
    unlisted-host-denied
    disallowed-scheme-denied
    metadata-denied
    private-denied
    redirect-not-followed
)

usage() {
    cat >&2 <<'EOF'
usage: tools/run-workerd-fetch-policy-demo.sh [OPTIONS] [all|CHECK ...]

Options:
  --base-url URL       Running workerd-demo base URL (default http://127.0.0.1:8787)
  --output-dir DIR     Raw JSON output directory (default $HOME/results/workerd-fetch-policy)
  --upstream-port PORT Deterministic allowed localhost upstream port (default 18080)
  --pause              Wait for Enter between checks; requires terminal stdin
  --list               List stable check names and descriptions
  -h, --help           Show this help
EOF
}

describe() {
    case "$1" in
        allowed) echo "The explicitly listed localhost HTTP endpoint succeeds." ;;
        wrong-port-denied) echo "The listed host is denied when its destination port is not allowed." ;;
        unlisted-host-denied) echo "An unlisted hostname is rejected before any connection attempt." ;;
        disallowed-scheme-denied) echo "HTTPS is rejected when only HTTP is in the host-owned scheme policy." ;;
        metadata-denied) echo "Cloud metadata remains denied without the explicit metadata opt-in." ;;
        private-denied) echo "Private address space remains denied without the explicit private opt-in." ;;
        redirect-not-followed) echo "The broker returns the upstream redirect response without following it." ;;
        *) return 1 ;;
    esac
}

target_for() {
    case "$1" in
        allowed) printf 'http://localhost:%s/ok' "$upstream_port" ;;
        wrong-port-denied) printf 'http://localhost:%s/ok' "$((upstream_port + 1))" ;;
        unlisted-host-denied) printf 'http://example.com:%s/ok' "$upstream_port" ;;
        disallowed-scheme-denied) printf 'https://localhost:%s/ok' "$upstream_port" ;;
        metadata-denied) printf 'http://169.254.169.254:%s/metadata' "$upstream_port" ;;
        private-denied) printf 'http://10.0.0.1:%s/private' "$upstream_port" ;;
        redirect-not-followed) printf 'http://localhost:%s/redirect' "$upstream_port" ;;
        *) return 1 ;;
    esac
}

list_checks() {
    local name
    for name in "${checks[@]}"; do
        printf '%-28s %s\n' "$name" "$(describe "$name")"
    done
}

requested=()
while (($#)); do
    case "$1" in
        --base-url)
            (($# >= 2)) || { echo "--base-url requires a value" >&2; usage; exit 2; }
            base_url="$2"
            shift 2
            ;;
        --output-dir)
            (($# >= 2)) || { echo "--output-dir requires a value" >&2; usage; exit 2; }
            output_dir="$2"
            shift 2
            ;;
        --upstream-port)
            (($# >= 2)) || { echo "--upstream-port requires a value" >&2; usage; exit 2; }
            upstream_port="$2"
            [[ "$upstream_port" =~ ^[0-9]+$ ]] \
                && ((upstream_port > 0 && upstream_port < 65535)) || {
                echo "--upstream-port must be between 1 and 65534" >&2
                exit 2
            }
            shift 2
            ;;
        --pause)
            pause=true
            shift
            ;;
        --list)
            list=true
            shift
            ;;
        -h | --help)
            usage
            exit 0
            ;;
        --)
            shift
            requested+=("$@")
            break
            ;;
        -*)
            echo "unknown option: $1" >&2
            usage
            exit 2
            ;;
        *)
            requested+=("$1")
            shift
            ;;
    esac
done

if "$list"; then
    list_checks
    exit 0
fi
if "$pause" && [[ ! -t 0 ]]; then
    echo "--pause requires terminal stdin" >&2
    exit 2
fi
for command in curl jq tee; do
    command -v "$command" >/dev/null || {
        echo "missing required command: $command" >&2
        exit 2
    }
done

base_url="${base_url%/}"
mkdir -p "$output_dir"
if ((${#requested[@]} == 0)); then
    requested=(all)
fi

selected=()
declare -A selected_names
add_check() {
    if [[ -z "${selected_names[$1]:-}" ]]; then
        selected+=("$1")
        selected_names["$1"]=1
    fi
}
for name in "${requested[@]}"; do
    if [[ "$name" == all ]]; then
        for check in "${checks[@]}"; do
            add_check "$check"
        done
    elif describe "$name" >/dev/null; then
        add_check "$name"
    else
        echo "unknown check: $name" >&2
        usage
        exit 2
    fi
done

pass_count=0
fail_count=0
check_index=0

validate_json() {
    local name="$1"
    local output="$2"
    case "$name" in
        allowed)
            jq -e '.outcome == "success" and .status == 200 and .body == "allowed-upstream\n"' \
                "$output" >/dev/null
            ;;
        redirect-not-followed)
            jq -e '.outcome == "success" and .status == 302 and .location == "/ok"' \
                "$output" >/dev/null
            ;;
        *)
            jq -e '.outcome == "error" and (.error.message | length > 0)' "$output" >/dev/null
            ;;
    esac
}

for name in "${selected[@]}"; do
    if "$pause" && ((check_index > 0)); then
        read -r -p "Press Enter to continue..."
        printf '\n'
    fi
    target="$(target_for "$name")"
    endpoint="/fetch-policy?target=$target"
    output="$output_dir/$name.json"
    passed=true
    printf '=== %s ===\n%s\nEndpoint: %s\nTarget: %s\n' \
        "$name" "$(describe "$name")" "/fetch-policy?target=<encoded-url>" "$target"
    curl --fail-with-body -sS --get \
        --data-urlencode "target=$target" \
        "$base_url/fetch-policy" |
        tee "$output" |
        jq -C .
    statuses=("${PIPESTATUS[@]}")
    if ((statuses[0] != 0 || statuses[1] != 0 || statuses[2] != 0)); then
        printf 'Request/display failed: curl=%s tee=%s jq=%s\n' \
            "${statuses[0]}" "${statuses[1]}" "${statuses[2]}" >&2
        passed=false
    elif ! validate_json "$name" "$output"; then
        echo "Evidence assertion failed; raw response: $output" >&2
        passed=false
    fi
    if "$passed"; then
        printf 'PASS %s\n\n' "$name"
        ((pass_count += 1))
    else
        printf 'FAIL %s\n\n' "$name" >&2
        ((fail_count += 1))
    fi
    ((check_index += 1))
done

printf '=== summary ===\nPASS: %d\nFAIL: %d\nRaw JSON: %s\n' \
    "$pass_count" "$fail_count" "$output_dir"
((fail_count == 0))
