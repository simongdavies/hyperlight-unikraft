#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 The Hyperlight Authors.
set -uo pipefail

base_url="http://127.0.0.1:8787"
output_dir="${HOME}/results/workerd-storage-policy"
pause=false
list=false
checks=(
    allowed-read
    ro-write-denied
    rw-write
    traversal-denied
    unlisted-denied
    quota-denied
)

usage() {
    cat >&2 <<'EOF'
usage: tools/run-workerd-storage-policy-demo.sh [OPTIONS] [all|CHECK ...]

Options:
  --base-url URL    Running workerd-demo base URL (default http://127.0.0.1:8787)
  --output-dir DIR  Raw JSON output directory (default $HOME/results/workerd-storage-policy)
  --pause           Wait for Enter between checks; requires terminal stdin
  --list            List stable check names and descriptions
  -h, --help        Show this help
EOF
}

describe() {
    case "$1" in
        allowed-read) echo "The executor reads the explicitly named read-only binding." ;;
        ro-write-denied) echo "A write through the read-only binding fails with EROFS." ;;
        rw-write) echo "A bounded write through the named read-write binding succeeds." ;;
        traversal-denied) echo "A symlink escape below the binding is denied by capability-based resolution." ;;
        unlisted-denied) echo "A logical binding absent from the host policy is not mounted." ;;
        quota-denied) echo "A write beyond the per-sandbox mount byte budget fails with EDQUOT." ;;
        *) return 1 ;;
    esac
}

list_checks() {
    local name
    for name in "${checks[@]}"; do
        printf '%-20s %s\n' "$name" "$(describe "$name")"
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
for name in "${selected[@]}"; do
    if "$pause" && ((check_index > 0)); then
        read -r -p "Press Enter to continue..."
        printf '\n'
    fi
    output="$output_dir/$name.json"
    passed=true
    printf '=== %s ===\n%s\nEndpoint: /storage-%s\n' \
        "$name" "$(describe "$name")" "$name"
    curl --fail-with-body -sS "$base_url/storage-$name" |
        tee "$output" |
        jq -C .
    statuses=("${PIPESTATUS[@]}")
    if ((statuses[0] != 0 || statuses[1] != 0 || statuses[2] != 0)); then
        printf 'Request/display failed: curl=%s tee=%s jq=%s\n' \
            "${statuses[0]}" "${statuses[1]}" "${statuses[2]}" >&2
        passed=false
    elif ! jq -e --arg expected "$name" \
        '.outcome == $expected and (keys | length) == 1' "$output" >/dev/null; then
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
