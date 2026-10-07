#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 The Hyperlight Authors.
set -uo pipefail

base_url="http://127.0.0.1:8787"
output_dir="${HOME}/results/workerd-vfs"
pause=false
list=false
checks=(bundle tmp-reset dev-null dev-zero dev-random)

usage() {
    cat >&2 <<'EOF'
usage: tools/run-workerd-vfs-demo.sh [OPTIONS] [all|CHECK ...]

Options:
  --base-url URL     Running workerd-demo base URL (default http://127.0.0.1:8787)
  --output-dir DIR   Raw JSON output directory (default $HOME/results/workerd-vfs)
  --pause            Wait for Enter between checks; requires terminal stdin
  --list             List stable check names and descriptions
  -h, --help         Show this help
EOF
}

describe() {
    case "$1" in
        bundle) echo "Worker node:fs reads /bundle while its packaged modules remain immutable." ;;
        tmp-reset) echo "Worker node:fs writes and reads /tmp; two requests prove fresh-VM reset." ;;
        dev-null) echo "Worker node:fs writes are discarded and reads return EOF on /dev/null." ;;
        dev-zero) echo "Worker node:fs reads deterministic zero bytes from /dev/zero." ;;
        dev-random) echo "Worker node:fs reads two nonzero, distinct samples from /dev/random." ;;
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
            (($# >= 2)) || {
                echo "--base-url requires a value" >&2
                usage
                exit 2
            }
            base_url="$2"
            shift 2
            ;;
        --output-dir)
            (($# >= 2)) || {
                echo "--output-dir requires a value" >&2
                usage
                exit 2
            }
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
        for name in "${checks[@]}"; do
            add_check "$name"
        done
    elif describe "$name" >/dev/null; then
        add_check "$name"
    else
        echo "unknown check: $name" >&2
        usage
        exit 2
    fi
done

base_url="${base_url%/}"
mkdir -p "$output_dir"
pass_count=0
fail_count=0
check_index=0
PIPE_STATUSES=()

pause_between_checks() {
    if "$pause" && ((check_index > 0)); then
        read -r -p "Press Enter to continue..."
        printf '\n'
    fi
}

capture_json() {
    local endpoint="$1"
    local output="$2"
    curl --fail-with-body -sS "$base_url$endpoint" |
        tee "$output" |
        jq -C .
    PIPE_STATUSES=("${PIPESTATUS[@]}")
    ((PIPE_STATUSES[0] == 0 && PIPE_STATUSES[1] == 0 && PIPE_STATUSES[2] == 0))
}

validate_json() {
    local name="$1"
    local output="$2"
    case "$name" in
        bundle)
            jq -e '.outcome == "pass" and .readable == true and (.entries | type == "array") and .immutable == true and .writeError != null' "$output" >/dev/null
            ;;
        tmp-reset)
            jq -s -e 'length == 2 and all(.[]; .outcome == "pass" and .previousExists == false and .body == "tmp-read-write-ok")' "$output_dir/tmp-reset-first.json" "$output_dir/tmp-reset-second.json" >/dev/null
            ;;
        dev-null)
            jq -e '.outcome == "pass" and .written == 10 and .read == 0' "$output" >/dev/null
            ;;
        dev-zero)
            jq -e '.outcome == "pass" and .read == 32 and .allZero == true' "$output" >/dev/null
            ;;
        dev-random)
            jq -e '.outcome == "pass" and .firstRead == 32 and .secondRead == 32 and .firstNonZero == true and .secondNonZero == true and .distinct == true' "$output" >/dev/null
            ;;
        *)
            return 1
            ;;
    esac
}

record_result() {
    local name="$1"
    local passed="$2"
    if "$passed"; then
        printf 'PASS %s\n\n' "$name"
        ((pass_count += 1))
    else
        printf 'FAIL %s\n\n' "$name" >&2
        ((fail_count += 1))
    fi
    ((check_index += 1))
}

run_check() {
    local name="$1"
    local endpoint="/evidence/vfs-${name#dev-}"
    local output="$output_dir/$name.json"
    local passed=true
    if [[ "$name" == bundle ]]; then
        endpoint="/evidence/vfs-bundle"
    elif [[ "$name" == dev-* ]]; then
        endpoint="/evidence/vfs-dev-${name#dev-}"
    fi
    pause_between_checks
    printf '=== %s ===\n%s\nEndpoint: %s\n' "$name" "$(describe "$name")" "$endpoint"
    if ! capture_json "$endpoint" "$output"; then
        printf 'Request/display failed: curl=%s tee=%s jq=%s\n' \
            "${PIPE_STATUSES[0]}" "${PIPE_STATUSES[1]}" "${PIPE_STATUSES[2]}" >&2
        passed=false
    elif ! validate_json "$name" "$output"; then
        echo "Evidence assertion failed; raw response: $output" >&2
        passed=false
    fi
    record_result "$name" "$passed"
}

run_tmp_reset() {
    local name=tmp-reset
    local endpoint="/evidence/vfs-tmp"
    local first="$output_dir/tmp-reset-first.json"
    local second="$output_dir/tmp-reset-second.json"
    local passed=true
    pause_between_checks
    printf '=== %s ===\n%s\n' "$name" "$(describe "$name")"
    printf 'Both fresh request VMs must report previousExists=false.\n'
    printf 'Endpoint: %s\nFirst request:\n' "$endpoint"
    if ! capture_json "$endpoint" "$first"; then
        printf 'First request/display failed: curl=%s tee=%s jq=%s\n' \
            "${PIPE_STATUSES[0]}" "${PIPE_STATUSES[1]}" "${PIPE_STATUSES[2]}" >&2
        passed=false
    fi
    printf 'Second request:\n'
    if ! capture_json "$endpoint" "$second"; then
        printf 'Second request/display failed: curl=%s tee=%s jq=%s\n' \
            "${PIPE_STATUSES[0]}" "${PIPE_STATUSES[1]}" "${PIPE_STATUSES[2]}" >&2
        passed=false
    fi
    if "$passed" && ! validate_json "$name" "$first"; then
        echo "Fresh-VM /tmp reset assertion failed; raw responses: $first $second" >&2
        passed=false
    fi
    record_result "$name" "$passed"
}

for name in "${selected[@]}"; do
    if [[ "$name" == tmp-reset ]]; then
        run_tmp_reset
    else
        run_check "$name"
    fi
done

printf '=== summary ===\nPASS: %d\nFAIL: %d\nRaw JSON: %s\n' \
    "$pass_count" "$fail_count" "$output_dir"
((fail_count == 0))
