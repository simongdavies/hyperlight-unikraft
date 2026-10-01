#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 The Hyperlight Authors.
set -uo pipefail

base_url="http://127.0.0.1:8787"
output_dir="${HOME}/results/wintertc-demo"
pause=false
list=false

messageport_stages=(
    construct
    listener-registration
    start
    post-message
    queued-delivery
    close
    transfer-reentanglement
    clone-failure
)
core_checks=(
    core
    timers
    global-handlers
    byob
    byte-stream-tee
    core-wasm
    state
    fetch
)

usage() {
    cat >&2 <<'EOF'
usage: tools/run-wintertc-demo.sh [OPTIONS] [all|CHECK ...]

Options:
  --base-url URL     Running workerd-demo base URL (default http://127.0.0.1:8787)
  --output-dir DIR   Raw JSON output directory (default $HOME/results/wintertc-demo)
  --pause            Wait for Enter between checks; requires terminal stdin
  --list             List stable check names and descriptions
  -h, --help         Show this help
EOF
}

describe() {
    case "$1" in
        core) echo "Core WinterTC behavior and isolated capability boundaries." ;;
        timers) echo "Timer creation, ordering, cancellation, and callback delivery." ;;
        global-handlers) echo "Global error and unhandled-rejection event handlers." ;;
        byob) echo "Readable byte streams with bring-your-own-buffer readers." ;;
        byte-stream-tee) echo "Independent consumption of both byte-stream tee branches." ;;
        core-wasm) echo "Core WebAssembly module compilation and execution." ;;
        state) echo "Two fresh request VMs do not share mutable module state." ;;
        fetch) echo "Streaming upload and host-brokered loopback fetch response." ;;
        messageport) echo "Every MessagePort lifecycle and transfer stage." ;;
        messageport-construct) echo "MessageChannel construction creates two usable ports." ;;
        messageport-listener-registration) echo "A message listener can be registered on a port." ;;
        messageport-start) echo "A port can explicitly start message delivery." ;;
        messageport-post-message) echo "Posting a message returns after queueing it." ;;
        messageport-queued-delivery) echo "A queued message is delivered after the peer starts." ;;
        messageport-close) echo "Both ports close without leaking lifecycle state." ;;
        messageport-transfer-reentanglement) echo "A transferred port re-entangles and delivers data." ;;
        messageport-clone-failure) echo "Uncloneable values fail without corrupting the channel." ;;
        *) return 1 ;;
    esac
}

list_checks() {
    local name
    for name in "${core_checks[@]}" messageport; do
        printf '%-36s %s\n' "$name" "$(describe "$name")"
    done
    for name in "${messageport_stages[@]}"; do
        name="messageport-$name"
        printf '%-36s %s\n' "$name" "$(describe "$name")"
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
expand_name() {
    local name="$1"
    local stage
    case "$name" in
        all)
            for name in "${core_checks[@]}"; do
                add_check "$name"
            done
            for stage in "${messageport_stages[@]}"; do
                add_check "messageport-$stage"
            done
            ;;
        messageport)
            for stage in "${messageport_stages[@]}"; do
                add_check "messageport-$stage"
            done
            ;;
        core | timers | global-handlers | byob | byte-stream-tee | core-wasm | state | fetch)
            add_check "$name"
            ;;
        messageport-construct | messageport-listener-registration | messageport-start | messageport-post-message | messageport-queued-delivery | messageport-close | messageport-transfer-reentanglement | messageport-clone-failure)
            add_check "$name"
            ;;
        *)
            echo "unknown check: $name" >&2
            usage
            exit 2
            ;;
    esac
}
for name in "${requested[@]}"; do
    expand_name "$name"
done

pass_count=0
fail_count=0
check_index=0

pause_between_checks() {
    if "$pause" && ((check_index > 0)); then
        read -r -p "Press Enter to continue..."
        printf '\n'
    fi
}

capture_json() {
    local url="$1"
    local output="$2"
    curl --fail-with-body -sS "$url" |
        tee "$output" |
        jq -C .
    PIPE_STATUSES=("${PIPESTATUS[@]}")
    ((PIPE_STATUSES[0] == 0 && PIPE_STATUSES[1] == 0 && PIPE_STATUSES[2] == 0))
}

validate_json() {
    local name="$1"
    local output="$2"
    case "$name" in
        core)
            jq -e '[.. | objects | .status? // empty] | all(. != "error")' "$output" >/dev/null
            ;;
        timers | global-handlers | byob | byte-stream-tee | core-wasm | messageport-*)
            jq -e '.status == "pass"' "$output" >/dev/null
            ;;
        fetch)
            jq -e '.status == 200 and .body == "loopback-upstream\n"' "$output" >/dev/null
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

run_single() {
    local name="$1"
    local route="$name"
    local endpoint
    local output="$output_dir/$name.json"
    local passed=true
    if [[ "$name" == messageport-* ]]; then
        route="${name#messageport-}"
        endpoint="/evidence/messageport?stage=$route"
    elif [[ "$name" == fetch ]]; then
        endpoint="/evidence/fetch?upstream=http://localhost:18080/"
    else
        endpoint="/evidence/$route"
    fi
    pause_between_checks
    printf '=== %s ===\n%s\nEndpoint: %s\n' "$name" "$(describe "$name")" "$endpoint"
    if ! capture_json "$base_url$endpoint" "$output"; then
        printf 'Request/display failed: curl=%s tee=%s jq=%s\n' \
            "${PIPE_STATUSES[0]}" "${PIPE_STATUSES[1]}" "${PIPE_STATUSES[2]}" >&2
        passed=false
    elif ! validate_json "$name" "$output"; then
        echo "Evidence assertion failed; raw response: $output" >&2
        passed=false
    fi
    record_result "$name" "$passed"
}

run_state() {
    local name=state
    local endpoint="/evidence/state?token=azure-kvm-demo"
    local first="$output_dir/state-first.json"
    local second="$output_dir/state-second.json"
    local passed=true
    pause_between_checks
    printf '=== state ===\n%s\n' "$(describe "$name")"
    printf 'Both responses must report requestCount=1 and previousStateToken=null.\n'
    printf 'Endpoint: %s\nFirst request:\n' "$endpoint"
    if ! capture_json "$base_url$endpoint" "$first"; then
        printf 'First request/display failed: curl=%s tee=%s jq=%s\n' \
            "${PIPE_STATUSES[0]}" "${PIPE_STATUSES[1]}" "${PIPE_STATUSES[2]}" >&2
        passed=false
    fi
    printf 'Second request:\n'
    if ! capture_json "$base_url$endpoint" "$second"; then
        printf 'Second request/display failed: curl=%s tee=%s jq=%s\n' \
            "${PIPE_STATUSES[0]}" "${PIPE_STATUSES[1]}" "${PIPE_STATUSES[2]}" >&2
        passed=false
    fi
    if "$passed" && ! jq -s -e \
        'length == 2 and all(.[]; .requestCount == 1 and .previousStateToken == null)' \
        "$first" "$second" >/dev/null; then
        echo "State isolation assertion failed; raw responses: $first $second" >&2
        passed=false
    fi
    record_result "$name" "$passed"
}

for name in "${selected[@]}"; do
    if [[ "$name" == state ]]; then
        run_state
    else
        run_single "$name"
    fi
done

printf '=== summary ===\nPASS: %d\nFAIL: %d\nRaw JSON: %s\n' \
    "$pass_count" "$fail_count" "$output_dir"
((fail_count == 0))
