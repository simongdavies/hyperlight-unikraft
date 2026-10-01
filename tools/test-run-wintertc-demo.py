#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 The Hyperlight Authors.

import json
import os
import subprocess
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RUNNER = ROOT / "tools" / "run-wintertc-demo.sh"


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        path, _, query = self.path.partition("?")
        status = 200
        if path == "/evidence/state":
            body = {
                "requestCount": 1,
                "stateToken": "azure-kvm-demo",
                "previousStateToken": None,
            }
        elif path == "/evidence/fetch":
            if query == "upstream=http://localhost:18080/":
                body = {"status": 200, "body": "loopback-upstream\n"}
            else:
                status = 400
                body = {"status": "error", "message": "missing upstream"}
        elif path == "/evidence/core":
            body = {"coreWasm": {"status": "pass"}}
        elif path == "/evidence/byob" and self.server.fail_byob:
            status = 500
            body = {"status": "error", "message": "mock failure"}
        elif path == "/evidence/messageport":
            stage = query.removeprefix("stage=")
            body = {"status": "pass", "result": {"stage": stage}}
        elif path.startswith("/evidence/"):
            body = {"status": "pass"}
        else:
            status = 404
            body = {"status": "error"}
        payload = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format, *args):
        pass


def run(base_url, output_dir, *args, stdin=None):
    return subprocess.run(
        [
            "bash",
            str(RUNNER),
            "--base-url",
            base_url,
            "--output-dir",
            str(output_dir),
            *args,
        ],
        cwd=ROOT,
        stdin=stdin,
        text=True,
        capture_output=True,
        check=False,
    )


def main():
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.fail_byob = False
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base_url = f"http://127.0.0.1:{server.server_port}"
    try:
        listed = subprocess.run(
            ["bash", str(RUNNER), "--list"],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
        )
        assert listed.returncode == 0, listed.stderr
        assert "fetch" in listed.stdout
        assert "messageport-queued-delivery" in listed.stdout

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            complete = run(base_url, output, "all")
            assert complete.returncode == 0, complete.stderr
            assert "PASS: 16" in complete.stdout
            assert (output / "core.json").is_file()
            assert (output / "fetch.json").is_file()
            assert (output / "messageport-clone-failure.json").is_file()

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            selected = run(
                base_url,
                output,
                "timers",
                "state",
                "fetch",
                "messageport-queued-delivery",
            )
            assert selected.returncode == 0, selected.stderr
            assert (output / "timers.json").is_file()
            assert (output / "state-first.json").is_file()
            assert (output / "state-second.json").is_file()
            assert (output / "fetch.json").is_file()
            assert (output / "messageport-queued-delivery.json").is_file()
            assert not (output / "core.json").exists()
            assert "PASS: 4" in selected.stdout

        with tempfile.TemporaryDirectory() as directory:
            server.fail_byob = True
            failed = run(base_url, Path(directory), "timers", "byob", "core-wasm")
            assert failed.returncode != 0
            assert "PASS: 2" in failed.stdout
            assert "FAIL: 1" in failed.stdout
            assert (Path(directory) / "byob.json").is_file()
            server.fail_byob = False

        unknown = run(base_url, Path(tempfile.gettempdir()), "not-a-check")
        assert unknown.returncode == 2
        assert "unknown check" in unknown.stderr

        paused = run(
            base_url,
            Path(tempfile.gettempdir()),
            "--pause",
            "timers",
            stdin=subprocess.DEVNULL,
        )
        assert paused.returncode == 2
        assert "--pause requires terminal stdin" in paused.stderr
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
    print("run-wintertc-demo tests passed")


if __name__ == "__main__":
    main()
