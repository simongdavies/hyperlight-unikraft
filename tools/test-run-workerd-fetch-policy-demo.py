#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 The Hyperlight Authors.

import json
import subprocess
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse


ROOT = Path(__file__).resolve().parents[1]
RUNNER = ROOT / "tools" / "run-workerd-fetch-policy-demo.sh"


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        target = parse_qs(parsed.query).get("target", [""])[0]
        status = 200
        if parsed.path != "/fetch-policy":
            status = 404
            body = {"outcome": "invalid"}
        elif self.server.fail_allowed and target.endswith("/ok"):
            body = {"outcome": "error", "error": {"message": "mock allowed failure"}}
        elif target.endswith("/redirect"):
            body = {
                "outcome": "success",
                "status": 302,
                "location": "/ok",
                "body": "",
            }
        elif target == f"http://localhost:{self.server.upstream_port}/ok":
            body = {
                "outcome": "success",
                "status": 200,
                "location": None,
                "body": "allowed-upstream\n",
            }
        else:
            body = {
                "outcome": "error",
                "error": {"message": "destination rejected by host policy"},
            }
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
            "--upstream-port",
            "18080",
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
    server.upstream_port = 18080
    server.fail_allowed = False
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
        assert "allowed" in listed.stdout
        assert "redirect-not-followed" in listed.stdout

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            complete = run(base_url, output, "all")
            assert complete.returncode == 0, complete.stderr
            assert "PASS: 7" in complete.stdout
            assert "FAIL: 0" in complete.stdout
            for name in (
                "allowed",
                "wrong-port-denied",
                "unlisted-host-denied",
                "disallowed-scheme-denied",
                "metadata-denied",
                "private-denied",
                "redirect-not-followed",
            ):
                assert (output / f"{name}.json").is_file()

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            selected = run(
                base_url,
                output,
                "wrong-port-denied",
                "redirect-not-followed",
            )
            assert selected.returncode == 0, selected.stderr
            assert "PASS: 2" in selected.stdout
            assert (output / "wrong-port-denied.json").is_file()
            assert (output / "redirect-not-followed.json").is_file()
            assert not (output / "allowed.json").exists()

        with tempfile.TemporaryDirectory() as directory:
            server.fail_allowed = True
            failed = run(
                base_url,
                Path(directory),
                "allowed",
                "wrong-port-denied",
            )
            assert failed.returncode != 0
            assert "PASS: 1" in failed.stdout
            assert "FAIL: 1" in failed.stdout
            assert (Path(directory) / "allowed.json").is_file()
            assert (Path(directory) / "wrong-port-denied.json").is_file()
            server.fail_allowed = False

        unknown = run(base_url, Path(tempfile.gettempdir()), "not-a-check")
        assert unknown.returncode == 2
        assert "unknown check" in unknown.stderr

        paused = run(
            base_url,
            Path(tempfile.gettempdir()),
            "--pause",
            "allowed",
            stdin=subprocess.DEVNULL,
        )
        assert paused.returncode == 2
        assert "--pause requires terminal stdin" in paused.stderr
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
    print("run-workerd-fetch-policy-demo tests passed")


if __name__ == "__main__":
    main()
