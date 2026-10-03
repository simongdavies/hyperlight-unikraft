#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 The Hyperlight Authors.

import json
import subprocess
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse


ROOT = Path(__file__).resolve().parents[1]
RUNNER = ROOT / "tools" / "run-workerd-storage-policy-demo.sh"
CHECKS = (
    "allowed-read",
    "ro-write-denied",
    "rw-write",
    "traversal-denied",
    "unlisted-denied",
    "quota-denied",
)


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        name = urlparse(self.path).path.removeprefix("/storage-")
        if name not in CHECKS:
            self.send_response(404)
            body = {"outcome": "unknown"}
        else:
            self.send_response(200)
            body = {"outcome": "wrong" if self.server.fail == name else name}
        payload = json.dumps(body).encode()
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
    server.fail = None
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
        assert all(name in listed.stdout for name in CHECKS)

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            complete = run(base_url, output, "all")
            assert complete.returncode == 0, complete.stderr
            assert "PASS: 6" in complete.stdout
            assert "FAIL: 0" in complete.stdout
            assert all((output / f"{name}.json").is_file() for name in CHECKS)

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            selected = run(base_url, output, "rw-write", "quota-denied")
            assert selected.returncode == 0, selected.stderr
            assert "PASS: 2" in selected.stdout
            assert (output / "rw-write.json").is_file()
            assert (output / "quota-denied.json").is_file()
            assert not (output / "allowed-read.json").exists()

        with tempfile.TemporaryDirectory() as directory:
            server.fail = "ro-write-denied"
            failed = run(
                base_url,
                Path(directory),
                "ro-write-denied",
                "unlisted-denied",
            )
            assert failed.returncode != 0
            assert "PASS: 1" in failed.stdout
            assert "FAIL: 1" in failed.stdout
            server.fail = None

        unknown = run(base_url, Path(tempfile.gettempdir()), "not-a-check")
        assert unknown.returncode == 2
        assert "unknown check" in unknown.stderr

        paused = run(
            base_url,
            Path(tempfile.gettempdir()),
            "--pause",
            "allowed-read",
            stdin=subprocess.DEVNULL,
        )
        assert paused.returncode == 2
        assert "--pause requires terminal stdin" in paused.stderr
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
    print("run-workerd-storage-policy-demo tests passed")


if __name__ == "__main__":
    main()
